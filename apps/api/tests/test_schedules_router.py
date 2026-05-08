"""Integration tests for /api/v1/migrations/{id}/schedule."""

from __future__ import annotations

import time
import uuid
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient
from jose import jwt
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.config import settings as env_settings
from src.main import app
from src.models import (
    InstanceSettings,
    MigrationRecord,
    MigrationSchedule,
    User,
)
from src.services.settings_service import set_license_jwt
pytestmark = pytest.mark.integration


client = TestClient(app)


_DEV_PRIVATE_KEY_PATH = Path.home() / ".hafen-keys" / "license_private_dev.pem"


def _mint_license(*, features=("scheduled_migrations",), days: int = 30) -> str:
    if not _DEV_PRIVATE_KEY_PATH.exists():
        pytest.skip(f"dev license signing key missing at {_DEV_PRIVATE_KEY_PATH}")
    now = int(time.time())
    return jwt.encode(
        {
            "sub": "test@acme.com",
            "project": "test",
            "tier": "pro",
            "features": list(features),
            "iat": now,
            "exp": now + days * 86400,
        },
        _DEV_PRIVATE_KEY_PATH.read_text(),
        algorithm="RS256",
    )


@contextmanager
def auth_on():
    previous = env_settings.enable_self_hosted_auth
    env_settings.enable_self_hosted_auth = True
    try:
        yield
    finally:
        env_settings.enable_self_hosted_auth = previous


@pytest.fixture(autouse=True)
def clean_state():
    engine = create_engine(env_settings.database_url)
    S = sessionmaker(bind=engine)

    def wipe():
        s = S()
        s.query(MigrationSchedule).delete()
        s.query(MigrationRecord).delete()
        s.query(InstanceSettings).delete()
        s.query(User).delete()
        s.commit()
        s.close()

    wipe()
    yield
    wipe()
    engine.dispose()


def _bootstrap_admin() -> dict:
    client.post(
        "/api/v1/setup/bootstrap",
        json={"email": "admin@acme.com", "password": "s3cret-password"},
    )
    r = client.post(
        "/api/v1/auth/login",
        json={"email": "admin@acme.com", "password": "s3cret-password"},
    )
    return {"Authorization": f"Bearer {r.json()['access_token']}"}


def _seed_license():
    engine = create_engine(env_settings.database_url)
    S = sessionmaker(bind=engine)
    s = S()
    set_license_jwt(s, _mint_license())
    s.close()
    engine.dispose()


def _seed_migration() -> str:
    engine = create_engine(env_settings.database_url)
    S = sessionmaker(bind=engine)
    s = S()
    rec = MigrationRecord(
        id=uuid.uuid4(),
        name="prod-to-stage",
        schema_name="hr",
        source_url="oracle://prod",
        target_url="postgresql+psycopg://stage",
        source_schema="HR",
        target_schema="hr",
        status="pending",
        batch_size=5000,
        create_tables=True,
    )
    s.add(rec)
    s.commit()
    mid = str(rec.id)
    s.close()
    engine.dispose()
    return mid


# ─── Gating ──────────────────────────────────────────────────────────


def test_non_admin_blocked():
    headers = _bootstrap_admin()
    with auth_on():
        client.post(
            "/api/v1/auth/users",
            json={
                "email": "op@acme.com",
                "password": "test-password-abc",
                "role": "operator",
            },
            headers=headers,
        )
    r = client.post(
        "/api/v1/auth/login",
        json={"email": "op@acme.com", "password": "test-password-abc"},
    )
    op_headers = {"Authorization": f"Bearer {r.json()['access_token']}"}
    _seed_license()
    mid = _seed_migration()

    with auth_on():
        r = client.get(f"/api/v1/migrations/{mid}/schedule", headers=op_headers)
    assert r.status_code == 403


def test_no_license_returns_402():
    headers = _bootstrap_admin()
    mid = _seed_migration()
    with auth_on():
        r = client.get(f"/api/v1/migrations/{mid}/schedule", headers=headers)
    assert r.status_code == 402
    assert r.json()["detail"]["feature"] == "scheduled_migrations"


# ─── Validation ──────────────────────────────────────────────────────


def test_put_rejects_bad_cron():
    headers = _bootstrap_admin()
    _seed_license()
    mid = _seed_migration()
    with auth_on():
        r = client.put(
            f"/api/v1/migrations/{mid}/schedule",
            json={
                "name": "nightly",
                "cron_expr": "not cron at all",
                "timezone": "UTC",
                "enabled": True,
            },
            headers=headers,
        )
    assert r.status_code == 400
    assert "invalid cron" in r.json()["detail"].lower()


def test_put_rejects_bad_timezone():
    headers = _bootstrap_admin()
    _seed_license()
    mid = _seed_migration()
    with auth_on():
        r = client.put(
            f"/api/v1/migrations/{mid}/schedule",
            json={
                "name": "nightly",
                "cron_expr": "0 2 * * *",
                "timezone": "Mars/Olympus",
                "enabled": True,
            },
            headers=headers,
        )
    assert r.status_code == 400
    assert "timezone" in r.json()["detail"].lower()


# ─── CRUD round trip ─────────────────────────────────────────────────


def test_get_404_when_no_schedule():
    headers = _bootstrap_admin()
    _seed_license()
    mid = _seed_migration()
    with auth_on():
        r = client.get(f"/api/v1/migrations/{mid}/schedule", headers=headers)
    assert r.status_code == 404


def test_upsert_create_then_update_then_delete():
    headers = _bootstrap_admin()
    _seed_license()
    mid = _seed_migration()

    with auth_on():
        r = client.put(
            f"/api/v1/migrations/{mid}/schedule",
            json={
                "name": "nightly",
                "cron_expr": "0 2 * * *",
                "timezone": "America/New_York",
                "enabled": True,
            },
            headers=headers,
        )
        assert r.status_code == 200, r.text
        created = r.json()
        assert created["migration_id"] == mid
        assert created["cron_expr"] == "0 2 * * *"
        assert created["timezone"] == "America/New_York"
        assert created["next_run_at"] is not None

        # Update — same migration id, new cadence.
        r = client.put(
            f"/api/v1/migrations/{mid}/schedule",
            json={
                "name": "hourly",
                "cron_expr": "0 * * * *",
                "timezone": "UTC",
                "enabled": False,
            },
            headers=headers,
        )
        updated = r.json()
        assert updated["id"] == created["id"]
        assert updated["cron_expr"] == "0 * * * *"
        assert updated["enabled"] is False

        r = client.get(f"/api/v1/migrations/{mid}/schedule", headers=headers)
        assert r.status_code == 200
        assert r.json()["cron_expr"] == "0 * * * *"

        r = client.delete(f"/api/v1/migrations/{mid}/schedule", headers=headers)
        assert r.status_code == 204

        r = client.get(f"/api/v1/migrations/{mid}/schedule", headers=headers)
        assert r.status_code == 404


# ─── Run-now ─────────────────────────────────────────────────────────


def test_run_now_clones_and_enqueues():
    headers = _bootstrap_admin()
    _seed_license()
    mid = _seed_migration()

    # No real Redis in the test env → patch enqueue_migration to
    # return a synthetic job id and avoid the fallback.
    async def fake_enqueue(migration_id, background=None):
        return f"job-{migration_id}"

    with auth_on():
        # Create the schedule first.
        client.put(
            f"/api/v1/migrations/{mid}/schedule",
            json={
                "name": "nightly",
                "cron_expr": "0 2 * * *",
                "timezone": "UTC",
                "enabled": True,
            },
            headers=headers,
        )

        with patch(
            "src.routers.schedules.enqueue_migration", fake_enqueue
        ):
            r = client.post(
                f"/api/v1/migrations/{mid}/schedule/run-now",
                headers=headers,
            )
        assert r.status_code == 202, r.text
        body = r.json()
        assert body["migration_id"] != mid  # it's the clone
        assert body["job_id"].startswith("job-")

    # Verify the clone exists and points back at the schedule.
    engine = create_engine(env_settings.database_url)
    S = sessionmaker(bind=engine)
    s = S()
    clone = s.get(MigrationRecord, uuid.UUID(body["migration_id"]))
    assert clone is not None
    assert clone.status == "pending"
    assert clone.spawned_from_schedule_id is not None
    s.close()
    engine.dispose()


def test_run_now_404_when_no_schedule():
    headers = _bootstrap_admin()
    _seed_license()
    mid = _seed_migration()
    with auth_on():
        r = client.post(
            f"/api/v1/migrations/{mid}/schedule/run-now", headers=headers
        )
    assert r.status_code == 404
