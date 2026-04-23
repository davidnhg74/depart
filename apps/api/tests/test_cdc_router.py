"""Integration tests for /api/v1/migrations/{id}/cdc/status."""

from __future__ import annotations

import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from jose import jwt
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.config import settings as env_settings
from src.main import app
from src.models import (
    InstanceSettings,
    MigrationCdcChange,
    MigrationRecord,
    User,
)
from src.services.settings_service import set_license_jwt


client = TestClient(app)


_DEV_PRIVATE_KEY_PATH = Path.home() / ".depart-keys" / "license_private_dev.pem"


def _mint_license(*, features=("ongoing_cdc",), days: int = 30) -> str:
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
        s.query(MigrationCdcChange).delete()
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


def _seed_migration(**overrides) -> str:
    engine = create_engine(env_settings.database_url)
    S = sessionmaker(bind=engine)
    s = S()
    defaults = dict(
        id=uuid.uuid4(),
        name="cdc-test",
        schema_name="hr",
        source_url="oracle://...",
        target_url="postgresql+psycopg://...",
        source_schema="HR",
        target_schema="hr",
        status="pending",
    )
    defaults.update(overrides)
    rec = MigrationRecord(**defaults)
    s.add(rec)
    s.commit()
    mid = str(rec.id)
    s.close()
    engine.dispose()
    return mid


def _seed_changes(mid: str, count: int, *, applied: int = 0, failed: int = 0) -> None:
    """Insert `count` pending changes; mark `applied` of them applied
    and `failed` of the still-pending ones as apply_error'd."""
    engine = create_engine(env_settings.database_url)
    S = sessionmaker(bind=engine)
    s = S()
    rows = [
        MigrationCdcChange(
            migration_id=uuid.UUID(mid),
            scn=i * 10,
            source_schema="HR",
            source_table="emp",
            op="I",
            pk_json={"id": i},
            after_json={"id": i, "name": f"row-{i}"},
            committed_at=datetime(2026, 4, 23, tzinfo=timezone.utc),
        )
        for i in range(1, count + 1)
    ]
    s.add_all(rows)
    s.commit()
    for r in rows[:applied]:
        r.applied_at = datetime(2026, 4, 23, 12, tzinfo=timezone.utc)
    for r in rows[applied : applied + failed]:
        r.apply_error = "simulated failure"
    s.commit()
    s.close()
    engine.dispose()


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
        r = client.get(f"/api/v1/migrations/{mid}/cdc/status", headers=op_headers)
    assert r.status_code == 403


def test_no_license_returns_402():
    headers = _bootstrap_admin()
    mid = _seed_migration()
    with auth_on():
        r = client.get(f"/api/v1/migrations/{mid}/cdc/status", headers=headers)
    assert r.status_code == 402
    assert r.json()["detail"]["feature"] == "ongoing_cdc"


# ─── Status shape ────────────────────────────────────────────────────


def test_status_fresh_migration_has_null_scns_and_zero_counts():
    headers = _bootstrap_admin()
    _seed_license()
    mid = _seed_migration()
    with auth_on():
        r = client.get(f"/api/v1/migrations/{mid}/cdc/status", headers=headers)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["migration_id"] == mid
    assert body["last_captured_scn"] is None
    assert body["last_applied_scn"] is None
    assert body["apply_mode"] == "per_row"
    assert body["pending_count"] == 0
    assert body["applied_count"] == 0
    assert body["failed_count"] == 0


def test_status_reflects_counts_and_scns():
    headers = _bootstrap_admin()
    _seed_license()
    mid = _seed_migration(
        last_captured_scn=50, last_applied_scn=30, cdc_apply_mode="atomic"
    )
    # 10 changes total: 3 applied, 2 failed, 5 pending-clean
    _seed_changes(mid, count=10, applied=3, failed=2)

    with auth_on():
        r = client.get(f"/api/v1/migrations/{mid}/cdc/status", headers=headers)
    assert r.status_code == 200
    body = r.json()
    assert body["last_captured_scn"] == 50
    assert body["last_applied_scn"] == 30
    assert body["apply_mode"] == "atomic"
    assert body["applied_count"] == 3
    assert body["failed_count"] == 2
    assert body["pending_count"] == 7  # failed + not-yet-attempted


def test_status_404_for_unknown_migration():
    headers = _bootstrap_admin()
    _seed_license()
    with auth_on():
        r = client.get(
            f"/api/v1/migrations/{uuid.uuid4()}/cdc/status", headers=headers
        )
    assert r.status_code == 404
