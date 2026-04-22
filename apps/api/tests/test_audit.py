"""Audit log end-to-end tests.

Uses the same pattern as test_self_hosted_auth.py — flips the auth
flag on at request time, bootstraps an admin, exercises gated
endpoints, then reads the audit log back.
"""

from __future__ import annotations

import time
import uuid
from contextlib import contextmanager
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from jose import jwt
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.config import settings as env_settings
from src.main import app
from src.models import (
    AuditEvent,
    InstanceSettings,
    MigrationCheckpointRecord,
    MigrationRecord,
    User,
)
from src.services.settings_service import set_license_jwt


client = TestClient(app)
_DEV_KEY = Path.home() / ".depart-keys" / "license_private_dev.pem"


@contextmanager
def auth_on():
    previous = env_settings.enable_self_hosted_auth
    env_settings.enable_self_hosted_auth = True
    try:
        yield
    finally:
        env_settings.enable_self_hosted_auth = previous


@pytest.fixture(autouse=True)
def clean_tables():
    engine = create_engine(env_settings.database_url)
    S = sessionmaker(bind=engine)

    def wipe():
        s = S()
        # audit_events depends on users (FK SET NULL but still faster
        # to drop audit first); migrations depends on checkpoints.
        s.query(AuditEvent).delete()
        s.query(MigrationCheckpointRecord).delete()
        s.query(MigrationRecord).delete()
        s.query(InstanceSettings).delete()
        s.query(User).delete()
        s.commit()
        s.close()

    wipe()
    yield
    wipe()
    engine.dispose()


def _bootstrap_and_login() -> dict:
    client.post(
        "/api/v1/setup/bootstrap",
        json={
            "email": "admin@acme.com",
            "password": "s3cret-password",
            "full_name": "Admin",
        },
    )
    r = client.post(
        "/api/v1/auth/login",
        json={"email": "admin@acme.com", "password": "s3cret-password"},
    )
    return {"Authorization": f"Bearer {r.json()['access_token']}"}


def _sign_license() -> str:
    if not _DEV_KEY.exists():
        pytest.skip(f"dev signing key missing at {_DEV_KEY}")
    now = int(time.time())
    return jwt.encode(
        {
            "sub": "test",
            "project": "test",
            "tier": "pro",
            "features": ["ai_conversion", "runbook_pdf"],
            "iat": now,
            "exp": now + 3600,
        },
        _DEV_KEY.read_text(),
        algorithm="RS256",
    )


# ─── Bootstrap + login get logged ────────────────────────────────────────────


def test_bootstrap_is_logged():
    client.post(
        "/api/v1/setup/bootstrap",
        json={"email": "a@b.com", "password": "s3cret-password"},
    )
    headers = {
        "Authorization": "Bearer "
        + client.post(
            "/api/v1/auth/login",
            json={"email": "a@b.com", "password": "s3cret-password"},
        ).json()["access_token"]
    }
    with auth_on():
        r = client.get("/api/v1/audit", headers=headers)
    assert r.status_code == 200
    actions = {e["action"] for e in r.json()["items"]}
    assert "install.bootstrapped" in actions
    assert "user.login" in actions


def test_bad_login_logged_as_failed():
    # Seed an admin so login failure is the only event of its kind.
    client.post(
        "/api/v1/setup/bootstrap",
        json={"email": "a@b.com", "password": "s3cret-password"},
    )
    # Bad password attempt
    r = client.post(
        "/api/v1/auth/login",
        json={"email": "a@b.com", "password": "WRONG-WRONG-WRONG"},
    )
    assert r.status_code == 401

    # Now log in for real and read the audit log
    headers = _bootstrap_and_login_skip_bootstrap()  # reuse existing admin
    with auth_on():
        r = client.get("/api/v1/audit?action=user.login_failed", headers=headers)
    assert r.status_code == 200
    items = r.json()["items"]
    assert len(items) >= 1
    assert items[0]["details"]["reason"] == "bad_credentials"
    assert items[0]["user_email"] == "a@b.com"


def _bootstrap_and_login_skip_bootstrap() -> dict:
    r = client.post(
        "/api/v1/auth/login",
        json={"email": "a@b.com", "password": "s3cret-password"},
    )
    return {"Authorization": f"Bearer {r.json()['access_token']}"}


# ─── Migration create + run get logged ───────────────────────────────────────


def test_migration_create_and_run_logged():
    headers = _bootstrap_and_login()
    with auth_on():
        created = client.post(
            "/api/v1/migrations",
            json={
                "name": "audit-test",
                "source_url": "postgresql+psycopg://u:p@s/x",
                "target_url": "postgresql+psycopg://u:p@d/y",
                "source_schema": "public",
                "target_schema": "public",
            },
            headers=headers,
        ).json()

        # Patch the enqueue helper so we don't need Redis + don't
        # actually kick off a runner during this test.
        from unittest.mock import patch

        async def _fake(mid, background=None):
            return "test-job"

        with patch(
            "src.routers.migrations.enqueue_migration", side_effect=_fake
        ):
            client.post(f"/api/v1/migrations/{created['id']}/run", headers=headers)

        r = client.get("/api/v1/audit?action=migration.run", headers=headers)

    events = r.json()["items"]
    assert len(events) == 1
    assert events[0]["resource_id"] == created["id"]
    assert events[0]["details"]["name"] == "audit-test"


# ─── License upload logged ───────────────────────────────────────────────────


def test_license_upload_logged():
    headers = _bootstrap_and_login()
    token = _sign_license()
    with auth_on():
        client.put("/api/v1/license", json={"jwt": token}, headers=headers)
        r = client.get("/api/v1/audit?action=license.uploaded", headers=headers)

    events = r.json()["items"]
    assert len(events) == 1
    assert events[0]["details"]["valid"] is True
    assert events[0]["details"]["tier"] == "pro"


# ─── Role gating on the audit endpoint itself ────────────────────────────────


def test_operator_cannot_read_audit():
    admin_headers = _bootstrap_and_login()
    with auth_on():
        client.post(
            "/api/v1/auth/users",
            json={
                "email": "op@acme.com",
                "password": "s3cret-password",
                "role": "operator",
            },
            headers=admin_headers,
        )
    r = client.post(
        "/api/v1/auth/login",
        json={"email": "op@acme.com", "password": "s3cret-password"},
    )
    op_headers = {"Authorization": f"Bearer {r.json()['access_token']}"}
    with auth_on():
        resp = client.get("/api/v1/audit", headers=op_headers)
    assert resp.status_code == 403


# ─── Filter + pagination ─────────────────────────────────────────────────────


def test_chain_intact_after_normal_writes():
    """A sequence of normal log_event calls should produce an intact
    chain. The /verify endpoint returns ok=True + the correct row
    count."""
    headers = _bootstrap_and_login()
    with auth_on():
        # A handful of random logged actions
        client.put(
            "/api/v1/settings/anthropic-key",
            json={"api_key": "sk-test-aaaaaaaaaaaa"},
            headers=headers,
        )
        client.put(
            "/api/v1/settings/anthropic-key",
            json={"api_key": "sk-test-bbbbbbbbbbbb"},
            headers=headers,
        )
        r = client.get("/api/v1/audit/verify", headers=headers)
    body = r.json()
    assert r.status_code == 200
    assert body["ok"] is True
    assert body["checked"] >= 3  # bootstrap + login + 2 settings updates
    assert body["first_break"] is None


def test_tamper_detected_on_mutation():
    """Mutate a single row's details column out-of-band and verify the
    chain flags that exact row as the first break."""
    headers = _bootstrap_and_login()
    with auth_on():
        client.put(
            "/api/v1/settings/anthropic-key",
            json={"api_key": "sk-test-aaaaaaaaaaaa"},
            headers=headers,
        )

    # Direct tamper: reach into the DB and flip a field on the oldest
    # row. This is what a malicious admin with direct DB access would do.
    from src.models import AuditEvent

    engine = create_engine(env_settings.database_url)
    S = sessionmaker(bind=engine)
    s = S()
    target = (
        s.query(AuditEvent).order_by(AuditEvent.created_at.asc()).first()
    )
    target.details = {"hacker_was_here": True}
    s.commit()
    s.close()
    engine.dispose()

    with auth_on():
        r = client.get("/api/v1/audit/verify", headers=headers)
    body = r.json()
    assert body["ok"] is False
    assert body["first_break"] is not None
    assert body["first_break"]["expected"] != body["first_break"]["stored"]


def test_filter_by_action_and_limit():
    headers = _bootstrap_and_login()
    # Trigger a bunch of settings updates
    with auth_on():
        for i in range(5):
            client.put(
                "/api/v1/settings/anthropic-key",
                json={"api_key": f"sk-test-{i}-padding-xxxxxx"},
                headers=headers,
            )

        r = client.get(
            "/api/v1/audit?action=settings.anthropic_key_updated&limit=3",
            headers=headers,
        )
    body = r.json()
    assert body["total"] == 5
    assert len(body["items"]) == 3
    assert all(e["action"] == "settings.anthropic_key_updated" for e in body["items"])
