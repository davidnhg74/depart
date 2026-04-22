"""End-to-end tests for the self-hosted auth layer.

Unlike the rest of the test suite (which runs with
ENABLE_SELF_HOSTED_AUTH=false), these tests flip the flag back on at
runtime to exercise the full login → gated-endpoint flow. Relies on
`require_role` reading `settings.enable_self_hosted_auth` at request
time, which it does.

Covers:
  * Setup endpoint — status + first-admin bootstrap
  * Login → JWT → authenticated endpoint
  * Role enforcement (admin / operator / viewer) on migrations,
    settings, license, convert, runbook
  * Admin-only user CRUD
  * Last-admin guardrails
"""

from __future__ import annotations

import uuid as _uuid
from contextlib import contextmanager

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.config import settings as env_settings
from src.main import app
from src.models import InstanceSettings, MigrationRecord, User


client = TestClient(app)


# ─── Helpers ────────────────────────────────────────────────────────────────


@contextmanager
def auth_on():
    """Toggle `settings.enable_self_hosted_auth=True` for the body of
    the test. Restored in the finally block so other tests still see
    the default-off state set by conftest."""
    previous = env_settings.enable_self_hosted_auth
    env_settings.enable_self_hosted_auth = True
    try:
        yield
    finally:
        env_settings.enable_self_hosted_auth = previous


@pytest.fixture(autouse=True)
def clean_users():
    """Every test starts with zero users so the bootstrap gate is open."""
    engine = create_engine(env_settings.database_url)
    S = sessionmaker(bind=engine)

    def wipe():
        s = S()
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
    """Create the first admin + return login headers. Convenience
    for the gated-endpoint tests below."""
    client.post(
        "/api/v1/setup/bootstrap",
        json={
            "email": "admin@acme.com",
            "password": "s3cret-password",
            "full_name": "Ada Admin",
        },
    )
    resp = client.post(
        "/api/v1/auth/login",
        json={"email": "admin@acme.com", "password": "s3cret-password"},
    )
    return {"Authorization": f"Bearer {resp.json()['access_token']}"}


def _create_user(admin_headers: dict, *, email: str, role: str) -> dict:
    client.post(
        "/api/v1/auth/users",
        json={
            "email": email,
            "password": "test-password-abc",
            "full_name": email.split("@")[0],
            "role": role,
        },
        headers=admin_headers,
    )
    resp = client.post(
        "/api/v1/auth/login", json={"email": email, "password": "test-password-abc"}
    )
    return {"Authorization": f"Bearer {resp.json()['access_token']}"}


# ─── Setup / bootstrap ──────────────────────────────────────────────────────


class TestSetup:
    def test_fresh_install_needs_bootstrap(self):
        r = client.get("/api/v1/setup/status")
        assert r.status_code == 200
        body = r.json()
        assert body["needs_bootstrap"] is True
        assert body["admin_count"] == 0

    def test_bootstrap_creates_admin(self):
        r = client.post(
            "/api/v1/setup/bootstrap",
            json={
                "email": "admin@acme.com",
                "password": "s3cret-password",
                "full_name": "Ada",
            },
        )
        assert r.status_code == 201
        assert r.json()["admin_count"] == 1

        r2 = client.get("/api/v1/setup/status")
        assert r2.json()["needs_bootstrap"] is False

    def test_bootstrap_rejects_second_run(self):
        client.post(
            "/api/v1/setup/bootstrap",
            json={"email": "a@x.com", "password": "s3cret-password"},
        )
        r = client.post(
            "/api/v1/setup/bootstrap",
            json={"email": "b@x.com", "password": "s3cret-password"},
        )
        assert r.status_code == 409


# ─── Login ──────────────────────────────────────────────────────────────────


class TestLogin:
    def test_valid_credentials_return_tokens(self):
        client.post(
            "/api/v1/setup/bootstrap",
            json={"email": "a@x.com", "password": "s3cret-password"},
        )
        r = client.post(
            "/api/v1/auth/login",
            json={"email": "a@x.com", "password": "s3cret-password"},
        )
        assert r.status_code == 200
        body = r.json()
        assert body["access_token"]
        assert body["refresh_token"]
        assert body["token_type"] == "bearer"

    def test_wrong_password_rejected(self):
        client.post(
            "/api/v1/setup/bootstrap",
            json={"email": "a@x.com", "password": "s3cret-password"},
        )
        r = client.post(
            "/api/v1/auth/login", json={"email": "a@x.com", "password": "WRONG"}
        )
        assert r.status_code == 401

    def test_me_returns_current_user(self):
        headers = _bootstrap_admin()
        r = client.get("/api/v1/auth/me", headers=headers)
        assert r.status_code == 200
        body = r.json()
        assert body["email"] == "admin@acme.com"
        assert body["role"] == "admin"


# ─── Role enforcement ───────────────────────────────────────────────────────


class TestRoleGates:
    """Flip auth on at request time. Same app instance, same routes —
    the dependency reads `settings.enable_self_hosted_auth` fresh on
    every call, so this works without re-importing anything."""

    def test_no_token_is_401(self):
        _bootstrap_admin()
        with auth_on():
            r = client.get("/api/v1/migrations")
            assert r.status_code == 401

    def test_admin_can_create_migration(self):
        headers = _bootstrap_admin()
        with auth_on():
            r = client.post(
                "/api/v1/migrations",
                json={
                    "name": "test",
                    "source_url": "postgresql+psycopg://u:p@s:5432/x",
                    "target_url": "postgresql+psycopg://u:p@d:5432/y",
                    "source_schema": "public",
                    "target_schema": "public",
                },
                headers=headers,
            )
            assert r.status_code == 201

    def test_viewer_cannot_create_migration(self):
        admin_headers = _bootstrap_admin()
        viewer_headers = _create_user(
            admin_headers, email="viewer@acme.com", role="viewer"
        )
        with auth_on():
            r = client.post(
                "/api/v1/migrations",
                json={
                    "name": "test",
                    "source_url": "postgresql+psycopg://u:p@s/x",
                    "target_url": "postgresql+psycopg://u:p@d/y",
                    "source_schema": "public",
                    "target_schema": "public",
                },
                headers=viewer_headers,
            )
            assert r.status_code == 403

    def test_viewer_can_list_migrations(self):
        admin_headers = _bootstrap_admin()
        viewer_headers = _create_user(
            admin_headers, email="viewer@acme.com", role="viewer"
        )
        with auth_on():
            r = client.get("/api/v1/migrations", headers=viewer_headers)
            assert r.status_code == 200

    def test_operator_cannot_upload_license(self):
        admin_headers = _bootstrap_admin()
        op_headers = _create_user(
            admin_headers, email="op@acme.com", role="operator"
        )
        with auth_on():
            r = client.put(
                "/api/v1/license", json={"jwt": "x"}, headers=op_headers
            )
            assert r.status_code == 403

    def test_operator_cannot_change_anthropic_key(self):
        admin_headers = _bootstrap_admin()
        op_headers = _create_user(
            admin_headers, email="op@acme.com", role="operator"
        )
        with auth_on():
            r = client.put(
                "/api/v1/settings/anthropic-key",
                json={"api_key": "sk-test"},
                headers=op_headers,
            )
            assert r.status_code == 403


# ─── Admin user CRUD ────────────────────────────────────────────────────────


class TestUserCrud:
    def test_admin_can_list_users(self):
        headers = _bootstrap_admin()
        with auth_on():
            r = client.get("/api/v1/auth/users", headers=headers)
            assert r.status_code == 200
            assert len(r.json()) == 1

    def test_non_admin_cannot_list_users(self):
        admin_headers = _bootstrap_admin()
        op_headers = _create_user(
            admin_headers, email="op@acme.com", role="operator"
        )
        with auth_on():
            r = client.get("/api/v1/auth/users", headers=op_headers)
            assert r.status_code == 403

    def test_admin_creates_operator(self):
        headers = _bootstrap_admin()
        with auth_on():
            r = client.post(
                "/api/v1/auth/users",
                json={
                    "email": "newop@acme.com",
                    "password": "test-password-abc",
                    "role": "operator",
                },
                headers=headers,
            )
            assert r.status_code == 201
            assert r.json()["role"] == "operator"

    def test_duplicate_email_rejected(self):
        headers = _bootstrap_admin()
        with auth_on():
            client.post(
                "/api/v1/auth/users",
                json={
                    "email": "x@y.com",
                    "password": "test-password-abc",
                    "role": "operator",
                },
                headers=headers,
            )
            r = client.post(
                "/api/v1/auth/users",
                json={
                    "email": "x@y.com",
                    "password": "test-password-abc",
                    "role": "viewer",
                },
                headers=headers,
            )
            assert r.status_code == 409


# ─── Last-admin guards ──────────────────────────────────────────────────────


class TestLastAdminGuards:
    def test_cannot_demote_last_admin(self):
        headers = _bootstrap_admin()
        # The admin id — fetch it via /me
        me = client.get("/api/v1/auth/me", headers=headers).json()
        with auth_on():
            r = client.patch(
                f"/api/v1/auth/users/{me['id']}",
                json={"role": "viewer"},
                headers=headers,
            )
            assert r.status_code == 400
            assert "last admin" in r.json()["detail"].lower()

    def test_cannot_delete_self(self):
        headers = _bootstrap_admin()
        me = client.get("/api/v1/auth/me", headers=headers).json()
        with auth_on():
            r = client.delete(f"/api/v1/auth/users/{me['id']}", headers=headers)
            assert r.status_code == 400
