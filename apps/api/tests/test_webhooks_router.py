"""Integration tests for /api/v1/webhooks.

Exercise auth/admin/license gating and the CRUD + test-send round
trip. The delivery path itself is covered by test_webhook_service;
these tests only care that the router wires the right guards and
response shape around the service layer."""

from __future__ import annotations

import time
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch

import httpx
import pytest
from fastapi.testclient import TestClient
from jose import jwt
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.config import settings as env_settings
from src.main import app
from src.models import InstanceSettings, User, WebhookEndpoint
from src.services.settings_service import set_license_jwt


client = TestClient(app)


_DEV_PRIVATE_KEY_PATH = Path.home() / ".depart-keys" / "license_private_dev.pem"


def _mint_license(*, features=("webhooks",), days: int = 30) -> str:
    if not _DEV_PRIVATE_KEY_PATH.exists():
        pytest.skip(
            f"dev license signing key missing at {_DEV_PRIVATE_KEY_PATH} — "
            "generate one before running license-gated tests"
        )
    now = int(time.time())
    claims = {
        "sub": "test@acme.com",
        "project": "test-project",
        "tier": "pro",
        "features": list(features),
        "iat": now,
        "exp": now + days * 86400,
    }
    return jwt.encode(claims, _DEV_PRIVATE_KEY_PATH.read_text(), algorithm="RS256")


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
        s.query(WebhookEndpoint).delete()
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


def _mock_httpx_ok():
    """Any webhook delivery in the router tests hits httpx.Client.
    Patch the constructor so no real network call escapes."""
    real_client_cls = httpx.Client

    def _transport_factory(*args, **kwargs):
        def handler(_req):
            return httpx.Response(200, text="ok")
        kwargs["transport"] = httpx.MockTransport(handler)
        return real_client_cls(*args, **kwargs)
    return patch("src.services.webhook_service.httpx.Client", _transport_factory)


# ─── Gating ──────────────────────────────────────────────────────────


def test_list_requires_auth():
    _seed_license()
    with auth_on():
        r = client.get("/api/v1/webhooks")
    assert r.status_code in (401, 403)


def test_non_admin_blocked():
    headers = _bootstrap_admin()
    with auth_on():
        client.post(
            "/api/v1/auth/users",
            json={
                "email": "viewer@acme.com",
                "password": "test-password-abc",
                "role": "viewer",
            },
            headers=headers,
        )
    r = client.post(
        "/api/v1/auth/login",
        json={"email": "viewer@acme.com", "password": "test-password-abc"},
    )
    viewer_headers = {"Authorization": f"Bearer {r.json()['access_token']}"}
    _seed_license()

    with auth_on():
        r = client.get("/api/v1/webhooks", headers=viewer_headers)
    assert r.status_code == 403


def test_no_license_returns_402():
    headers = _bootstrap_admin()
    # No _seed_license — fresh install, Community tier.
    with auth_on():
        r = client.get("/api/v1/webhooks", headers=headers)
    assert r.status_code == 402
    assert r.json()["detail"]["feature"] == "webhooks"


# ─── CRUD round trip ─────────────────────────────────────────────────


def test_create_list_get_update_delete():
    headers = _bootstrap_admin()
    _seed_license()

    with auth_on():
        # Create
        r = client.post(
            "/api/v1/webhooks",
            json={
                "name": "ops-slack",
                "url": "https://hooks.example.com/t/abc",
                "secret": "shh",
                "events": ["migration.completed", "migration.failed"],
                "enabled": True,
            },
            headers=headers,
        )
        assert r.status_code == 201, r.text
        created = r.json()
        webhook_id = created["id"]
        assert created["name"] == "ops-slack"
        # Secret and full URL are write-only.
        assert "url" not in created
        assert "secret" not in created
        assert created["url_set"] is True
        assert created["secret_set"] is True
        assert created["url_host"] == "hooks.example.com"
        assert set(created["events"]) == {"migration.completed", "migration.failed"}

        # List
        r = client.get("/api/v1/webhooks", headers=headers)
        assert r.status_code == 200
        listing = r.json()
        assert len(listing) == 1
        assert listing[0]["id"] == webhook_id

        # Get
        r = client.get(f"/api/v1/webhooks/{webhook_id}", headers=headers)
        assert r.status_code == 200
        assert r.json()["id"] == webhook_id

        # Update — empty-string secret clears; None leaves unchanged.
        r = client.patch(
            f"/api/v1/webhooks/{webhook_id}",
            json={"secret": "", "enabled": False},
            headers=headers,
        )
        assert r.status_code == 200
        patched = r.json()
        assert patched["secret_set"] is False
        assert patched["enabled"] is False

        # Delete
        r = client.delete(f"/api/v1/webhooks/{webhook_id}", headers=headers)
        assert r.status_code == 204
        r = client.get(f"/api/v1/webhooks/{webhook_id}", headers=headers)
        assert r.status_code == 404


# ─── Validation ──────────────────────────────────────────────────────


def test_rejects_non_http_url():
    headers = _bootstrap_admin()
    _seed_license()
    with auth_on():
        r = client.post(
            "/api/v1/webhooks",
            json={
                "name": "bad",
                "url": "ftp://example.com/hook",
                "events": ["migration.completed"],
            },
            headers=headers,
        )
    assert r.status_code == 400


def test_rejects_unknown_events():
    headers = _bootstrap_admin()
    _seed_license()
    with auth_on():
        r = client.post(
            "/api/v1/webhooks",
            json={
                "name": "bad",
                "url": "https://example.com/hook",
                "events": ["migration.exploded"],
            },
            headers=headers,
        )
    assert r.status_code == 400
    detail = r.json()["detail"]
    assert detail["error"] == "unknown_events"
    assert "migration.exploded" in detail["unknown"]


# ─── Test-send ───────────────────────────────────────────────────────


def test_send_test_delivery():
    headers = _bootstrap_admin()
    _seed_license()
    with auth_on():
        r = client.post(
            "/api/v1/webhooks",
            json={
                "name": "ops",
                "url": "https://hooks.example.com/a",
                "events": ["migration.completed"],
            },
            headers=headers,
        )
        webhook_id = r.json()["id"]

        with _mock_httpx_ok():
            r = client.post(
                f"/api/v1/webhooks/{webhook_id}/test", headers=headers
            )
    assert r.status_code == 200
    body = r.json()
    assert body["last_status"] == 200
    assert body["last_error"] is None
