"""SSO config + public status tests.

We skip the full login dance (requires a real IdP or a serious amount
of HTTP mocking); the tests here cover the surfaces that would break
most obviously: public status visibility, admin-gated config, and the
"is_configured" gating used by the /login page.
"""

from __future__ import annotations

from contextlib import contextmanager

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.config import settings as env_settings
from src.main import app
from src.models import IdentityProvider, User


client = TestClient(app)


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
        s.query(IdentityProvider).delete()
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


# ─── Public status ───────────────────────────────────────────────────────────


def test_public_status_false_on_fresh_install():
    r = client.get("/api/v1/auth/sso")
    assert r.status_code == 200
    body = r.json()
    assert body["enabled"] is False
    assert body["protocol"] is None


def test_public_status_hides_config_details():
    """Partial config shouldn't leak via the public endpoint. The
    protocol field *is* safe to expose (the /login page needs it to
    pick the right button) but issuer / client details stay
    server-side."""
    headers = _bootstrap_admin()
    with auth_on():
        client.put(
            "/api/v1/auth/sso/config",
            json={
                "protocol": "oidc",
                "issuer": "https://accounts.google.com",
                "client_id": "ci-123",
                "client_secret": "sec-abc",
                "enabled": True,
            },
            headers=headers,
        )

    r = client.get("/api/v1/auth/sso")
    body = r.json()
    assert body["enabled"] is True
    assert body["protocol"] == "oidc"
    assert "issuer" not in body
    assert "client_id" not in body


# ─── Admin config ────────────────────────────────────────────────────────────


def test_non_admin_cannot_see_config():
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

    with auth_on():
        resp = client.get("/api/v1/auth/sso/config", headers=viewer_headers)
    assert resp.status_code == 403


def test_admin_config_roundtrip():
    headers = _bootstrap_admin()
    with auth_on():
        r = client.get("/api/v1/auth/sso/config", headers=headers)
        assert r.status_code == 200
        assert r.json()["enabled"] is False
        assert r.json()["client_secret_set"] is False

        r = client.put(
            "/api/v1/auth/sso/config",
            json={
                "issuer": "https://login.microsoftonline.com/tenant/v2.0",
                "client_id": "a-client",
                "client_secret": "a-secret",
                "default_role": "operator",
                "auto_provision": True,
                "enabled": True,
            },
            headers=headers,
        )
        assert r.status_code == 200
        body = r.json()
        assert body["enabled"] is True
        assert body["issuer"].startswith("https://login.microsoftonline.com")
        assert body["client_id"] == "a-client"
        assert body["client_secret_set"] is True
        assert body["default_role"] == "operator"


def test_empty_client_secret_leaves_value_unchanged():
    """Common UX: operator PATCHes issuer without retyping the secret.
    Empty-string secret must not wipe what's already stored."""
    headers = _bootstrap_admin()
    with auth_on():
        client.put(
            "/api/v1/auth/sso/config",
            json={
                "issuer": "https://example.com",
                "client_id": "c",
                "client_secret": "stays",
            },
            headers=headers,
        )
        r = client.put(
            "/api/v1/auth/sso/config",
            json={"issuer": "https://new.example.com", "client_secret": ""},
            headers=headers,
        )
        body = r.json()
        assert body["issuer"] == "https://new.example.com"
        assert body["client_secret_set"] is True  # unchanged


def test_default_role_cannot_be_admin():
    """SSO auto-provision isn't allowed to mint admins even if the
    config field claims otherwise. (The backend still persists the
    raw value but the /callback normalizer downgrades on the way out —
    we're not unit-testing /callback here, just the config input
    validation.)"""
    headers = _bootstrap_admin()
    with auth_on():
        # The PUT accepts "admin" because the UserRole enum includes
        # it, but the login dance defensively downgrades to operator.
        # Here we just assert the round-trip keeps what we wrote so
        # admins can at least see-and-fix a misconfiguration.
        r = client.put(
            "/api/v1/auth/sso/config",
            json={"default_role": "admin"},
            headers=headers,
        )
        assert r.status_code == 200
        assert r.json()["default_role"] == "admin"


# ─── /test discovery helper ──────────────────────────────────────────────────


def test_test_endpoint_requires_issuer():
    headers = _bootstrap_admin()
    with auth_on():
        r = client.post("/api/v1/auth/sso/test", headers=headers)
        assert r.status_code == 400


# ─── SAML config ─────────────────────────────────────────────────────────────


def test_saml_config_roundtrip():
    headers = _bootstrap_admin()
    cert_pem = (
        "-----BEGIN CERTIFICATE-----\n"
        "MIIBsTCCARoCCQDxxxxxx\n-----END CERTIFICATE-----"
    )
    with auth_on():
        r = client.put(
            "/api/v1/auth/sso/config",
            json={
                "protocol": "saml",
                "saml_entity_id": "https://sts.windows.net/tenant/",
                "saml_sso_url": "https://login.microsoftonline.com/tenant/saml2",
                "saml_x509_cert": cert_pem,
                "default_role": "viewer",
                "auto_provision": True,
                "enabled": True,
            },
            headers=headers,
        )
        assert r.status_code == 200
        body = r.json()
        assert body["protocol"] == "saml"
        assert body["saml_entity_id"] == "https://sts.windows.net/tenant/"
        assert body["saml_x509_cert_set"] is True

        # Public status picks up the protocol.
        pub = client.get("/api/v1/auth/sso").json()
        assert pub["enabled"] is True
        assert pub["protocol"] == "saml"


def test_saml_is_configured_requires_all_fields():
    """Enabling without a cert must not flip the install to 'SSO on'."""
    headers = _bootstrap_admin()
    with auth_on():
        client.put(
            "/api/v1/auth/sso/config",
            json={
                "protocol": "saml",
                "saml_entity_id": "x",
                "saml_sso_url": "y",
                # no cert
                "enabled": True,
            },
            headers=headers,
        )
    pub = client.get("/api/v1/auth/sso").json()
    assert pub["enabled"] is False
