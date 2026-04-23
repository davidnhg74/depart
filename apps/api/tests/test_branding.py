"""Tests for /api/v1/branding (white-label).

Two surfaces under test:
  * GET — public, returns defaults on a fresh install, returns overrides
    once they're stored.
  * PUT — gated on the license carrying the `white_label` feature.
    Without it: 403, regardless of the body. With it: validates and
    persists.
"""

from __future__ import annotations

import time
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from jose import jwt
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.config import settings as env_settings
from src.main import app
from src.models import InstanceSettings
from src.services.branding_service import (
    DEFAULT_COMPANY_NAME,
    DEFAULT_PRIMARY_COLOR,
    DEFAULT_PRODUCT_NAME,
    DEFAULT_SUPPORT_EMAIL,
)


client = TestClient(app)
_DEV_KEY = Path.home() / ".hafen-keys" / "license_private_dev.pem"


def _mint(claims: dict) -> str:
    if not _DEV_KEY.exists():
        pytest.skip(f"dev signing key missing: {_DEV_KEY}")
    return jwt.encode(claims, _DEV_KEY.read_text(), algorithm="RS256")


def _wl_token(extra_features: list[str] | None = None) -> str:
    """A pro-tier license with white_label in features."""
    now = int(time.time())
    feats = ["white_label"] + (extra_features or [])
    return _mint(
        {
            "sub": "ops@acme.com",
            "project": "acme-wl",
            "tier": "pro",
            "features": feats,
            "iat": now,
            "exp": now + 3600,
        }
    )


def _no_wl_token() -> str:
    """A pro-tier license that does NOT carry white_label."""
    now = int(time.time())
    return _mint(
        {
            "sub": "ops@acme.com",
            "project": "acme-basic",
            "tier": "pro",
            "features": ["ai_conversion"],
            "iat": now,
            "exp": now + 3600,
        }
    )


@pytest.fixture(autouse=True)
def reset_settings():
    engine = create_engine(env_settings.database_url)
    Session = sessionmaker(bind=engine)
    s = Session()
    s.query(InstanceSettings).delete()
    s.commit()
    s.close()
    engine.dispose()
    yield


# ─── GET ─────────────────────────────────────────────────────────────────────


class TestGetBranding:
    def test_returns_defaults_on_fresh_install(self):
        resp = client.get("/api/v1/branding")
        assert resp.status_code == 200
        body = resp.json()
        assert body["company_name"] == DEFAULT_COMPANY_NAME
        assert body["product_name"] == DEFAULT_PRODUCT_NAME
        assert body["logo_url"] is None
        assert body["primary_color"] == DEFAULT_PRIMARY_COLOR
        assert body["support_email"] == DEFAULT_SUPPORT_EMAIL
        assert body["white_label_enabled"] is False  # no license

    def test_white_label_enabled_flag_reflects_license(self):
        client.put("/api/v1/license", json={"jwt": _wl_token()})
        body = client.get("/api/v1/branding").json()
        assert body["white_label_enabled"] is True

    def test_white_label_enabled_false_when_feature_missing(self):
        client.put("/api/v1/license", json={"jwt": _no_wl_token()})
        body = client.get("/api/v1/branding").json()
        assert body["white_label_enabled"] is False


# ─── PUT — license gating ────────────────────────────────────────────────────


class TestLicenseGate:
    def test_put_without_license_returns_403(self):
        resp = client.put(
            "/api/v1/branding", json={"company_name": "Acme"}
        )
        assert resp.status_code == 403
        assert "white_label" in resp.json()["detail"]

    def test_put_with_non_wl_license_returns_403(self):
        client.put("/api/v1/license", json={"jwt": _no_wl_token()})
        resp = client.put(
            "/api/v1/branding", json={"company_name": "Acme"}
        )
        assert resp.status_code == 403

    def test_put_with_wl_license_persists(self):
        client.put("/api/v1/license", json={"jwt": _wl_token()})
        resp = client.put(
            "/api/v1/branding",
            json={
                "company_name": "Acme Corp",
                "product_name": "Acme Migrator",
                "logo_url": "https://acme.example.com/logo.png",
                "primary_color": "#FF8800",
                "support_email": "help@acme.example.com",
            },
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["company_name"] == "Acme Corp"
        assert body["product_name"] == "Acme Migrator"
        assert body["logo_url"] == "https://acme.example.com/logo.png"
        assert body["primary_color"] == "#FF8800"
        assert body["support_email"] == "help@acme.example.com"
        assert body["white_label_enabled"] is True

        # And a follow-up GET sees the same values
        echo = client.get("/api/v1/branding").json()
        assert echo["company_name"] == "Acme Corp"


# ─── PUT — validation ────────────────────────────────────────────────────────


class TestValidation:
    @pytest.fixture(autouse=True)
    def _seed_license(self):
        client.put("/api/v1/license", json={"jwt": _wl_token()})

    def test_bad_color_rejected(self):
        resp = client.put(
            "/api/v1/branding", json={"primary_color": "purple"}
        )
        assert resp.status_code == 400
        assert "primary_color" in resp.json()["detail"]

    def test_bad_email_rejected(self):
        resp = client.put(
            "/api/v1/branding", json={"support_email": "not-an-email"}
        )
        assert resp.status_code == 400

    def test_logo_url_must_be_http(self):
        resp = client.put(
            "/api/v1/branding", json={"logo_url": "javascript:alert(1)"}
        )
        assert resp.status_code == 400
        assert "logo_url" in resp.json()["detail"]

    def test_clearing_field_reverts_to_default(self):
        # First set a custom name.
        client.put("/api/v1/branding", json={"company_name": "Acme"})
        assert client.get("/api/v1/branding").json()["company_name"] == "Acme"
        # Then clear by sending null. Other fields stay cleared too — the
        # PUT contract is "replaces the override set", not partial merge.
        client.put("/api/v1/branding", json={"company_name": None})
        body = client.get("/api/v1/branding").json()
        assert body["company_name"] == DEFAULT_COMPANY_NAME

    def test_empty_string_treated_as_clear(self):
        client.put("/api/v1/branding", json={"company_name": "Acme"})
        client.put("/api/v1/branding", json={"company_name": ""})
        body = client.get("/api/v1/branding").json()
        assert body["company_name"] == DEFAULT_COMPANY_NAME


# ─── CORS — UI must be able to bootstrap from the browser ────────────────────
#
# /api/v1/branding is unauthenticated and called from the Next.js dev
# server (http://localhost:3000) before the user logs in. If CORS is
# misconfigured, the browser swallows the response and the UI silently
# stays on the Hafen defaults — exactly the kind of "it works in
# Postman but not the browser" bug that's a pain to diagnose.


_DEV_ORIGIN = "http://localhost:3000"


class TestBrandingCORS:
    def test_preflight_allows_dev_origin(self):
        # The browser issues an OPTIONS preflight before any cross-
        # origin GET; the response must echo the origin and list the
        # method as allowed, otherwise the actual GET never fires.
        resp = client.options(
            "/api/v1/branding",
            headers={
                "Origin": _DEV_ORIGIN,
                "Access-Control-Request-Method": "GET",
            },
        )
        assert resp.status_code in (200, 204), resp.text
        # Starlette's CORSMiddleware lower-cases the header keys.
        assert resp.headers.get("access-control-allow-origin") == _DEV_ORIGIN

    def test_get_includes_cors_origin_header(self):
        # A real cross-origin GET must come back with the
        # access-control-allow-origin header echoing the request
        # origin — that's what makes the response usable to fetch().
        resp = client.get(
            "/api/v1/branding",
            headers={"Origin": _DEV_ORIGIN},
        )
        assert resp.status_code == 200
        assert resp.headers.get("access-control-allow-origin") == _DEV_ORIGIN
        # Body still parses — the CORS layer doesn't intercept the
        # response payload, only adorns the headers.
        body = resp.json()
        assert body["company_name"] == DEFAULT_COMPANY_NAME

    def test_unknown_origin_does_not_get_allow_header(self):
        # Defense-in-depth: a request from an unlisted origin must NOT
        # come back with that origin echoed in allow-origin, otherwise
        # the CORS gate is effectively open.
        resp = client.get(
            "/api/v1/branding",
            headers={"Origin": "https://evil.example.com"},
        )
        # The endpoint itself still responds 200 (CORS is browser-side
        # enforcement only); the absence of the allow-origin header is
        # what stops the browser from using the response.
        assert resp.status_code == 200
        echoed = resp.headers.get("access-control-allow-origin")
        assert echoed != "https://evil.example.com"
