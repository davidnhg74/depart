"""Tests for the /settings endpoints and the BYOK-gated POST /convert.

The live Claude call in POST /convert is patched — we assert the
request plumbing (key precedence, no-key failure mode, payload
shaping) without burning tokens in CI. We also sign a dev license
JWT per test run so the license gate lets us through.
"""

from __future__ import annotations

import time
from pathlib import Path
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient
from jose import jwt
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.config import settings as env_settings
from src.main import app
from src.models import InstanceSettings
from src.services.settings_service import set_license_jwt


client = TestClient(app)


_DEV_PRIVATE_KEY_PATH = Path.home() / ".depart-keys" / "license_private_dev.pem"


def _mint_dev_license(*, features=("ai_conversion", "runbook_pdf"), days: int = 30) -> str:
    """Sign a throwaway JWT against the dev private key so tests can
    satisfy the require_feature('ai_conversion') gate. Mirrors what
    `scripts/sign_license.py` does at the CLI."""
    if not _DEV_PRIVATE_KEY_PATH.exists():
        pytest.skip(
            f"dev license signing key missing at {_DEV_PRIVATE_KEY_PATH} — "
            "generate one before running BYOK tests"
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


@pytest.fixture
def session_factory():
    engine = create_engine(env_settings.database_url)
    Session = sessionmaker(bind=engine)
    yield Session
    engine.dispose()


@pytest.fixture(autouse=True)
def reset_instance_settings(session_factory):
    """Wipe any prior run's InstanceSettings row so each test starts
    with a clean slate."""
    s = session_factory()
    s.query(InstanceSettings).delete()
    s.commit()
    s.close()
    yield


@pytest.fixture
def pro_license(session_factory):
    """Seed the install with a valid Pro license. Use this on tests
    that hit license-gated routes."""
    s = session_factory()
    set_license_jwt(s, _mint_dev_license())
    s.close()
    yield


# ─── Settings GET/PUT ────────────────────────────────────────────────────────


class TestSettingsEndpoint:
    def test_fresh_install_returns_unconfigured(self):
        resp = client.get("/api/v1/settings")
        assert resp.status_code == 200
        body = resp.json()
        assert body["anthropic_key_masked"] is None
        assert body["anthropic_key_configured"] is False
        assert body["license_configured"] is False

    def test_put_anthropic_key_stores_and_masks(self):
        resp = client.put(
            "/api/v1/settings/anthropic-key",
            json={"api_key": "sk-ant-api03-abcdefghijklmnopqrstuvwxyz-EXAMPLE"},
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["anthropic_key_configured"] is True
        # First 4 + last 4 visible; middle masked.
        assert body["anthropic_key_masked"].startswith("sk-a")
        assert body["anthropic_key_masked"].endswith("MPLE")
        assert "•" in body["anthropic_key_masked"]

    def test_clearing_anthropic_key_with_empty_string(self):
        # seed
        client.put(
            "/api/v1/settings/anthropic-key",
            json={"api_key": "sk-ant-api03-SEED-KEY-XYZ-12345"},
        )
        # clear
        resp = client.put("/api/v1/settings/anthropic-key", json={"api_key": ""})
        assert resp.status_code == 200
        assert resp.json()["anthropic_key_configured"] is False


# ─── POST /convert/{tag} — BYOK-gated ────────────────────────────────────────


class TestLiveConvert:
    def test_no_license_returns_402(self):
        """No license JWT stored → 402 Payment Required before we even
        check for a BYOK key. The UI uses this to route to /settings."""
        resp = client.post(
            "/api/v1/convert/MERGE",
            json={"snippet": "MERGE INTO t USING s ON (t.id=s.id) ..."},
        )
        assert resp.status_code == 402
        detail = resp.json()["detail"]
        assert detail["error"] == "license_required"
        assert detail["feature"] == "ai_conversion"
        assert detail["upgrade_url"] == "/settings/instance"

    def test_no_key_with_license_returns_412(self, pro_license):
        """With a valid license but no BYOK key, we fail with 412 so
        the operator can tell the two configuration gaps apart."""
        with patch.object(env_settings, "anthropic_api_key", ""):
            resp = client.post(
                "/api/v1/convert/MERGE",
                json={"snippet": "MERGE INTO t USING s ON (t.id=s.id) ..."},
            )
        assert resp.status_code == 412
        assert "anthropic" in resp.json()["detail"].lower()

    def test_unknown_tag_returns_404(self, pro_license):
        resp = client.post(
            "/api/v1/convert/NOT_A_TAG", json={"snippet": "SELECT 1;"}
        )
        assert resp.status_code == 404

    def test_empty_snippet_rejected_by_pydantic(self, pro_license):
        resp = client.post("/api/v1/convert/MERGE", json={"snippet": ""})
        assert resp.status_code == 422

    def test_happy_path_calls_claude_with_byok_key(self, pro_license):
        """Set an InstanceSettings key, mock the Claude call, and verify
        the endpoint shapes the response into our ConversionExample."""
        key = "sk-ant-api03-LIVE-CONVERT-TEST-KEY"
        client.put("/api/v1/settings/anthropic-key", json={"api_key": key})

        claude_reply = {
            "oracle": "MERGE INTO t USING s ...",
            "postgres": "INSERT INTO t ... ON CONFLICT (id) DO UPDATE ...",
            "reasoning": "Postgres upsert syntax differs from Oracle MERGE.",
            "confidence": "high",
        }

        with patch("src.routers.convert.AIClient") as MockClient:
            instance = MockClient.return_value
            instance.complete_json.return_value = claude_reply

            resp = client.post(
                "/api/v1/convert/MERGE",
                json={"snippet": "MERGE INTO t USING s ON (t.id=s.id) ..."},
            )

            assert resp.status_code == 200
            # Client was constructed with the BYOK key, not the env default.
            MockClient.assert_called_once()
            kwargs = MockClient.call_args.kwargs
            assert kwargs["api_key"] == key
            assert kwargs["feature"] == "live-convert"

        body = resp.json()
        assert body["tag"] == "MERGE"
        assert body["postgres"].startswith("INSERT INTO t")
        assert body["confidence"] == "high"

    def test_claude_failure_returns_502(self, pro_license):
        client.put(
            "/api/v1/settings/anthropic-key",
            json={"api_key": "sk-ant-api03-FAIL-TEST"},
        )
        with patch("src.routers.convert.AIClient") as MockClient:
            MockClient.return_value.complete_json.side_effect = RuntimeError(
                "upstream Anthropic 500"
            )
            resp = client.post(
                "/api/v1/convert/MERGE", json={"snippet": "MERGE INTO t USING s ..."}
            )
        assert resp.status_code == 502
        assert "AI conversion failed" in resp.json()["detail"]
