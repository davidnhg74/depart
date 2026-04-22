"""Tests for the offline license verifier.

Covers the three states the rest of the app cares about: missing,
invalid, valid. Uses the dev signing key at ~/.depart-keys/ to mint
test tokens — same key the test_settings_and_byok.py fixture uses.
"""

from __future__ import annotations

import time
from pathlib import Path

import pytest
from jose import jwt

from src.license import Tier, verify
from src.license.verifier import _load_public_key


_DEV_PRIVATE_KEY_PATH = Path.home() / ".depart-keys" / "license_private_dev.pem"


def _sign(claims: dict) -> str:
    if not _DEV_PRIVATE_KEY_PATH.exists():
        pytest.skip(f"dev private key missing at {_DEV_PRIVATE_KEY_PATH}")
    return jwt.encode(claims, _DEV_PRIVATE_KEY_PATH.read_text(), algorithm="RS256")


# ─── Valid-token paths ───────────────────────────────────────────────────────


class TestValidLicense:
    def test_pro_license_is_valid(self):
        now = int(time.time())
        token = _sign(
            {
                "sub": "ops@acme.com",
                "project": "acme-q2",
                "tier": "pro",
                "features": ["ai_conversion", "runbook_pdf"],
                "iat": now,
                "exp": now + 86400,
            }
        )
        status = verify(token)
        assert status.valid is True
        assert status.tier == Tier.PRO
        assert status.features == ["ai_conversion", "runbook_pdf"]
        assert status.subject == "ops@acme.com"
        assert status.project == "acme-q2"
        assert status.expires_at is not None
        assert status.reason is None

    def test_has_feature_respects_feature_list(self):
        now = int(time.time())
        token = _sign(
            {
                "sub": "x",
                "tier": "pro",
                "features": ["ai_conversion"],
                "iat": now,
                "exp": now + 86400,
            }
        )
        status = verify(token)
        assert status.has_feature("ai_conversion")
        assert not status.has_feature("runbook_pdf")


# ─── Failure modes (must not raise) ──────────────────────────────────────────


class TestInvalidLicense:
    def test_none_token_is_invalid(self):
        status = verify(None)
        assert status.valid is False
        assert status.reason == "no license uploaded"

    def test_empty_string_is_invalid(self):
        status = verify("")
        assert status.valid is False

    def test_malformed_token(self):
        status = verify("not.a.jwt")
        assert status.valid is False
        assert "invalid" in (status.reason or "").lower()

    def test_tampered_signature(self):
        """Valid JWT structure but signature bytes mangled — must fail
        verification without raising. Flipping a single base64 char
        sometimes still decodes to a valid signature because of padding
        bits, so we mangle a middle chunk to make the test reliable."""
        now = int(time.time())
        token = _sign({"sub": "x", "tier": "pro", "features": [], "iat": now, "exp": now + 60})
        head, payload, sig = token.split(".")
        # Replace the middle 10 chars — guaranteed to invalidate the signature.
        mid = len(sig) // 2
        tampered = f"{head}.{payload}.{sig[:mid]}AAAAAAAAAA{sig[mid + 10:]}"
        status = verify(tampered)
        assert status.valid is False

    def test_expired_token(self):
        now = int(time.time())
        token = _sign(
            {
                "sub": "x",
                "tier": "pro",
                "features": ["ai_conversion"],
                "iat": now - 100_000,
                "exp": now - 60,  # expired a minute ago
            }
        )
        status = verify(token)
        assert status.valid is False

    def test_unknown_tier(self):
        now = int(time.time())
        token = _sign(
            {"sub": "x", "tier": "wizard", "features": [], "iat": now, "exp": now + 60}
        )
        status = verify(token)
        assert status.valid is False
        assert "tier" in (status.reason or "").lower()

    def test_community_tier_token_is_not_a_grant(self):
        """A token with tier=community must not satisfy any feature
        check — it's semantically equivalent to having no license."""
        now = int(time.time())
        token = _sign(
            {"sub": "x", "tier": "community", "features": [], "iat": now, "exp": now + 60}
        )
        status = verify(token)
        assert status.valid is False


# ─── Key loading ─────────────────────────────────────────────────────────────


def test_bundled_public_key_loads():
    key = _load_public_key()
    assert "BEGIN PUBLIC KEY" in key
