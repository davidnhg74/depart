"""Tests for /api/v1/license — upload JWT, read status."""

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


client = TestClient(app)
_DEV_KEY = Path.home() / ".depart-keys" / "license_private_dev.pem"


def _mint(claims: dict) -> str:
    if not _DEV_KEY.exists():
        pytest.skip(f"dev signing key missing: {_DEV_KEY}")
    return jwt.encode(claims, _DEV_KEY.read_text(), algorithm="RS256")


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


def test_status_community_on_fresh_install():
    resp = client.get("/api/v1/license")
    assert resp.status_code == 200
    body = resp.json()
    assert body["valid"] is False
    assert body["tier"] == "community"
    assert body["reason"] == "no license uploaded"


def test_upload_valid_license_flips_to_pro():
    now = int(time.time())
    token = _mint(
        {
            "sub": "ops@acme.com",
            "project": "acme-q2",
            "tier": "pro",
            "features": ["ai_conversion", "runbook_pdf"],
            "iat": now,
            "exp": now + 86400,
        }
    )
    resp = client.put("/api/v1/license", json={"jwt": token})
    assert resp.status_code == 200
    body = resp.json()
    assert body["valid"] is True
    assert body["tier"] == "pro"
    assert body["features"] == ["ai_conversion", "runbook_pdf"]
    assert body["subject"] == "ops@acme.com"
    assert body["project"] == "acme-q2"


def test_upload_garbage_stored_but_reported_invalid():
    """Operators who paste a broken string deserve a clear error
    message, not a silent acceptance."""
    resp = client.put("/api/v1/license", json={"jwt": "not-a-jwt"})
    assert resp.status_code == 200
    body = resp.json()
    assert body["valid"] is False
    assert "invalid" in (body["reason"] or "").lower()


def test_clearing_license_reverts_to_community():
    # seed a good one
    now = int(time.time())
    client.put(
        "/api/v1/license",
        json={
            "jwt": _mint(
                {
                    "sub": "x",
                    "tier": "pro",
                    "features": [],
                    "iat": now,
                    "exp": now + 60,
                }
            )
        },
    )
    # clear
    resp = client.put("/api/v1/license", json={"jwt": ""})
    assert resp.status_code == 200
    body = resp.json()
    assert body["valid"] is False
    assert body["tier"] == "community"


def test_settings_status_reflects_license_upload():
    """/settings should surface `license_configured=True` once a JWT
    is stored, even if it's invalid — the operator uploaded something,
    the settings status just reflects that."""
    resp = client.put("/api/v1/license", json={"jwt": "not-a-jwt"})
    assert resp.status_code == 200

    s = client.get("/api/v1/settings")
    assert s.status_code == 200
    assert s.json()["license_configured"] is True
