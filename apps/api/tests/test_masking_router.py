"""Integration tests for /api/v1/migrations/{id}/masking."""

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
from src.models import InstanceSettings, MigrationRecord, User
from src.services.settings_service import set_license_jwt


client = TestClient(app)


_DEV_PRIVATE_KEY_PATH = Path.home() / ".depart-keys" / "license_private_dev.pem"


def _mint_license(*, features=("data_masking",), days: int = 30) -> str:
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


def _seed_migration(source_url: str = "postgresql+psycopg://user:pw@host/db") -> str:
    engine = create_engine(env_settings.database_url)
    S = sessionmaker(bind=engine)
    s = S()
    rec = MigrationRecord(
        id=uuid.uuid4(),
        name="test-mig",
        schema_name="hr",
        source_url=source_url,
        target_url="postgresql+psycopg://user:pw@host/db",
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
        r = client.get(f"/api/v1/migrations/{mid}/masking", headers=op_headers)
    assert r.status_code == 403


def test_no_license_returns_402():
    headers = _bootstrap_admin()
    mid = _seed_migration()
    with auth_on():
        r = client.get(f"/api/v1/migrations/{mid}/masking", headers=headers)
    assert r.status_code == 402
    assert r.json()["detail"]["feature"] == "data_masking"


# ─── CRUD round trip ─────────────────────────────────────────────────


def test_get_before_put_returns_empty_rules():
    headers = _bootstrap_admin()
    _seed_license()
    mid = _seed_migration()
    with auth_on():
        r = client.get(f"/api/v1/migrations/{mid}/masking", headers=headers)
    assert r.status_code == 200
    assert r.json() == {"rules": {}}


def test_put_then_get_then_delete():
    headers = _bootstrap_admin()
    _seed_license()
    mid = _seed_migration()
    rules = {
        "HR.EMPLOYEES": {
            "EMAIL": {"strategy": "hash"},
            "SSN": {"strategy": "partial", "keep_first": 0, "keep_last": 4},
        }
    }
    with auth_on():
        r = client.put(
            f"/api/v1/migrations/{mid}/masking",
            json={"rules": rules},
            headers=headers,
        )
        assert r.status_code == 200, r.text
        assert r.json()["rules"] == rules

        r = client.get(f"/api/v1/migrations/{mid}/masking", headers=headers)
        assert r.json()["rules"] == rules

        r = client.delete(f"/api/v1/migrations/{mid}/masking", headers=headers)
        assert r.status_code == 204

        r = client.get(f"/api/v1/migrations/{mid}/masking", headers=headers)
        assert r.json() == {"rules": {}}


def test_put_rejects_invalid_strategy():
    headers = _bootstrap_admin()
    _seed_license()
    mid = _seed_migration()
    with auth_on():
        r = client.put(
            f"/api/v1/migrations/{mid}/masking",
            json={"rules": {"T": {"C": {"strategy": "bogus"}}}},
            headers=headers,
        )
    assert r.status_code == 400
    assert "strategy" in r.json()["detail"].lower()


def test_put_rejects_bad_regex():
    headers = _bootstrap_admin()
    _seed_license()
    mid = _seed_migration()
    with auth_on():
        r = client.put(
            f"/api/v1/migrations/{mid}/masking",
            json={"rules": {"T": {"C": {"strategy": "regex", "pattern": "(open"}}}},
            headers=headers,
        )
    assert r.status_code == 400


# ─── Preview ─────────────────────────────────────────────────────────


def test_preview_400_when_no_rules():
    headers = _bootstrap_admin()
    _seed_license()
    mid = _seed_migration()
    with auth_on():
        r = client.post(
            f"/api/v1/migrations/{mid}/masking/preview",
            json={"sample_size": 5},
            headers=headers,
        )
    assert r.status_code == 400
    assert "no masking rules" in r.json()["detail"].lower()


def test_preview_runs_against_real_source(monkeypatch):
    """End-to-end: seed a small source table in Postgres, configure
    masking rules on the migration, hit the preview endpoint, verify
    the response contains masked rows only."""
    monkeypatch.setenv("HAFEN_MASKING_KEY", "preview-test-key")

    import psycopg

    headers = _bootstrap_admin()
    _seed_license()

    # Set the source_url to point at the test DB so preview can
    # actually read from it.
    source_url = env_settings.database_url
    schema = f"preview_src_{uuid.uuid4().hex[:6]}"
    pg_url = source_url.replace("postgresql+psycopg://", "postgresql://")
    conn = psycopg.connect(pg_url, autocommit=True)
    try:
        with conn.cursor() as cur:
            cur.execute(f"CREATE SCHEMA {schema}")
            cur.execute(
                f"CREATE TABLE {schema}.users (id INT PRIMARY KEY, email TEXT)"
            )
            cur.executemany(
                f"INSERT INTO {schema}.users (id, email) VALUES (%s, %s)",
                [(i, f"user-{i}@example.com") for i in range(1, 4)],
            )

        mid = _seed_migration(source_url=source_url)
        rules = {f"{schema}.users": {"email": {"strategy": "hash", "length": 16}}}
        with auth_on():
            r = client.put(
                f"/api/v1/migrations/{mid}/masking",
                json={"rules": rules},
                headers=headers,
            )
            assert r.status_code == 200
            r = client.post(
                f"/api/v1/migrations/{mid}/masking/preview",
                json={"sample_size": 3},
                headers=headers,
            )
        assert r.status_code == 200, r.text
        body = r.json()
        key = f"{schema}.users"
        assert key in body["samples"]
        samples = body["samples"][key]
        assert len(samples) == 3
        for row in samples:
            assert "@" not in row["email"]
            assert len(row["email"]) == 16
        assert body["errors"] == {}
    finally:
        with conn.cursor() as cur:
            cur.execute(f"DROP SCHEMA {schema} CASCADE")
        conn.close()
