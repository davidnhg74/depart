"""Tests for /api/v1/migrations.

The /run endpoint is exercised with the background task patched, so
tests never open real Oracle/Postgres connections — we're validating
the CRUD layer and the handoff shape, not the runner engine (which
has its own tests under test_migrate_runner.py).
"""

from __future__ import annotations

from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.config import settings as env_settings
from src.main import app
from src.models import MigrationCheckpointRecord, MigrationRecord


client = TestClient(app)


@pytest.fixture(autouse=True)
def clean_migrations_table():
    """Wipe migrations + checkpoints both before AND after each test.

    The post-test cleanup matters: these tests leave checkpoint rows
    (e.g. test_progress_shows_checkpoints seeds one with
    table_name='public.items'), which otherwise collide with
    independent checkpoint-adapter tests that assume a clean table."""
    engine = create_engine(env_settings.database_url)
    S = sessionmaker(bind=engine)

    def wipe():
        s = S()
        s.query(MigrationCheckpointRecord).delete()
        s.query(MigrationRecord).delete()
        s.commit()
        s.close()

    wipe()
    yield
    wipe()
    engine.dispose()


def _create_payload(**overrides) -> dict:
    """Happy-path body with sensible defaults so individual tests can
    override a field or two without re-typing the whole thing."""
    base = {
        "name": "acme-q2-2026",
        "source_url": "postgresql+psycopg://u:p@src:5432/src",
        "target_url": "postgresql+psycopg://u:p@dst:5432/dst",
        "source_schema": "HR",
        "target_schema": "public",
        "tables": None,
        "batch_size": 5000,
        "create_tables": False,
    }
    base.update(overrides)
    return base


# ─── POST / (create) ────────────────────────────────────────────────────────


class TestCreate:
    def test_happy_path_returns_pending_summary(self):
        r = client.post("/api/v1/migrations", json=_create_payload())
        assert r.status_code == 201
        body = r.json()
        assert body["name"] == "acme-q2-2026"
        assert body["source_schema"] == "HR"
        assert body["target_schema"] == "public"
        assert body["status"] == "pending"
        assert body["rows_transferred"] == 0

    def test_create_persists_full_config(self):
        r = client.post(
            "/api/v1/migrations",
            json=_create_payload(
                tables=["EMPLOYEES", "DEPARTMENTS"],
                batch_size=2000,
                create_tables=True,
            ),
        )
        assert r.status_code == 201
        detail = client.get(f"/api/v1/migrations/{r.json()['id']}").json()
        assert detail["tables"] == ["EMPLOYEES", "DEPARTMENTS"]
        assert detail["batch_size"] == 2000
        assert detail["create_tables"] is True
        # Full DSN round-trips through GET so the UI can show it masked.
        assert detail["source_url"].startswith("postgresql+psycopg://u:p@src")

    def test_empty_name_rejected(self):
        r = client.post("/api/v1/migrations", json=_create_payload(name=""))
        assert r.status_code == 422

    def test_bad_batch_size_rejected(self):
        r = client.post("/api/v1/migrations", json=_create_payload(batch_size=0))
        assert r.status_code == 422


# ─── GET / (list) ───────────────────────────────────────────────────────────


class TestList:
    def test_empty_list(self):
        r = client.get("/api/v1/migrations")
        assert r.status_code == 200
        assert r.json() == []

    def test_newest_first(self):
        for name in ("first", "second", "third"):
            client.post("/api/v1/migrations", json=_create_payload(name=name))
        r = client.get("/api/v1/migrations")
        names = [m["name"] for m in r.json()]
        assert names == ["third", "second", "first"]


# ─── GET /{id} ──────────────────────────────────────────────────────────────


class TestDetail:
    def test_404_on_unknown(self):
        r = client.get("/api/v1/migrations/00000000-0000-0000-0000-000000000000")
        assert r.status_code == 404

    def test_400_on_bad_uuid(self):
        r = client.get("/api/v1/migrations/not-a-uuid")
        assert r.status_code == 400


# ─── POST /{id}/run ─────────────────────────────────────────────────────────


class TestRun:
    def test_run_queues_and_flips_status(self):
        """The endpoint returns 202 immediately and flips the record
        to `queued` before the worker picks it up. We patch the
        enqueue helper so no real runner kicks off and we don't need
        Redis for the test."""
        create = client.post("/api/v1/migrations", json=_create_payload()).json()

        async def fake_enqueue(migration_id, background=None):
            return f"test-job:{migration_id}"

        with patch(
            "src.routers.migrations.enqueue_migration", side_effect=fake_enqueue
        ) as mock_enqueue:
            r = client.post(f"/api/v1/migrations/{create['id']}/run")
            assert r.status_code == 202
            body = r.json()
            assert body["status"] == "queued"
            mock_enqueue.assert_called_once()
            assert mock_enqueue.call_args.args[0] == create["id"]

    def test_run_unknown_id_is_404(self):
        r = client.post(
            "/api/v1/migrations/00000000-0000-0000-0000-000000000000/run"
        )
        assert r.status_code == 404


# ─── POST /test-connection ──────────────────────────────────────────────────


class TestConnectionTester:
    def test_valid_postgres_url_returns_ok(self):
        # Use the live test DB from env — it's the same one the test
        # suite already relies on for every other test.
        from src.config import settings as env_settings

        url = env_settings.database_url
        resp = client.post(
            "/api/v1/migrations/test-connection",
            json={"url": url},
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["ok"] is True
        assert body["dialect"] == "postgres"
        assert "success" in body["message"].lower()

    def test_bad_host_returns_ok_false(self):
        resp = client.post(
            "/api/v1/migrations/test-connection",
            json={"url": "postgresql+psycopg://u:p@nonexistent-host:5432/x"},
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["ok"] is False
        assert body["dialect"] == "postgres"
        assert body["tables_found"] is None

    def test_schema_enum_returns_table_count(self):
        from src.config import settings as env_settings

        resp = client.post(
            "/api/v1/migrations/test-connection",
            json={"url": env_settings.database_url, "schema": "public"},
        )
        body = resp.json()
        assert body["ok"] is True
        # public schema in the test DB has at least the migrations/users/etc.
        assert body["tables_found"] is not None
        assert body["tables_found"] >= 1


# ─── GET /{id}/progress ─────────────────────────────────────────────────────


class TestProgress:
    def test_progress_matches_detail_shape(self):
        create = client.post("/api/v1/migrations", json=_create_payload()).json()
        a = client.get(f"/api/v1/migrations/{create['id']}").json()
        b = client.get(f"/api/v1/migrations/{create['id']}/progress").json()
        assert a == b

    def test_progress_shows_checkpoints(self):
        """Seed a checkpoint row and verify the progress response
        reflects it — this is the read path the UI polls."""
        create = client.post("/api/v1/migrations", json=_create_payload()).json()

        # Add a checkpoint directly — mirrors what the real runner
        # would do per batch via CheckpointManager.create_checkpoint.
        engine = create_engine(env_settings.database_url)
        S = sessionmaker(bind=engine)
        s = S()
        cp = MigrationCheckpointRecord(
            migration_id=create["id"],
            table_name="public.items",
            rows_processed=1500,
            total_rows=5000,
            progress_percentage=30.0,
            last_rowid="[1500]",
            status="in_progress",
        )
        s.add(cp)
        s.commit()
        s.close()
        engine.dispose()

        body = client.get(f"/api/v1/migrations/{create['id']}/progress").json()
        assert len(body["checkpoints"]) == 1
        cp_resp = body["checkpoints"][0]
        assert cp_resp["table_name"] == "public.items"
        assert cp_resp["rows_processed"] == 1500
        assert cp_resp["progress_percentage"] == 30.0
