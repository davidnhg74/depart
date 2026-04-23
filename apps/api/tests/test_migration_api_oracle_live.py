"""End-to-end API workflow against live Oracle.

The library-level Runner test (`test_migrate_runner_oracle_live.py`)
proves the data-movement core. This test proves the layer above it —
the operator-facing HTTP API:

  POST   /api/v1/migrations/test-connection   verify Oracle DSN
  POST   /api/v1/migrations                   create record
  POST   /api/v1/migrations/{id}/plan         dry-run the planner
  POST   /api/v1/migrations/{id}/run          (we skip the queue layer
                                              and call run_migration
                                              directly so the test is
                                              deterministic)
  GET    /api/v1/migrations/{id}              read final state
  GET    /api/v1/migrations/{id}/progress     read progress

Skipped automatically if Oracle isn't reachable.
"""

from __future__ import annotations

import os
import uuid

import psycopg
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, text
from sqlalchemy.exc import DatabaseError, OperationalError
from sqlalchemy.orm import sessionmaker

from src.config import settings as env_settings
from src.main import app
from src.models import (
    MigrationCdcChange,
    MigrationCheckpointRecord,
    MigrationRecord,
)
from src.services.migration_runner import run_migration


ORACLE_URL = os.environ.get(
    "ORACLE_TEST_URL",
    "oracle+oracledb://system:oracle@localhost:1521/?service_name=FREEPDB1",
)


client = TestClient(app)


# ─── Skip-if-Oracle-down gate ────────────────────────────────────────────────


@pytest.fixture(scope="module", autouse=True)
def _require_oracle():
    try:
        e = create_engine(ORACLE_URL)
        s = sessionmaker(bind=e)()
        s.execute(text("SELECT 1 FROM dual"))
        s.close()
        e.dispose()
    except (DatabaseError, OperationalError, Exception) as exc:  # noqa: BLE001
        pytest.skip(f"Oracle not reachable at {ORACLE_URL!r}: {exc}")


# ─── Per-test isolation ──────────────────────────────────────────────────────


@pytest.fixture
def pg_url():
    return env_settings.database_url.replace("postgresql+psycopg://", "postgresql://")


@pytest.fixture
def pg_target_schema(pg_url):
    schema = f"api_e2e_{uuid.uuid4().hex[:8]}"
    conn = psycopg.connect(pg_url)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(f'CREATE SCHEMA "{schema}"')
    conn.close()
    yield schema
    conn = psycopg.connect(pg_url)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')
    conn.close()


@pytest.fixture
def cleanup_migration_records():
    """Strip migration rows the test creates so re-runs don't pile up
    or trip a UNIQUE constraint. Done before AND after to be robust to
    a prior failed run leaving rows behind."""
    engine = create_engine(env_settings.database_url)
    Session = sessionmaker(bind=engine)
    s = Session()
    s.query(MigrationCdcChange).delete()
    s.query(MigrationCheckpointRecord).delete()
    s.query(MigrationRecord).filter(
        MigrationRecord.name.like("api-e2e-%")
    ).delete()
    s.commit()
    s.close()
    yield
    s = Session()
    s.query(MigrationCdcChange).delete()
    s.query(MigrationCheckpointRecord).delete()
    s.query(MigrationRecord).filter(
        MigrationRecord.name.like("api-e2e-%")
    ).delete()
    s.commit()
    s.close()
    engine.dispose()


# ─── The flow ────────────────────────────────────────────────────────────────


# A SQLAlchemy URL the API can hand to create_engine. The URL the
# migration record stores is what the worker will use to actually
# connect; it must be parseable by SQLAlchemy.
_API_ORACLE_URL = ORACLE_URL


def test_full_api_workflow_oracle_to_postgres(
    cleanup_migration_records, pg_target_schema, pg_url
):
    """Drive the full operator-facing flow: test-connection, create,
    plan, run (synchronously, bypassing the arq queue), read progress.

    Asserts on the HTTP-layer contract — status codes, response shapes,
    state transitions on MigrationRecord — not just on whether rows
    landed. Library-level data correctness is covered separately in
    test_migrate_runner_oracle_live.py.
    """
    # 1. Test the connection — the same call the UI makes before
    #    showing the "Create migration" form.
    resp = client.post(
        "/api/v1/migrations/test-connection",
        json={"url": _API_ORACLE_URL, "schema": "HR"},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["ok"] is True
    assert body["dialect"] == "oracle"
    # Standard HR has 7 tables.
    assert body["tables_found"] == 7

    # 2. Create the migration record.
    create_resp = client.post(
        "/api/v1/migrations",
        json={
            "name": "api-e2e-regions",
            "source_url": _API_ORACLE_URL,
            "target_url": env_settings.database_url,
            "source_schema": "HR",
            "target_schema": pg_target_schema,
            "tables": ["REGIONS"],  # smallest table; whole flow is the point
            "batch_size": 100,
            "create_tables": True,  # API path emits the CREATE TABLE itself
        },
    )
    assert create_resp.status_code == 201, create_resp.text
    summary = create_resp.json()
    migration_id = summary["id"]
    assert summary["status"] == "pending"
    assert summary["target_schema"] == pg_target_schema

    # 3. Dry-run the plan. /plan introspects the source and reports
    #    what the run would do, without doing it.
    plan_resp = client.post(f"/api/v1/migrations/{migration_id}/plan")
    assert plan_resp.status_code == 200, plan_resp.text
    plan = plan_resp.json()
    assert plan["load_order"] == [f"{pg_target_schema}.REGIONS"]
    assert any(
        "REGIONS" in stmt and "CREATE TABLE" in stmt
        for stmt in plan["create_table_ddl"]
    )
    # REGIONS has 2 columns — both should map cleanly.
    region_id_mapping = next(
        m for m in plan["type_mappings"] if m["column"] == "REGION_ID"
    )
    assert region_id_mapping["source_type"] == "NUMBER"
    assert region_id_mapping["pg_type"].startswith(("NUMERIC", "INTEGER"))

    # 4. Run synchronously. The /run endpoint enqueues onto arq with a
    #    fallback to BackgroundTasks; for a deterministic test we
    #    invoke run_migration directly (which is what BOTH paths
    #    eventually call). The API contract is covered separately by
    #    the existing migrations-router tests.
    engine = create_engine(env_settings.database_url)
    Session = sessionmaker(bind=engine)
    db = Session()
    try:
        run_migration(db, migration_id)
    finally:
        db.close()
        engine.dispose()

    # 5. Read final state via the GET endpoints.
    detail_resp = client.get(f"/api/v1/migrations/{migration_id}")
    assert detail_resp.status_code == 200
    detail = detail_resp.json()
    # Successful runs flip to one of these terminal states.
    assert detail["status"] in ("completed", "completed_with_warnings"), (
        f"unexpected status: {detail['status']}, error={detail.get('error_message')}"
    )
    assert detail["error_message"] in (None, ""), detail["error_message"]

    # The /progress endpoint is the same shape as GET /{id} but the
    # UI uses it for polling — confirm it returns identical data.
    progress_resp = client.get(f"/api/v1/migrations/{migration_id}/progress")
    assert progress_resp.status_code == 200
    progress = progress_resp.json()
    assert progress["status"] == detail["status"]
    assert progress["rows_transferred"] == detail["rows_transferred"]

    # 6. And the data really landed on PG.
    conn = psycopg.connect(pg_url)
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(f'SELECT COUNT(*) FROM "{pg_target_schema}"."REGIONS"')
            assert cur.fetchone()[0] == 5
    finally:
        conn.close()


# ─── Quality-check endpoint ─────────────────────────────────────────────────


def test_quality_check_endpoint_against_live_oracle(
    cleanup_migration_records, pg_target_schema, pg_url
):
    """End-to-end POST /api/v1/migrations/{id}/quality-check.

    Sequence: create + run a migration of HR.REGIONS → throwaway PG
    schema, then call /quality-check and assert the report is `ok`
    (clean migration, no findings)."""
    create_resp = client.post(
        "/api/v1/migrations",
        json={
            "name": "api-e2e-quality",
            "source_url": _API_ORACLE_URL,
            "target_url": env_settings.database_url,
            "source_schema": "HR",
            "target_schema": pg_target_schema,
            "tables": ["REGIONS"],
            "batch_size": 100,
            "create_tables": True,
        },
    )
    assert create_resp.status_code == 201, create_resp.text
    migration_id = create_resp.json()["id"]

    # Run synchronously, same trick as the workflow test.
    engine = create_engine(env_settings.database_url)
    Session = sessionmaker(bind=engine)
    db = Session()
    try:
        run_migration(db, migration_id)
    finally:
        db.close()
        engine.dispose()

    # Now the quality-check call: post-copy compare should run because
    # the target table now exists.
    qc_resp = client.post(f"/api/v1/migrations/{migration_id}/quality-check")
    assert qc_resp.status_code == 200, qc_resp.text
    qc = qc_resp.json()
    # A clean copy of REGIONS should produce zero findings — same row
    # counts, no NULLs to disagree about, REGION_NAME is short text
    # well within the declared 25-char limit.
    assert qc["overall_severity"] == "ok", (
        f"unexpected findings on a clean migration: {qc['findings']}"
    )
    assert qc["findings"] == []


def test_quality_check_runs_pre_copy_only_when_target_missing(
    cleanup_migration_records, pg_target_schema
):
    """When the migration record exists but no run has happened yet,
    the post-copy compare must skip cleanly (no target table to read
    from). The pre-copy scan still runs against the source."""
    create_resp = client.post(
        "/api/v1/migrations",
        json={
            "name": "api-e2e-quality-pre",
            "source_url": _API_ORACLE_URL,
            "target_url": env_settings.database_url,
            "source_schema": "HR",
            "target_schema": pg_target_schema,
            "tables": ["REGIONS"],
            "batch_size": 100,
            "create_tables": True,
        },
    )
    migration_id = create_resp.json()["id"]

    # No /run call — target is empty.
    qc_resp = client.post(f"/api/v1/migrations/{migration_id}/quality-check")
    assert qc_resp.status_code == 200, qc_resp.text
    qc = qc_resp.json()
    # Source-side scan didn't find anything wrong with REGIONS either,
    # but the call itself must have succeeded (not 502'd on missing
    # target table).
    assert qc["overall_severity"] in ("ok", "warning")
