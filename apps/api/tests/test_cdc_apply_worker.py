"""End-to-end drain tests — seeded queue → target Postgres.

The apply worker is the piece that closes the CDC loop: pulls pending
changes, UPSERTs them on target, advances last_applied_scn. These
tests exercise that against a real Postgres target (the same test
DB we use everywhere else) via a throwaway schema.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone

import psycopg
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.config import settings as env_settings
from src.models import MigrationCdcChange, MigrationRecord
from src.services.cdc import apply_worker
pytestmark = pytest.mark.integration


# ─── Fixtures ────────────────────────────────────────────────────────


@pytest.fixture
def pg_url():
    return env_settings.database_url.replace(
        "postgresql+psycopg://", "postgresql://"
    )


@pytest.fixture
def target_schema(pg_url):
    schema = f"drain_tgt_{uuid.uuid4().hex[:6]}"
    conn = psycopg.connect(pg_url)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA {schema}")
        cur.execute(
            f"CREATE TABLE {schema}.emp (id INTEGER PRIMARY KEY, name TEXT)"
        )
        cur.execute(
            f"CREATE TABLE {schema}.dept (id INTEGER PRIMARY KEY, name TEXT)"
        )
    conn.close()
    yield schema
    conn = psycopg.connect(pg_url)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(f"DROP SCHEMA {schema} CASCADE")
    conn.close()


@pytest.fixture
def db():
    engine = create_engine(env_settings.database_url)
    Session = sessionmaker(bind=engine)
    s = Session()
    s.query(MigrationCdcChange).delete()
    s.query(MigrationRecord).delete()
    s.commit()
    try:
        yield s
    finally:
        s.query(MigrationCdcChange).delete()
        s.query(MigrationRecord).delete()
        s.commit()
        s.close()
        engine.dispose()


def _seed_migration(
    db, target_schema: str, *, apply_mode: str = "per_row"
) -> uuid.UUID:
    rec = MigrationRecord(
        id=uuid.uuid4(),
        name="drain-test",
        schema_name="hr",
        source_url="oracle://...",
        target_url=env_settings.database_url,  # we drain onto the same test DB
        source_schema="HR",
        target_schema=target_schema,
        status="pending",
        cdc_apply_mode=apply_mode,
    )
    db.add(rec)
    db.commit()
    return rec.id


def _seed_change(
    db,
    migration_id: uuid.UUID,
    *,
    scn: int,
    op: str,
    table: str = "emp",
    pk: dict | None = None,
    after: dict | None = None,
    before: dict | None = None,
) -> None:
    row = MigrationCdcChange(
        migration_id=migration_id,
        scn=scn,
        source_schema="HR",
        source_table=table,
        op=op,
        pk_json=pk or {"id": scn},
        before_json=before,
        after_json=after,
        committed_at=datetime(2026, 4, 23, tzinfo=timezone.utc),
    )
    db.add(row)
    db.commit()


def _target_count(pg_url, target_schema: str, table: str) -> int:
    conn = psycopg.connect(pg_url)
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {target_schema}.{table}")
            return cur.fetchone()[0]
    finally:
        conn.close()


# ─── Basic drain ─────────────────────────────────────────────────────


def test_drain_applies_pending_changes_and_advances_scn(
    db, target_schema, pg_url
):
    mid = _seed_migration(db, target_schema)
    _seed_change(db, mid, scn=10, op="I", after={"id": 10, "name": "Alice"})
    _seed_change(db, mid, scn=20, op="I", after={"id": 20, "name": "Bob"})

    result = apply_worker.drain_migration(db, mid)

    assert result.drained_count == 2
    assert result.applied_count == 2
    assert result.failed_count == 0
    assert result.new_last_applied_scn == 20
    assert _target_count(pg_url, target_schema, "emp") == 2

    # Queue rows stamped applied.
    remaining = (
        db.query(MigrationCdcChange)
        .filter(MigrationCdcChange.applied_at.is_(None))
        .count()
    )
    assert remaining == 0


def test_drain_is_noop_when_nothing_pending(db, target_schema, pg_url):
    mid = _seed_migration(db, target_schema)
    result = apply_worker.drain_migration(db, mid)
    assert result.drained_count == 0
    assert result.new_last_applied_scn is None  # never set


def test_drain_uses_migration_apply_mode(db, target_schema, pg_url):
    """Atomic mode rolls the whole batch back on one bad row —
    per_row would leave the good rows applied."""
    mid = _seed_migration(db, target_schema, apply_mode="atomic")
    _seed_change(db, mid, scn=10, op="I", after={"id": 10, "name": "Alice"})
    # Bogus target_table → UPSERT fails → whole batch rolls back
    _seed_change(
        db, mid, scn=20, op="I", table="nonexistent",
        after={"id": 99, "name": "Ghost"},
    )
    _seed_change(db, mid, scn=30, op="I", after={"id": 30, "name": "Carol"})

    result = apply_worker.drain_migration(db, mid)

    # All three were attempted; none landed in atomic mode.
    assert result.drained_count == 3
    assert result.applied_count == 0
    assert result.failed_count == 3
    assert _target_count(pg_url, target_schema, "emp") == 0


def test_drain_per_row_forwards_past_failures(db, target_schema, pg_url):
    mid = _seed_migration(db, target_schema, apply_mode="per_row")
    _seed_change(db, mid, scn=10, op="I", after={"id": 10, "name": "Alice"})
    _seed_change(
        db, mid, scn=20, op="I", table="nonexistent",
        after={"id": 99, "name": "Ghost"},
    )
    _seed_change(db, mid, scn=30, op="I", after={"id": 30, "name": "Carol"})

    result = apply_worker.drain_migration(db, mid)

    assert result.applied_count == 2
    assert result.failed_count == 1
    assert _target_count(pg_url, target_schema, "emp") == 2
    # Watermark advances to the highest successfully applied SCN,
    # even though SCN 20 failed in between. Operators see the gap via
    # failed_count > 0 on the status endpoint.
    assert result.new_last_applied_scn == 30


def test_drain_handles_multiple_tables(db, target_schema, pg_url):
    mid = _seed_migration(db, target_schema)
    _seed_change(db, mid, scn=10, op="I", after={"id": 10, "name": "Alice"}, table="emp")
    _seed_change(db, mid, scn=20, op="I", after={"id": 1, "name": "Eng"}, table="dept")
    _seed_change(db, mid, scn=30, op="I", after={"id": 11, "name": "Bob"}, table="emp")

    result = apply_worker.drain_migration(db, mid)
    assert result.applied_count == 3
    assert _target_count(pg_url, target_schema, "emp") == 2
    assert _target_count(pg_url, target_schema, "dept") == 1


# ─── migrations_with_pending ────────────────────────────────────────


def test_migrations_with_pending_returns_only_ones_with_unapplied(
    db, target_schema
):
    quiet = _seed_migration(db, target_schema)
    busy = _seed_migration(db, target_schema)
    _seed_change(db, busy, scn=10, op="I", after={"id": 10, "name": "Alice"})

    mids = apply_worker.migrations_with_pending(db)
    assert busy in mids
    assert quiet not in mids


# ─── Edge cases ─────────────────────────────────────────────────────


def test_drain_raises_on_missing_migration(db):
    with pytest.raises(ValueError, match="not found"):
        apply_worker.drain_migration(db, uuid.uuid4())


def test_drain_raises_on_missing_target_url(db, target_schema):
    mid = _seed_migration(db, target_schema)
    # Blank out the target URL to simulate a half-configured migration
    rec = db.get(MigrationRecord, mid)
    rec.target_url = None
    db.commit()
    with pytest.raises(ValueError, match="target_url"):
        apply_worker.drain_migration(db, mid)
