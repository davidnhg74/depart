"""Tests for cdc.apply — idempotent UPSERT against a real Postgres target.

We spin up a throwaway schema in the same test Postgres, seed a
simple `emp` table, and run Change records through ``apply_changes``
in both ``per_row`` and ``atomic`` modes. No Oracle needed — the
apply side is dialect-agnostic Postgres.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone

import psycopg
import pytest

from src.config import settings as env_settings
from src.services.cdc.apply import apply_changes
from src.services.cdc.queue import Change


# ─── Fixtures ────────────────────────────────────────────────────────


@pytest.fixture
def pg_url():
    return env_settings.database_url.replace("postgresql+psycopg://", "postgresql://")


@pytest.fixture
def target_schema(pg_url):
    """Fresh schema per test; creates an `emp` table and a junction
    table `emp_dept` (all-PK) for the DO-NOTHING edge case."""
    schema = f"cdc_tgt_{uuid.uuid4().hex[:6]}"
    conn = psycopg.connect(pg_url)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA {schema}")
        cur.execute(
            f"""
            CREATE TABLE {schema}.emp (
                id INTEGER PRIMARY KEY,
                name TEXT,
                dept_id INTEGER
            )
            """
        )
        cur.execute(
            f"""
            CREATE TABLE {schema}.emp_dept (
                emp_id INTEGER,
                dept_id INTEGER,
                PRIMARY KEY (emp_id, dept_id)
            )
            """
        )
    conn.close()
    yield schema
    conn = psycopg.connect(pg_url)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(f"DROP SCHEMA {schema} CASCADE")
    conn.close()


@pytest.fixture
def pg_conn(pg_url):
    """Autocommit conn — psycopg's `conn.transaction()` handles
    BEGIN/COMMIT explicitly around each block regardless of mode, and
    autocommit guarantees we don't hold any implicit locks that
    block the fixture teardown's DROP SCHEMA CASCADE."""
    conn = psycopg.connect(pg_url)
    conn.autocommit = True
    yield conn
    conn.close()


def _mk_change(
    cid: int,
    scn: int,
    op: str,
    pk: dict,
    after: dict | None = None,
    before: dict | None = None,
    table: str = "emp",
) -> Change:
    return Change(
        id=cid,
        scn=scn,
        source_schema="HR",
        source_table=table,
        op=op,
        pk=pk,
        before=before,
        after=after,
        committed_at=datetime(2026, 4, 23, 12, 0, 0, tzinfo=timezone.utc),
    )


def _count(pg_conn, schema, table):
    with pg_conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {schema}.{table}")
        return cur.fetchone()[0]


def _read(pg_conn, schema, table, where_sql):
    pg_conn.rollback()
    with pg_conn.cursor() as cur:
        cur.execute(f"SELECT * FROM {schema}.{table} WHERE {where_sql}")
        return cur.fetchone()


# ─── Basic ops ──────────────────────────────────────────────────────


def test_insert_lands_on_target(pg_conn, target_schema):
    c = _mk_change(
        1, scn=10, op="I", pk={"id": 1},
        after={"id": 1, "name": "Alice", "dept_id": 10},
    )
    results = apply_changes(pg_conn, target_schema, [c])
    assert results[0].ok is True, results[0].error
    assert _count(pg_conn, target_schema, "emp") == 1
    row = _read(pg_conn, target_schema, "emp", "id = 1")
    assert row == (1, "Alice", 10)


def test_update_overwrites(pg_conn, target_schema):
    apply_changes(
        pg_conn,
        target_schema,
        [_mk_change(1, 10, "I", {"id": 1}, {"id": 1, "name": "Alice", "dept_id": 10})],
    )
    apply_changes(
        pg_conn,
        target_schema,
        [
            _mk_change(
                2,
                20,
                "U",
                {"id": 1},
                before={"id": 1, "name": "Alice", "dept_id": 10},
                after={"id": 1, "name": "Alicia", "dept_id": 20},
            )
        ],
    )
    row = _read(pg_conn, target_schema, "emp", "id = 1")
    assert row == (1, "Alicia", 20)


def test_delete_removes(pg_conn, target_schema):
    apply_changes(
        pg_conn,
        target_schema,
        [_mk_change(1, 10, "I", {"id": 1}, {"id": 1, "name": "Alice", "dept_id": 10})],
    )
    apply_changes(
        pg_conn,
        target_schema,
        [_mk_change(2, 20, "D", {"id": 1}, before={"id": 1, "name": "Alice", "dept_id": 10})],
    )
    assert _count(pg_conn, target_schema, "emp") == 0


def test_upsert_is_idempotent(pg_conn, target_schema):
    """Apply the same INSERT twice — no error, no duplicate. This is
    the central guarantee that makes retries safe."""
    c = _mk_change(1, 10, "I", {"id": 1}, {"id": 1, "name": "Alice", "dept_id": 10})
    r1 = apply_changes(pg_conn, target_schema, [c])
    assert r1[0].ok
    r2 = apply_changes(pg_conn, target_schema, [c])
    assert r2[0].ok
    assert _count(pg_conn, target_schema, "emp") == 1


def test_all_pk_table_uses_do_nothing(pg_conn, target_schema):
    """Junction table with no non-PK columns can't UPDATE on
    conflict — we emit DO NOTHING to keep idempotency."""
    c = _mk_change(
        1, 10, "I",
        pk={"emp_id": 1, "dept_id": 5},
        after={"emp_id": 1, "dept_id": 5},
        table="emp_dept",
    )
    apply_changes(pg_conn, target_schema, [c])
    apply_changes(pg_conn, target_schema, [c])  # second time is no-op
    assert _count(pg_conn, target_schema, "emp_dept") == 1


# ─── Mode semantics ─────────────────────────────────────────────────


def test_per_row_mode_forwards_past_failures(pg_conn, target_schema):
    """One bad row shouldn't stop the good ones. Second change
    references a missing table, but the first and third still land."""
    good1 = _mk_change(1, 10, "I", {"id": 1}, {"id": 1, "name": "A", "dept_id": 10})
    bad = _mk_change(
        2, 20, "I", {"id": 99}, {"id": 99, "name": "Ghost"}, table="no_such_table"
    )
    good2 = _mk_change(3, 30, "I", {"id": 3}, {"id": 3, "name": "C", "dept_id": 30})

    results = apply_changes(
        pg_conn, target_schema, [good1, bad, good2], mode="per_row"
    )
    assert results[0].ok is True, results[0].error
    assert results[1].ok is False
    assert "no_such_table" in (results[1].error or "")
    assert results[2].ok is True, results[2].error
    # Target has both good rows.
    assert _count(pg_conn, target_schema, "emp") == 2


def test_atomic_mode_rolls_back_all_on_any_failure(pg_conn, target_schema):
    """The whole batch shares a transaction — one bad row takes
    every row in the batch down with it."""
    good = _mk_change(1, 10, "I", {"id": 1}, {"id": 1, "name": "A", "dept_id": 10})
    bad = _mk_change(
        2, 20, "I", {"id": 2}, {"id": 2, "name": "B"}, table="no_such_table"
    )

    results = apply_changes(pg_conn, target_schema, [good, bad], mode="atomic")
    assert all(r.ok is False for r in results)
    # Atomic mode rolled back — neither row made it.
    assert _count(pg_conn, target_schema, "emp") == 0


def test_empty_batch_is_noop():
    # No pg_conn fixture — shouldn't even need one.
    class _StubConn:
        def transaction(self):  # pragma: no cover — not called
            raise AssertionError("should not be called for empty batch")
    assert apply_changes(_StubConn(), "t", [], mode="per_row") == []
    assert apply_changes(_StubConn(), "t", [], mode="atomic") == []


def test_apply_raises_on_unpersisted_change(pg_conn, target_schema):
    """A Change with no .id means the caller skipped the queue —
    that's a programming error, not a runtime condition we should
    silently work around. Precondition-check it up front."""
    c = _mk_change(1, 10, "I", {"id": 1}, {"id": 1, "name": "A"})
    c.id = None
    with pytest.raises(ValueError, match="no .id"):
        apply_changes(pg_conn, target_schema, [c])
