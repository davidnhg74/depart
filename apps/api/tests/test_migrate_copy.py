"""Integration tests for the Postgres COPY writer.

These hit the real Postgres at `settings.database_url` (the dev/CI
container). They create a temp table per test, COPY into it, and verify
the round-trip. No mocking — COPY's binary format is too fiddly to
mock usefully.
"""

from __future__ import annotations

import uuid
from decimal import Decimal

import psycopg
import pytest

from src.config import settings
from src.migrate.copy import copy_rows_to_postgres

pytestmark = pytest.mark.integration


@pytest.fixture
def pg_conn():
    """Raw psycopg connection — the COPY API isn't on SQLAlchemy's
    Session, and we need the binary COPY protocol."""
    # `settings.database_url` is `postgresql+psycopg://...`; psycopg.connect
    # wants the bare URL.
    url = settings.database_url.replace("postgresql+psycopg://", "postgresql://")
    conn = psycopg.connect(url, autocommit=False)
    yield conn
    conn.rollback()
    conn.close()


@pytest.fixture
def temp_table(pg_conn):
    """Create a throwaway table with a few common types and yield its
    name. Drops on teardown even if the test left the connection in an
    aborted transaction state."""
    name = f"copy_test_{uuid.uuid4().hex[:8]}"
    with pg_conn.cursor() as cur:
        cur.execute(
            f"""
            CREATE TABLE public.{name} (
                id          INTEGER PRIMARY KEY,
                description TEXT,
                amount      NUMERIC(10, 2),
                created_at  TIMESTAMP
            )
            """
        )
    pg_conn.commit()
    yield name
    # Reset transaction state before DROP — a failed COPY leaves the
    # transaction in an aborted state where any further statement errors.
    pg_conn.rollback()
    with pg_conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS public.{name}")
    pg_conn.commit()


# ─── End-to-end COPY ─────────────────────────────────────────────────────────


def test_copy_rows_round_trip(pg_conn, temp_table):
    """Three rows in, three rows out — same values, same order."""
    from datetime import datetime

    rows = [
        (1, "first", Decimal("10.50"), datetime(2026, 1, 1, 12, 0, 0)),
        (2, "second", Decimal("99.99"), datetime(2026, 1, 2, 13, 30, 0)),
        (3, "third", Decimal("0.01"), datetime(2026, 1, 3, 8, 15, 0)),
    ]
    result = copy_rows_to_postgres(
        pg_conn=pg_conn,
        table=f"public.{temp_table}",
        columns=["id", "description", "amount", "created_at"],
        rows=rows,
        pk_column_indexes=[0],
    )
    assert result.rows_written == 3
    assert result.last_pk == (3,)

    pg_conn.commit()
    with pg_conn.cursor() as cur:
        cur.execute(f"SELECT id, description, amount FROM public.{temp_table} ORDER BY id")
        fetched = cur.fetchall()
    assert fetched == [
        (1, "first", Decimal("10.50")),
        (2, "second", Decimal("99.99")),
        (3, "third", Decimal("0.01")),
    ]


def test_copy_records_composite_pk_for_keyset(pg_conn):
    """When the table's PK is composite, `last_pk` is the full tuple of
    the final row — that's what the keyset builder needs to resume."""
    name = f"copy_kset_{uuid.uuid4().hex[:8]}"
    with pg_conn.cursor() as cur:
        cur.execute(
            f"""
            CREATE TABLE public.{name} (
                order_id INTEGER,
                line_no  INTEGER,
                sku      TEXT,
                PRIMARY KEY (order_id, line_no)
            )
            """
        )
    pg_conn.commit()
    try:
        rows = [(7, 1, "A"), (7, 2, "B"), (8, 1, "C")]
        result = copy_rows_to_postgres(
            pg_conn=pg_conn,
            table=f"public.{name}",
            columns=["order_id", "line_no", "sku"],
            rows=rows,
            pk_column_indexes=[0, 1],
        )
        assert result.rows_written == 3
        assert result.last_pk == (8, 1)
    finally:
        with pg_conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS public.{name}")
        pg_conn.commit()


def test_copy_empty_iterable(pg_conn, temp_table):
    """Empty input is a valid no-op — runner uses this when a batch
    boundary lines up exactly with end-of-table."""
    result = copy_rows_to_postgres(
        pg_conn=pg_conn,
        table=f"public.{temp_table}",
        columns=["id", "description", "amount", "created_at"],
        rows=[],
        pk_column_indexes=[0],
    )
    assert result.rows_written == 0
    assert result.last_pk is None


def test_copy_unknown_column_raises(pg_conn, temp_table):
    """Introspection-vs-spec drift surfaces here — a column we ask to
    write that doesn't exist in pg_catalog throws LookupError instead of
    flailing at the COPY layer."""
    with pytest.raises(LookupError, match="not found in"):
        copy_rows_to_postgres(
            pg_conn=pg_conn,
            table=f"public.{temp_table}",
            columns=["id", "does_not_exist"],
            rows=[(1, "x")],
            pk_column_indexes=[0],
        )
