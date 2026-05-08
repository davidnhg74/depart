"""Sequence catch-up tests against the real Postgres dev DB."""

from __future__ import annotations

import uuid

import psycopg
import pytest

from src.config import settings
from src.migrate.sequences import (
    catch_up_all,
    catch_up_sequence,
    discover_owned_sequences,
)

pytestmark = pytest.mark.integration


@pytest.fixture
def pg_conn():
    url = settings.database_url.replace("postgresql+psycopg://", "postgresql://")
    conn = psycopg.connect(url, autocommit=False)
    yield conn
    conn.rollback()
    conn.close()


@pytest.fixture
def schema(pg_conn):
    """Throwaway schema so the discovery query has predictable scope and
    we don't fight with the app's real tables."""
    name = f"seq_test_{uuid.uuid4().hex[:8]}"
    with pg_conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA {name}")
    pg_conn.commit()
    yield name
    pg_conn.rollback()
    with pg_conn.cursor() as cur:
        cur.execute(f"DROP SCHEMA {name} CASCADE")
    pg_conn.commit()


def _create_serial_table(pg_conn, schema: str, name: str) -> None:
    with pg_conn.cursor() as cur:
        cur.execute(
            f"CREATE TABLE {schema}.{name} (id SERIAL PRIMARY KEY, label TEXT)"
        )
    pg_conn.commit()


# ─── discover_owned_sequences ────────────────────────────────────────────────


class TestDiscovery:
    def test_finds_serial_sequence(self, pg_conn, schema):
        _create_serial_table(pg_conn, schema, "items")
        links = discover_owned_sequences(pg_conn, schema=schema)
        assert len(links) == 1
        assert links[0].table == f"{schema}.items"
        assert links[0].column == "id"
        # Default `_seq` naming.
        assert links[0].sequence.endswith(".items_id_seq")

    def test_empty_schema_returns_empty_list(self, pg_conn, schema):
        assert discover_owned_sequences(pg_conn, schema=schema) == []

    def test_unowned_sequence_skipped(self, pg_conn, schema):
        with pg_conn.cursor() as cur:
            cur.execute(f"CREATE SEQUENCE {schema}.orphan_seq")
        pg_conn.commit()
        # Free-standing sequence — no owner — must not appear.
        assert discover_owned_sequences(pg_conn, schema=schema) == []


# ─── catch_up_sequence ───────────────────────────────────────────────────────


class TestCatchUp:
    def test_advances_to_max_plus_one(self, pg_conn, schema):
        _create_serial_table(pg_conn, schema, "items")
        with pg_conn.cursor() as cur:
            cur.execute(
                f"INSERT INTO {schema}.items (id, label) VALUES (1000, 'a'), (1001, 'b')"
            )
        pg_conn.commit()

        link = discover_owned_sequences(pg_conn, schema=schema)[0]
        result = catch_up_sequence(pg_conn, link)
        pg_conn.commit()

        assert result.set_to == 1001
        # nextval should return 1002 — the next id we'd assign.
        with pg_conn.cursor() as cur:
            cur.execute(f"SELECT nextval('{link.sequence}')")
            (next_id,) = cur.fetchone()
        assert next_id == 1002

    def test_empty_table_skipped(self, pg_conn, schema):
        _create_serial_table(pg_conn, schema, "items")
        link = discover_owned_sequences(pg_conn, schema=schema)[0]
        result = catch_up_sequence(pg_conn, link)
        pg_conn.commit()

        assert result.set_to is None
        assert result.skipped_reason == "empty table"
        # Sequence is still at its initial state — first nextval = 1.
        with pg_conn.cursor() as cur:
            cur.execute(f"SELECT nextval('{link.sequence}')")
            (next_id,) = cur.fetchone()
        assert next_id == 1


# ─── catch_up_all ────────────────────────────────────────────────────────────


class TestCatchUpAll:
    def test_handles_multiple_tables(self, pg_conn, schema):
        for name in ("alpha", "beta", "gamma"):
            _create_serial_table(pg_conn, schema, name)
            with pg_conn.cursor() as cur:
                cur.execute(
                    f"INSERT INTO {schema}.{name} (id, label) VALUES (5, 'x')"
                )
        pg_conn.commit()

        results = catch_up_all(pg_conn, schema=schema)
        pg_conn.commit()

        assert {r.link.table for r in results} == {
            f"{schema}.alpha",
            f"{schema}.beta",
            f"{schema}.gamma",
        }
        assert all(r.set_to == 5 for r in results)
