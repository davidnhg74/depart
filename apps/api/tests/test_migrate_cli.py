"""End-to-end CLI test: Postgres source → Postgres target through the CLI.

The CLI is a thin shell over the library code; this test exercises the
wiring (arg parsing, introspection → plan → runner → result reporting)
without hitting Oracle. The Oracle path is identical except for the
introspection queries, which have their own dialect-specific tests.
"""

from __future__ import annotations

import sys
import uuid

import psycopg
import pytest

from src.config import settings
from src.migrate.__main__ import main
pytestmark = pytest.mark.integration


@pytest.fixture
def pg_url():
    return settings.database_url.replace("postgresql+psycopg://", "postgresql://")


@pytest.fixture
def schemas(pg_url):
    src = f"cli_src_{uuid.uuid4().hex[:6]}"
    dst = f"cli_dst_{uuid.uuid4().hex[:6]}"
    conn = psycopg.connect(pg_url, autocommit=True)
    with conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA {src}")
        cur.execute(f"CREATE SCHEMA {dst}")
    conn.close()
    yield src, dst
    conn = psycopg.connect(pg_url, autocommit=True)
    with conn.cursor() as cur:
        cur.execute(f"DROP SCHEMA {src} CASCADE")
        cur.execute(f"DROP SCHEMA {dst} CASCADE")
    conn.close()


def test_cli_runs_full_pipeline(schemas, pg_url, capsys):
    src_schema, dst_schema = schemas
    conn = psycopg.connect(pg_url, autocommit=True)
    with conn.cursor() as cur:
        # Source data: customers + orders (FK), so the planner has work.
        cur.execute(
            f"CREATE TABLE {src_schema}.customers (id INTEGER PRIMARY KEY, name TEXT)"
        )
        cur.execute(
            f"""CREATE TABLE {src_schema}.orders (
                id INTEGER PRIMARY KEY,
                customer_id INTEGER REFERENCES {src_schema}.customers(id),
                amount INTEGER
            )"""
        )
        cur.executemany(
            f"INSERT INTO {src_schema}.customers VALUES (%s, %s)",
            [(i, f"cust-{i}") for i in range(1, 6)],
        )
        cur.executemany(
            f"INSERT INTO {src_schema}.orders VALUES (%s, %s, %s)",
            [(i, ((i - 1) % 5) + 1, i * 100) for i in range(1, 21)],
        )
        # Empty target schema with the same shape — the CLI doesn't
        # do schema migration, just data movement, so the DDL must
        # already exist on the target.
        cur.execute(
            f"CREATE TABLE {dst_schema}.customers (id INTEGER PRIMARY KEY, name TEXT)"
        )
        cur.execute(
            f"""CREATE TABLE {dst_schema}.orders (
                id INTEGER PRIMARY KEY,
                customer_id INTEGER REFERENCES {dst_schema}.customers(id) DEFERRABLE INITIALLY DEFERRED,
                amount INTEGER
            )"""
        )
    conn.close()

    rc = main(
        [
            "--source", settings.database_url,
            "--target", settings.database_url,
            "--source-schema", src_schema,
            "--target-schema", dst_schema,
            "--batch-size", "10",
        ]
    )
    out = capsys.readouterr()
    assert rc == 0, f"CLI exit code {rc}\nstdout: {out.out}\nstderr: {out.err}"

    # The summary table mentions both tables and reports verification.
    assert f"{dst_schema}.customers" in out.out
    assert f"{dst_schema}.orders" in out.out
    assert "all verified: True" in out.out

    # Row counts in the destination match the source.
    conn = psycopg.connect(pg_url, autocommit=True)
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {dst_schema}.customers")
        assert cur.fetchone()[0] == 5
        cur.execute(f"SELECT COUNT(*) FROM {dst_schema}.orders")
        assert cur.fetchone()[0] == 20
    conn.close()


def test_cli_filters_by_table_whitelist(schemas, pg_url, capsys):
    """`--tables` restricts the migration to a named subset; the
    excluded tables stay empty on the target."""
    src_schema, dst_schema = schemas
    conn = psycopg.connect(pg_url, autocommit=True)
    with conn.cursor() as cur:
        cur.execute(f"CREATE TABLE {src_schema}.keep (id INTEGER PRIMARY KEY)")
        cur.execute(f"CREATE TABLE {src_schema}.skip (id INTEGER PRIMARY KEY)")
        cur.execute(f"INSERT INTO {src_schema}.keep VALUES (1), (2)")
        cur.execute(f"INSERT INTO {src_schema}.skip VALUES (1), (2)")
        cur.execute(f"CREATE TABLE {dst_schema}.keep (id INTEGER PRIMARY KEY)")
        cur.execute(f"CREATE TABLE {dst_schema}.skip (id INTEGER PRIMARY KEY)")
    conn.close()

    rc = main(
        [
            "--source", settings.database_url,
            "--target", settings.database_url,
            "--source-schema", src_schema,
            "--target-schema", dst_schema,
            "--tables", "keep",
        ]
    )
    out = capsys.readouterr()
    assert rc == 0, f"stdout: {out.out}\nstderr: {out.err}"

    conn = psycopg.connect(pg_url, autocommit=True)
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {dst_schema}.keep")
        assert cur.fetchone()[0] == 2
        cur.execute(f"SELECT COUNT(*) FROM {dst_schema}.skip")
        assert cur.fetchone()[0] == 0
    conn.close()


def test_cli_reports_no_migratable_tables(schemas, capsys):
    """An empty source schema (or one without PKs) returns exit code 1
    with a clear message instead of crashing."""
    src_schema, dst_schema = schemas
    rc = main(
        [
            "--source", settings.database_url,
            "--target", settings.database_url,
            "--source-schema", src_schema,
            "--target-schema", dst_schema,
        ]
    )
    out = capsys.readouterr()
    assert rc == 1
    assert "no migratable tables" in out.err
