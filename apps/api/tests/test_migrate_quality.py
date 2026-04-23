"""Tests for the Layer 3 quality validator.

Two helpers under test:

  * scan_varchar_lengths  — pre-copy: warns when a VARCHAR column's
    actual content is at or over its declared length, the case
    that breaks COPY when Oracle byte-semantics meets PG char-semantics
  * compare_basic_stats   — post-copy: surfaces row-count, NULL-count,
    and MIN/MAX divergence between source and target

Uses two schemas in the same Postgres for the test rig — same shape
as test_migrate_runner.py.
"""

from __future__ import annotations

import uuid

import psycopg
import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from src.config import settings
from src.migrate.ddl import ColumnMeta
from src.migrate.keyset import Dialect
from src.migrate.quality import (
    QualityFinding,
    compare_basic_stats,
    scan_varchar_lengths,
)


# ─── Test rig ────────────────────────────────────────────────────────────────


@pytest.fixture
def pg_url():
    return settings.database_url.replace("postgresql+psycopg://", "postgresql://")


@pytest.fixture
def schemas(pg_url):
    src = f"qual_src_{uuid.uuid4().hex[:6]}"
    dst = f"qual_dst_{uuid.uuid4().hex[:6]}"
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


@pytest.fixture
def sessions():
    engine = create_engine(settings.database_url)
    Session = sessionmaker(bind=engine)
    src = Session()
    dst = Session()
    yield src, dst
    src.close()
    dst.close()


def _exec(session, sql: str) -> None:
    session.execute(text(sql))
    session.commit()


# ─── scan_varchar_lengths ────────────────────────────────────────────────────


class TestScanVarcharLengths:
    def test_returns_no_findings_for_well_within_limit(self, schemas, sessions):
        src, _ = schemas
        src_session, _ = sessions
        _exec(src_session, f'CREATE TABLE {src}.t (id INT, name VARCHAR(50))')
        _exec(src_session, f"INSERT INTO {src}.t VALUES (1, 'short'), (2, 'also short')")

        cols = [
            ColumnMeta(name="id", data_type="INTEGER"),
            ColumnMeta(name="name", data_type="VARCHAR", length=50),
        ]
        findings = scan_varchar_lengths(
            src_session, Dialect.POSTGRES, f"{src}.t", cols
        )
        assert findings == []

    def test_warns_when_within_10pct_of_limit(self, schemas, sessions):
        src, _ = schemas
        src_session, _ = sessions
        _exec(src_session, f'CREATE TABLE {src}.t (id INT, name VARCHAR(20))')
        # 18 chars / 20 declared = 90% — at the warning threshold.
        _exec(src_session, f"INSERT INTO {src}.t VALUES (1, '{'x' * 18}')")

        cols = [ColumnMeta(name="name", data_type="VARCHAR", length=20)]
        findings = scan_varchar_lengths(
            src_session, Dialect.POSTGRES, f"{src}.t", cols
        )
        assert len(findings) == 1
        f = findings[0]
        assert f.severity == "warning"
        assert f.check == "varchar_near_limit"
        assert f.column == "name"
        assert "18" in f.message and "20" in f.message

    def test_errors_when_over_limit(self, schemas, sessions):
        # PG enforces VARCHAR(n), so to simulate "Oracle byte-semantics
        # column held content that won't fit a same-width target", we
        # create the source as a wider column and pass a narrower
        # ColumnMeta — that's the case the operator hits when target
        # DDL was written assuming bytes.
        src, _ = schemas
        src_session, _ = sessions
        _exec(src_session, f'CREATE TABLE {src}.t (id INT, name VARCHAR(100))')
        _exec(src_session, f"INSERT INTO {src}.t VALUES (1, '{'x' * 60}')")

        cols = [ColumnMeta(name="name", data_type="VARCHAR", length=20)]
        findings = scan_varchar_lengths(
            src_session, Dialect.POSTGRES, f"{src}.t", cols
        )
        assert len(findings) == 1
        f = findings[0]
        assert f.severity == "error"
        assert f.check == "varchar_overflow"
        assert "60" in f.message and "20" in f.message

    def test_skips_non_varchar_columns(self, schemas, sessions):
        src, _ = schemas
        src_session, _ = sessions
        _exec(src_session, f'CREATE TABLE {src}.t (id INT, big TEXT, n NUMERIC(10,2))')
        _exec(src_session, f"INSERT INTO {src}.t VALUES (1, 'anything', 99.99)")

        cols = [
            ColumnMeta(name="id", data_type="INTEGER"),
            ColumnMeta(name="big", data_type="TEXT"),
            ColumnMeta(name="n", data_type="NUMERIC", precision=10, scale=2),
        ]
        findings = scan_varchar_lengths(
            src_session, Dialect.POSTGRES, f"{src}.t", cols
        )
        assert findings == []

    def test_skips_when_all_null(self, schemas, sessions):
        src, _ = schemas
        src_session, _ = sessions
        _exec(src_session, f'CREATE TABLE {src}.t (id INT, name VARCHAR(20))')
        _exec(src_session, f"INSERT INTO {src}.t (id) VALUES (1), (2)")

        cols = [ColumnMeta(name="name", data_type="VARCHAR", length=20)]
        findings = scan_varchar_lengths(
            src_session, Dialect.POSTGRES, f"{src}.t", cols
        )
        # All NULLs → no length to evaluate → silent no-op.
        assert findings == []


# ─── compare_basic_stats ─────────────────────────────────────────────────────


class TestCompareBasicStats:
    def test_identical_tables_emit_no_findings(self, schemas, sessions):
        src, dst = schemas
        src_session, dst_session = sessions
        ddl = "(id INT, qty INT, label VARCHAR(20))"
        _exec(src_session, f"CREATE TABLE {src}.t {ddl}")
        _exec(dst_session, f"CREATE TABLE {dst}.t {ddl}")
        rows = "VALUES (1, 10, 'a'), (2, 20, 'b'), (3, 30, NULL)"
        _exec(src_session, f"INSERT INTO {src}.t {rows}")
        _exec(dst_session, f"INSERT INTO {dst}.t {rows}")

        cols = [
            ColumnMeta(name="id", data_type="INTEGER"),
            ColumnMeta(name="qty", data_type="INTEGER"),
            ColumnMeta(name="label", data_type="VARCHAR", length=20),
        ]
        findings = compare_basic_stats(
            src_session, Dialect.POSTGRES, f"{src}.t",
            dst_session, f"{dst}.t",
            cols,
        )
        assert findings == []

    def test_row_count_mismatch_is_error(self, schemas, sessions):
        src, dst = schemas
        src_session, dst_session = sessions
        _exec(src_session, f"CREATE TABLE {src}.t (id INT)")
        _exec(dst_session, f"CREATE TABLE {dst}.t (id INT)")
        _exec(src_session, f"INSERT INTO {src}.t VALUES (1), (2), (3)")
        _exec(dst_session, f"INSERT INTO {dst}.t VALUES (1), (2)")

        cols = [ColumnMeta(name="id", data_type="INTEGER")]
        findings = compare_basic_stats(
            src_session, Dialect.POSTGRES, f"{src}.t",
            dst_session, f"{dst}.t",
            cols,
        )
        # row_count error + min/max OK (both sides are subsets of the same range)
        # plus null_count OK. Just check the row_count error is present.
        row_count_findings = [f for f in findings if f.check == "row_count"]
        assert len(row_count_findings) == 1
        assert row_count_findings[0].severity == "error"
        assert "source=3" in row_count_findings[0].message
        assert "target=2" in row_count_findings[0].message

    def test_null_count_mismatch_is_error(self, schemas, sessions):
        src, dst = schemas
        src_session, dst_session = sessions
        _exec(src_session, f"CREATE TABLE {src}.t (id INT, label TEXT)")
        _exec(dst_session, f"CREATE TABLE {dst}.t (id INT, label TEXT)")
        _exec(src_session, f"INSERT INTO {src}.t VALUES (1, 'a'), (2, NULL), (3, NULL)")
        # Target lost a NULL — either masked or accidentally defaulted.
        _exec(dst_session, f"INSERT INTO {dst}.t VALUES (1, 'a'), (2, ''), (3, NULL)")

        cols = [
            ColumnMeta(name="id", data_type="INTEGER"),
            ColumnMeta(name="label", data_type="TEXT"),
        ]
        findings = compare_basic_stats(
            src_session, Dialect.POSTGRES, f"{src}.t",
            dst_session, f"{dst}.t",
            cols,
        )
        null_findings = [f for f in findings if f.check == "null_count"]
        assert len(null_findings) == 1
        assert null_findings[0].column == "label"
        assert null_findings[0].severity == "error"

    def test_min_max_mismatch_for_numeric_column(self, schemas, sessions):
        src, dst = schemas
        src_session, dst_session = sessions
        _exec(src_session, f"CREATE TABLE {src}.t (n INT)")
        _exec(dst_session, f"CREATE TABLE {dst}.t (n INT)")
        _exec(src_session, f"INSERT INTO {src}.t VALUES (1), (5), (10)")
        _exec(dst_session, f"INSERT INTO {dst}.t VALUES (1), (5), (99)")  # max diverged

        cols = [ColumnMeta(name="n", data_type="INTEGER")]
        findings = compare_basic_stats(
            src_session, Dialect.POSTGRES, f"{src}.t",
            dst_session, f"{dst}.t",
            cols,
        )
        max_findings = [f for f in findings if f.check == "max_value"]
        assert len(max_findings) == 1
        assert "10" in max_findings[0].message
        assert "99" in max_findings[0].message

    def test_skips_min_max_for_text_columns(self, schemas, sessions):
        src, dst = schemas
        src_session, dst_session = sessions
        _exec(src_session, f"CREATE TABLE {src}.t (label TEXT)")
        _exec(dst_session, f"CREATE TABLE {dst}.t (label TEXT)")
        _exec(src_session, f"INSERT INTO {src}.t VALUES ('a'), ('z')")
        _exec(dst_session, f"INSERT INTO {dst}.t VALUES ('b'), ('y')")  # different range

        cols = [ColumnMeta(name="label", data_type="TEXT")]
        findings = compare_basic_stats(
            src_session, Dialect.POSTGRES, f"{src}.t",
            dst_session, f"{dst}.t",
            cols,
        )
        # No min/max check on TEXT — different ranges shouldn't fire.
        assert all(f.check not in ("min_value", "max_value") for f in findings)
