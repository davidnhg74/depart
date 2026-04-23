"""Unit tests for the DDL generator.

Pure-function tests for the type mapper + `CREATE TABLE` emitter.
These don't need a database.
"""

from __future__ import annotations

import pytest

from src.migrate.ddl import (
    ColumnMeta,
    generate_create_table,
    generate_schema_ddl,
    map_oracle_type,
    map_pg_type,
)
from src.migrate.planner import TableRef


# ─── Oracle → PG type mapping ────────────────────────────────────────────────


class TestOracleTypeMapping:
    def test_number_no_precision_is_numeric(self):
        assert map_oracle_type(ColumnMeta("c", "NUMBER")) == "NUMERIC"

    def test_number_scale_zero_fits_smallint(self):
        assert map_oracle_type(ColumnMeta("c", "NUMBER", precision=3, scale=0)) == "SMALLINT"

    def test_number_scale_zero_fits_integer(self):
        assert map_oracle_type(ColumnMeta("c", "NUMBER", precision=9, scale=0)) == "INTEGER"

    def test_number_scale_zero_fits_bigint(self):
        assert map_oracle_type(ColumnMeta("c", "NUMBER", precision=18, scale=0)) == "BIGINT"

    def test_number_scale_zero_overflows_to_numeric(self):
        assert map_oracle_type(ColumnMeta("c", "NUMBER", precision=30, scale=0)) == "NUMERIC(30)"

    def test_number_with_scale(self):
        assert (
            map_oracle_type(ColumnMeta("c", "NUMBER", precision=12, scale=2))
            == "NUMERIC(12, 2)"
        )

    def test_varchar2_uses_length(self):
        assert map_oracle_type(ColumnMeta("c", "VARCHAR2", length=100)) == "VARCHAR(100)"

    def test_varchar2_without_length_becomes_text(self):
        assert map_oracle_type(ColumnMeta("c", "VARCHAR2")) == "TEXT"

    def test_clob_is_text(self):
        assert map_oracle_type(ColumnMeta("c", "CLOB")) == "TEXT"

    def test_blob_is_bytea(self):
        assert map_oracle_type(ColumnMeta("c", "BLOB")) == "BYTEA"

    def test_date_is_timestamp(self):
        # Oracle DATE carries a time-of-day; plain PG DATE would lose it.
        assert map_oracle_type(ColumnMeta("c", "DATE")) == "TIMESTAMP"

    def test_timestamp_plain(self):
        assert map_oracle_type(ColumnMeta("c", "TIMESTAMP(6)")) == "TIMESTAMP"

    def test_timestamp_with_time_zone(self):
        assert map_oracle_type(ColumnMeta("c", "TIMESTAMP(6) WITH TIME ZONE")) == "TIMESTAMPTZ"

    def test_timestamp_with_local_time_zone_maps_to_plain(self):
        # LOCAL TIME ZONE doesn't have a PG equivalent; plain TIMESTAMP
        # is the closest honest mapping.
        assert (
            map_oracle_type(ColumnMeta("c", "TIMESTAMP(6) WITH LOCAL TIME ZONE"))
            == "TIMESTAMP"
        )

    def test_binary_float_and_double(self):
        assert map_oracle_type(ColumnMeta("c", "BINARY_FLOAT")) == "REAL"
        assert map_oracle_type(ColumnMeta("c", "BINARY_DOUBLE")) == "DOUBLE PRECISION"

    def test_unknown_type_raises(self):
        with pytest.raises(ValueError, match="no Postgres mapping"):
            map_oracle_type(ColumnMeta("c", "SDO_TOPO_GEOMETRY"))

    def test_bfile_maps_to_text_with_warning(self, caplog):
        # BFILE used to raise ValueError mid-migration. Now it lands as
        # TEXT (the locator string) and surfaces a warning instead, so
        # the migration finishes and the operator sees the issue in the
        # log instead of a stack trace.
        import logging

        with caplog.at_level(logging.WARNING, logger="src.migrate.ddl"):
            assert map_oracle_type(ColumnMeta("doc_path", "BFILE")) == "TEXT"
        assert any(
            "BFILE" in rec.message and "doc_path" in rec.message
            for rec in caplog.records
        )


# ─── Postgres → Postgres (identity load) ─────────────────────────────────────


class TestPgTypeMapping:
    def test_varchar_with_length(self):
        assert map_pg_type(ColumnMeta("c", "character varying", length=50)) == "VARCHAR(50)"

    def test_varchar_without_length_becomes_text(self):
        assert map_pg_type(ColumnMeta("c", "character varying")) == "TEXT"

    def test_numeric_with_precision_scale(self):
        assert map_pg_type(ColumnMeta("c", "numeric", precision=10, scale=2)) == "NUMERIC(10, 2)"

    def test_numeric_precision_only(self):
        assert map_pg_type(ColumnMeta("c", "numeric", precision=10, scale=0)) == "NUMERIC(10)"

    def test_integer_passthrough(self):
        assert map_pg_type(ColumnMeta("c", "integer")) == "INTEGER"

    def test_bigint_passthrough(self):
        assert map_pg_type(ColumnMeta("c", "bigint")) == "BIGINT"


# ─── generate_create_table ──────────────────────────────────────────────────


class TestCreateTable:
    def test_simple_table(self):
        ddl = generate_create_table(
            TableRef(schema="public", name="items"),
            [
                ColumnMeta("id", "NUMBER", nullable=False, precision=9, scale=0),
                ColumnMeta("label", "VARCHAR2", nullable=True, length=100),
            ],
            pk_columns=["id"],
        )
        assert 'CREATE TABLE IF NOT EXISTS "public"."items"' in ddl
        assert '"id" INTEGER NOT NULL' in ddl
        assert '"label" VARCHAR(100)' in ddl
        assert 'PRIMARY KEY ("id")' in ddl

    def test_composite_pk(self):
        ddl = generate_create_table(
            TableRef(schema="public", name="line_items"),
            [
                ColumnMeta("order_id", "NUMBER", nullable=False, precision=9, scale=0),
                ColumnMeta("line_no", "NUMBER", nullable=False, precision=4, scale=0),
                ColumnMeta("sku", "VARCHAR2", length=50),
            ],
            pk_columns=["order_id", "line_no"],
        )
        assert 'PRIMARY KEY ("order_id", "line_no")' in ddl

    def test_no_pk_emits_no_pk_clause(self):
        ddl = generate_create_table(
            TableRef(schema="public", name="t"),
            [ColumnMeta("c", "NUMBER")],
            pk_columns=[],
        )
        assert "PRIMARY KEY" not in ddl

    def test_pg_mapper_used_when_passed(self):
        ddl = generate_create_table(
            TableRef(schema="public", name="t"),
            [ColumnMeta("c", "character varying", length=50)],
            pk_columns=[],
            map_type=map_pg_type,
        )
        assert "VARCHAR(50)" in ddl

    def test_empty_columns_raises(self):
        with pytest.raises(ValueError, match="no columns"):
            generate_create_table(TableRef(schema="public", name="t"), [], pk_columns=[])


class TestGenerateSchemaDDL:
    def test_ordered_per_table_statements(self):
        t1 = TableRef(schema="public", name="parents")
        t2 = TableRef(schema="public", name="children")
        cols = {
            t1.qualified(): [ColumnMeta("id", "NUMBER", nullable=False, precision=9, scale=0)],
            t2.qualified(): [
                ColumnMeta("id", "NUMBER", nullable=False, precision=9, scale=0),
                ColumnMeta("parent_id", "NUMBER", precision=9, scale=0),
            ],
        }
        pks = {t1.qualified(): ["id"], t2.qualified(): ["id"]}
        stmts = generate_schema_ddl([t1, t2], cols, pks)
        assert len(stmts) == 2
        assert '"parents"' in stmts[0]
        assert '"children"' in stmts[1]
