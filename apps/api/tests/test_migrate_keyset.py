"""Tests for the keyset-pagination query builder.

Pure-string assertions — no DB. We verify shape, identifier quoting,
parameter binding, and dialect-specific syntax (Oracle FETCH FIRST vs PG
LIMIT).
"""

from __future__ import annotations

import pytest

from src.migrate.keyset import Dialect, build_first_page, build_next_page


# ─── First page ──────────────────────────────────────────────────────────────


class TestFirstPage:
    def test_postgres_basic(self):
        q = build_first_page(
            dialect=Dialect.POSTGRES,
            table="employees",
            columns=["id", "name", "salary"],
            pk_columns=["id"],
            batch_size=1000,
        )
        assert q.sql == (
            'SELECT "id", "name", "salary" FROM "employees" '
            'ORDER BY "id" LIMIT 1000'
        )
        assert q.params == {"limit": 1000}

    def test_oracle_uses_fetch_first(self):
        q = build_first_page(
            dialect=Dialect.ORACLE,
            table="HR.EMPLOYEES",
            columns=["EMPLOYEE_ID", "FIRST_NAME"],
            pk_columns=["EMPLOYEE_ID"],
            batch_size=500,
        )
        assert "FETCH FIRST 500 ROWS ONLY" in q.sql
        assert 'FROM "HR"."EMPLOYEES"' in q.sql
        assert "LIMIT" not in q.sql

    def test_composite_pk_orders_in_declared_sequence(self):
        q = build_first_page(
            dialect=Dialect.POSTGRES,
            table="line_items",
            columns=["order_id", "line_no", "sku"],
            pk_columns=["order_id", "line_no"],
            batch_size=100,
        )
        assert 'ORDER BY "order_id", "line_no"' in q.sql


# ─── Next page (keyset) ──────────────────────────────────────────────────────


class TestNextPage:
    def test_single_pk_keyset(self):
        q = build_next_page(
            dialect=Dialect.POSTGRES,
            table="employees",
            columns=["id", "name"],
            pk_columns=["id"],
            last_pk=[42],
            batch_size=1000,
        )
        assert "WHERE (\"id\") > (:k0)" in q.sql
        assert q.params == {"k0": 42, "limit": 1000}

    def test_composite_pk_uses_row_value_comparison(self):
        q = build_next_page(
            dialect=Dialect.POSTGRES,
            table="line_items",
            columns=["order_id", "line_no", "sku"],
            pk_columns=["order_id", "line_no"],
            last_pk=[7, 3],
            batch_size=500,
        )
        # Row-value comparison — both PG and Oracle understand this.
        assert "WHERE (\"order_id\", \"line_no\") > (:k0, :k1)" in q.sql
        assert q.params["k0"] == 7
        assert q.params["k1"] == 3

    def test_oracle_fetch_first_on_next_page(self):
        q = build_next_page(
            dialect=Dialect.ORACLE,
            table="t",
            columns=["id"],
            pk_columns=["id"],
            last_pk=["abc"],
            batch_size=10,
        )
        assert "FETCH FIRST 10 ROWS ONLY" in q.sql

    def test_pk_arity_mismatch_raises(self):
        with pytest.raises(ValueError, match="last_pk has 1 values but pk_columns has 2"):
            build_next_page(
                dialect=Dialect.POSTGRES,
                table="t",
                columns=["a", "b"],
                pk_columns=["a", "b"],
                last_pk=[1],
                batch_size=10,
            )

    def test_null_in_single_pk_refused(self):
        # Without this guard, the WHERE clause would be `(pk) > (NULL)`,
        # which evaluates to NULL and silently halts the walk mid-table.
        with pytest.raises(ValueError, match="NULL primary-key value"):
            build_next_page(
                dialect=Dialect.POSTGRES,
                table="t",
                columns=["id"],
                pk_columns=["id"],
                last_pk=[None],
                batch_size=10,
            )

    def test_null_in_composite_pk_refused_with_column_name(self):
        with pytest.raises(ValueError) as exc:
            build_next_page(
                dialect=Dialect.POSTGRES,
                table="t",
                columns=["a", "b", "c"],
                pk_columns=["a", "b"],
                last_pk=[1, None],
                batch_size=10,
            )
        # Operator needs to know which column is the offender.
        assert "'b'" in str(exc.value) or "b" in str(exc.value)


# ─── Identifier quoting ──────────────────────────────────────────────────────


class TestQuoting:
    def test_reserved_words_quoted(self):
        # `order` and `select` are reserved; quoting protects them.
        q = build_first_page(
            dialect=Dialect.POSTGRES,
            table="order",
            columns=["select", "from"],
            pk_columns=["select"],
            batch_size=10,
        )
        assert '"select", "from"' in q.sql
        assert 'FROM "order"' in q.sql

    def test_embedded_quote_refused(self):
        with pytest.raises(ValueError, match="contains quote"):
            build_first_page(
                dialect=Dialect.POSTGRES,
                table='evil"name',
                columns=["a"],
                pk_columns=["a"],
                batch_size=10,
            )

    def test_schema_qualified_table(self):
        q = build_first_page(
            dialect=Dialect.POSTGRES,
            table="finance.gl_entries",
            columns=["id"],
            pk_columns=["id"],
            batch_size=10,
        )
        assert 'FROM "finance"."gl_entries"' in q.sql
