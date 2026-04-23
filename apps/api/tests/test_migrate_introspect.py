"""Introspection tests against the live Postgres dev DB.

The Oracle introspector ships SQL strings only — no live test until
the Oracle container is up (`docker compose --profile oracle up`).
We exercise the Postgres path here, which mirrors the same shape and
catches typos in either dialect's queries since they share a code path.
"""

from __future__ import annotations

import uuid

import psycopg
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.config import settings
from src.migrate.introspect import introspect
from src.migrate.keyset import Dialect
from src.migrate.planner import TableRef, plan_load_order


@pytest.fixture
def pg_url():
    return settings.database_url.replace("postgresql+psycopg://", "postgresql://")


@pytest.fixture
def schema(pg_url):
    name = f"introspect_{uuid.uuid4().hex[:6]}"
    conn = psycopg.connect(pg_url, autocommit=True)
    with conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA {name}")
    conn.close()
    yield name
    conn = psycopg.connect(pg_url, autocommit=True)
    with conn.cursor() as cur:
        cur.execute(f"DROP SCHEMA {name} CASCADE")
    conn.close()


@pytest.fixture
def session():
    engine = create_engine(settings.database_url)
    Session = sessionmaker(bind=engine)
    s = Session()
    yield s
    s.close()


def _ddl(pg_url, statements: list[str]) -> None:
    conn = psycopg.connect(pg_url, autocommit=True)
    with conn.cursor() as cur:
        for stmt in statements:
            cur.execute(stmt)
    conn.close()


# ─── Empty schema baseline ───────────────────────────────────────────────────


def test_empty_schema(session, schema):
    result = introspect(session, Dialect.POSTGRES, schema)
    assert result.tables == []
    assert result.foreign_keys == []
    assert result.build_specs() == {}


# ─── Tables, columns, primary keys ───────────────────────────────────────────


def test_lists_tables_in_alphabetical_order(session, schema, pg_url):
    _ddl(
        pg_url,
        [
            f"CREATE TABLE {schema}.zeta (id INTEGER PRIMARY KEY)",
            f"CREATE TABLE {schema}.alpha (id INTEGER PRIMARY KEY)",
            f"CREATE TABLE {schema}.mu (id INTEGER PRIMARY KEY)",
        ],
    )
    result = introspect(session, Dialect.POSTGRES, schema)
    assert [t.name for t in result.tables] == ["alpha", "mu", "zeta"]


def test_columns_in_ordinal_order(session, schema, pg_url):
    _ddl(
        pg_url,
        [
            f"""CREATE TABLE {schema}.t (
                id INTEGER PRIMARY KEY,
                name TEXT,
                email TEXT,
                created_at TIMESTAMP
            )""",
        ],
    )
    result = introspect(session, Dialect.POSTGRES, schema)
    qn = f"{schema}.t"
    assert result.columns[qn] == ["id", "name", "email", "created_at"]


def test_composite_primary_key_preserves_declared_order(session, schema, pg_url):
    _ddl(
        pg_url,
        [
            f"""CREATE TABLE {schema}.line_items (
                order_id INTEGER,
                line_no INTEGER,
                sku TEXT,
                PRIMARY KEY (order_id, line_no)
            )""",
        ],
    )
    result = introspect(session, Dialect.POSTGRES, schema)
    assert result.primary_keys[f"{schema}.line_items"] == ["order_id", "line_no"]


def test_table_without_pk_excluded_from_specs(session, schema, pg_url):
    """No PK = no keyset cursor = can't migrate. Better to surface the
    omission via build_specs than to emit a degenerate spec the runner
    will reject anyway."""
    _ddl(
        pg_url,
        [
            f"CREATE TABLE {schema}.with_pk (id INTEGER PRIMARY KEY)",
            f"CREATE TABLE {schema}.no_pk (val TEXT)",
        ],
    )
    result = introspect(session, Dialect.POSTGRES, schema)
    specs = result.build_specs()
    qn_with_pk = TableRef(schema=schema, name="with_pk").qualified()
    qn_no_pk = TableRef(schema=schema, name="no_pk").qualified()
    assert qn_with_pk in specs
    assert qn_no_pk not in specs


# ─── Foreign keys feed the planner ───────────────────────────────────────────


def test_foreign_keys_drive_load_order(session, schema, pg_url):
    _ddl(
        pg_url,
        [
            f"CREATE TABLE {schema}.customers (id INTEGER PRIMARY KEY, name TEXT)",
            f"""CREATE TABLE {schema}.orders (
                id INTEGER PRIMARY KEY,
                customer_id INTEGER REFERENCES {schema}.customers(id)
            )""",
        ],
    )
    result = introspect(session, Dialect.POSTGRES, schema)
    plan = plan_load_order(result.tables, result.foreign_keys)
    flat = [t.name for g in plan.groups for t in g.tables]
    # customers must precede orders.
    assert flat.index("customers") < flat.index("orders")


def test_cycle_emits_deferred_constraints(session, schema, pg_url):
    """Two-table FK cycle — planner returns one cyclic group whose
    `deferred_constraints` lists both edges, just like the runner needs."""
    _ddl(
        pg_url,
        [
            f"CREATE TABLE {schema}.a (id INTEGER PRIMARY KEY, b_id INTEGER)",
            f"CREATE TABLE {schema}.b (id INTEGER PRIMARY KEY, a_id INTEGER)",
            f"ALTER TABLE {schema}.a ADD CONSTRAINT a_b_fk "
            f"FOREIGN KEY (b_id) REFERENCES {schema}.b(id) DEFERRABLE INITIALLY DEFERRED",
            f"ALTER TABLE {schema}.b ADD CONSTRAINT b_a_fk "
            f"FOREIGN KEY (a_id) REFERENCES {schema}.a(id) DEFERRABLE INITIALLY DEFERRED",
        ],
    )
    result = introspect(session, Dialect.POSTGRES, schema)
    plan = plan_load_order(result.tables, result.foreign_keys)
    assert len(plan.groups) == 1
    group = plan.groups[0]
    assert {t.name for t in group.tables} == {"a", "b"}
    assert {fk.name for fk in group.deferred_constraints} == {"a_b_fk", "b_a_fk"}


def test_self_referential_fk_does_not_block(session, schema, pg_url):
    _ddl(
        pg_url,
        [
            f"""CREATE TABLE {schema}.employees (
                id INTEGER PRIMARY KEY,
                manager_id INTEGER REFERENCES {schema}.employees(id)
            )""",
        ],
    )
    result = introspect(session, Dialect.POSTGRES, schema)
    # The FK is present but self-referential.
    assert len(result.foreign_keys) == 1
    plan = plan_load_order(result.tables, result.foreign_keys)
    # Single table, single group, no deferred constraints (planner
    # treats self-FKs as a runner concern, not a cycle).
    assert [t.name for g in plan.groups for t in g.tables] == ["employees"]
    assert plan.groups[0].deferred_constraints == []


# ─── build_specs target_schema rewrite ───────────────────────────────────────


def test_build_specs_rewrites_target_schema(session, schema, pg_url):
    """For Oracle HR -> Postgres public migrations the destination
    schema differs from the source. build_specs swaps it cleanly."""
    _ddl(pg_url, [f"CREATE TABLE {schema}.items (id INTEGER PRIMARY KEY, label TEXT)"])
    result = introspect(session, Dialect.POSTGRES, schema)
    specs = result.build_specs(target_schema="public")
    qn = "public.items"
    assert qn in specs
    spec = specs[qn]
    assert spec.source_table.schema == schema
    assert spec.target_table.schema == "public"
    assert spec.columns == ["id", "label"]
    assert spec.pk_columns == ["id"]


# ─── Nullable-PK detection ───────────────────────────────────────────────────
#
# Pure unit tests — hand-construct an IntrospectedSchema so we can
# represent the rare "PK column is nullable" state without actually
# creating one in Postgres (which won't let us).


from src.migrate.ddl import ColumnMeta
from src.migrate.introspect import IntrospectedSchema


def _schema_with(table_name: str, cols: list[ColumnMeta], pk: list[str]) -> IntrospectedSchema:
    qn = f"public.{table_name}"
    return IntrospectedSchema(
        dialect=Dialect.POSTGRES,
        schema="public",
        tables=[TableRef(schema="public", name=table_name)],
        columns={qn: [c.name for c in cols]},
        primary_keys={qn: pk},
        foreign_keys=[],
        column_metadata={qn: cols},
    )


def test_nullable_pk_columns_empty_for_safe_schema():
    s = _schema_with(
        "ok",
        [ColumnMeta(name="id", data_type="INTEGER", nullable=False)],
        pk=["id"],
    )
    assert s.nullable_pk_columns() == {}


def test_nullable_pk_columns_flags_offender():
    # Composite "PK" where the second column is nullable — the case
    # that breaks keyset paging.
    s = _schema_with(
        "weak",
        [
            ColumnMeta(name="a", data_type="INTEGER", nullable=False),
            ColumnMeta(name="b", data_type="INTEGER", nullable=True),
            ColumnMeta(name="c", data_type="TEXT", nullable=True),
        ],
        pk=["a", "b"],
    )
    assert s.nullable_pk_columns() == {"public.weak": ["b"]}


def test_nullable_pk_columns_skips_tables_without_metadata():
    # If a hand-built schema didn't populate column_metadata for a
    # table, we don't have nullability info — treat as unknown rather
    # than crash. The runtime guard in keyset.build_next_page is the
    # backstop.
    s = IntrospectedSchema(
        dialect=Dialect.POSTGRES,
        schema="public",
        tables=[TableRef(schema="public", name="bare")],
        columns={"public.bare": ["id"]},
        primary_keys={"public.bare": ["id"]},
        foreign_keys=[],
        column_metadata={},
    )
    assert s.nullable_pk_columns() == {}
