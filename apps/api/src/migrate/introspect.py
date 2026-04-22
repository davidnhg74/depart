"""Schema introspection: turn a live database into runner inputs.

The runner's `Runner.execute()` needs:

  • A `LoadPlan` describing per-table dependency order.
  • A `dict[qualified_name → TableSpec]` describing each table's
    columns and primary key.

Both are built from the catalog. Oracle and Postgres expose this through
different views (`ALL_*` vs `pg_catalog`/`information_schema`), so this
module ships two introspectors behind a small interface. Production
wires the Oracle one to source-side introspection and the Postgres one
to target-side introspection (so we can produce the FK graph from the
side that already declares them).

All queries are read-only and use schema-qualified parameter binds —
no identifier interpolation, so user-supplied schema names are safe.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Sequence, Tuple

from sqlalchemy import text

from .ddl import ColumnMeta
from .keyset import Dialect
from .planner import ForeignKey, TableRef
from .runner import TableSpec


# ─── Public types ────────────────────────────────────────────────────────────


@dataclass
class IntrospectedSchema:
    """Everything the planner + runner need to build a plan against
    `schema`. Tables are listed in catalog order; the planner re-orders
    them by FK dependencies."""

    dialect: Dialect
    schema: str
    tables: List[TableRef]
    columns: dict  # qualified_name -> List[str], in ordinal order
    primary_keys: dict  # qualified_name -> List[str], in PK position order
    foreign_keys: List[ForeignKey]
    # Optional: full column metadata (types, nullability, sizes) keyed on
    # qualified name. Populated by the introspectors but left empty by
    # hand-constructed test fixtures. DDL generation needs this; the
    # data-movement runner does not.
    column_metadata: dict = None  # qualified_name -> List[ColumnMeta]

    def build_specs(self, target_schema: str | None = None) -> dict[str, TableSpec]:
        """Convert per-table introspection data into a TableSpec map keyed
        on the destination's qualified name. If `target_schema` is given,
        the destination ref is rewritten to that schema (typical for
        Oracle HR -> Postgres public migrations); otherwise source and
        destination share the same ref."""
        out: dict[str, TableSpec] = {}
        for src in self.tables:
            qn = src.qualified()
            cols = self.columns.get(qn, [])
            pk = self.primary_keys.get(qn, [])
            if not pk:
                # Without a PK we can't keyset-paginate. Skip these rather
                # than emit a degenerate spec — the runner would refuse it
                # anyway.
                continue
            target = (
                TableRef(schema=target_schema, name=src.name) if target_schema else src
            )
            out[target.qualified()] = TableSpec(
                source_table=src,
                target_table=target,
                columns=cols,
                pk_columns=pk,
            )
        return out


# ─── Top-level entry point ───────────────────────────────────────────────────


def introspect(session, dialect: Dialect, schema: str) -> IntrospectedSchema:
    """Run all four introspection queries and assemble the result."""
    if dialect == Dialect.ORACLE:
        tables = _oracle_tables(session, schema)
        col_meta = {t.qualified(): _oracle_column_meta(session, schema, t.name) for t in tables}
        columns = {qn: [c.name for c in metas] for qn, metas in col_meta.items()}
        pks = {t.qualified(): _oracle_primary_keys(session, schema, t.name) for t in tables}
        fks = _oracle_foreign_keys(session, schema)
    elif dialect == Dialect.POSTGRES:
        tables = _pg_tables(session, schema)
        col_meta = {t.qualified(): _pg_column_meta(session, schema, t.name) for t in tables}
        columns = {qn: [c.name for c in metas] for qn, metas in col_meta.items()}
        pks = {t.qualified(): _pg_primary_keys(session, schema, t.name) for t in tables}
        fks = _pg_foreign_keys(session, schema)
    else:
        raise ValueError(f"unsupported dialect: {dialect}")

    return IntrospectedSchema(
        dialect=dialect,
        schema=schema,
        tables=tables,
        columns=columns,
        primary_keys=pks,
        foreign_keys=fks,
        column_metadata=col_meta,
    )


# ─── Oracle queries ──────────────────────────────────────────────────────────


_ORACLE_TABLES_SQL = """
    SELECT table_name
    FROM all_tables
    WHERE owner = :owner
      AND table_name NOT LIKE 'BIN$%'  -- exclude recyclebin tombstones
    ORDER BY table_name
"""

_ORACLE_COLUMNS_SQL = """
    SELECT column_name,
           data_type,
           nullable,
           data_length,
           data_precision,
           data_scale
    FROM all_tab_columns
    WHERE owner = :owner AND table_name = :tbl
    ORDER BY column_id
"""

_ORACLE_PK_SQL = """
    SELECT acc.column_name
    FROM all_constraints ac
    JOIN all_cons_columns acc
      ON acc.owner = ac.owner
     AND acc.constraint_name = ac.constraint_name
    WHERE ac.owner = :owner
      AND ac.table_name = :tbl
      AND ac.constraint_type = 'P'
    ORDER BY acc.position
"""

_ORACLE_FK_SQL = """
    SELECT
        c.constraint_name,
        c.table_name      AS child_table,
        r.table_name      AS parent_table,
        c.deferrable
    FROM all_constraints c
    JOIN all_constraints r
      ON r.owner = c.r_owner
     AND r.constraint_name = c.r_constraint_name
    WHERE c.owner = :owner
      AND c.constraint_type = 'R'
    ORDER BY c.constraint_name
"""


def _oracle_tables(session, schema: str) -> List[TableRef]:
    rows = session.execute(text(_ORACLE_TABLES_SQL), {"owner": schema.upper()}).all()
    return [TableRef(schema=schema, name=r[0]) for r in rows]


def _oracle_column_meta(session, schema: str, table: str) -> List[ColumnMeta]:
    rows = session.execute(
        text(_ORACLE_COLUMNS_SQL), {"owner": schema.upper(), "tbl": table.upper()}
    ).all()
    out: List[ColumnMeta] = []
    for name, data_type, nullable, data_length, precision, scale in rows:
        # VARCHAR2/CHAR/RAW report size in `data_length`; numeric types
        # leave it as the full-NUMBER byte size (22), which isn't useful.
        # Keep length only for string/binary types.
        dtype = (data_type or "").upper()
        length = data_length if dtype in _ORACLE_LENGTH_TYPES else None
        out.append(
            ColumnMeta(
                name=name,
                data_type=dtype,
                nullable=(nullable == "Y"),
                length=length,
                precision=precision,
                scale=scale,
            )
        )
    return out


_ORACLE_LENGTH_TYPES = {"VARCHAR2", "NVARCHAR2", "VARCHAR", "CHAR", "NCHAR", "RAW"}


def _oracle_primary_keys(session, schema: str, table: str) -> List[str]:
    rows = session.execute(
        text(_ORACLE_PK_SQL), {"owner": schema.upper(), "tbl": table.upper()}
    ).all()
    return [r[0] for r in rows]


def _oracle_foreign_keys(session, schema: str) -> List[ForeignKey]:
    rows = session.execute(text(_ORACLE_FK_SQL), {"owner": schema.upper()}).all()
    out: List[ForeignKey] = []
    for name, child, parent, deferrable in rows:
        out.append(
            ForeignKey(
                name=name,
                from_table=TableRef(schema=schema, name=child),
                to_table=TableRef(schema=schema, name=parent),
                deferrable=str(deferrable or "").upper().startswith("DEFERRABLE"),
            )
        )
    return out


# ─── Postgres queries ────────────────────────────────────────────────────────


_PG_TABLES_SQL = """
    SELECT tablename
    FROM pg_tables
    WHERE schemaname = :schema
    ORDER BY tablename
"""

_PG_COLUMNS_SQL = """
    SELECT column_name,
           data_type,
           is_nullable,
           character_maximum_length,
           numeric_precision,
           numeric_scale
    FROM information_schema.columns
    WHERE table_schema = :schema AND table_name = :tbl
    ORDER BY ordinal_position
"""

# Walks pg_index for the primary-key index, then array_position to
# preserve composite-key declared order.
_PG_PK_SQL = """
    SELECT a.attname
    FROM pg_index i
    JOIN pg_class c   ON c.oid = i.indrelid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = ANY(i.indkey)
    WHERE n.nspname = :schema
      AND c.relname = :tbl
      AND i.indisprimary
    ORDER BY array_position(i.indkey, a.attnum)
"""

_PG_FK_SQL = """
    SELECT
        con.conname,
        ns_child.nspname  AS child_schema,
        cls_child.relname AS child_table,
        ns_parent.nspname AS parent_schema,
        cls_parent.relname AS parent_table,
        con.condeferrable
    FROM pg_constraint con
    JOIN pg_class       cls_child   ON cls_child.oid  = con.conrelid
    JOIN pg_namespace   ns_child    ON ns_child.oid   = cls_child.relnamespace
    JOIN pg_class       cls_parent  ON cls_parent.oid = con.confrelid
    JOIN pg_namespace   ns_parent   ON ns_parent.oid  = cls_parent.relnamespace
    WHERE con.contype = 'f'
      AND ns_child.nspname = :schema
    ORDER BY con.conname
"""


def _pg_tables(session, schema: str) -> List[TableRef]:
    rows = session.execute(text(_PG_TABLES_SQL), {"schema": schema}).all()
    return [TableRef(schema=schema, name=r[0]) for r in rows]


def _pg_column_meta(session, schema: str, table: str) -> List[ColumnMeta]:
    rows = session.execute(text(_PG_COLUMNS_SQL), {"schema": schema, "tbl": table}).all()
    out: List[ColumnMeta] = []
    for name, data_type, is_nullable, char_max_len, numeric_precision, numeric_scale in rows:
        # information_schema reports numeric_precision for integer types
        # too (bigint → 64). That's noisy when we round-trip through the
        # PG mapper, so drop it for non-numeric types.
        is_numeric = (data_type or "").lower() == "numeric"
        out.append(
            ColumnMeta(
                name=name,
                data_type=data_type,
                nullable=(is_nullable == "YES"),
                length=char_max_len,
                precision=numeric_precision if is_numeric else None,
                scale=numeric_scale if is_numeric else None,
            )
        )
    return out


def _pg_primary_keys(session, schema: str, table: str) -> List[str]:
    rows = session.execute(text(_PG_PK_SQL), {"schema": schema, "tbl": table}).all()
    return [r[0] for r in rows]


def _pg_foreign_keys(session, schema: str) -> List[ForeignKey]:
    rows = session.execute(text(_PG_FK_SQL), {"schema": schema}).all()
    out: List[ForeignKey] = []
    for name, c_schema, c_table, p_schema, p_table, deferrable in rows:
        out.append(
            ForeignKey(
                name=name,
                from_table=TableRef(schema=c_schema, name=c_table),
                to_table=TableRef(schema=p_schema, name=p_table),
                deferrable=bool(deferrable),
            )
        )
    return out
