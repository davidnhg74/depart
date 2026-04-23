"""Schema/DDL generation for the destination Postgres.

The data-movement runner assumes every target table already exists. For
greenfield migrations that's a burden on the operator — they have to
hand-craft DDL that matches the Oracle source. This module closes that
gap:

  • `ColumnMeta` captures everything we need to emit a PG column
    definition (name, data type, nullability, and the length/precision/
    scale qualifiers that drive type selection for NUMBER/VARCHAR2).
  • `map_oracle_type()` and `map_pg_type()` turn a `ColumnMeta` into a
    Postgres type string. Oracle is the interesting case; PG→PG is
    largely pass-through but still normalizes naming.
  • `generate_create_table()` emits a single `CREATE TABLE IF NOT
    EXISTS` statement with inline PK. We deliberately do NOT emit FKs
    here — they're added in a second pass so forward references don't
    fail, and the runner already has a constraint-deferral path for
    cycles.
  • `generate_schema_ddl()` takes the full introspected schema + the PK
    map and returns a list of CREATE TABLE statements in planner order.

DDL is a destructive-adjacent operation, so the runner doesn't execute
it automatically. The CLI exposes `--create-tables` which runs these
statements against the target before the data-load loop starts.

Type mapping notes (mostly Oracle idiosyncrasies):
  • `NUMBER` with no precision → NUMERIC (unbounded). Oracle allows it;
    Postgres does too.
  • `NUMBER(p, 0)` → smallest integer type that fits. p ≤ 4 → SMALLINT,
    p ≤ 9 → INTEGER, p ≤ 18 → BIGINT, else NUMERIC(p).
  • `NUMBER(p, s)` with s > 0 → NUMERIC(p, s).
  • `VARCHAR2(n)` → VARCHAR(n). Oracle measures n in bytes by default
    (or chars if `NLS_LENGTH_SEMANTICS=CHAR`); we round up to VARCHAR(n)
    in PG either way — undersized strings will error on the COPY, which
    is the right failure mode.
  • `DATE` → TIMESTAMP. Oracle DATE carries a time-of-day, so plain PG
    DATE loses information.
  • `CLOB`/`NCLOB` → TEXT. `BLOB`/`RAW` → BYTEA.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence

from .planner import TableRef


logger = logging.getLogger(__name__)


# ─── Metadata shape ──────────────────────────────────────────────────────────


@dataclass(frozen=True)
class ColumnMeta:
    """A single column's physical shape. Fields we don't have (e.g. no
    precision on a VARCHAR2) are None, not 0 — zero is a legal scale and
    we need to distinguish 'not specified' from 'specified as zero'."""

    name: str
    data_type: str  # Uppercased source-dialect type, e.g. "NUMBER", "VARCHAR2"
    nullable: bool = True
    length: Optional[int] = None  # char/byte length for VARCHAR2/CHAR/RAW
    precision: Optional[int] = None  # total digits for NUMBER
    scale: Optional[int] = None  # decimal digits for NUMBER (0 = integer)


# ─── Oracle → Postgres type mapping ──────────────────────────────────────────


def map_oracle_type(col: ColumnMeta) -> str:
    """Return the Postgres column type string for an Oracle column."""
    t = col.data_type.upper()

    if t == "NUMBER":
        return _map_number(col.precision, col.scale)

    if t in ("VARCHAR2", "NVARCHAR2", "VARCHAR"):
        return f"VARCHAR({col.length})" if col.length else "TEXT"

    if t in ("CHAR", "NCHAR"):
        return f"CHAR({col.length})" if col.length else "CHAR(1)"

    if t in ("CLOB", "NCLOB", "LONG"):
        return "TEXT"

    if t in ("BLOB", "LONG RAW"):
        return "BYTEA"

    if t == "RAW":
        return "BYTEA"

    if t == "BFILE":
        # BFILE is a *pointer* to an external file on the Oracle server's
        # filesystem, not the file's contents. Postgres has no equivalent;
        # the pragmatic choice is to land the locator string as TEXT so
        # the migration doesn't abort, then leave it to the operator to
        # rehome the underlying files (DBMS_LOB.LOADCLOBFROMFILE-style
        # extraction during a follow-up pass).
        #
        # We deliberately don't raise here — crashing during DDL after
        # the operator already kicked off the migration is the worst-
        # case UX. The warning shows up in the migration log instead.
        logger.warning(
            "BFILE column %r mapped to TEXT — the column will hold Oracle "
            "file *locators*, not file contents. Plan a follow-up pass to "
            "extract the underlying files if you need the data itself.",
            col.name,
        )
        return "TEXT"

    if t == "DATE":
        # Oracle DATE carries time-of-day; plain PG DATE would truncate.
        return "TIMESTAMP"

    if t.startswith("TIMESTAMP"):
        # Oracle has TIMESTAMP, TIMESTAMP WITH TIME ZONE, and TIMESTAMP
        # WITH LOCAL TIME ZONE. PG only has the first two; LOCAL maps to
        # plain TIMESTAMP (the data is stored in session tz in Oracle,
        # so WITHOUT TZ is the honest equivalent).
        if "WITH TIME ZONE" in t and "LOCAL" not in t:
            return "TIMESTAMPTZ"
        return "TIMESTAMP"

    if t == "FLOAT":
        # Oracle FLOAT(b) is precision in binary bits. Map conservatively
        # to DOUBLE PRECISION — the only loss is space, never precision.
        return "DOUBLE PRECISION"

    if t == "BINARY_FLOAT":
        return "REAL"
    if t == "BINARY_DOUBLE":
        return "DOUBLE PRECISION"

    if t == "ROWID" or t == "UROWID":
        return "TEXT"

    if t in ("XMLTYPE", "SDO_GEOMETRY"):
        # Best-effort — operators can swap in a proper PG type after the
        # fact. Falling back to TEXT keeps the COPY pipeline alive.
        return "TEXT"

    # Unknown Oracle type — raise rather than silently guess. The
    # operator can extend the mapping or patch the source DDL.
    raise ValueError(f"no Postgres mapping for Oracle type: {col.data_type!r}")


def _map_number(precision: Optional[int], scale: Optional[int]) -> str:
    """NUMBER / NUMBER(p) / NUMBER(p, s) → smallest PG integer or NUMERIC."""
    if precision is None:
        return "NUMERIC"
    s = scale or 0
    if s == 0:
        if precision <= 4:
            return "SMALLINT"
        if precision <= 9:
            return "INTEGER"
        if precision <= 18:
            return "BIGINT"
        return f"NUMERIC({precision})"
    return f"NUMERIC({precision}, {s})"


# ─── Postgres → Postgres mapping (for PG→PG identity loads) ──────────────────


def map_pg_type(col: ColumnMeta) -> str:
    """Return a canonical Postgres type string for a Postgres column.

    The source side already speaks PG, so this is nearly pass-through;
    we just echo back the data_type with its qualifiers. The one
    normalization: `information_schema.columns` often reports
    `character varying` / `numeric` — we keep those names, just add
    the qualifier if present."""
    t = col.data_type.lower()

    if t in ("character varying", "varchar"):
        return f"VARCHAR({col.length})" if col.length else "TEXT"
    if t in ("character", "char"):
        return f"CHAR({col.length})" if col.length else "CHAR(1)"
    if t == "numeric":
        if col.precision is not None:
            s = col.scale or 0
            return f"NUMERIC({col.precision}, {s})" if s else f"NUMERIC({col.precision})"
        return "NUMERIC"

    # Everything else (integer, bigint, text, bytea, timestamp, uuid, …)
    # round-trips via the declared type name.
    return col.data_type.upper()


# ─── CREATE TABLE generation ─────────────────────────────────────────────────


def generate_create_table(
    table: TableRef,
    columns: Sequence[ColumnMeta],
    pk_columns: Sequence[str],
    *,
    map_type=map_oracle_type,
) -> str:
    """Emit a single `CREATE TABLE IF NOT EXISTS` statement.

    `map_type` is pluggable so callers can pick the Oracle or Postgres
    mapper without the DDL module having to know the source dialect. It
    receives a `ColumnMeta` and returns a PG type string.
    """
    if not columns:
        raise ValueError(f"cannot generate DDL for {table.qualified()!r}: no columns")

    col_lines: List[str] = []
    for col in columns:
        pg_type = map_type(col)
        nullability = "" if col.nullable else " NOT NULL"
        col_lines.append(f'    "{col.name}" {pg_type}{nullability}')

    pk_line = ""
    if pk_columns:
        pk_cols_quoted = ", ".join(f'"{c}"' for c in pk_columns)
        pk_line = f",\n    PRIMARY KEY ({pk_cols_quoted})"

    qualified = _quote_qualified(table)
    return (
        f"CREATE TABLE IF NOT EXISTS {qualified} (\n"
        + ",\n".join(col_lines)
        + pk_line
        + "\n)"
    )


def generate_schema_ddl(
    tables: Sequence[TableRef],
    columns_by_table: Dict[str, Sequence[ColumnMeta]],
    pks_by_table: Dict[str, Sequence[str]],
    *,
    map_type=map_oracle_type,
) -> List[str]:
    """Return one `CREATE TABLE IF NOT EXISTS` statement per table, in
    the order `tables` is given. Callers typically pass the load-plan
    order so parents exist before children."""
    stmts: List[str] = []
    for t in tables:
        qn = t.qualified()
        cols = columns_by_table.get(qn, [])
        pk = pks_by_table.get(qn, [])
        stmts.append(generate_create_table(t, cols, pk, map_type=map_type))
    return stmts


def _quote_qualified(table: TableRef) -> str:
    """Quote schema + name for inclusion in a DDL statement. Postgres
    unquoted identifiers get lowercased, which breaks round-trips with
    Oracle's usually-uppercase names; quoting keeps everything literal."""
    if table.schema:
        return f'"{table.schema}"."{table.name}"'
    return f'"{table.name}"'


# ─── Apply DDL ───────────────────────────────────────────────────────────────


def apply_ddl(target_pg_conn, statements: Sequence[str]) -> None:
    """Execute each statement on `target_pg_conn` in a single
    transaction. Any failure rolls the whole batch back — we'd rather
    leave the target untouched than end up with a half-created schema
    that confuses a subsequent run.

    NOTE: `target_pg_conn` must NOT be in autocommit mode, otherwise
    each CREATE TABLE commits individually and a mid-batch failure
    leaves a partial schema. The CLI opens a dedicated non-autocommit
    psycopg connection for DDL; the COPY connection (which is
    autocommit) is kept separate."""
    try:
        with target_pg_conn.cursor() as cur:
            for stmt in statements:
                cur.execute(stmt)
        target_pg_conn.commit()
    except Exception:
        target_pg_conn.rollback()
        raise
