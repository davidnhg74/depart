"""Keyset pagination query builder.

OFFSET-based paging is fatal at scale: every page repeats the work of
all prior pages. Keyset (a.k.a. seek-method) paging walks the table by
remembering the last row's primary-key tuple and saying "give me the
next N rows after that". Cost stays roughly constant.

This module produces the query *text* for both Oracle and PostgreSQL —
neither is portable enough through SQLAlchemy expression-language for
this case (FETCH FIRST vs LIMIT, tuple-comparison support, identifier
quoting). The runner then feeds the text through `session.execute(text())`.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import List, Mapping, Sequence


class Dialect(str, Enum):
    ORACLE = "oracle"
    POSTGRES = "postgres"


@dataclass(frozen=True)
class KeysetQuery:
    """A keyset SELECT and the bind parameters it expects."""

    sql: str
    params: Mapping[str, object]


def build_first_page(
    *,
    dialect: Dialect,
    table: str,
    columns: Sequence[str],
    pk_columns: Sequence[str],
    batch_size: int,
) -> KeysetQuery:
    """First page of a keyset walk — no WHERE, just ORDER BY + LIMIT.

    `table` and column names are passed through `_quote_ident` so reserved
    words and mixed-case identifiers survive. The runner is responsible
    for handing in already-validated identifier strings (no SQL injection
    surface — these come from `information_schema` introspection)."""
    cols = ", ".join(_quote_ident(c, dialect) for c in columns)
    order = ", ".join(_quote_ident(c, dialect) for c in pk_columns)
    table_q = _quote_table(table, dialect)
    limit = _limit_clause(dialect, batch_size)
    sql = f"SELECT {cols} FROM {table_q} ORDER BY {order} {limit}"
    return KeysetQuery(sql=sql, params={"limit": batch_size})


def build_next_page(
    *,
    dialect: Dialect,
    table: str,
    columns: Sequence[str],
    pk_columns: Sequence[str],
    last_pk: Sequence[object],
    batch_size: int,
) -> KeysetQuery:
    """Subsequent pages — same shape plus a WHERE that uses the
    composite-key comparison `(pk1, pk2, ...) > (:k0, :k1, ...)`.

    Both Oracle and PostgreSQL support row-value comparison (Oracle since
    9i, PG always), which is the cleanest way to express "rows whose key
    tuple sorts strictly after the last one we saw". Decomposing into
    `pk1 > :k0 OR (pk1 = :k0 AND pk2 > :k1) OR ...` is equivalent but
    misses index-only access on multi-column PK indexes in PG.
    """
    if len(last_pk) != len(pk_columns):
        raise ValueError(
            f"last_pk has {len(last_pk)} values but pk_columns has {len(pk_columns)}"
        )
    # NULL in any PK component is fatal for keyset paging: row-value
    # comparison treats `(NULL, ...) > (...)` as NULL (unknown), not
    # true, so the WHERE clause silently rejects every remaining row
    # and the walk halts mid-table with no error. Reject up front
    # rather than emit a query that loses data.
    null_positions = [pk_columns[i] for i, v in enumerate(last_pk) if v is None]
    if null_positions:
        raise ValueError(
            "keyset pagination cannot resume from a NULL primary-key value; "
            f"NULL found in column(s): {null_positions}. PK columns used for "
            "keyset must be NOT NULL — fix the source schema, choose a "
            "different PK, or skip this table."
        )
    cols = ", ".join(_quote_ident(c, dialect) for c in columns)
    order = ", ".join(_quote_ident(c, dialect) for c in pk_columns)
    pk_quoted = ", ".join(_quote_ident(c, dialect) for c in pk_columns)
    table_q = _quote_table(table, dialect)
    binds = ", ".join(f":k{i}" for i in range(len(pk_columns)))
    limit = _limit_clause(dialect, batch_size)
    sql = (
        f"SELECT {cols} FROM {table_q} "
        f"WHERE ({pk_quoted}) > ({binds}) "
        f"ORDER BY {order} {limit}"
    )
    params: dict = {f"k{i}": v for i, v in enumerate(last_pk)}
    params["limit"] = batch_size
    return KeysetQuery(sql=sql, params=params)


# ─── helpers ─────────────────────────────────────────────────────────────────


def _limit_clause(dialect: Dialect, batch_size: int) -> str:
    """Both dialects accept the bind parameter — we keep the literal in
    the string for explainability, but the param is what the planner
    sees. Using the literal makes the query trivially cacheable on the
    DB side too (one prepared statement per batch_size)."""
    if dialect == Dialect.ORACLE:
        # Oracle 12c+ syntax. 11g would need ROWNUM — out of scope here;
        # the runner refuses to start against pre-12c sources.
        return f"FETCH FIRST {batch_size} ROWS ONLY"
    return f"LIMIT {batch_size}"


def _quote_ident(ident: str, dialect: Dialect) -> str:
    """Both Oracle and Postgres use double-quotes for identifier quoting,
    and both fold unquoted identifiers (Oracle to upper, PG to lower).
    The simple rule we follow: quote everything. The introspector hands
    us the canonical case from the source catalog."""
    if '"' in ident:
        # Defense-in-depth: the introspector should never emit names with
        # embedded quotes, but if it does, refuse rather than mis-escape.
        raise ValueError(f"identifier contains quote character: {ident!r}")
    return f'"{ident}"'


def _quote_table(qualified: str, dialect: Dialect) -> str:
    """`schema.table` -> "schema"."table"; bare names stay bare-quoted.
    Cross-dialect: same syntax."""
    if "." in qualified:
        schema, name = qualified.split(".", 1)
        return f"{_quote_ident(schema, dialect)}.{_quote_ident(name, dialect)}"
    return _quote_ident(qualified, dialect)
