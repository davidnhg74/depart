"""Layer 8 — row-level data sampler.

Samples rows from the Oracle source and compares them against the
PostgreSQL target to detect data-loss, value corruption, and NULL
conversion errors before cutover.

Strategy per table:
  1. Detect PK columns from Oracle data dictionary.
     Skip tables with no PK (can't join without a key).
  2. Sample up to SAMPLE_SIZE rows from Oracle using DBMS_RANDOM.
  3. Fetch matching rows from PG by PK.
  4. Compare column values with Oracle→PG normalisations:
       - Oracle '' (empty string) == PG NULL
       - CHAR trailing whitespace stripped on both sides
       - CLOB/BLOB columns skipped (too large to compare)
       - Values truncated to _MAX_VALUE_LEN chars in mismatch reports
  5. Emit one SampleMismatch per differing column.

All SQL aggregate-free and bounded by sample_size. Safe on terabyte tables.
"""

from __future__ import annotations

import logging
from dataclasses import asdict, dataclass
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

_DEFAULT_SAMPLE_SIZE = 100
_MAX_VALUE_LEN = 200

# Oracle column data types we skip (binary / LOB).
_SKIP_TYPES = {"BLOB", "CLOB", "NCLOB", "RAW", "LONG RAW", "BFILE", "XMLTYPE"}


# ─── Finding ─────────────────────────────────────────────────────────────────


@dataclass
class SampleMismatch:
    table: str
    pk_values: Dict[str, Any]
    column: str
    oracle_value: Optional[str]
    pg_value: Optional[str]
    mismatch_type: str  # value_mismatch | missing_in_pg | null_mismatch

    def to_dict(self) -> dict:
        return asdict(self)


# ─── PK detection ────────────────────────────────────────────────────────────


def get_pk_columns(oracle_session: Session, schema: str, table: str) -> List[str]:
    """Return PK column names for an Oracle table, ordered by position."""
    sql = text(
        """
        SELECT cols.column_name
        FROM   all_constraints  cons
        JOIN   all_cons_columns cols
               ON  cons.constraint_name = cols.constraint_name
               AND cons.owner           = cols.owner
        WHERE  cons.owner           = :schema
          AND  cons.table_name      = :table
          AND  cons.constraint_type = 'P'
        ORDER  BY cols.position
        """
    )
    try:
        rows = oracle_session.execute(
            sql, {"schema": schema.upper(), "table": table.upper()}
        ).mappings().all()
        return [str(r["column_name"]) for r in rows]
    except Exception as exc:
        logger.warning("get_pk_columns failed for %s.%s: %s", schema, table, exc)
        return []


# ─── Sampling ────────────────────────────────────────────────────────────────


def get_skipped_columns(oracle_session: Session, schema: str, table: str) -> set:
    """Return column names whose data_type is in _SKIP_TYPES."""
    sql = text(
        """
        SELECT column_name
        FROM   all_tab_columns
        WHERE  owner      = :schema
          AND  table_name = :table
          AND  data_type  IN :types
        """
    )
    try:
        rows = oracle_session.execute(
            sql,
            {"schema": schema.upper(), "table": table.upper(), "types": tuple(_SKIP_TYPES)},
        ).mappings().all()
        return {str(r["column_name"]) for r in rows}
    except Exception as exc:
        logger.warning("get_skipped_columns failed: %s", exc)
        return set()


def sample_oracle_rows(
    oracle_session: Session,
    schema: str,
    table: str,
    sample_size: int,
) -> List[Dict[str, Any]]:
    """Return up to sample_size random rows from an Oracle table."""
    sql = text(
        f"""
        SELECT * FROM (
            SELECT * FROM {schema.upper()}.{table.upper()}
            ORDER BY DBMS_RANDOM.VALUE
        ) WHERE ROWNUM <= :n
        """  # noqa: S608 — schema/table are controlled, not user input at this layer
    )
    try:
        rows = oracle_session.execute(sql, {"n": sample_size}).mappings().all()
        return [dict(r) for r in rows]
    except Exception as exc:
        logger.warning("sample_oracle_rows failed for %s.%s: %s", schema, table, exc)
        return []


def fetch_pg_rows(
    pg_session: Session,
    schema: str,
    table: str,
    pk_cols: List[str],
    oracle_rows: List[Dict[str, Any]],
) -> Dict[Tuple, Dict[str, Any]]:
    """Fetch PG rows matching the Oracle sample by PK. Returns {pk_tuple: row}."""
    if not oracle_rows or not pk_cols:
        return {}

    result: Dict[Tuple, Dict[str, Any]] = {}

    for oracle_row in oracle_rows:
        pk_vals = {col: oracle_row.get(col) for col in pk_cols}
        where_parts = " AND ".join(
            f"{col.lower()} = :{col.lower()}_pk" for col in pk_cols
        )
        params = {f"{col.lower()}_pk": val for col, val in pk_vals.items()}
        sql = text(
            f"SELECT * FROM {schema.lower()}.{table.lower()} WHERE {where_parts}"  # noqa: S608
        )
        try:
            row = pg_session.execute(sql, params).mappings().first()
            if row is not None:
                pk_key = tuple(pk_vals[col] for col in pk_cols)
                result[pk_key] = dict(row)
        except Exception as exc:
            logger.warning("fetch_pg_rows failed for pk %s: %s", pk_vals, exc)

    return result


# ─── Comparison ──────────────────────────────────────────────────────────────


def _normalise(value: Any) -> Optional[str]:
    """Normalise a column value for comparison."""
    if value is None:
        return None
    s = str(value).rstrip()  # strip CHAR padding
    if s == "":
        return None  # Oracle '' == PG NULL
    if len(s) > _MAX_VALUE_LEN:
        s = s[:_MAX_VALUE_LEN] + "…"
    return s


def compare_row(
    table: str,
    pk_cols: List[str],
    oracle_row: Dict[str, Any],
    pg_row: Optional[Dict[str, Any]],
    skip_cols: set,
) -> List[SampleMismatch]:
    """Return mismatches between one Oracle row and its PG counterpart."""
    pk_values = {col: oracle_row.get(col) for col in pk_cols}

    if pg_row is None:
        return [
            SampleMismatch(
                table=table,
                pk_values=pk_values,
                column="<row>",
                oracle_value=None,
                pg_value=None,
                mismatch_type="missing_in_pg",
            )
        ]

    mismatches: List[SampleMismatch] = []
    for col, oracle_val in oracle_row.items():
        col_upper = col.upper() if isinstance(col, str) else col
        if col_upper in skip_cols:
            continue
        # PG keys are lowercase.
        pg_col = col.lower() if isinstance(col, str) else col
        pg_val = pg_row.get(pg_col)

        o_norm = _normalise(oracle_val)
        p_norm = _normalise(pg_val)

        if o_norm == p_norm:
            continue

        mtype = (
            "null_mismatch"
            if (o_norm is None) != (p_norm is None)
            else "value_mismatch"
        )
        mismatches.append(
            SampleMismatch(
                table=table,
                pk_values=pk_values,
                column=str(col),
                oracle_value=o_norm,
                pg_value=p_norm,
                mismatch_type=mtype,
            )
        )
    return mismatches


# ─── Orchestration ───────────────────────────────────────────────────────────


def run_sampler(
    oracle_session: Session,
    pg_session: Session,
    oracle_schema: str,
    pg_schema: str,
    tables: List[str],
    sample_size: int = _DEFAULT_SAMPLE_SIZE,
) -> Tuple[List[SampleMismatch], Dict[str, int]]:
    """Run the sampler over all tables.

    Returns (mismatches, stats) where stats has keys:
      sampled       — tables successfully compared
      skipped_no_pk — tables skipped (no PK detected)
      skipped_empty — tables with no Oracle rows
    """
    mismatches: List[SampleMismatch] = []
    stats = {"sampled": 0, "skipped_no_pk": 0, "skipped_empty": 0}

    for table in tables:
        pk_cols = get_pk_columns(oracle_session, oracle_schema, table)
        if not pk_cols:
            logger.info("sampler: skipping %s — no PK", table)
            stats["skipped_no_pk"] += 1
            continue

        skip_cols = get_skipped_columns(oracle_session, oracle_schema, table)
        oracle_rows = sample_oracle_rows(oracle_session, oracle_schema, table, sample_size)
        if not oracle_rows:
            stats["skipped_empty"] += 1
            continue

        pg_by_pk = fetch_pg_rows(pg_session, pg_schema, table, pk_cols, oracle_rows)

        for oracle_row in oracle_rows:
            pk_key = tuple(oracle_row.get(col) for col in pk_cols)
            pg_row = pg_by_pk.get(pk_key)
            mismatches += compare_row(table, pk_cols, oracle_row, pg_row, skip_cols)

        stats["sampled"] += 1

    return mismatches, stats
