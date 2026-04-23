"""Layer 3 — quality validation.

The Merkle verifier proves the bytes round-tripped. This module proves
they're not pathological in the first place: VARCHAR columns that were
sized in Oracle bytes won't fit a Postgres column sized in characters
once multi-byte UTF-8 lands; per-column NULL ratios should match;
numeric and temporal min/max should agree.

Both helpers are pure: they take SQLAlchemy sessions and column shapes
and return findings. Callers (the runner, an API endpoint, the CLI)
decide whether to surface them as warnings, gate the run, or persist
them on the migration record.

Design notes:
  * `scan_varchar_lengths` runs once on the SOURCE before the COPY
    starts — that's when the operator can still abort or widen the
    target column. Post-copy is too late: data is already on the wire.
  * `compare_basic_stats` runs after the COPY (and after the Merkle
    pass succeeds). Merkle catches "the data didn't round-trip", this
    catches "the data round-tripped but somehow ended up with wrong
    distributions" — a stricter sanity check before cutover.
  * Neither helper reads the full table — the SQL is COUNT/MIN/MAX/
    LENGTH aggregation that PG and Oracle both push to the storage
    layer. Cheap even on terabyte tables.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Sequence

from sqlalchemy import text

from .ddl import ColumnMeta
from .keyset import Dialect


# ─── Findings ────────────────────────────────────────────────────────────────


@dataclass
class QualityFinding:
    """A single observation. `severity` lets callers triage:

      * `info`    — observed normally; surface for transparency
      * `warning` — anomaly worth a human glance, doesn't block the run
      * `error`   — almost-certain data corruption / loss; should block
    """

    severity: str  # "info" | "warning" | "error"
    table: str
    column: str | None
    check: str  # short identifier for the check that produced this
    message: str


# ─── Pre-copy: VARCHAR length / encoding sanity ──────────────────────────────


# Threshold above which a VARCHAR/CHAR column's actual max length
# warrants a warning. Picked at 90% of declared length so we flag
# columns approaching their limit (where multi-byte UTF-8 could push
# them over) rather than just literally-at-limit ones.
_LENGTH_WARN_RATIO = 0.9
_VARCHAR_TYPES = {"VARCHAR", "VARCHAR2", "NVARCHAR2", "CHAR", "NCHAR"}


def scan_varchar_lengths(
    session,
    dialect: Dialect,
    table: str,
    columns: Sequence[ColumnMeta],
) -> List[QualityFinding]:
    """For each VARCHAR/CHAR column with a declared length, check the
    actual max length on the source and flag any column whose contents
    are within 10% of the limit OR exceed the limit.

    Catches the byte-vs-char semantics gotcha: an Oracle VARCHAR2(20
    BYTE) holding 20 bytes of single-byte ASCII fits a PG VARCHAR(20),
    but the same column with NLS_LENGTH_SEMANTICS=CHAR holding 20
    multi-byte characters (60 bytes of UTF-8) does NOT fit until the
    target is widened. Surfacing this pre-copy lets the operator widen
    once instead of debugging a mid-migration COPY failure.

    Skipped columns (CLOB, NUMBER, etc.) are silently ignored — the
    finding list only contains rows for columns we actually inspected.
    """
    findings: List[QualityFinding] = []
    table_q = _quote_table(table)

    for col in columns:
        dtype = (col.data_type or "").upper().strip()
        if dtype not in _VARCHAR_TYPES or not col.length:
            continue

        col_q = f'"{col.name}"'
        # Both Oracle and PG support LENGTH(col). On Oracle, LENGTH
        # returns characters by default, which is what we want for
        # comparing against the column's declared length when the
        # column was declared in CHAR semantics. For BYTE-semantics
        # columns the same result is also the right warning trigger
        # (we'd over-warn, never under-warn — acceptable for v1).
        sql = f"SELECT MAX(LENGTH({col_q})) FROM {table_q}"
        max_len = session.execute(text(sql)).scalar()

        if max_len is None:
            continue  # all NULL — nothing to evaluate

        if max_len > col.length:
            findings.append(
                QualityFinding(
                    severity="error",
                    table=table,
                    column=col.name,
                    check="varchar_overflow",
                    message=(
                        f"observed max length {max_len} exceeds declared "
                        f"length {col.length} — data will be truncated on "
                        f"a same-width target. Widen the target column or "
                        f"abort."
                    ),
                )
            )
        elif max_len >= col.length * _LENGTH_WARN_RATIO:
            findings.append(
                QualityFinding(
                    severity="warning",
                    table=table,
                    column=col.name,
                    check="varchar_near_limit",
                    message=(
                        f"observed max length {max_len} is within 10% of "
                        f"declared length {col.length} — UTF-8 expansion "
                        f"on the target may overflow. Verify the target "
                        f"column is sized in characters, not bytes."
                    ),
                )
            )

    return findings


# ─── Post-copy: distribution sanity ──────────────────────────────────────────


# Numeric / temporal types we know how to MIN/MAX. We only run those
# on columns whose source dtype matches; everything else (TEXT, BLOB)
# is skipped. Lower-case for the PG side, upper for Oracle — the type
# strings in ColumnMeta are dialect-specific, so we match either form.
_RANGE_TYPES = {
    "INTEGER", "INT", "BIGINT", "SMALLINT",
    "NUMERIC", "NUMBER", "DECIMAL",
    "REAL", "DOUBLE PRECISION", "FLOAT",
    "DATE", "TIMESTAMP",
    "TIMESTAMP WITHOUT TIME ZONE", "TIMESTAMP WITH TIME ZONE",
}


def compare_basic_stats(
    src_session,
    src_dialect: Dialect,
    src_table: str,
    dst_session,
    dst_table: str,
    columns: Sequence[ColumnMeta],
) -> List[QualityFinding]:
    """Compare source and target on COUNT(*), per-column NULL counts,
    and per-column MIN/MAX (for numeric and temporal types).

    Ordered the same way every time so callers diffing two runs see a
    stable list. Run after the Merkle verifier — Merkle gives you
    bit-identical proof; this catches "Merkle says match" with a
    clearer-to-explain second opinion."""
    findings: List[QualityFinding] = []

    src_table_q = _quote_table(src_table)
    dst_table_q = _quote_table(dst_table)

    # 1. Row count agreement.
    src_count = src_session.execute(
        text(f"SELECT COUNT(*) FROM {src_table_q}")
    ).scalar()
    dst_count = dst_session.execute(
        text(f"SELECT COUNT(*) FROM {dst_table_q}")
    ).scalar()
    if src_count != dst_count:
        findings.append(
            QualityFinding(
                severity="error",
                table=src_table,
                column=None,
                check="row_count",
                message=f"row count mismatch: source={src_count}, target={dst_count}",
            )
        )

    # 2. Per-column NULL counts and (for range types) MIN/MAX.
    for col in columns:
        col_q = f'"{col.name}"'

        src_nulls = src_session.execute(
            text(f"SELECT COUNT(*) FROM {src_table_q} WHERE {col_q} IS NULL")
        ).scalar()
        dst_nulls = dst_session.execute(
            text(f"SELECT COUNT(*) FROM {dst_table_q} WHERE {col_q} IS NULL")
        ).scalar()
        if src_nulls != dst_nulls:
            findings.append(
                QualityFinding(
                    severity="error",
                    table=src_table,
                    column=col.name,
                    check="null_count",
                    message=(
                        f"NULL-count mismatch: source={src_nulls}, "
                        f"target={dst_nulls}"
                    ),
                )
            )

        dtype = (col.data_type or "").upper().strip()
        if dtype not in _RANGE_TYPES:
            continue

        src_min, src_max = src_session.execute(
            text(f"SELECT MIN({col_q}), MAX({col_q}) FROM {src_table_q}")
        ).one()
        dst_min, dst_max = dst_session.execute(
            text(f"SELECT MIN({col_q}), MAX({col_q}) FROM {dst_table_q}")
        ).one()
        # Compare via str() so Decimal vs int and datetime variants
        # don't false-flag — same canonicalization principle as the
        # verify hash, just simpler since this is one value at a time.
        if str(src_min) != str(dst_min):
            findings.append(
                QualityFinding(
                    severity="error",
                    table=src_table,
                    column=col.name,
                    check="min_value",
                    message=f"MIN mismatch: source={src_min!r}, target={dst_min!r}",
                )
            )
        if str(src_max) != str(dst_max):
            findings.append(
                QualityFinding(
                    severity="error",
                    table=src_table,
                    column=col.name,
                    check="max_value",
                    message=f"MAX mismatch: source={src_max!r}, target={dst_max!r}",
                )
            )

    return findings


# ─── helpers ─────────────────────────────────────────────────────────────────


def _quote_table(qualified: str) -> str:
    """Quote schema + table for safe inclusion in an aggregate query.
    Mirrors the rule in keyset.py: quote everything, no bare names."""
    if "." in qualified:
        schema, name = qualified.split(".", 1)
        return f'"{schema}"."{name}"'
    return f'"{qualified}"'
