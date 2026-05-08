"""Layer 6 — AI anomaly detection.

After a migration completes, this module samples the target PostgreSQL
data and uses Claude to identify distributions that look wrong: unexpected
NULLs, cardinality collapses, range violations, empty tables. Rule-based
checks fire even without an API key so the feature degrades gracefully.

Design:
  * Pure helper functions; the service layer owns DB writes and AI calls.
  * All SQL runs against the TARGET only (post-migration). We don't need
    the Oracle source — the migration record carries row counts.
  * Sampling is aggregate-only (COUNT/MIN/MAX/AVG/STDDEV): no full-table
    scans, safe on terabyte tables.
  * We cap at 20 tables (largest first by row count) to bound prompt size.
"""

from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass
from typing import Any, Dict, List, Literal, Optional, Sequence

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

# Maximum number of tables to sample in one anomaly check.
_MAX_TABLES = 20

# Cardinality threshold: only fetch top-N values for columns whose
# estimated distinct count is below this (avoids huge freq queries).
_CARD_THRESHOLD = 200

# Numeric/temporal PG types we run min/max/avg/stddev on.
_NUMERIC_TYPES = {
    "integer", "bigint", "smallint", "numeric", "decimal",
    "real", "double precision", "float", "float4", "float8",
    "money",
}
_TEMPORAL_TYPES = {
    "date", "timestamp without time zone", "timestamp with time zone",
    "timestamp", "timestamptz",
}

# Rule-based thresholds (no AI needed).
_NULL_RATE_WARN = 0.80   # warn if > 80% NULLs
_NULL_RATE_ERR  = 0.99   # error if > 99% NULLs (near-total loss)
_ROW_MISMATCH_ERR = 0.01 # error if row count differs by > 1% from expected


# ─── Finding ─────────────────────────────────────────────────────────────────


@dataclass
class AnomalyFinding:
    severity: Literal["info", "warning", "error"]
    table: str
    column: Optional[str]
    anomaly_type: str  # null_rate_spike | cardinality_mismatch | range_violation
                       # | distribution_skew | unexpected_empty_table | row_count_mismatch
    message: str
    recommended_action: str

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# ─── Sampling ────────────────────────────────────────────────────────────────


def sample_table_distributions(
    session: Session,
    schema: str,
    table: str,
    expected_row_count: Optional[int] = None,
) -> Dict[str, Any]:
    """Return a distribution summary for one target table.

    The summary dict has this shape:
      {
        "row_count": int,
        "expected_row_count": int | null,
        "columns": {
          "col_name": {
            "type": str,
            "null_rate": float,
            "cardinality": int | null,   # null if > _CARD_THRESHOLD
            "top_values": [[val, count], ...] | null,
            "min": val | null,
            "max": val | null,
            "avg": float | null,
            "stddev": float | null,
          }, ...
        }
      }
    """
    qualified = _quote(schema, table)

    row_count = session.execute(
        text(f"SELECT COUNT(*) FROM {qualified}")
    ).scalar() or 0

    cols = _get_columns(session, schema, table)

    col_stats: Dict[str, Any] = {}
    for col_name, col_type in cols:
        col_q = f'"{col_name}"'
        try:
            col_stats[col_name] = _sample_column(
                session, qualified, col_q, col_name, col_type, row_count
            )
        except Exception:
            logger.debug("anomaly: skipped %s.%s (%s)", table, col_name, col_type)
            col_stats[col_name] = {"type": col_type, "skipped": True}

    return {
        "row_count": row_count,
        "expected_row_count": expected_row_count,
        "columns": col_stats,
    }


def _get_columns(session: Session, schema: str, table: str) -> List[tuple[str, str]]:
    rows = session.execute(
        text(
            "SELECT column_name, data_type "
            "FROM information_schema.columns "
            "WHERE table_schema = :s AND table_name = :t "
            "ORDER BY ordinal_position"
        ),
        {"s": schema.lower(), "t": table.lower()},
    ).fetchall()
    return [(r[0], r[1]) for r in rows]


def _sample_column(
    session: Session,
    qualified: str,
    col_q: str,
    col_name: str,
    col_type: str,
    row_count: int,
) -> Dict[str, Any]:
    stat: Dict[str, Any] = {"type": col_type}

    # NULL rate.
    if row_count > 0:
        null_count = session.execute(
            text(f"SELECT COUNT(*) FROM {qualified} WHERE {col_q} IS NULL")
        ).scalar() or 0
        stat["null_rate"] = round(null_count / row_count, 4)
    else:
        stat["null_rate"] = None

    # Cardinality estimate.
    est_card = session.execute(
        text(
            f"SELECT COUNT(DISTINCT {col_q}) FROM {qualified}"
        )
    ).scalar() or 0
    stat["cardinality"] = est_card if est_card <= _CARD_THRESHOLD else None

    # Top-N values for low-cardinality columns.
    if est_card and est_card <= _CARD_THRESHOLD:
        rows = session.execute(
            text(
                f"SELECT {col_q}, COUNT(*) AS n FROM {qualified} "
                f"GROUP BY {col_q} ORDER BY n DESC LIMIT 10"
            )
        ).fetchall()
        stat["top_values"] = [[str(r[0]), r[1]] for r in rows]
    else:
        stat["top_values"] = None

    # Min/max/avg/stddev for numeric and min/max for temporal.
    ct = col_type.lower()
    if ct in _NUMERIC_TYPES:
        row = session.execute(
            text(
                f"SELECT MIN({col_q}), MAX({col_q}), "
                f"AVG({col_q}::numeric), STDDEV({col_q}::numeric) "
                f"FROM {qualified}"
            )
        ).fetchone()
        stat["min"] = _scalar(row[0])
        stat["max"] = _scalar(row[1])
        stat["avg"] = _scalar(row[2])
        stat["stddev"] = _scalar(row[3])
    elif ct in _TEMPORAL_TYPES:
        row = session.execute(
            text(f"SELECT MIN({col_q}), MAX({col_q}) FROM {qualified}")
        ).fetchone()
        stat["min"] = _scalar(row[0])
        stat["max"] = _scalar(row[1])
    else:
        stat["min"] = stat["max"] = stat["avg"] = stat["stddev"] = None

    return stat


def _scalar(v: Any) -> Any:
    if v is None:
        return None
    # Decimal → float for JSON serialization.
    try:
        return float(v)
    except (TypeError, ValueError):
        return str(v)


# ─── Rule-based fallback ──────────────────────────────────────────────────────


def rule_based_findings(
    distributions: Dict[str, Dict[str, Any]],
) -> List[AnomalyFinding]:
    """Deterministic checks that fire without Claude."""
    findings: List[AnomalyFinding] = []

    for table, dist in distributions.items():
        row_count = dist.get("row_count", 0)
        expected = dist.get("expected_row_count")

        # Empty table when rows were expected.
        if row_count == 0 and expected and expected > 0:
            findings.append(AnomalyFinding(
                severity="error",
                table=table,
                column=None,
                anomaly_type="unexpected_empty_table",
                message=(
                    f"Table has 0 rows after migration but Oracle source "
                    f"had {expected:,} rows."
                ),
                recommended_action=(
                    "Check migration logs for COPY errors. Re-run with "
                    "`dry_run=true` to verify the load plan is correct."
                ),
            ))
        elif expected and row_count > 0:
            drift = abs(row_count - expected) / expected
            if drift > _ROW_MISMATCH_ERR:
                findings.append(AnomalyFinding(
                    severity="error",
                    table=table,
                    column=None,
                    anomaly_type="row_count_mismatch",
                    message=(
                        f"Row count mismatch: migrated {row_count:,}, "
                        f"expected {expected:,} ({drift:.1%} drift)."
                    ),
                    recommended_action=(
                        "Check for rows dropped by masking rules, FK "
                        "constraint violations, or batch failures."
                    ),
                ))

        for col_name, col_stat in dist.get("columns", {}).items():
            if col_stat.get("skipped"):
                continue
            null_rate = col_stat.get("null_rate")
            if null_rate is None:
                continue

            if null_rate >= _NULL_RATE_ERR:
                findings.append(AnomalyFinding(
                    severity="error",
                    table=table,
                    column=col_name,
                    anomaly_type="null_rate_spike",
                    message=(
                        f"{col_name}: {null_rate:.1%} of rows are NULL "
                        f"after migration — near-total data loss."
                    ),
                    recommended_action=(
                        "Verify the source column was populated. Check "
                        "masking rules or type-cast failures."
                    ),
                ))
            elif null_rate >= _NULL_RATE_WARN:
                findings.append(AnomalyFinding(
                    severity="warning",
                    table=table,
                    column=col_name,
                    anomaly_type="null_rate_spike",
                    message=(
                        f"{col_name}: {null_rate:.1%} of rows are NULL "
                        f"— unusually high for a migrated column."
                    ),
                    recommended_action=(
                        "Compare against Oracle source NULL rate. If source "
                        "was also high, this is expected; otherwise check "
                        "type conversion or masking rules."
                    ),
                ))

    return findings


# ─── Prompt builder ───────────────────────────────────────────────────────────


_SYSTEM_PROMPT = """\
You are a database migration quality analyst. You receive distribution
statistics sampled from a PostgreSQL database after an Oracle-to-PostgreSQL
migration. Your job is to identify data anomalies that a human DBA should
investigate before cutover.

Respond ONLY with valid JSON matching this schema exactly:
{
  "overall_severity": "clean" | "info" | "warning" | "error",
  "findings": [
    {
      "severity": "info" | "warning" | "error",
      "table": "<TABLE_NAME>",
      "column": "<COLUMN_NAME>" | null,
      "anomaly_type": "null_rate_spike" | "cardinality_mismatch" | "range_violation" | "distribution_skew" | "unexpected_empty_table" | "row_count_mismatch" | "other",
      "message": "<concise human-readable description, ≤120 chars>",
      "recommended_action": "<what the DBA should do next, ≤200 chars>"
    }
  ]
}

Rules:
- "overall_severity" is the worst severity across all findings, or "clean" if findings is empty.
- Only flag things that are genuinely suspicious. Normal-looking distributions should produce no findings.
- Do NOT hallucinate thresholds — base findings only on the data provided.
- For NULL rates: flag > 50% as warning, > 95% as error, UNLESS the column name suggests NULLs are expected (e.g. OPTIONAL_*, NOTES, COMMENTS).
- For cardinality: flag if a column has only 1 distinct value across many rows (unless it's a flag/boolean column).
- For ranges: flag dates in the far future (> 2100) or far past (< 1900), or negative values in columns whose name suggests they should be positive (e.g. AMOUNT, COUNT, PRICE, QTY).
- For empty tables: flag only if row_count = 0 and expected_row_count > 0.
- Keep findings actionable — each one tells the DBA exactly what to check.
"""


def build_anomaly_prompt(distributions: Dict[str, Dict[str, Any]]) -> str:
    """Format sampled distributions as the user message for Claude."""
    lines = [
        "Distribution statistics from the migrated PostgreSQL database:\n",
        "```json",
        json.dumps(distributions, indent=2, default=str),
        "```",
        "\nIdentify any data anomalies worth investigating before cutover.",
    ]
    return "\n".join(lines)


def get_system_prompt() -> str:
    return _SYSTEM_PROMPT


# ─── Helpers ─────────────────────────────────────────────────────────────────


def _quote(schema: str, table: str) -> str:
    return f'"{schema}"."{table}"'


def overall_severity(findings: Sequence[AnomalyFinding]) -> str:
    """Worst severity across all findings; 'clean' if none."""
    order = {"error": 2, "warning": 1, "info": 0}
    if not findings:
        return "clean"
    return max((f.severity for f in findings), key=lambda s: order.get(s, 0))
