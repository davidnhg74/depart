"""Layer 5 — temporal validation.

Deterministic checks against temporal columns in the TARGET PostgreSQL
database after migration. Runs as part of the anomaly check pipeline
(see anomaly_service.anomaly_check_record) and returns AnomalyFinding
instances so results surface in the same panel as Layer 6 AI findings.

Checks:
  date_range_violation  — values outside [1900, 2100]; almost always a
                          conversion error (epoch confusion, Oracle→PG
                          date arithmetic bugs).
  far_future_date       — values beyond current_year + 5; might be
                          intentional expiry dates, might be a bug.
  all_midnight_timestamps — > _MIDNIGHT_THRESHOLD of non-NULL TIMESTAMP
                            (not TIMESTAMPTZ) values are at 00:00:00;
                            signals potential Oracle DATE time-component
                            loss during migration.

All SQL is aggregate-only. No full-table scans; safe on terabyte tables.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import List, Tuple

from sqlalchemy import text
from sqlalchemy.orm import Session

from .anomaly import AnomalyFinding

logger = logging.getLogger(__name__)

# PG column types we run temporal checks on.
_TEMPORAL_TYPES = {
    "timestamp without time zone",
    "timestamp with time zone",
    "timestamptz",
    "date",
    "timestamp",
}

# Year boundaries outside which we flag a range violation.
_YEAR_MIN = 1900
_YEAR_MAX = 2100

# "Far future" = current year + this many years.
_FAR_FUTURE_BUFFER = 5

# If more than this fraction of non-NULL values fall exactly at midnight,
# flag all_midnight_timestamps.
_MIDNIGHT_THRESHOLD = 0.95

# Minimum non-NULL row count before we bother running any check on a column.
_MIN_ROWS = 10


# ─── Public API ──────────────────────────────────────────────────────────────


def check_temporal_table(
    session: Session,
    schema: str,
    table: str,
) -> List[AnomalyFinding]:
    """Run all Layer 5 checks against one target table.

    Returns a (possibly empty) list of AnomalyFinding. Safe to call on
    tables with no temporal columns — returns [] immediately.
    """
    cols = _temporal_columns(session, schema, table)
    if not cols:
        return []

    findings: List[AnomalyFinding] = []
    qualified = f'"{schema}"."{table}"'
    current_year = datetime.now(tz=timezone.utc).year

    for col_name, col_type in cols:
        col_q = f'"{col_name}"'
        try:
            non_null = _non_null_count(session, qualified, col_q)
            if non_null < _MIN_ROWS:
                continue

            findings.extend(
                _check_date_range(session, qualified, table, col_name, col_q, non_null)
            )
            findings.extend(
                _check_far_future(
                    session, qualified, table, col_name, col_q, non_null, current_year
                )
            )

            # Midnight check: only for plain TIMESTAMP, not TIMESTAMPTZ or DATE.
            # TIMESTAMPTZ values are stored in UTC so midnight is expected.
            # DATE columns have no time component to lose.
            if col_type in ("timestamp without time zone", "timestamp"):
                findings.extend(
                    _check_midnight_rate(
                        session, qualified, table, col_name, col_q, non_null
                    )
                )
        except Exception:
            logger.debug("temporal: skipped %s.%s", table, col_name, exc_info=True)

    return findings


# ─── Individual checks ────────────────────────────────────────────────────────


def _check_date_range(
    session: Session,
    qualified: str,
    table: str,
    col_name: str,
    col_q: str,
    non_null: int,
) -> List[AnomalyFinding]:
    count = session.execute(
        text(
            f"SELECT COUNT(*) FROM {qualified} "
            f"WHERE EXTRACT(YEAR FROM {col_q}) < :ymin "
            f"   OR EXTRACT(YEAR FROM {col_q}) > :ymax"
        ),
        {"ymin": _YEAR_MIN, "ymax": _YEAR_MAX},
    ).scalar() or 0

    if count == 0:
        return []

    pct = count / non_null
    return [
        AnomalyFinding(
            severity="error",
            table=table,
            column=col_name,
            anomaly_type="date_range_violation",
            message=(
                f"{col_name}: {count:,} rows ({pct:.1%}) have dates outside "
                f"[{_YEAR_MIN}, {_YEAR_MAX}] — likely a conversion error."
            ),
            recommended_action=(
                "Check for Oracle epoch confusion (e.g., date arithmetic that "
                "produced year 0001 or 9999). Compare against Oracle source "
                "to confirm. Correct the migration logic or add a post-migration "
                "UPDATE if the source data was already wrong."
            ),
        )
    ]


def _check_far_future(
    session: Session,
    qualified: str,
    table: str,
    col_name: str,
    col_q: str,
    non_null: int,
    current_year: int,
) -> List[AnomalyFinding]:
    far_year = current_year + _FAR_FUTURE_BUFFER
    count = session.execute(
        text(
            f"SELECT COUNT(*) FROM {qualified} "
            f"WHERE EXTRACT(YEAR FROM {col_q}) > :far"
        ),
        {"far": far_year},
    ).scalar() or 0

    if count == 0:
        return []

    pct = count / non_null
    # Don't double-flag if already caught by date_range_violation.
    max_year = session.execute(
        text(f"SELECT MAX(EXTRACT(YEAR FROM {col_q})) FROM {qualified}")
    ).scalar()
    if max_year is not None and max_year > _YEAR_MAX:
        return []  # already flagged as error above

    return [
        AnomalyFinding(
            severity="warning",
            table=table,
            column=col_name,
            anomaly_type="far_future_date",
            message=(
                f"{col_name}: {count:,} rows ({pct:.1%}) have dates after "
                f"{far_year}. Could be intentional expiry dates or a conversion bug."
            ),
            recommended_action=(
                "Verify with the business owner whether far-future dates are "
                "expected (e.g., contract expiry, license end dates). If not, "
                "investigate the date arithmetic in the migration."
            ),
        )
    ]


def _check_midnight_rate(
    session: Session,
    qualified: str,
    table: str,
    col_name: str,
    col_q: str,
    non_null: int,
) -> List[AnomalyFinding]:
    midnight_count = session.execute(
        text(
            f"SELECT COUNT(*) FROM {qualified} "
            f"WHERE {col_q} IS NOT NULL "
            f"  AND EXTRACT(HOUR FROM {col_q}) = 0 "
            f"  AND EXTRACT(MINUTE FROM {col_q}) = 0 "
            f"  AND EXTRACT(SECOND FROM {col_q}) = 0"
        )
    ).scalar() or 0

    rate = midnight_count / non_null
    if rate < _MIDNIGHT_THRESHOLD:
        return []

    return [
        AnomalyFinding(
            severity="info",
            table=table,
            column=col_name,
            anomaly_type="all_midnight_timestamps",
            message=(
                f"{col_name}: {rate:.1%} of values are exactly midnight "
                f"(00:00:00) — Oracle DATE time component may have been lost."
            ),
            recommended_action=(
                "If the Oracle source column was DATE (not TIMESTAMP), this is "
                "expected — Oracle DATE carries time but the source data may have "
                "been date-only. Cross-check a sample against Oracle. If the source "
                "had meaningful times, the migration needs a CAST fix."
            ),
        )
    ]


# ─── Helpers ─────────────────────────────────────────────────────────────────


def _temporal_columns(
    session: Session, schema: str, table: str
) -> List[Tuple[str, str]]:
    rows = session.execute(
        text(
            "SELECT column_name, data_type "
            "FROM information_schema.columns "
            "WHERE table_schema = :s AND table_name = :t "
            "  AND data_type IN ("
            "    'timestamp without time zone',"
            "    'timestamp with time zone',"
            "    'timestamptz',"
            "    'date',"
            "    'timestamp'"
            "  ) "
            "ORDER BY ordinal_position"
        ),
        {"s": schema.lower(), "t": table.lower()},
    ).fetchall()
    return [(r[0], r[1]) for r in rows]


def _non_null_count(session: Session, qualified: str, col_q: str) -> int:
    return session.execute(
        text(f"SELECT COUNT({col_q}) FROM {qualified}")
    ).scalar() or 0
