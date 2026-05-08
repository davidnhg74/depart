"""Layer 7 — production monitor.

Runs against the target PostgreSQL after migration cutover. Collects
health metrics and flags drift, bloat, and replication lag.

Three deterministic checks (no AI required):
  row_drift         — table row counts changed significantly vs baseline
  dead_tuple_bloat  — VACUUM is not keeping up with dead rows
  cdc_lag           — applied SCN is far behind captured SCN

All SQL is aggregate-only against pg_stat_user_tables; safe on
terabyte tables and requires no superuser privileges.
"""

from __future__ import annotations

import logging
from dataclasses import asdict, dataclass
from typing import Dict, List, Literal, Optional, Tuple

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

# Row drift thresholds (fraction of baseline).
_ROW_DRIFT_WARN = 0.05   # 5% change → warning
_ROW_DRIFT_ERR  = 0.20   # 20% change or total table loss → error

# Dead-tuple bloat thresholds (dead / (live + dead)).
_BLOAT_WARN = 0.20
_BLOAT_ERR  = 0.50

# CDC SCN lag thresholds.
_CDC_LAG_WARN = 10_000
_CDC_LAG_ERR  = 100_000

# Minimum live rows before we bother reporting bloat for a table.
_BLOAT_MIN_ROWS = 100


# ─── Finding ─────────────────────────────────────────────────────────────────


@dataclass
class MonitorFinding:
    severity: Literal["info", "warning", "error"]
    check_name: str  # row_drift | dead_tuple_bloat | cdc_lag
    table: Optional[str]
    message: str
    recommended_action: str

    def to_dict(self) -> dict:
        return asdict(self)


# ─── Checks ──────────────────────────────────────────────────────────────────


def collect_row_counts(session: Session, schema: str) -> Dict[str, int]:
    """Return {table_name: live_row_count} from pg_stat_user_tables.

    Uses the stats daemon's estimate — no full scan, no lock.
    """
    sql = text(
        """
        SELECT relname AS table_name, n_live_tup AS row_count
        FROM   pg_stat_user_tables
        WHERE  schemaname = :schema
        ORDER  BY n_live_tup DESC
        """
    )
    try:
        rows = session.execute(sql, {"schema": schema}).mappings().all()
        return {str(r["table_name"]): int(r["row_count"]) for r in rows}
    except Exception as exc:
        logger.warning("collect_row_counts failed: %s", exc)
        return {}


def check_row_drift(
    current_counts: Dict[str, int],
    baseline_counts: Dict[str, int],
) -> List[MonitorFinding]:
    """Compare current row counts against baseline. Skip if no baseline."""
    findings: List[MonitorFinding] = []
    if not baseline_counts:
        return findings

    for table, baseline in baseline_counts.items():
        current = current_counts.get(table)
        if current is None:
            # Table missing entirely.
            if baseline > 0:
                findings.append(
                    MonitorFinding(
                        severity="error",
                        check_name="row_drift",
                        table=table,
                        message=f"Table '{table}' no longer visible in pg_stat_user_tables "
                                f"(baseline: {baseline:,} rows).",
                        recommended_action="Verify ANALYZE has run and the table still exists.",
                    )
                )
            continue

        if baseline == 0:
            continue  # Can't compute meaningful drift from zero baseline.

        drift = abs(current - baseline) / baseline
        if drift >= _ROW_DRIFT_ERR:
            sev: Literal["info", "warning", "error"] = "error"
        elif drift >= _ROW_DRIFT_WARN:
            sev = "warning"
        else:
            continue

        direction = "grown" if current > baseline else "shrunk"
        findings.append(
            MonitorFinding(
                severity=sev,
                check_name="row_drift",
                table=table,
                message=(
                    f"Table '{table}' has {direction} by {drift:.1%}: "
                    f"{baseline:,} → {current:,} rows."
                ),
                recommended_action=(
                    "Confirm the change is expected (backfill, purge, CDC). "
                    "Run ANALYZE to refresh statistics."
                ),
            )
        )

    return findings


def check_dead_tuple_bloat(session: Session, schema: str) -> List[MonitorFinding]:
    """Flag tables where dead tuples exceed bloat thresholds."""
    findings: List[MonitorFinding] = []
    sql = text(
        """
        SELECT relname         AS table_name,
               n_live_tup      AS live_rows,
               n_dead_tup      AS dead_rows
        FROM   pg_stat_user_tables
        WHERE  schemaname = :schema
          AND  n_live_tup  > :min_rows
          AND  n_dead_tup  > 0
        ORDER  BY n_dead_tup DESC
        """
    )
    try:
        rows = session.execute(sql, {"schema": schema, "min_rows": _BLOAT_MIN_ROWS}).mappings().all()
    except Exception as exc:
        logger.warning("check_dead_tuple_bloat failed: %s", exc)
        return findings

    for r in rows:
        live = int(r["live_rows"])
        dead = int(r["dead_rows"])
        total = live + dead
        if total == 0:
            continue
        bloat = dead / total
        if bloat >= _BLOAT_ERR:
            sev: Literal["info", "warning", "error"] = "error"
        elif bloat >= _BLOAT_WARN:
            sev = "warning"
        else:
            continue

        table = str(r["table_name"])
        findings.append(
            MonitorFinding(
                severity=sev,
                check_name="dead_tuple_bloat",
                table=table,
                message=(
                    f"Table '{table}' has {bloat:.1%} dead tuples "
                    f"({dead:,} dead / {total:,} total)."
                ),
                recommended_action="Run VACUUM ANALYZE on this table to reclaim space.",
            )
        )

    return findings


def check_cdc_lag(
    captured_scn: Optional[int],
    applied_scn: Optional[int],
) -> List[MonitorFinding]:
    """Flag if the CDC applied SCN lags significantly behind captured SCN."""
    if captured_scn is None or applied_scn is None:
        return []

    lag = captured_scn - applied_scn
    if lag < 0:
        # applied_scn ahead of captured_scn — impossible in healthy state, log only.
        logger.warning("cdc_lag: applied_scn (%d) > captured_scn (%d)", applied_scn, captured_scn)
        return []

    if lag >= _CDC_LAG_ERR:
        sev: Literal["info", "warning", "error"] = "error"
    elif lag >= _CDC_LAG_WARN:
        sev = "warning"
    else:
        return []

    return [
        MonitorFinding(
            severity=sev,
            check_name="cdc_lag",
            table=None,
            message=f"CDC replication lag: {lag:,} SCNs behind (captured={captured_scn}, applied={applied_scn}).",
            recommended_action=(
                "Check the CDC apply worker is running and the target PG connection is healthy."
            ),
        )
    ]


# ─── Severity rollup ─────────────────────────────────────────────────────────


def overall_severity(findings: List[MonitorFinding]) -> str:
    if any(f.severity == "error" for f in findings):
        return "error"
    if any(f.severity == "warning" for f in findings):
        return "warning"
    if findings:
        return "info"
    return "clean"


# ─── Orchestration ───────────────────────────────────────────────────────────


def run_monitor(
    session: Session,
    schema: str,
    baseline_counts: Dict[str, int],
    captured_scn: Optional[int] = None,
    applied_scn: Optional[int] = None,
) -> Tuple[List[MonitorFinding], Dict[str, int]]:
    """Run all checks. Returns (findings, current_row_counts).

    current_row_counts is persisted by the service layer so the next
    invocation can use it as the new baseline.
    """
    current_counts = collect_row_counts(session, schema)
    findings: List[MonitorFinding] = []
    findings += check_row_drift(current_counts, baseline_counts)
    findings += check_dead_tuple_bloat(session, schema)
    findings += check_cdc_lag(captured_scn, applied_scn)
    return findings, current_counts
