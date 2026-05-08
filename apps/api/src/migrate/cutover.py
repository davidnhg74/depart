"""Cutover readiness gate — Layer 9.

Aggregates signals from layers 3–8 and produces a go/no-go verdict
for cutting production traffic from Oracle to PostgreSQL.

Pure module: no DB, no network. The service layer fetches the latest
snapshot from each contributing layer and calls evaluate_readiness().
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

# ─── Public types ─────────────────────────────────────────────────────────────


@dataclass
class ReadinessSignal:
    """One check signal contributing to cutover readiness."""

    layer: str     # "migration_status" | "cdc_lag" | "L6_anomaly" | "L7_monitor" | "L8_sampler"
    label: str     # human-readable name shown in UI
    status: str    # "ok" | "advisory" | "blocking" | "not_run"
    summary: str   # one-line summary
    detail: Optional[str] = None


@dataclass
class CutoverReadiness:
    signals: List[ReadinessSignal]
    blocking_count: int
    advisory_count: int
    not_run_count: int
    ready_to_cut: bool  # True only when blocking_count == 0
    score: int          # 0–100; 100 = all green, 0 = one or more blocking


# ─── Evaluator ────────────────────────────────────────────────────────────────

# CDC lag thresholds (SCN units)
_CDC_ADVISORY = 10_000
_CDC_BLOCKING = 100_000


def evaluate_readiness(
    *,
    migration_status: str,
    last_captured_scn: Optional[int],
    last_applied_scn: Optional[int],
    # L6 anomaly
    anomaly_severity: Optional[str] = None,
    anomaly_tables: int = 0,
    # L7 monitor
    monitor_severity: Optional[str] = None,
    monitor_findings_count: int = 0,
    # L8 sampler
    sample_status: Optional[str] = None,
    sample_mismatch_count: int = 0,
    sample_tables: int = 0,
) -> CutoverReadiness:
    """Evaluate cutover readiness from latest layer snapshots.

    All inputs are optional (layers may not have run yet). Absent layer
    results are reported as *not_run* — advisory, not blocking — so
    operators can cut over without running every check if they accept the
    risk. Only explicit errors from layers that *have* run are blocking.
    """
    signals: List[ReadinessSignal] = []

    # ── Migration status ──────────────────────────────────────────────────────
    if migration_status == "completed":
        signals.append(ReadinessSignal(
            layer="migration_status",
            label="Migration completed",
            status="ok",
            summary="Data movement finished successfully",
        ))
    elif migration_status == "in_progress":
        signals.append(ReadinessSignal(
            layer="migration_status",
            label="Migration completed",
            status="blocking",
            summary="Migration still running — cutover not safe during active transfer",
        ))
    else:
        signals.append(ReadinessSignal(
            layer="migration_status",
            label="Migration completed",
            status="blocking",
            summary=f"Migration in state '{migration_status}' — must be 'completed' before cutover",
        ))

    # ── CDC lag ───────────────────────────────────────────────────────────────
    if last_captured_scn is not None and last_applied_scn is not None:
        lag = last_captured_scn - last_applied_scn
        if lag <= 0:
            signals.append(ReadinessSignal(
                layer="cdc_lag",
                label="CDC lag",
                status="ok",
                summary="CDC fully caught up (no SCN gap)",
            ))
        elif lag < _CDC_ADVISORY:
            signals.append(ReadinessSignal(
                layer="cdc_lag",
                label="CDC lag",
                status="ok",
                summary=f"Minimal CDC lag: {lag:,} SCNs — negligible cutover impact",
            ))
        elif lag < _CDC_BLOCKING:
            signals.append(ReadinessSignal(
                layer="cdc_lag",
                label="CDC lag",
                status="advisory",
                summary=f"CDC lag: {lag:,} SCNs — will close during cutover window",
                detail="Monitor lag trend; proceed if lag is shrinking",
            ))
        else:
            signals.append(ReadinessSignal(
                layer="cdc_lag",
                label="CDC lag",
                status="blocking",
                summary=f"High CDC lag: {lag:,} SCNs — replication not caught up",
                detail="Wait for the apply worker to catch up before cutting over",
            ))
    else:
        signals.append(ReadinessSignal(
            layer="cdc_lag",
            label="CDC lag",
            status="not_run",
            summary="CDC not active — SCN tracking unavailable",
        ))

    # ── Layer 6 — AI anomaly detection ────────────────────────────────────────
    if anomaly_severity is None:
        signals.append(ReadinessSignal(
            layer="L6_anomaly",
            label="AI anomaly check (L6)",
            status="not_run",
            summary="Anomaly check not run — run before cutover to detect data integrity issues",
        ))
    elif anomaly_severity in ("clean", "info"):
        signals.append(ReadinessSignal(
            layer="L6_anomaly",
            label="AI anomaly check (L6)",
            status="ok",
            summary=f"No anomalies across {anomaly_tables} table{'' if anomaly_tables == 1 else 's'}",
        ))
    elif anomaly_severity == "warning":
        signals.append(ReadinessSignal(
            layer="L6_anomaly",
            label="AI anomaly check (L6)",
            status="advisory",
            summary=f"Anomaly warnings on {anomaly_tables} table{'' if anomaly_tables == 1 else 's'} — review before cutover",
        ))
    else:  # error
        signals.append(ReadinessSignal(
            layer="L6_anomaly",
            label="AI anomaly check (L6)",
            status="blocking",
            summary="Anomaly errors detected — data integrity risk requires investigation",
        ))

    # ── Layer 7 — production monitor ──────────────────────────────────────────
    if monitor_severity is None:
        signals.append(ReadinessSignal(
            layer="L7_monitor",
            label="Production monitor (L7)",
            status="not_run",
            summary="Production monitor not run — run to check row drift, bloat, and CDC lag",
        ))
    elif monitor_severity in ("clean", "info"):
        signals.append(ReadinessSignal(
            layer="L7_monitor",
            label="Production monitor (L7)",
            status="ok",
            summary="No monitor findings — target database healthy",
        ))
    elif monitor_severity == "warning":
        signals.append(ReadinessSignal(
            layer="L7_monitor",
            label="Production monitor (L7)",
            status="advisory",
            summary=f"{monitor_findings_count} warning finding{'' if monitor_findings_count == 1 else 's'} — review bloat or drift",
        ))
    else:  # error
        signals.append(ReadinessSignal(
            layer="L7_monitor",
            label="Production monitor (L7)",
            status="blocking",
            summary=f"{monitor_findings_count} critical finding{'' if monitor_findings_count == 1 else 's'} — target database not ready",
        ))

    # ── Layer 8 — row-level sampler ───────────────────────────────────────────
    if sample_status is None:
        signals.append(ReadinessSignal(
            layer="L8_sampler",
            label="Row-level sampler (L8)",
            status="not_run",
            summary="Data sampler not run — run to verify row-level data integrity",
        ))
    elif sample_status == "clean":
        signals.append(ReadinessSignal(
            layer="L8_sampler",
            label="Row-level sampler (L8)",
            status="ok",
            summary=f"No mismatches across {sample_tables} sampled table{'' if sample_tables == 1 else 's'}",
        ))
    else:  # mismatches_found
        signals.append(ReadinessSignal(
            layer="L8_sampler",
            label="Row-level sampler (L8)",
            status="blocking",
            summary=f"{sample_mismatch_count} column mismatch{'' if sample_mismatch_count == 1 else 'es'} detected in sample",
            detail="Investigate and resolve all mismatches before cutting over",
        ))

    # ── Aggregate ─────────────────────────────────────────────────────────────
    blocking = sum(1 for s in signals if s.status == "blocking")
    advisory = sum(1 for s in signals if s.status == "advisory")
    not_run = sum(1 for s in signals if s.status == "not_run")

    # Score: each blocking costs 30 pts, advisory 10, not_run 5.
    score = max(0, 100 - blocking * 30 - advisory * 10 - not_run * 5)

    return CutoverReadiness(
        signals=signals,
        blocking_count=blocking,
        advisory_count=advisory,
        not_run_count=not_run,
        ready_to_cut=blocking == 0,
        score=score,
    )
