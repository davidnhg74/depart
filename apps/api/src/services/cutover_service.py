"""Layer 9 — cutover readiness service.

Fetches the latest Layer 6/7/8 snapshots for a migration,
calls evaluate_readiness(), persists the verdict, and returns it.
"""

from __future__ import annotations

from dataclasses import asdict

from sqlalchemy.orm import Session

from ..migrate.cutover import CutoverReadiness, evaluate_readiness
from ..models import (
    AnomalyAnalysis,
    CutoverReadinessSnapshot,
    DataSampleResult,
    MigrationRecord,
    ProductionMonitorSnapshot,
)


def assess_cutover_readiness(
    record: MigrationRecord,
    db: Session,
) -> tuple[CutoverReadiness, CutoverReadinessSnapshot]:
    """Evaluate and persist a cutover readiness snapshot for *record*."""

    anomaly: AnomalyAnalysis | None = (
        db.query(AnomalyAnalysis)
        .filter(AnomalyAnalysis.migration_id == record.id)
        .order_by(AnomalyAnalysis.created_at.desc())
        .first()
    )

    monitor: ProductionMonitorSnapshot | None = (
        db.query(ProductionMonitorSnapshot)
        .filter(ProductionMonitorSnapshot.migration_id == record.id)
        .order_by(ProductionMonitorSnapshot.created_at.desc())
        .first()
    )

    sample: DataSampleResult | None = (
        db.query(DataSampleResult)
        .filter(DataSampleResult.migration_id == record.id)
        .order_by(DataSampleResult.created_at.desc())
        .first()
    )

    readiness = evaluate_readiness(
        migration_status=record.status,
        last_captured_scn=record.last_captured_scn,
        last_applied_scn=record.last_applied_scn,
        anomaly_severity=anomaly.overall_severity if anomaly else None,
        anomaly_tables=anomaly.tables_sampled if anomaly else 0,
        monitor_severity=monitor.overall_severity if monitor else None,
        monitor_findings_count=len(monitor.findings or []) if monitor else 0,
        sample_status=sample.overall_status if sample else None,
        sample_mismatch_count=sample.mismatch_count if sample else 0,
        sample_tables=sample.tables_sampled if sample else 0,
    )

    snap = CutoverReadinessSnapshot(
        migration_id=record.id,
        user_id=record.user_id,
        signals=[asdict(s) for s in readiness.signals],
        blocking_count=readiness.blocking_count,
        advisory_count=readiness.advisory_count,
        not_run_count=readiness.not_run_count,
        ready_to_cut=readiness.ready_to_cut,
        score=readiness.score,
    )
    db.add(snap)
    db.commit()
    db.refresh(snap)
    return readiness, snap
