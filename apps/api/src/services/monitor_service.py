"""Service layer for Validation Layer 7 — production monitor.

Orchestrates: target PG connection → row-count collection + three health
checks → DB write → result.

Called from:
  * POST /api/v1/migrations/{id}/monitor  (on-demand)
"""

from __future__ import annotations

import json
import logging
import uuid
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from ..migrate.monitor import (
    MonitorFinding,
    overall_severity,
    run_monitor,
)
from ..models import MigrationRecord, ProductionMonitorSnapshot
from ..utils.time import utc_now

logger = logging.getLogger(__name__)


@dataclass
class MonitorResult:
    findings: List[MonitorFinding]
    overall_severity: str
    snapshot_id: str
    tables_checked: int
    table_row_counts: Dict[str, int]

    def to_response_dict(self) -> Dict[str, Any]:
        return {
            "findings": [f.to_dict() for f in self.findings],
            "overall_severity": self.overall_severity,
            "snapshot_id": self.snapshot_id,
            "tables_checked": self.tables_checked,
        }


def monitor_migration(record: MigrationRecord, db: Session) -> MonitorResult:
    """Run Layer 7 production monitor against a completed migration.

    Loads the most recent previous snapshot to use as the row-count
    baseline. On first invocation there is no baseline, so row_drift
    is skipped — only bloat and CDC lag are checked. The new snapshot
    is always persisted so the *next* call has a baseline.
    """
    if not record.target_url:
        raise ValueError("Migration has no target_url — cannot run production monitor.")

    target_schema = record.target_schema or record.source_schema

    # Load baseline from the most recent previous snapshot.
    prev = (
        db.query(ProductionMonitorSnapshot)
        .filter(ProductionMonitorSnapshot.migration_id == record.id)
        .order_by(ProductionMonitorSnapshot.created_at.desc())
        .first()
    )
    baseline_counts: Dict[str, int] = {}
    if prev and prev.table_row_counts:
        raw = prev.table_row_counts
        baseline_counts = raw if isinstance(raw, dict) else json.loads(raw)

    engine = create_engine(record.target_url)
    Sess = sessionmaker(bind=engine)
    dst = Sess()
    try:
        findings, current_counts = run_monitor(
            dst,
            target_schema,
            baseline_counts,
            captured_scn=record.last_captured_scn,
            applied_scn=record.last_applied_scn,
        )
    finally:
        dst.close()
        engine.dispose()

    sev = overall_severity(findings)
    snapshot_id = str(uuid.uuid4())

    row = ProductionMonitorSnapshot(
        id=uuid.UUID(snapshot_id),
        user_id=record.user_id,
        migration_id=record.id,
        table_row_counts=current_counts,
        findings=[f.to_dict() for f in findings],
        overall_severity=sev,
        tables_checked=len(current_counts),
        created_at=utc_now(),
    )
    db.add(row)
    db.commit()

    return MonitorResult(
        findings=findings,
        overall_severity=sev,
        snapshot_id=snapshot_id,
        tables_checked=len(current_counts),
        table_row_counts=current_counts,
    )
