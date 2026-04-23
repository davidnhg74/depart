"""CDC status endpoint.

Single read-only endpoint in this first slice:

    GET /api/v1/migrations/{id}/cdc/status

Returns SCN progress + queue counts so the UI can show "captured
through X, applied through Y, lag Z" while CDC is running. The
start / stop / prepare-cutover endpoints land in the follow-up
session when the LogMiner capture worker is built.

Admin-gated + license-gated by ``ongoing_cdc``. Per
docs/CDC_DESIGN.md.
"""

from __future__ import annotations

import uuid
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.orm import Session

from ..auth.roles import require_role
from ..db import get_db
from ..license.dependencies import require_feature
from ..models import MigrationRecord
from ..services.cdc.queue import queue_status


router = APIRouter(prefix="/api/v1/migrations", tags=["cdc"])


class CdcStatusView(BaseModel):
    migration_id: str
    last_captured_scn: Optional[int]
    last_applied_scn: Optional[int]
    apply_mode: str
    pending_count: int
    applied_count: int
    failed_count: int


def _parse_migration_id(raw: str) -> uuid.UUID:
    try:
        return uuid.UUID(raw)
    except (ValueError, AttributeError):
        raise HTTPException(status_code=404, detail="migration not found")


@router.get("/{migration_id}/cdc/status", response_model=CdcStatusView)
def get_cdc_status(
    migration_id: str,
    db: Session = Depends(get_db),
    _admin=Depends(require_role("admin")),
    _license=Depends(require_feature("ongoing_cdc")),
) -> CdcStatusView:
    mid = _parse_migration_id(migration_id)
    rec = db.get(MigrationRecord, mid)
    if rec is None:
        raise HTTPException(status_code=404, detail="migration not found")
    counts = queue_status(db, mid)
    return CdcStatusView(
        migration_id=str(rec.id),
        last_captured_scn=rec.last_captured_scn,
        last_applied_scn=rec.last_applied_scn,
        apply_mode=rec.cdc_apply_mode,
        pending_count=counts.pending_count,
        applied_count=counts.applied_count,
        failed_count=counts.failed_count,
    )
