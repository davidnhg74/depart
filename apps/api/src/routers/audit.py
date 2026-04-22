"""Audit log reader endpoint.

Admins + viewers can see the full audit trail. Operators are
deliberately NOT granted read access — they can already see the
actions they trigger via the migrations / runbook UIs, and keeping
the audit log as an admin-oversight surface matches how compliance
teams want it scoped.

Pagination is cursor-less (simple offset/limit) — fine at the
hundreds-to-thousands row scale this table stays at. If any install
exceeds ~100K events we'll move to id-keyset pagination then.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, List, Optional

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel
from sqlalchemy.orm import Session

from ..auth.roles import require_role
from ..db import get_db
from ..models import AuditEvent
from ..services.audit import verify_chain
from ..utils.time import utc_now


router = APIRouter(prefix="/api/v1/audit", tags=["audit"])


# ─── Schema ─────────────────────────────────────────────────────────────────


class AuditEventResponse(BaseModel):
    id: str
    user_email: Optional[str]
    action: str
    resource_type: Optional[str]
    resource_id: Optional[str]
    details: Optional[dict[str, Any]]
    ip: Optional[str]
    created_at: datetime


class AuditPage(BaseModel):
    """Paginated response. `total` is the count that matches the filter
    so the UI can render 'X of Y results' and detect when to hide
    load-more."""

    items: List[AuditEventResponse]
    total: int
    limit: int
    offset: int


# ─── Handler ─────────────────────────────────────────────────────────────────


@router.get("", response_model=AuditPage)
def list_events(
    db: Session = Depends(get_db),
    _caller=Depends(require_role("admin", "viewer")),
    action: Optional[str] = Query(default=None, max_length=64),
    days: int = Query(default=30, ge=1, le=3650),
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
) -> AuditPage:
    """Return events newest-first. Filters:
      * `action` — exact match on the verb (e.g. "migration.run")
      * `days` — lookback window; defaults to 30
      * `limit` / `offset` — standard pagination
    """
    cutoff = utc_now() - timedelta(days=days)

    q = db.query(AuditEvent).filter(AuditEvent.created_at >= cutoff)
    if action:
        q = q.filter(AuditEvent.action == action)

    total = q.count()
    rows = (
        q.order_by(AuditEvent.created_at.desc())
        .limit(limit)
        .offset(offset)
        .all()
    )

    return AuditPage(
        items=[
            AuditEventResponse(
                id=str(r.id),
                user_email=r.user_email,
                action=r.action,
                resource_type=r.resource_type,
                resource_id=r.resource_id,
                details=r.details,
                ip=r.ip,
                created_at=r.created_at,
            )
            for r in rows
        ],
        total=total,
        limit=limit,
        offset=offset,
    )


# ─── Integrity verification ─────────────────────────────────────────────────


class VerifyResponse(BaseModel):
    ok: bool
    checked: int
    first_break: Optional[dict[str, Any]]


@router.get("/verify", response_model=VerifyResponse)
def verify(
    db: Session = Depends(get_db),
    _caller=Depends(require_role("admin", "viewer")),
) -> VerifyResponse:
    """Walk the audit chain and recompute every row's hash. Returns
    ok=true with the row count when the chain is intact; ok=false plus
    the first breakpoint's details when tampering is detected.

    A break at row N means row N was modified OR some row before N was
    deleted — both manifest identically because they both break the
    prev_hash→row_hash relationship at that point."""
    return VerifyResponse(**verify_chain(db))
