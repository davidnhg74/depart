"""Write-only helper for the audit_events table.

One function — `log_event` — that routers call at the end of mutating
operations. Deliberately not wrapped in middleware: a route-by-route
call site forces us to think about what the audit message actually
says (what fields in `details`, what `resource_id` means for this
verb), which middleware can't do reliably.

Each write extends a SHA-256 hash chain: the new row's `row_hash` is
computed over (prev_hash || canonical payload), where prev_hash is
the row_hash of the most recent event. A tamper-evident audit trail
falls out naturally — mutate or delete any row and every subsequent
row's recomputed hash no longer matches what's stored. See
`verify_chain()` for the verification walk.

Failure mode: audit writes must never break the user's request. We
catch + swallow inside `log_event`. The only realistic failure is a
DB outage, in which case the entire app is already degraded. A
production deployment can tail the logs for `audit.log_event failed`.
"""

from __future__ import annotations

import hashlib
import json
import logging
from datetime import datetime
from typing import Any, Optional

from fastapi import Request
from sqlalchemy.orm import Session

from ..models import AuditEvent, User
from ..utils.time import utc_now


logger = logging.getLogger(__name__)


def log_event(
    db: Session,
    *,
    request: Optional[Request],
    user: Optional[User],
    action: str,
    resource_type: Optional[str] = None,
    resource_id: Optional[str] = None,
    details: Optional[dict[str, Any]] = None,
    # Some actions (failed login) know the attempted email without
    # having a User row yet. Allow callers to pass it explicitly.
    user_email_override: Optional[str] = None,
) -> None:
    """Append one row to audit_events. Never raises."""
    try:
        # created_at is computed here (not defaulted) so the hash
        # includes the exact timestamp we serialize.
        created_at = utc_now()
        user_email = user_email_override or (user.email if user is not None else None)

        # Fetch the most-recent row's hash to chain from. We scan by
        # (created_at DESC, id DESC) so the ordering matches what
        # verify_chain uses.
        prev = (
            db.query(AuditEvent.row_hash)
            .order_by(AuditEvent.created_at.desc(), AuditEvent.id.desc())
            .first()
        )
        prev_hash = prev[0] if prev and prev[0] else None
        row_hash = _compute_hash(
            prev_hash=prev_hash,
            action=action,
            user_email=user_email,
            created_at=created_at,
            details=details,
            resource_type=resource_type,
            resource_id=resource_id,
        )

        event = AuditEvent(
            user_id=user.id if user is not None else None,
            user_email=user_email,
            action=action,
            resource_type=resource_type,
            resource_id=resource_id,
            details=details,
            ip=_client_ip(request) if request else None,
            user_agent=(request.headers.get("user-agent") if request else None),
            created_at=created_at,
            prev_hash=prev_hash,
            row_hash=row_hash,
        )
        db.add(event)
        db.commit()
    except Exception as exc:  # noqa: BLE001 — audit must never break the request
        logger.warning("audit.log_event failed for %s: %s", action, exc)
        try:
            db.rollback()
        except Exception:
            pass


# ─── Hash chain ──────────────────────────────────────────────────────────────


def _compute_hash(
    *,
    prev_hash: Optional[str],
    action: str,
    user_email: Optional[str],
    created_at: datetime,
    details: Optional[dict],
    resource_type: Optional[str],
    resource_id: Optional[str],
) -> str:
    """Canonical hash input. Fields concatenated with `|` separators.

    The canonicalization matters for verify_chain: small changes
    (JSON key order, datetime precision) mean the chain won't match
    even for legitimate rows. We sort details keys and use ISO 8601
    to keep both writers deterministic."""
    parts = [
        prev_hash or "",
        action,
        user_email or "",
        created_at.isoformat(),
        json.dumps(details or {}, sort_keys=True, default=str),
        resource_type or "",
        resource_id or "",
    ]
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()


def verify_chain(db: Session) -> dict[str, Any]:
    """Walk the audit chain from oldest to newest and recompute each
    hash. Returns a dict the API endpoint can render:

        { ok: bool, checked: int, first_break: { id, expected, stored } | None }

    A "break" is the first row whose stored hash doesn't match the
    one we recompute from its content + the previous row's stored
    hash. Deletes and updates both show up as a break at exactly the
    row that was tampered with."""
    rows = (
        db.query(AuditEvent)
        .order_by(AuditEvent.created_at.asc(), AuditEvent.id.asc())
        .all()
    )
    prev = ""
    for r in rows:
        expected = _compute_hash(
            prev_hash=prev or None,
            action=r.action,
            user_email=r.user_email,
            created_at=r.created_at,
            details=r.details,
            resource_type=r.resource_type,
            resource_id=r.resource_id,
        )
        if expected != (r.row_hash or ""):
            return {
                "ok": False,
                "checked": len([x for x in rows if x.created_at < r.created_at]),
                "first_break": {
                    "id": str(r.id),
                    "action": r.action,
                    "created_at": r.created_at.isoformat(),
                    "expected": expected,
                    "stored": r.row_hash,
                },
            }
        prev = r.row_hash or ""
    return {"ok": True, "checked": len(rows), "first_break": None}


# ─── Misc ────────────────────────────────────────────────────────────────────


def _client_ip(request: Request) -> Optional[str]:
    """Best-effort client-IP extraction. If the operator puts depart
    behind a reverse proxy, `X-Forwarded-For` or `X-Real-IP` will
    carry the real address; fall back to the socket peer otherwise."""
    xff = request.headers.get("x-forwarded-for")
    if xff:
        # Leftmost entry is the original client by convention.
        return xff.split(",")[0].strip()[:45]
    real = request.headers.get("x-real-ip")
    if real:
        return real.strip()[:45]
    if request.client and request.client.host:
        return request.client.host[:45]
    return None
