"""License status + upload endpoint.

Like /settings, this is local-only: self-hosted, localhost, no auth.
The UI at /settings/instance renders whatever GET returns and lets
the operator paste a JWT into the PUT body.
"""

from __future__ import annotations

from datetime import datetime
from typing import List, Optional

from fastapi import APIRouter, Depends, Request
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from ..auth.roles import require_role
from ..db import get_db
from ..license.verifier import get_license_status, verify
from ..services.audit import log_event
from ..services.settings_service import set_license_jwt


router = APIRouter(prefix="/api/v1/license", tags=["license"])


# ─── Schemas ─────────────────────────────────────────────────────────────────


class LicenseStatusResponse(BaseModel):
    """Everything the /settings/instance UI needs to render the
    license section. `valid=False` with `reason` surfaces the specific
    failure (no license uploaded / expired / tampered) so the operator
    can act on it."""

    valid: bool
    tier: str  # "community" | "pro" | "enterprise"
    features: List[str]
    expires_at: Optional[datetime]
    subject: Optional[str]
    project: Optional[str]
    reason: Optional[str]


class LicenseUploadRequest(BaseModel):
    """PUT body. Empty string or null clears the license (reverts to
    Community)."""

    jwt: Optional[str] = Field(default=None, max_length=4000)


# ─── Handlers ────────────────────────────────────────────────────────────────


def _to_response(status) -> LicenseStatusResponse:
    return LicenseStatusResponse(
        valid=status.valid,
        tier=status.tier.value,
        features=list(status.features),
        expires_at=status.expires_at,
        subject=status.subject,
        project=status.project,
        reason=status.reason,
    )


@router.get("", response_model=LicenseStatusResponse)
def get_status(
    db: Session = Depends(get_db),
    _caller=Depends(require_role("admin", "operator", "viewer")),
) -> LicenseStatusResponse:
    return _to_response(get_license_status(db))


@router.put("", response_model=LicenseStatusResponse)
def upload_license(
    body: LicenseUploadRequest,
    request: Request,
    db: Session = Depends(get_db),
    caller=Depends(require_role("admin")),
) -> LicenseStatusResponse:
    """Store the uploaded JWT after a best-effort verification. Even if
    the token is invalid we still store it — the subsequent GET will
    surface the reason, which makes misconfigurations easier to
    diagnose than silently rejecting the upload."""
    # Verify first, but don't reject on failure — store whatever the
    # operator uploaded and let the status call explain what's wrong.
    set_license_jwt(db, body.jwt)
    status_after = get_license_status(db)
    log_event(
        db,
        request=request,
        user=caller,
        action="license.uploaded" if body.jwt else "license.cleared",
        details={
            "valid": status_after.valid,
            "tier": status_after.tier.value,
            "project": status_after.project,
        },
    )
    return _to_response(status_after)
