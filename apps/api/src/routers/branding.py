"""White-label branding endpoints.

GET is unauthenticated — the UI calls it before login to render the
correct logo/title on the sign-in page. PUT is admin-only AND license-
gated on the `white_label` feature; without that feature the endpoint
returns 403, so a Community-tier install can read defaults but not
override them.
"""

from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Request, status
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from ..auth.roles import require_role
from ..db import get_db
from ..license.verifier import get_license_status
from ..services.audit import log_event
from ..services.branding_service import (
    WHITE_LABEL_FEATURE,
    BrandingConfig,
    BrandingUpdate,
    BrandingValidationError,
    get_branding,
    update_branding,
)


router = APIRouter(prefix="/api/v1/branding", tags=["branding"])


# ─── Schemas ─────────────────────────────────────────────────────────────────


class BrandingResponse(BaseModel):
    company_name: str
    product_name: str
    logo_url: Optional[str]
    primary_color: str
    support_email: str
    white_label_enabled: bool  # mirrors the license; UI uses it to show the editor


class BrandingUpdateRequest(BaseModel):
    """All fields optional — sending null/empty for any field clears
    the override and reverts that field to the default. Omitted fields
    are not implemented as 'no change'; the contract is 'PUT replaces
    the override set' to keep the surface small."""

    company_name: Optional[str] = Field(default=None, max_length=255)
    product_name: Optional[str] = Field(default=None, max_length=255)
    logo_url: Optional[str] = Field(default=None, max_length=2000)
    primary_color: Optional[str] = Field(default=None, max_length=7)
    support_email: Optional[str] = Field(default=None, max_length=255)


# ─── Handlers ────────────────────────────────────────────────────────────────


def _to_response(cfg: BrandingConfig, *, white_label_enabled: bool) -> BrandingResponse:
    return BrandingResponse(
        company_name=cfg.company_name,
        product_name=cfg.product_name,
        logo_url=cfg.logo_url,
        primary_color=cfg.primary_color,
        support_email=cfg.support_email,
        white_label_enabled=white_label_enabled,
    )


@router.get("", response_model=BrandingResponse)
def get_current(db: Session = Depends(get_db)) -> BrandingResponse:
    """Public endpoint — no auth. Used by the UI to render the sign-in
    page with the operator's branding before any user is logged in."""
    cfg = get_branding(db)
    license_status = get_license_status(db)
    return _to_response(
        cfg, white_label_enabled=license_status.has_feature(WHITE_LABEL_FEATURE)
    )


@router.put("", response_model=BrandingResponse)
def put_branding(
    body: BrandingUpdateRequest,
    request: Request,
    db: Session = Depends(get_db),
    caller=Depends(require_role("admin")),
) -> BrandingResponse:
    """Replace the current branding override set. Admin role + an
    active license carrying `white_label` are both required."""
    license_status = get_license_status(db)
    if not license_status.has_feature(WHITE_LABEL_FEATURE):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="white_label feature not enabled by current license",
        )

    update = BrandingUpdate(
        company_name=body.company_name,
        product_name=body.product_name,
        logo_url=body.logo_url,
        primary_color=body.primary_color,
        support_email=body.support_email,
    )
    try:
        cfg = update_branding(db, update)
    except BrandingValidationError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    log_event(
        db,
        request=request,
        user=caller,
        action="branding.updated",
        details={
            "company_name": cfg.company_name,
            "product_name": cfg.product_name,
            "primary_color": cfg.primary_color,
            "support_email": cfg.support_email,
            "logo_url": cfg.logo_url,
        },
    )
    return _to_response(cfg, white_label_enabled=True)
