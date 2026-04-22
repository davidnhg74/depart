"""FastAPI dependencies that gate routes behind a valid license.

Use `require_feature("ai_conversion")` to protect an endpoint. The
dependency returns the verified LicenseStatus, which handlers can
use for auditing / tier-aware behavior:

    @router.post("/convert/{tag}")
    def convert(
        tag: str,
        license: LicenseStatus = Depends(require_feature("ai_conversion")),
        ...
    ):
        ...

A missing or invalid license raises `402 Payment Required` with a
JSON body that the UI parses to route the operator to the license
upload page. We deliberately do NOT use 401/403: those have
established auth semantics and would get caught by middleware that
attempts re-auth flows.
"""

from __future__ import annotations

from typing import Callable

from fastapi import Depends, HTTPException, status
from sqlalchemy.orm import Session

from ..db import get_db
from .verifier import LicenseStatus, get_license_status


def require_feature(feature: str) -> Callable[..., LicenseStatus]:
    """Return a dependency callable that enforces `feature` being
    present in the current license. Closed over `feature` so the
    router declaration is compact:

        Depends(require_feature("ai_conversion"))
    """

    def dep(db: Session = Depends(get_db)) -> LicenseStatus:
        status_obj = get_license_status(db)
        if not status_obj.valid:
            raise HTTPException(
                status_code=status.HTTP_402_PAYMENT_REQUIRED,
                detail={
                    "error": "license_required",
                    "feature": feature,
                    "reason": status_obj.reason or "no valid license",
                    "upgrade_url": "/settings/instance",
                },
            )
        if not status_obj.has_feature(feature):
            raise HTTPException(
                status_code=status.HTTP_402_PAYMENT_REQUIRED,
                detail={
                    "error": "feature_not_licensed",
                    "feature": feature,
                    "tier": status_obj.tier.value,
                    "reason": f"license tier {status_obj.tier.value!r} does not include {feature!r}",
                    "upgrade_url": "/settings/instance",
                },
            )
        return status_obj

    return dep
