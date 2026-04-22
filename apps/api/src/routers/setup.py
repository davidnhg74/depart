"""First-run bootstrap for self-hosted installs.

A fresh `docker compose up` has no users. Before anyone can log in we
need exactly one admin. Two ways to create that admin:

  1. **Env-driven auto-bootstrap.** If DEPART_ADMIN_EMAIL and
     DEPART_ADMIN_PASSWORD are set when the app starts AND no users
     exist, we silently create the admin. Good for scripted / CI /
     air-gapped installs.

  2. **First-run UI screen.** /setup in the web app POSTs to
     /api/v1/setup/bootstrap with email + password + full_name.
     Only works when no users exist — after that the endpoint 409s.

Either path writes a single row to the users table with role=admin.
From then on the admin logs in via the normal auth endpoints and can
create additional operator / viewer users from /settings/users.
"""

from __future__ import annotations

import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Request, status
from pydantic import BaseModel, EmailStr, Field
from sqlalchemy.orm import Session

from ..auth.password import hash_password
from ..db import get_db
from ..models import User, UserRole
from ..services.audit import log_event
from ..utils.time import utc_now


logger = logging.getLogger(__name__)


router = APIRouter(prefix="/api/v1/setup", tags=["setup"])


# ─── Schemas ─────────────────────────────────────────────────────────────────


class SetupStatus(BaseModel):
    """The frontend polls this on load. When `needs_bootstrap=true`,
    the web app redirects to /setup instead of /login."""

    needs_bootstrap: bool
    admin_count: int


class BootstrapRequest(BaseModel):
    email: EmailStr
    password: str = Field(..., min_length=8, max_length=200)
    full_name: Optional[str] = Field(default=None, max_length=200)


# ─── Endpoints ───────────────────────────────────────────────────────────────


@router.get("/status", response_model=SetupStatus)
def setup_status(db: Session = Depends(get_db)) -> SetupStatus:
    admin_count = db.query(User).filter(User.role == UserRole.ADMIN).count()
    return SetupStatus(needs_bootstrap=admin_count == 0, admin_count=admin_count)


@router.post("/bootstrap", response_model=SetupStatus, status_code=status.HTTP_201_CREATED)
def bootstrap(
    body: BootstrapRequest, request: Request, db: Session = Depends(get_db)
) -> SetupStatus:
    """Create the initial admin. 409 if any admin already exists."""
    if db.query(User).filter(User.role == UserRole.ADMIN).first() is not None:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="admin already exists — bootstrap has already been run",
        )

    now = utc_now()
    admin = User(
        email=body.email,
        full_name=body.full_name,
        hashed_password=hash_password(body.password),
        role=UserRole.ADMIN,
        email_verified=True,  # no email flow to run — trust the bootstrap caller
        is_active=True,
        trial_starts_at=now,
        trial_expires_at=now,
    )
    db.add(admin)
    db.commit()
    db.refresh(admin)

    log_event(
        db,
        request=request,
        user=admin,
        action="install.bootstrapped",
        resource_type="user",
        resource_id=str(admin.id),
        details={"email": admin.email},
    )
    logger.info("bootstrapped initial admin %s", body.email)
    return SetupStatus(needs_bootstrap=False, admin_count=1)


# ─── Startup hook for env-driven bootstrap ───────────────────────────────────


def maybe_bootstrap_from_env(db: Session) -> None:
    """Called from main.py's startup event. If DEPART_ADMIN_EMAIL and
    DEPART_ADMIN_PASSWORD are set AND no admin exists yet, create one.
    Silent success + silent skip — only fail if creation itself errors."""
    import os

    email = os.environ.get("DEPART_ADMIN_EMAIL")
    password = os.environ.get("DEPART_ADMIN_PASSWORD")
    if not email or not password:
        return

    if db.query(User).filter(User.role == UserRole.ADMIN).first() is not None:
        return

    now = utc_now()
    admin = User(
        email=email,
        full_name=os.environ.get("DEPART_ADMIN_NAME"),
        hashed_password=hash_password(password),
        role=UserRole.ADMIN,
        email_verified=True,
        is_active=True,
        trial_starts_at=now,
        trial_expires_at=now,
    )
    db.add(admin)
    db.commit()
    logger.info("auto-bootstrapped admin %s from DEPART_ADMIN_* env vars", email)
