"""Settings endpoints — local, no auth.

This is a self-hosted product: the operator running `docker compose`
is the admin. Anyone who can reach localhost:8000 inside their own
firewall can already configure the instance at the OS level, so
hiding these endpoints behind application auth doesn't add meaningful
security. We do mask the stored key on GET so casual shoulder-surfing
through the UI doesn't leak it.
"""

from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends, Request
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session
from sqlalchemy.orm.attributes import flag_modified

from ..auth.roles import require_role
from ..db import get_db
from ..models import IdentityProvider, InstanceSettings, MigrationRecord
from ..services.audit import log_event
from ..services.crypto import encrypt, has_encryption_key
from ..services.settings_service import (
    get_instance_settings,
    mask_key,
    set_anthropic_key,
)


router = APIRouter(prefix="/api/v1/settings", tags=["settings"])


# ─── Schemas ─────────────────────────────────────────────────────────────────


class SettingsStatus(BaseModel):
    """What the /settings UI renders. Keys come back masked; the
    `*_configured` booleans let the UI show a green check without
    having to reason about the masked value.

    `encryption_key_configured` tells the UI whether sensitive columns
    are being encrypted at rest. When false, the UI surfaces a warning
    pointing the operator at DEPART_ENCRYPTION_KEY."""

    anthropic_key_masked: Optional[str]
    anthropic_key_configured: bool
    license_configured: bool
    encryption_key_configured: bool


class RotateKeyResponse(BaseModel):
    rotated: int  # number of rows re-encrypted
    ok: bool


class AnthropicKeyUpdate(BaseModel):
    """PUT body. Empty string or null clears the key."""

    api_key: Optional[str] = Field(default=None, max_length=500)


# ─── Handlers ────────────────────────────────────────────────────────────────


def _to_status(row) -> SettingsStatus:
    return SettingsStatus(
        anthropic_key_masked=mask_key(row.anthropic_api_key),
        anthropic_key_configured=bool(row.anthropic_api_key),
        license_configured=bool(row.license_jwt),
        encryption_key_configured=has_encryption_key(),
    )


@router.get("", response_model=SettingsStatus)
def get_settings(
    db: Session = Depends(get_db),
    _caller=Depends(require_role("admin", "operator", "viewer")),
) -> SettingsStatus:
    return _to_status(get_instance_settings(db))


@router.put("/anthropic-key", response_model=SettingsStatus)
def put_anthropic_key(
    body: AnthropicKeyUpdate,
    request: Request,
    db: Session = Depends(get_db),
    caller=Depends(require_role("admin")),
) -> SettingsStatus:
    row = set_anthropic_key(db, body.api_key)
    log_event(
        db,
        request=request,
        user=caller,
        action="settings.anthropic_key_updated",
        details={"configured": bool(row.anthropic_api_key)},
    )
    return _to_status(row)


@router.post("/rotate-encryption-key", response_model=RotateKeyResponse)
def rotate_encryption_key(
    request: Request,
    db: Session = Depends(get_db),
    caller=Depends(require_role("admin")),
) -> RotateKeyResponse:
    """Re-encrypt every sensitive column with the current primary key.

    Operators rotate keys by:
      1. Generate a fresh Fernet key.
      2. Prepend it to DEPART_ENCRYPTION_KEYS (old key(s) stay in the
         list so we can still decrypt existing rows during rollover).
      3. Restart the API so the env reload picks up the new key.
      4. Call this endpoint to re-encrypt every stored value with the
         new primary key.
      5. After every row is rotated, remove the old keys from the env
         on the next restart.

    The endpoint walks sensitive columns on `migrations`,
    `instance_settings`, and `identity_providers`. For each non-empty
    value, it reads (which decrypts through the TypeDecorator) and
    writes back (which re-encrypts with the current primary key).
    """
    if not has_encryption_key():
        from fastapi import HTTPException

        raise HTTPException(
            status_code=400,
            detail=(
                "DEPART_ENCRYPTION_KEY is not configured. Set the env var "
                "and restart the API before rotating."
            ),
        )

    rotated = 0

    # NOTE: reassigning a column to its existing decrypted value does
    # NOT mark it dirty — SQLAlchemy's equality check skips the UPDATE.
    # `flag_modified` forces the bind pass (and therefore the re-encrypt
    # with the new primary key) to run on flush.

    for m in db.query(MigrationRecord).all():
        changed = False
        if m.source_url:
            flag_modified(m, "source_url")
            changed = True
        if m.target_url:
            flag_modified(m, "target_url")
            changed = True
        if changed:
            rotated += 1

    for s in db.query(InstanceSettings).all():
        changed = False
        if s.anthropic_api_key:
            flag_modified(s, "anthropic_api_key")
            changed = True
        if s.license_jwt:
            flag_modified(s, "license_jwt")
            changed = True
        if changed:
            rotated += 1

    for idp in db.query(IdentityProvider).all():
        if idp.client_secret:
            flag_modified(idp, "client_secret")
            rotated += 1

    db.commit()

    log_event(
        db,
        request=request,
        user=caller,
        action="settings.encryption_key_rotated",
        details={"rotated_rows": rotated},
    )
    return RotateKeyResponse(rotated=rotated, ok=True)
