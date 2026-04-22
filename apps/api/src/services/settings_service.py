"""Helpers for reading and writing the single-row InstanceSettings.

Callers never construct or query InstanceSettings directly — they go
through `get_instance_settings(session)` and the setter functions
below. The singleton pattern (id=1) is enforced here so the model
doesn't need a DB-level unique constraint we'd have to migrate later
if we ever want per-tenant settings.

Precedence for the Anthropic key:
    InstanceSettings.anthropic_api_key (UI-set)
        > settings.anthropic_api_key   (env var / .env)
        > None                         (BYOK disabled)

This way a fresh install with an env key Just Works, an operator can
override it via the UI without restarting, and setting the UI key to
None falls back cleanly to the env value.
"""

from __future__ import annotations

from typing import Optional

from sqlalchemy.orm import Session

from ..config import settings as env_settings
from ..models import InstanceSettings


_SINGLETON_ID = 1


def get_instance_settings(session: Session) -> InstanceSettings:
    """Return the single settings row, creating it on first access."""
    row = session.get(InstanceSettings, _SINGLETON_ID)
    if row is None:
        row = InstanceSettings(id=_SINGLETON_ID)
        session.add(row)
        session.commit()
        session.refresh(row)
    return row


def get_effective_anthropic_key(session: Session) -> Optional[str]:
    """Resolve the BYOK precedence chain and return the effective key,
    or None if BYOK is not configured anywhere."""
    row = get_instance_settings(session)
    if row.anthropic_api_key:
        return row.anthropic_api_key
    if env_settings.anthropic_api_key:
        return env_settings.anthropic_api_key
    return None


def set_anthropic_key(session: Session, key: Optional[str]) -> InstanceSettings:
    """Store the operator's Anthropic key in the DB. Pass None to
    clear it and fall back to the env var / disable BYOK."""
    row = get_instance_settings(session)
    row.anthropic_api_key = key or None  # treat empty string as clear
    session.commit()
    session.refresh(row)
    return row


def set_license_jwt(session: Session, jwt: Optional[str]) -> InstanceSettings:
    """Store the uploaded license JWT. None clears it (drop to
    Community tier on the next request)."""
    row = get_instance_settings(session)
    row.license_jwt = jwt or None
    session.commit()
    session.refresh(row)
    return row


def mask_key(key: Optional[str]) -> Optional[str]:
    """Return a display-safe version of an API key — first 4 + last 4
    chars with the rest replaced by dots. `None` stays `None` so the
    UI can render a distinct 'not configured' state."""
    if not key:
        return None
    if len(key) <= 10:
        return "•" * len(key)
    return f"{key[:4]}{'•' * (len(key) - 8)}{key[-4:]}"
