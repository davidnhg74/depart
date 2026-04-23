"""Webhook subscription endpoints.

Admin-only + license-gated (`webhooks` feature). CRUD the set of
subscribers that the migration runner fires events at, plus a
`/test` endpoint that sends a synthetic event so operators can
validate their receiver without waiting for a real migration to
finish.

The URL and secret are write-only in responses — callers see
`url_set` / `secret_set` booleans and the last few characters of
the URL's host to confirm which endpoint is which. Same redaction
pattern as /settings/sso and /settings/instance.
"""

from __future__ import annotations

import logging
import uuid
from typing import Optional
from urllib.parse import urlparse

from fastapi import APIRouter, Depends, HTTPException, Request, status
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from ..auth.roles import require_role
from ..db import get_db
from ..license.dependencies import require_feature
from ..models import WebhookEndpoint
from ..services import webhook_service
from ..services.audit import log_event


logger = logging.getLogger(__name__)


router = APIRouter(prefix="/api/v1/webhooks", tags=["webhooks"])


# ─── Schemas ─────────────────────────────────────────────────────────


# Known event names — subscribers pick any subset. Kept as a flat
# list rather than an Enum so adding a new event is a one-line
# change and doesn't break subscribers storing older events.
KNOWN_EVENTS = ["migration.completed", "migration.failed"]


class WebhookCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=255)
    url: str = Field(..., min_length=1)
    secret: Optional[str] = None
    events: list[str] = Field(default_factory=list)
    enabled: bool = True


class WebhookUpdate(BaseModel):
    name: Optional[str] = Field(default=None, min_length=1, max_length=255)
    url: Optional[str] = Field(default=None, min_length=1)
    # Empty string → clear secret. None → leave unchanged.
    secret: Optional[str] = None
    events: Optional[list[str]] = None
    enabled: Optional[bool] = None


class WebhookView(BaseModel):
    id: str
    name: str
    url_host: Optional[str]
    url_set: bool
    secret_set: bool
    events: list[str]
    enabled: bool
    last_triggered_at: Optional[str]
    last_status: Optional[int]
    last_error: Optional[str]


# ─── Helpers ─────────────────────────────────────────────────────────


def _url_host(url: Optional[str]) -> Optional[str]:
    if not url:
        return None
    try:
        parsed = urlparse(url)
        return parsed.netloc or None
    except Exception:
        return None


def _view(ep: WebhookEndpoint) -> WebhookView:
    return WebhookView(
        id=str(ep.id),
        name=ep.name,
        url_host=_url_host(ep.url),
        url_set=bool(ep.url),
        secret_set=bool(ep.secret),
        events=list(ep.events or []),
        enabled=bool(ep.enabled),
        last_triggered_at=ep.last_triggered_at.isoformat() if ep.last_triggered_at else None,
        last_status=ep.last_status,
        last_error=ep.last_error,
    )


def _validate_events(events: list[str]) -> None:
    bad = [e for e in events if e not in KNOWN_EVENTS]
    if bad:
        raise HTTPException(
            status_code=400,
            detail={
                "error": "unknown_events",
                "unknown": bad,
                "known": KNOWN_EVENTS,
            },
        )


def _validate_url(url: str) -> None:
    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https"):
        raise HTTPException(
            status_code=400,
            detail="webhook url must start with http:// or https://",
        )
    if not parsed.netloc:
        raise HTTPException(status_code=400, detail="webhook url is missing a host")


def _parse_id(endpoint_id: str) -> uuid.UUID:
    try:
        return uuid.UUID(endpoint_id)
    except (ValueError, AttributeError):
        raise HTTPException(status_code=404, detail="webhook not found")


# ─── Routes ──────────────────────────────────────────────────────────


@router.get("", response_model=list[WebhookView])
def list_webhooks(
    db: Session = Depends(get_db),
    _admin=Depends(require_role("admin")),
    _license=Depends(require_feature("webhooks")),
) -> list[WebhookView]:
    return [_view(ep) for ep in webhook_service.list_endpoints(db)]


@router.post("", response_model=WebhookView, status_code=status.HTTP_201_CREATED)
def create_webhook(
    body: WebhookCreate,
    request: Request,
    db: Session = Depends(get_db),
    admin=Depends(require_role("admin")),
    _license=Depends(require_feature("webhooks")),
) -> WebhookView:
    _validate_url(body.url)
    _validate_events(body.events)
    ep = webhook_service.create_endpoint(
        db,
        name=body.name,
        url=body.url,
        secret=body.secret,
        events=body.events,
        enabled=body.enabled,
    )
    log_event(
        db,
        request=request,
        user=admin,
        action="webhook.created",
        details={
            "id": str(ep.id),
            "name": ep.name,
            "url_host": _url_host(ep.url),
            "events": list(ep.events or []),
            "enabled": bool(ep.enabled),
            "secret_set": bool(ep.secret),
        },
    )
    return _view(ep)


@router.get("/{endpoint_id}", response_model=WebhookView)
def get_webhook(
    endpoint_id: str,
    db: Session = Depends(get_db),
    _admin=Depends(require_role("admin")),
    _license=Depends(require_feature("webhooks")),
) -> WebhookView:
    ep = webhook_service.get_endpoint(db, _parse_id(endpoint_id))
    if ep is None:
        raise HTTPException(status_code=404, detail="webhook not found")
    return _view(ep)


@router.patch("/{endpoint_id}", response_model=WebhookView)
def update_webhook(
    endpoint_id: str,
    body: WebhookUpdate,
    request: Request,
    db: Session = Depends(get_db),
    admin=Depends(require_role("admin")),
    _license=Depends(require_feature("webhooks")),
) -> WebhookView:
    ep_id = _parse_id(endpoint_id)
    if body.url is not None:
        _validate_url(body.url)
    if body.events is not None:
        _validate_events(body.events)
    ep = webhook_service.update_endpoint(
        db,
        ep_id,
        name=body.name,
        url=body.url,
        secret=body.secret,
        events=body.events,
        enabled=body.enabled,
    )
    if ep is None:
        raise HTTPException(status_code=404, detail="webhook not found")
    log_event(
        db,
        request=request,
        user=admin,
        action="webhook.updated",
        details={
            "id": str(ep.id),
            "name": ep.name,
            "url_host": _url_host(ep.url),
            "events": list(ep.events or []),
            "enabled": bool(ep.enabled),
            "secret_set": bool(ep.secret),
        },
    )
    return _view(ep)


@router.delete("/{endpoint_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_webhook(
    endpoint_id: str,
    request: Request,
    db: Session = Depends(get_db),
    admin=Depends(require_role("admin")),
    _license=Depends(require_feature("webhooks")),
) -> None:
    ep_id = _parse_id(endpoint_id)
    if not webhook_service.delete_endpoint(db, ep_id):
        raise HTTPException(status_code=404, detail="webhook not found")
    log_event(
        db,
        request=request,
        user=admin,
        action="webhook.deleted",
        details={"id": str(ep_id)},
    )


@router.post("/{endpoint_id}/test")
def test_webhook(
    endpoint_id: str,
    request: Request,
    db: Session = Depends(get_db),
    admin=Depends(require_role("admin")),
    _license=Depends(require_feature("webhooks")),
) -> dict:
    """Send a synthetic `webhook.test` event to this endpoint so
    operators can confirm their receiver is wired up correctly
    without running a real migration."""
    ep = webhook_service.get_endpoint(db, _parse_id(endpoint_id))
    if ep is None:
        raise HTTPException(status_code=404, detail="webhook not found")

    webhook_service.deliver_to_endpoint(
        db,
        ep,
        "webhook.test",
        {
            "migration_id": None,
            "name": ep.name,
            "message": "This is a test delivery from Depart /settings/webhooks.",
        },
    )
    db.refresh(ep)
    log_event(
        db,
        request=request,
        user=admin,
        action="webhook.tested",
        details={
            "id": str(ep.id),
            "last_status": ep.last_status,
            "last_error": ep.last_error,
        },
    )
    return {
        "id": str(ep.id),
        "last_status": ep.last_status,
        "last_error": ep.last_error,
        "last_triggered_at": ep.last_triggered_at.isoformat() if ep.last_triggered_at else None,
    }
