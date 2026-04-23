"""Webhook subscriptions + delivery for migration lifecycle events.

Called from the runner's terminal state transitions
(`migration.completed`, `migration.failed`). The delivery path is
synchronous because the runner itself runs in a worker thread —
adding async here would buy nothing and complicate the call site.

Per-endpoint failures are caught and recorded on the endpoint row
(last_status / last_error). `fire_event` never raises: a dead
subscriber URL must not crash the migration that triggered it.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import uuid
from typing import Any

import httpx
from sqlalchemy.orm import Session

from ..models import WebhookEndpoint
from ..utils.time import utc_now


logger = logging.getLogger(__name__)


# Short timeout — a slow subscriber must not keep the runner thread
# busy. Retries are intentionally out of scope for v1; operators who
# need durability can front Depart with a queue like Zapier or use
# the REST API to poll migration state.
_DELIVERY_TIMEOUT_SECONDS = 5.0
_USER_AGENT = "Depart-Webhook/1"

# Bumped when the envelope shape changes. Subscribers switch on this
# to stay compatible across upgrades. Added in v1 so we never ship a
# payload without it.
PAYLOAD_SCHEMA_VERSION = 1


# ─── CRUD ────────────────────────────────────────────────────────────


def list_endpoints(db: Session) -> list[WebhookEndpoint]:
    return (
        db.query(WebhookEndpoint)
        .order_by(WebhookEndpoint.created_at.asc())
        .all()
    )


def get_endpoint(db: Session, endpoint_id: str | uuid.UUID) -> WebhookEndpoint | None:
    return db.get(WebhookEndpoint, _as_uuid(endpoint_id))


def create_endpoint(
    db: Session,
    *,
    name: str,
    url: str,
    secret: str | None,
    events: list[str],
    enabled: bool = True,
) -> WebhookEndpoint:
    ep = WebhookEndpoint(
        name=name,
        url=url,
        secret=secret or None,
        events=list(events),
        enabled=enabled,
    )
    db.add(ep)
    db.commit()
    db.refresh(ep)
    return ep


def update_endpoint(
    db: Session,
    endpoint_id: str | uuid.UUID,
    *,
    name: str | None = None,
    url: str | None = None,
    secret: str | None = None,
    events: list[str] | None = None,
    enabled: bool | None = None,
) -> WebhookEndpoint | None:
    ep = get_endpoint(db, endpoint_id)
    if ep is None:
        return None
    if name is not None:
        ep.name = name
    if url is not None:
        ep.url = url
    if secret is not None:
        # Empty string from the router means "clear the secret".
        # None means "leave it alone" and is filtered out above.
        ep.secret = secret or None
    if events is not None:
        ep.events = list(events)
    if enabled is not None:
        ep.enabled = enabled
    db.commit()
    db.refresh(ep)
    return ep


def delete_endpoint(db: Session, endpoint_id: str | uuid.UUID) -> bool:
    ep = get_endpoint(db, endpoint_id)
    if ep is None:
        return False
    db.delete(ep)
    db.commit()
    return True


# ─── Delivery ────────────────────────────────────────────────────────


def fire_event(
    db: Session,
    event: str,
    payload: dict[str, Any],
    *,
    http_client: httpx.Client | None = None,
) -> None:
    """POST `event` + `payload` to every enabled endpoint subscribed.

    Never raises. Per-endpoint errors land on the row itself so the
    /settings/webhooks UI can show operators which subscribers are
    healthy."""
    try:
        endpoints = (
            db.query(WebhookEndpoint)
            .filter(WebhookEndpoint.enabled.is_(True))
            .all()
        )
    except Exception:
        logger.exception("failed to load webhook endpoints for event %s", event)
        return

    targets = [ep for ep in endpoints if event in (ep.events or [])]
    if not targets:
        return

    envelope = {
        "schema_version": PAYLOAD_SCHEMA_VERSION,
        "event": event,
        "delivered_at": utc_now().isoformat(),
        "data": payload,
    }
    body = json.dumps(envelope, default=str).encode("utf-8")

    own_client = http_client is None
    client = http_client or httpx.Client(timeout=_DELIVERY_TIMEOUT_SECONDS)
    try:
        for ep in targets:
            _deliver(db, client, ep, event, body)
    finally:
        if own_client:
            client.close()


def deliver_to_endpoint(
    db: Session,
    ep: WebhookEndpoint,
    event: str,
    payload: dict[str, Any],
    *,
    http_client: httpx.Client | None = None,
) -> None:
    """Deliver a single event to a single endpoint, ignoring its
    subscription list. Used by the `/webhooks/{id}/test` admin
    endpoint so operators can validate a receiver without having to
    temporarily subscribe it to a test event."""
    envelope = {
        "schema_version": PAYLOAD_SCHEMA_VERSION,
        "event": event,
        "delivered_at": utc_now().isoformat(),
        "data": payload,
    }
    body = json.dumps(envelope, default=str).encode("utf-8")
    own_client = http_client is None
    client = http_client or httpx.Client(timeout=_DELIVERY_TIMEOUT_SECONDS)
    try:
        _deliver(db, client, ep, event, body)
    finally:
        if own_client:
            client.close()


def _deliver(
    db: Session,
    client: httpx.Client,
    ep: WebhookEndpoint,
    event: str,
    body: bytes,
) -> None:
    delivery_id = str(uuid.uuid4())
    headers = {
        "Content-Type": "application/json",
        "User-Agent": _USER_AGENT,
        "X-Depart-Event": event,
        "X-Depart-Delivery": delivery_id,
    }
    if ep.secret:
        sig = hmac.new(
            ep.secret.encode("utf-8"), body, hashlib.sha256
        ).hexdigest()
        headers["X-Depart-Signature"] = f"sha256={sig}"

    ep.last_triggered_at = utc_now()
    try:
        resp = client.post(ep.url, content=body, headers=headers)
        ep.last_status = resp.status_code
        if resp.status_code >= 400:
            ep.last_error = f"HTTP {resp.status_code}: {resp.text[:500]}"
        else:
            ep.last_error = None
        logger.info(
            "webhook %s delivered: endpoint=%s status=%d delivery=%s",
            event,
            ep.name,
            resp.status_code,
            delivery_id,
        )
    except Exception as exc:
        ep.last_status = None
        ep.last_error = f"{type(exc).__name__}: {exc}"[:500]
        logger.warning(
            "webhook %s delivery failed: endpoint=%s error=%s delivery=%s",
            event,
            ep.name,
            exc,
            delivery_id,
        )

    try:
        db.commit()
    except Exception:
        logger.exception("failed to record webhook delivery result")
        db.rollback()


def _as_uuid(value: str | uuid.UUID) -> uuid.UUID:
    return value if isinstance(value, uuid.UUID) else uuid.UUID(str(value))
