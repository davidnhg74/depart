"""Unit tests for `src.services.webhook_service`.

These tests exercise the service layer directly against a live
Postgres — no FastAPI, no auth, no license. They cover the pieces
most likely to break silently:

  * HMAC signatures match what a subscriber would compute
  * event filtering: only subscribed endpoints get called
  * disabled endpoints are skipped
  * per-endpoint failures don't propagate
  * delivery telemetry (last_status / last_error) is recorded
"""

from __future__ import annotations

import hashlib
import hmac
import json

import httpx
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.config import settings as env_settings
from src.models import WebhookEndpoint
from src.services import webhook_service


@pytest.fixture
def db():
    engine = create_engine(env_settings.database_url)
    Session = sessionmaker(bind=engine)
    s = Session()
    s.query(WebhookEndpoint).delete()
    s.commit()
    try:
        yield s
    finally:
        s.query(WebhookEndpoint).delete()
        s.commit()
        s.close()
        engine.dispose()


def _record_transport(captured: list):
    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        return httpx.Response(200, text="ok")
    return httpx.MockTransport(handler)


def test_fire_event_delivers_to_subscribed_endpoint(db):
    ep = webhook_service.create_endpoint(
        db,
        name="ops-slack",
        url="https://hooks.example.com/abc",
        secret="s3cret",
        events=["migration.completed"],
    )

    captured: list[httpx.Request] = []
    client = httpx.Client(transport=_record_transport(captured))
    webhook_service.fire_event(
        db,
        "migration.completed",
        {"migration_id": "m-1", "status": "completed"},
        http_client=client,
    )
    client.close()

    assert len(captured) == 1
    req = captured[0]
    envelope = json.loads(req.content)
    assert envelope["schema_version"] == webhook_service.PAYLOAD_SCHEMA_VERSION
    assert envelope["event"] == "migration.completed"
    assert envelope["data"]["migration_id"] == "m-1"
    assert req.headers["x-depart-event"] == "migration.completed"
    assert "x-depart-delivery" in req.headers

    # HMAC matches what a receiver computes over the exact bytes.
    expected = hmac.new(b"s3cret", req.content, hashlib.sha256).hexdigest()
    assert req.headers["x-depart-signature"] == f"sha256={expected}"

    db.refresh(ep)
    assert ep.last_status == 200
    assert ep.last_error is None
    assert ep.last_triggered_at is not None


def test_fire_event_filters_by_subscribed_events(db):
    subscriber = webhook_service.create_endpoint(
        db,
        name="completed-only",
        url="https://hooks.example.com/a",
        secret=None,
        events=["migration.completed"],
    )
    non_subscriber = webhook_service.create_endpoint(
        db,
        name="failures-only",
        url="https://hooks.example.com/b",
        secret=None,
        events=["migration.failed"],
    )

    captured: list[httpx.Request] = []
    client = httpx.Client(transport=_record_transport(captured))
    webhook_service.fire_event(
        db, "migration.completed", {"migration_id": "m-1"}, http_client=client
    )
    client.close()

    hosts = {req.url.host for req in captured}
    assert hosts == {"hooks.example.com"}
    # Exactly one delivery — the non-subscriber is skipped.
    assert len(captured) == 1
    db.refresh(non_subscriber)
    assert non_subscriber.last_triggered_at is None
    db.refresh(subscriber)
    assert subscriber.last_triggered_at is not None


def test_fire_event_skips_disabled_endpoints(db):
    ep = webhook_service.create_endpoint(
        db,
        name="paused",
        url="https://hooks.example.com/a",
        secret=None,
        events=["migration.completed"],
        enabled=False,
    )

    captured: list[httpx.Request] = []
    client = httpx.Client(transport=_record_transport(captured))
    webhook_service.fire_event(
        db, "migration.completed", {}, http_client=client
    )
    client.close()

    assert captured == []
    db.refresh(ep)
    assert ep.last_triggered_at is None


def test_fire_event_records_non_2xx_as_error(db):
    ep = webhook_service.create_endpoint(
        db,
        name="broken",
        url="https://hooks.example.com/a",
        secret=None,
        events=["migration.failed"],
    )

    def handler(_request):
        return httpx.Response(503, text="service unavailable")
    client = httpx.Client(transport=httpx.MockTransport(handler))
    webhook_service.fire_event(
        db, "migration.failed", {"migration_id": "m-1"}, http_client=client
    )
    client.close()

    db.refresh(ep)
    assert ep.last_status == 503
    assert "HTTP 503" in (ep.last_error or "")


def test_fire_event_records_transport_exception(db):
    ep = webhook_service.create_endpoint(
        db,
        name="dead-host",
        url="https://hooks.example.com/a",
        secret=None,
        events=["migration.failed"],
    )

    def handler(_request):
        raise httpx.ConnectError("refused")
    client = httpx.Client(transport=httpx.MockTransport(handler))
    # Must not raise — a dead subscriber can't crash the runner.
    webhook_service.fire_event(
        db, "migration.failed", {}, http_client=client
    )
    client.close()

    db.refresh(ep)
    assert ep.last_status is None
    assert ep.last_error and "ConnectError" in ep.last_error


def test_fire_event_continues_past_bad_endpoint(db):
    bad = webhook_service.create_endpoint(
        db,
        name="dead",
        url="https://bad.example.com/x",
        secret=None,
        events=["migration.completed"],
    )
    good = webhook_service.create_endpoint(
        db,
        name="good",
        url="https://good.example.com/x",
        secret=None,
        events=["migration.completed"],
    )

    def handler(request: httpx.Request):
        if request.url.host == "bad.example.com":
            raise httpx.ConnectError("down")
        return httpx.Response(200, text="ok")
    client = httpx.Client(transport=httpx.MockTransport(handler))
    webhook_service.fire_event(
        db, "migration.completed", {}, http_client=client
    )
    client.close()

    db.refresh(bad)
    db.refresh(good)
    assert bad.last_error is not None
    assert good.last_status == 200


def test_no_signature_header_when_secret_absent(db):
    webhook_service.create_endpoint(
        db,
        name="no-secret",
        url="https://hooks.example.com/a",
        secret=None,
        events=["migration.completed"],
    )

    captured: list[httpx.Request] = []
    client = httpx.Client(transport=_record_transport(captured))
    webhook_service.fire_event(
        db, "migration.completed", {}, http_client=client
    )
    client.close()

    assert "x-depart-signature" not in captured[0].headers


def test_deliver_to_endpoint_bypasses_subscription(db):
    """The /test endpoint uses deliver_to_endpoint to fire a
    `webhook.test` event even though the endpoint isn't subscribed
    to it. This is a correctness test: no subscription mutation is
    required to make a test delivery work."""
    ep = webhook_service.create_endpoint(
        db,
        name="ops",
        url="https://hooks.example.com/a",
        secret=None,
        events=["migration.completed"],  # NOT subscribed to webhook.test
    )

    captured: list[httpx.Request] = []
    client = httpx.Client(transport=_record_transport(captured))
    webhook_service.deliver_to_endpoint(
        db, ep, "webhook.test", {"message": "hi"}, http_client=client
    )
    client.close()

    assert len(captured) == 1
    assert json.loads(captured[0].content)["event"] == "webhook.test"
    # The endpoint's subscription list was not mutated.
    db.refresh(ep)
    assert ep.events == ["migration.completed"]


def test_update_endpoint_empty_secret_clears_and_none_preserves(db):
    ep = webhook_service.create_endpoint(
        db,
        name="ops",
        url="https://hooks.example.com/a",
        secret="original",
        events=[],
    )

    # None means "don't touch the secret" — common PATCH that only
    # edits other fields.
    webhook_service.update_endpoint(db, ep.id, name="renamed")
    db.refresh(ep)
    assert ep.secret == "original"

    # Empty string means "clear the secret".
    webhook_service.update_endpoint(db, ep.id, secret="")
    db.refresh(ep)
    assert ep.secret is None
