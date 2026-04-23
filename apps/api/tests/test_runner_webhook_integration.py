"""Verify the migration runner fires webhooks on terminal state.

A full runner invocation pulls in psycopg, real source/target
engines, and the introspection pipeline — far too heavy for a
focused integration test. Instead we test the wiring: the helper
`_fire_terminal_webhook` that each terminal branch calls, plus the
payload builder, so regressions in either will surface here.
"""

from __future__ import annotations

import uuid
from unittest.mock import patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.config import settings as env_settings
from src.models import MigrationRecord, WebhookEndpoint
from src.services import webhook_service
from src.services.migration_runner import (
    _fire_terminal_webhook,
    _migration_event_payload,
)
from src.utils.time import utc_now


@pytest.fixture
def db():
    engine = create_engine(env_settings.database_url)
    Session = sessionmaker(bind=engine)
    s = Session()
    s.query(WebhookEndpoint).delete()
    s.query(MigrationRecord).delete()
    s.commit()
    try:
        yield s
    finally:
        s.query(WebhookEndpoint).delete()
        s.query(MigrationRecord).delete()
        s.commit()
        s.close()
        engine.dispose()


def _make_record(db, *, status: str, error: str | None = None) -> MigrationRecord:
    rec = MigrationRecord(
        id=uuid.uuid4(),
        name="nightly-prod",
        schema_name="hr",
        source_url="oracle://...",
        target_url="postgresql+psycopg://...",
        source_schema="HR",
        target_schema="hr",
        status=status,
        started_at=utc_now(),
        completed_at=utc_now(),
        rows_transferred=1234,
        total_rows=1234,
        error_message=error,
    )
    db.add(rec)
    db.commit()
    db.refresh(rec)
    return rec


def test_payload_builder_shapes_expected_fields(db):
    rec = _make_record(db, status="completed")
    payload = _migration_event_payload(rec)
    assert payload["migration_id"] == str(rec.id)
    assert payload["name"] == "nightly-prod"
    assert payload["status"] == "completed"
    assert payload["source_schema"] == "HR"
    assert payload["target_schema"] == "hr"
    assert payload["rows_transferred"] == 1234
    assert payload["started_at"] is not None
    assert payload["completed_at"] is not None
    assert payload["error_message"] is None


def test_terminal_webhook_dispatches_on_completed(db):
    rec = _make_record(db, status="completed")
    captured = {}

    def fake_fire(_db, event, payload, **_kwargs):
        captured["event"] = event
        captured["payload"] = payload

    with patch.object(webhook_service, "fire_event", fake_fire):
        _fire_terminal_webhook(db, rec, "migration.completed")

    assert captured["event"] == "migration.completed"
    assert captured["payload"]["migration_id"] == str(rec.id)
    assert captured["payload"]["status"] == "completed"


def test_terminal_webhook_dispatches_on_failed(db):
    rec = _make_record(db, status="failed", error="Oracle TNS: refused")
    captured = {}

    def fake_fire(_db, event, payload, **_kwargs):
        captured["event"] = event
        captured["payload"] = payload

    with patch.object(webhook_service, "fire_event", fake_fire):
        _fire_terminal_webhook(db, rec, "migration.failed")

    assert captured["event"] == "migration.failed"
    assert captured["payload"]["status"] == "failed"
    assert "TNS" in captured["payload"]["error_message"]


def test_terminal_webhook_swallows_dispatch_errors(db):
    """A failure inside fire_event must not propagate out of the
    terminal hook — a broken webhook system can't crash the runner."""
    rec = _make_record(db, status="completed")

    def boom(_db, _event, _payload, **_kwargs):
        raise RuntimeError("webhook system on fire")

    with patch.object(webhook_service, "fire_event", boom):
        # Assert no exception — the guard in _fire_terminal_webhook
        # catches everything.
        _fire_terminal_webhook(db, rec, "migration.completed")
