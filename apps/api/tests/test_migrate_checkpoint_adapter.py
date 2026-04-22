"""Tests for the runner ↔ CheckpointManager adapter."""

from __future__ import annotations

import uuid

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.config import settings
from src.migration.checkpoint import CheckpointManager
from src.migrate.checkpoint_adapter import (
    decode_last_pk,
    encode_last_pk,
    make_checkpoint_callback,
    make_resume_callback,
    resume_pk,
)
from src.migrate.planner import TableRef
from src.models import MigrationCheckpointRecord, MigrationRecord


# ─── Pure encoding/decoding ──────────────────────────────────────────────────


class TestEncoding:
    def test_round_trip_single_pk(self):
        assert decode_last_pk(encode_last_pk((42,))) == (42,)

    def test_round_trip_composite(self):
        assert decode_last_pk(encode_last_pk((7, 3))) == (7, 3)

    def test_round_trip_string_pk(self):
        assert decode_last_pk(encode_last_pk(("alpha",))) == ("alpha",)

    def test_none_passes_through(self):
        assert encode_last_pk(None) is None
        assert decode_last_pk(None) is None


# ─── Live Postgres — the manager writes real rows ────────────────────────────


@pytest.fixture
def session():
    engine = create_engine(settings.database_url)
    Session = sessionmaker(bind=engine)
    s = Session()
    yield s
    # Clean up any rows we created.
    s.rollback()
    s.query(MigrationCheckpointRecord).delete()
    s.query(MigrationRecord).delete()
    s.commit()
    s.close()


@pytest.fixture
def migration_id(session):
    manager = CheckpointManager(session)
    return manager.create_migration("adapter-test")


def test_callback_persists_each_batch(session, migration_id):
    manager = CheckpointManager(session)
    callback = make_checkpoint_callback(
        manager,
        migration_id,
        total_rows_by_table={"public.items": 100},
    )

    table = TableRef(schema="public", name="items")
    callback(table, (10,), 10)
    callback(table, (20,), 20)
    callback(table, (30,), 30)

    # Three checkpoint rows were written for this table.
    rows = (
        session.query(MigrationCheckpointRecord)
        .filter(MigrationCheckpointRecord.table_name == "public.items")
        .order_by(MigrationCheckpointRecord.rows_processed)
        .all()
    )
    assert [r.rows_processed for r in rows] == [10, 20, 30]
    assert [r.last_rowid for r in rows] == ["[10]", "[20]", "[30]"]
    # Progress percentage uses the total we passed in.
    assert rows[-1].progress_percentage == pytest.approx(30.0)


def test_resume_pk_returns_latest(session, migration_id):
    manager = CheckpointManager(session)
    callback = make_checkpoint_callback(manager, migration_id)
    table = TableRef(schema="public", name="orders")

    callback(table, (5,), 5)
    callback(table, (15,), 15)
    callback(table, (25,), 25)

    assert resume_pk(manager, migration_id, table) == (25,)


def test_resume_pk_no_checkpoint_returns_none(session, migration_id):
    manager = CheckpointManager(session)
    table = TableRef(schema="public", name="never_loaded")
    assert resume_pk(manager, migration_id, table) is None


def test_composite_pk_round_trips_through_db(session, migration_id):
    manager = CheckpointManager(session)
    callback = make_checkpoint_callback(manager, migration_id)
    table = TableRef(schema="public", name="line_items")

    callback(table, (7, 3), 1)
    assert resume_pk(manager, migration_id, table) == (7, 3)


def test_make_resume_callback_returns_latest(session, migration_id):
    manager = CheckpointManager(session)
    write = make_checkpoint_callback(manager, migration_id)
    read = make_resume_callback(manager, migration_id)
    table = TableRef(schema="public", name="events")

    assert read(table) is None  # no checkpoint yet
    write(table, (100,), 100)
    write(table, (250,), 250)
    assert read(table) == (250,)
