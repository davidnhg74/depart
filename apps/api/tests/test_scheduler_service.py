"""Unit tests for scheduler_service — cron math + clone + tick loop."""

from __future__ import annotations

import uuid
from datetime import datetime, timezone

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.config import settings as env_settings
from src.models import MigrationRecord, MigrationSchedule
from src.services import scheduler_service
from src.utils.time import utc_now
pytestmark = pytest.mark.integration


@pytest.fixture
def db():
    engine = create_engine(env_settings.database_url)
    Session = sessionmaker(bind=engine)
    s = Session()
    # Schedules reference migrations — wipe in FK-safe order.
    s.query(MigrationSchedule).delete()
    s.query(MigrationRecord).delete()
    s.commit()
    try:
        yield s
    finally:
        s.query(MigrationSchedule).delete()
        s.query(MigrationRecord).delete()
        s.commit()
        s.close()
        engine.dispose()


def _make_migration(db, **overrides) -> MigrationRecord:
    defaults = dict(
        id=uuid.uuid4(),
        name="prod-to-stage",
        schema_name="hr",
        source_url="oracle://prod",
        target_url="postgresql+psycopg://stage",
        source_schema="HR",
        target_schema="hr",
        tables=None,
        batch_size=10_000,
        create_tables=True,
        status="pending",
        total_rows=0,
        rows_transferred=0,
    )
    defaults.update(overrides)
    rec = MigrationRecord(**defaults)
    db.add(rec)
    db.commit()
    db.refresh(rec)
    return rec


# ─── Cron math + validation ──────────────────────────────────────────


def test_compute_next_run_at_respects_timezone():
    """2am daily in ET from 10am UTC → next is 6am UTC tomorrow
    (EDT is UTC-4 in April)."""
    nxt = scheduler_service.compute_next_run_at(
        "0 2 * * *",
        "America/New_York",
        datetime(2026, 4, 23, 10, 0, 0),
    )
    assert nxt == datetime(2026, 4, 24, 6, 0, 0)


def test_compute_next_run_at_handles_dst_spring_forward():
    """Spring-forward (2026-03-08 02:00 → 03:00 ET). A 2am daily
    cron still fires once each real day; we don't double-fire or
    skip — croniter advances to the next valid local 2am, which
    after the transition is 06:00 UTC (EDT) instead of 07:00 UTC (EST)."""
    # From the moment just before the transition, next 2am ET is
    # March 9th at 2am EDT = 06:00 UTC.
    nxt = scheduler_service.compute_next_run_at(
        "0 2 * * *",
        "America/New_York",
        datetime(2026, 3, 8, 10, 0, 0),  # past the DST flip for that day
    )
    assert nxt == datetime(2026, 3, 9, 6, 0, 0)


def test_validate_cron_rejects_bad_expression():
    with pytest.raises(ValueError):
        scheduler_service.validate_cron("not a cron", "UTC")


def test_validate_cron_rejects_unknown_timezone():
    with pytest.raises(ValueError):
        scheduler_service.validate_cron("0 * * * *", "Mars/Olympus")


# ─── Upsert + CRUD ───────────────────────────────────────────────────


def test_upsert_creates_then_updates(db):
    source = _make_migration(db)
    sched = scheduler_service.upsert_schedule(
        db,
        source.id,
        name="nightly",
        cron_expr="0 2 * * *",
        timezone_name="UTC",
        enabled=True,
    )
    created_id = sched.id
    assert sched.next_run_at > utc_now()

    # Upsert again — same row id, updated fields.
    sched2 = scheduler_service.upsert_schedule(
        db,
        source.id,
        name="nightly-v2",
        cron_expr="30 1 * * *",
        timezone_name="America/New_York",
        enabled=False,
    )
    assert sched2.id == created_id
    assert sched2.name == "nightly-v2"
    assert sched2.timezone == "America/New_York"
    assert sched2.enabled is False


def test_delete_removes_schedule(db):
    source = _make_migration(db)
    scheduler_service.upsert_schedule(
        db,
        source.id,
        name="nightly",
        cron_expr="0 2 * * *",
        timezone_name="UTC",
        enabled=True,
    )
    assert scheduler_service.delete_schedule(db, source.id) is True
    assert scheduler_service.get_schedule_for_migration(db, source.id) is None
    # Second delete is a no-op.
    assert scheduler_service.delete_schedule(db, source.id) is False


# ─── Cloning ─────────────────────────────────────────────────────────


def test_clone_from_schedule_copies_config_and_resets_state(db):
    source = _make_migration(
        db,
        status="completed",  # run-state fields on the template are not copied
        total_rows=100,
        rows_transferred=100,
        error_message="old failure",
    )
    sched = scheduler_service.upsert_schedule(
        db,
        source.id,
        name="nightly",
        cron_expr="0 2 * * *",
        timezone_name="UTC",
        enabled=True,
    )

    clone = scheduler_service.clone_from_schedule(db, sched)

    # Config copied
    assert clone.source_url == source.source_url
    assert clone.target_url == source.target_url
    assert clone.source_schema == source.source_schema
    assert clone.target_schema == source.target_schema
    assert clone.batch_size == source.batch_size
    assert clone.create_tables == source.create_tables
    assert clone.name == source.name
    # Run state reset
    assert clone.status == "pending"
    assert clone.total_rows in (0, None)
    assert clone.rows_transferred in (0, None)
    assert clone.error_message is None
    # Provenance wired
    assert clone.spawned_from_schedule_id == sched.id
    assert clone.id != source.id


def test_clone_raises_if_source_missing(db):
    source = _make_migration(db)
    sched = scheduler_service.upsert_schedule(
        db,
        source.id,
        name="nightly",
        cron_expr="0 2 * * *",
        timezone_name="UTC",
        enabled=True,
    )
    # Delete the source out from under the schedule.
    # (FK is ON DELETE CASCADE, so this will also drop the schedule —
    #  re-add a detached schedule with the stale migration_id to
    #  simulate a race.)
    db.delete(source)
    db.commit()
    orphan = MigrationSchedule(
        id=uuid.uuid4(),
        migration_id=uuid.uuid4(),
        name="orphan",
        cron_expr="0 * * * *",
        timezone="UTC",
        enabled=True,
        next_run_at=utc_now(),
    )
    with pytest.raises(ValueError):
        scheduler_service.clone_from_schedule(db, orphan)


# ─── fire_schedule + tick ────────────────────────────────────────────


@pytest.mark.asyncio
async def test_fire_schedule_advances_next_run_and_records_last(db):
    source = _make_migration(db)
    sched = scheduler_service.upsert_schedule(
        db,
        source.id,
        name="hourly",
        cron_expr="0 * * * *",
        timezone_name="UTC",
        enabled=True,
    )
    # Simulate the tick path: schedule is due. Push next_run_at into
    # the past so fire_schedule advances past it.
    past = datetime(2026, 4, 23, 12, 0, 0)
    sched.next_run_at = past
    db.commit()

    enqueued: list[str] = []

    async def enqueue(mid: str) -> str:
        enqueued.append(mid)
        return f"job-{mid}"

    clone = await scheduler_service.fire_schedule(db, sched, enqueue)

    assert enqueued == [str(clone.id)]
    db.refresh(sched)
    assert sched.last_run_at is not None
    assert sched.last_run_migration_id == clone.id
    assert sched.last_run_status == "pending"
    # Advanced strictly past the prior slot.
    assert sched.next_run_at > past


@pytest.mark.asyncio
async def test_fire_schedule_records_enqueue_failure(db):
    source = _make_migration(db)
    sched = scheduler_service.upsert_schedule(
        db,
        source.id,
        name="hourly",
        cron_expr="0 * * * *",
        timezone_name="UTC",
        enabled=True,
    )

    async def bad_enqueue(_mid: str) -> str:
        raise RuntimeError("redis down")

    # Must not raise — a dead queue can't crash the tick.
    clone = await scheduler_service.fire_schedule(db, sched, bad_enqueue)
    db.refresh(sched)
    assert sched.last_run_status == "enqueue_failed"
    assert clone.id is not None


@pytest.mark.asyncio
async def test_tick_fires_only_due_enabled_schedules(db):
    src_due = _make_migration(db, name="due")
    src_future = _make_migration(db, name="future")
    src_disabled = _make_migration(db, name="disabled")

    now = utc_now()
    due = scheduler_service.upsert_schedule(
        db,
        src_due.id,
        name="due",
        cron_expr="0 * * * *",
        timezone_name="UTC",
        enabled=True,
    )
    # Force due immediately.
    due.next_run_at = now.replace(second=0, microsecond=0)
    db.commit()

    future = scheduler_service.upsert_schedule(
        db,
        src_future.id,
        name="future",
        cron_expr="0 * * * *",
        timezone_name="UTC",
        enabled=True,
    )
    # Not due yet.
    future.next_run_at = now.replace(year=now.year + 1)
    db.commit()

    disabled = scheduler_service.upsert_schedule(
        db,
        src_disabled.id,
        name="disabled",
        cron_expr="0 * * * *",
        timezone_name="UTC",
        enabled=False,
    )
    disabled.next_run_at = now.replace(second=0, microsecond=0)
    db.commit()

    enqueued: list[str] = []

    async def enqueue(mid: str) -> str:
        enqueued.append(mid)
        return f"job-{mid}"

    fired = await scheduler_service.tick(db, enqueue)

    assert len(fired) == 1
    assert len(enqueued) == 1
    # Verify the clone is linked to the *due* schedule, not the others.
    db.refresh(due)
    clone = db.get(MigrationRecord, due.last_run_migration_id)
    assert clone is not None
    assert clone.spawned_from_schedule_id == due.id


@pytest.mark.asyncio
async def test_tick_continues_past_failing_schedule(db, monkeypatch):
    """A crash inside one schedule's clone/fire path must not block
    the other schedules on the same tick."""
    good_src = _make_migration(db, name="good")
    bad_src = _make_migration(db, name="bad")

    now = utc_now()
    good = scheduler_service.upsert_schedule(
        db, good_src.id, name="good", cron_expr="0 * * * *",
        timezone_name="UTC", enabled=True,
    )
    good.next_run_at = now.replace(second=0, microsecond=0)
    bad = scheduler_service.upsert_schedule(
        db, bad_src.id, name="bad", cron_expr="0 * * * *",
        timezone_name="UTC", enabled=True,
    )
    bad.next_run_at = now.replace(second=0, microsecond=0)
    db.commit()

    real_clone = scheduler_service.clone_from_schedule

    def selective_clone(session, sched):
        if sched.migration_id == bad_src.id:
            raise RuntimeError("simulated clone failure")
        return real_clone(session, sched)

    monkeypatch.setattr(scheduler_service, "clone_from_schedule", selective_clone)

    enqueued: list[str] = []

    async def enqueue(mid: str) -> str:
        enqueued.append(mid)
        return "ok"

    fired = await scheduler_service.tick(db, enqueue)

    # Good one fired even though bad blew up.
    assert len(fired) == 1
    assert len(enqueued) == 1
