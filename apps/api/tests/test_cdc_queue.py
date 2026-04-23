"""Unit tests for src.services.cdc.queue — enqueue/fetch/mark.

No Oracle involved; we seed Change records directly and verify the
queue's ordering + bookkeeping. The capture worker that will
eventually populate this queue doesn't exist yet — tests stand in
for that role.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.config import settings as env_settings
from src.models import MigrationCdcChange, MigrationRecord
from src.services.cdc.queue import (
    Change,
    commit_apply_results,
    enqueue_changes,
    fetch_unapplied,
    mark_applied,
    mark_failed,
    queue_status,
)


@pytest.fixture
def db():
    engine = create_engine(env_settings.database_url)
    Session = sessionmaker(bind=engine)
    s = Session()
    s.query(MigrationCdcChange).delete()
    s.query(MigrationRecord).delete()
    s.commit()
    try:
        yield s
    finally:
        s.query(MigrationCdcChange).delete()
        s.query(MigrationRecord).delete()
        s.commit()
        s.close()
        engine.dispose()


def _seed_migration(db) -> uuid.UUID:
    rec = MigrationRecord(
        id=uuid.uuid4(),
        name="cdc-test",
        schema_name="hr",
        source_url="oracle://...",
        target_url="postgresql+psycopg://...",
        source_schema="HR",
        target_schema="hr",
        status="pending",
    )
    db.add(rec)
    db.commit()
    return rec.id


def _mk_change(scn: int, op: str = "I", table: str = "emp") -> Change:
    return Change(
        scn=scn,
        source_schema="HR",
        source_table=table,
        op=op,
        pk={"id": scn},
        before=None if op == "I" else {"name": "old"},
        after=None if op == "D" else {"id": scn, "name": f"row-{scn}"},
        committed_at=datetime(2026, 4, 23, 12, 0, 0, tzinfo=timezone.utc),
    )


# ─── enqueue ────────────────────────────────────────────────────────


def test_enqueue_empty_list_returns_zero(db):
    mid = _seed_migration(db)
    assert enqueue_changes(db, mid, []) == 0


def test_enqueue_inserts_all(db):
    mid = _seed_migration(db)
    changes = [_mk_change(10), _mk_change(20), _mk_change(30)]
    assert enqueue_changes(db, mid, changes) == 3
    count = db.query(MigrationCdcChange).count()
    assert count == 3


# ─── fetch_unapplied ────────────────────────────────────────────────


def test_fetch_returns_scn_ordered(db):
    """LogMiner can feed us changes in any order (different redo
    threads, merges across logs). The queue must serve them SCN-
    sorted."""
    mid = _seed_migration(db)
    enqueue_changes(
        db, mid, [_mk_change(30), _mk_change(10), _mk_change(20)]
    )
    out = fetch_unapplied(db, mid)
    assert [c.scn for c in out] == [10, 20, 30]


def test_fetch_skips_already_applied(db):
    mid = _seed_migration(db)
    enqueue_changes(db, mid, [_mk_change(10), _mk_change(20), _mk_change(30)])
    all_changes = fetch_unapplied(db, mid)
    # Mark the middle one applied
    mark_applied(db, [all_changes[1].id])
    remaining = fetch_unapplied(db, mid)
    assert [c.scn for c in remaining] == [10, 30]


def test_fetch_scope_by_source_table(db):
    """Apply workers run one per table so ordering is per-table.
    The fetch filter is how they stay in their lane."""
    mid = _seed_migration(db)
    enqueue_changes(
        db,
        mid,
        [
            _mk_change(10, table="emp"),
            _mk_change(20, table="dept"),
            _mk_change(30, table="emp"),
        ],
    )
    emp_only = fetch_unapplied(db, mid, source_table="emp")
    assert [c.scn for c in emp_only] == [10, 30]
    dept_only = fetch_unapplied(db, mid, source_table="dept")
    assert [c.scn for c in dept_only] == [20]


def test_fetch_respects_limit(db):
    mid = _seed_migration(db)
    enqueue_changes(db, mid, [_mk_change(i) for i in range(1, 11)])
    batch = fetch_unapplied(db, mid, limit=3)
    assert len(batch) == 3
    assert [c.scn for c in batch] == [1, 2, 3]


# ─── mark_applied / mark_failed ─────────────────────────────────────


def test_mark_applied_clears_prior_error(db):
    """A row that failed once, then succeeds on retry, is healthy.
    apply_error must not linger as a false ghost."""
    mid = _seed_migration(db)
    enqueue_changes(db, mid, [_mk_change(10)])
    cid = fetch_unapplied(db, mid)[0].id
    mark_failed(db, cid, "transient driver error")
    row = db.get(MigrationCdcChange, cid)
    assert row.apply_error is not None
    mark_applied(db, [cid])
    db.refresh(row)
    assert row.applied_at is not None
    assert row.apply_error is None


def test_mark_failed_does_not_advance_applied_at(db):
    """Failed rows stay pending so the next fetch picks them back up —
    apply logic is idempotent, so re-applying a transiently-failed
    row is safe."""
    mid = _seed_migration(db)
    enqueue_changes(db, mid, [_mk_change(10)])
    cid = fetch_unapplied(db, mid)[0].id
    mark_failed(db, cid, "nope")
    row = db.get(MigrationCdcChange, cid)
    assert row.applied_at is None
    assert row.apply_error == "nope"
    # Still appears as unapplied.
    assert fetch_unapplied(db, mid)[0].id == cid


# ─── queue_status ───────────────────────────────────────────────────


def test_queue_status_counts(db):
    mid = _seed_migration(db)
    enqueue_changes(
        db, mid, [_mk_change(10), _mk_change(20), _mk_change(30), _mk_change(40)]
    )
    all_changes = fetch_unapplied(db, mid)
    mark_applied(db, [all_changes[0].id, all_changes[1].id])
    mark_failed(db, all_changes[2].id, "stuck")

    status = queue_status(db, mid)
    assert status.applied_count == 2
    assert status.pending_count == 2  # failed + never-attempted
    assert status.failed_count == 1


# ─── commit_apply_results — single-commit drain bookkeeping ──────────


def _seed_migration_with_scn(db, prior_scn: int | None) -> uuid.UUID:
    rec = MigrationRecord(
        id=uuid.uuid4(),
        name="cdc-commit-test",
        schema_name="hr",
        source_url="oracle://...",
        target_url="postgresql+psycopg://...",
        source_schema="HR",
        target_schema="hr",
        status="pending",
        last_applied_scn=prior_scn,
    )
    db.add(rec)
    db.commit()
    return rec.id


class TestCommitApplyResults:
    def test_applies_failures_and_advances_scn_in_one_commit(self, db):
        # Three changes, two succeed, one fails. After commit_apply_results:
        #  - successes have applied_at set
        #  - failure has apply_error set, applied_at still NULL
        #  - migration's last_applied_scn advances to the max success SCN
        mid = _seed_migration_with_scn(db, prior_scn=None)
        enqueue_changes(
            db, mid, [_mk_change(10), _mk_change(20), _mk_change(30)]
        )
        rows = fetch_unapplied(db, mid)

        commit_apply_results(
            db,
            migration_id=mid,
            applied_change_ids=[rows[0].id, rows[2].id],
            failed=[(rows[1].id, "transient deadlock")],
            new_max_applied_scn=30,
        )

        rec = db.get(MigrationRecord, mid)
        db.refresh(rec)
        assert rec.last_applied_scn == 30

        st = queue_status(db, mid)
        assert st.applied_count == 2
        assert st.failed_count == 1
        assert st.pending_count == 1  # the failure stays in the queue

    def test_watermark_only_advances_forward(self, db):
        # If a later drain reports a smaller new_max_applied_scn (e.g.,
        # only retried older failures), the watermark must NOT regress.
        mid = _seed_migration_with_scn(db, prior_scn=100)
        enqueue_changes(db, mid, [_mk_change(50)])
        row = fetch_unapplied(db, mid)[0]

        commit_apply_results(
            db,
            migration_id=mid,
            applied_change_ids=[row.id],
            failed=[],
            new_max_applied_scn=50,  # below the prior 100
        )

        rec = db.get(MigrationRecord, mid)
        db.refresh(rec)
        assert rec.last_applied_scn == 100  # unchanged

    def test_no_max_scn_leaves_watermark_alone(self, db):
        # All-failures drain — pass new_max_applied_scn=None.
        mid = _seed_migration_with_scn(db, prior_scn=42)
        enqueue_changes(db, mid, [_mk_change(50)])
        row = fetch_unapplied(db, mid)[0]

        commit_apply_results(
            db,
            migration_id=mid,
            applied_change_ids=[],
            failed=[(row.id, "perma-broken")],
            new_max_applied_scn=None,
        )

        rec = db.get(MigrationRecord, mid)
        db.refresh(rec)
        assert rec.last_applied_scn == 42

    def test_empty_inputs_still_safe(self, db):
        # Nothing to do — should not crash and should not touch the
        # watermark.
        mid = _seed_migration_with_scn(db, prior_scn=10)
        commit_apply_results(
            db,
            migration_id=mid,
            applied_change_ids=[],
            failed=[],
            new_max_applied_scn=None,
        )
        rec = db.get(MigrationRecord, mid)
        assert rec.last_applied_scn == 10
