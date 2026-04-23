"""CDC change queue — enqueue, fetch, mark.

The capture worker (LogMiner-driven, lands next session) calls
``enqueue_changes`` to persist parsed redo records. The apply worker
drains them via ``fetch_unapplied`` in strict SCN order per table,
then calls ``mark_applied`` / ``mark_failed`` per change id.

Every function here is sync SQLAlchemy; the workers themselves run
under arq so they can cooperate with the rest of the system.

See docs/CDC_DESIGN.md for the overall mechanism.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Iterable

from sqlalchemy.orm import Session

from ...models import MigrationCdcChange


# ─── Public Change type ──────────────────────────────────────────────


@dataclass
class Change:
    """One captured change. The capture worker builds these from
    LogMiner output and hands them to ``enqueue_changes``. The apply
    worker receives them (reconstructed from DB rows) and writes to
    the target via ``cdc.apply.apply_changes``.

    Field shape intentionally matches the row layout so the two
    paths stay in sync without a separate transformer."""

    scn: int
    source_schema: str
    source_table: str
    op: str  # 'I', 'U', or 'D'
    pk: dict[str, Any]
    committed_at: datetime
    before: dict[str, Any] | None = None
    after: dict[str, Any] | None = None
    # Populated after the row hits the DB; the capture worker doesn't
    # set this. The apply worker reads it off the reconstructed row
    # to mark that exact change applied/failed.
    id: int | None = None


# ─── Enqueue ─────────────────────────────────────────────────────────


def enqueue_changes(
    db: Session,
    migration_id: uuid.UUID,
    changes: Iterable[Change],
) -> int:
    """Bulk-insert captured changes into the queue. Returns the count
    actually written. A single DB commit per call — caller is expected
    to hand in whole LogMiner batches, not single rows."""
    rows = [
        MigrationCdcChange(
            migration_id=migration_id,
            scn=c.scn,
            source_schema=c.source_schema,
            source_table=c.source_table,
            op=c.op,
            pk_json=c.pk,
            before_json=c.before,
            after_json=c.after,
            committed_at=c.committed_at,
        )
        for c in changes
    ]
    if not rows:
        return 0
    db.bulk_save_objects(rows)
    db.commit()
    return len(rows)


# ─── Fetch ───────────────────────────────────────────────────────────


def fetch_unapplied(
    db: Session,
    migration_id: uuid.UUID,
    source_table: str | None = None,
    *,
    limit: int = 500,
) -> list[Change]:
    """Return the next SCN-ordered slice of unapplied changes.

    If ``source_table`` is set, scope to just that table — apply
    workers run one per table so each stream is independent and
    strictly ordered. Without the filter, returns cross-table (used
    by cutover drain when we need everything).
    """
    q = db.query(MigrationCdcChange).filter(
        MigrationCdcChange.migration_id == migration_id,
        MigrationCdcChange.applied_at.is_(None),
    )
    if source_table is not None:
        q = q.filter(MigrationCdcChange.source_table == source_table)
    q = q.order_by(MigrationCdcChange.scn.asc()).limit(limit)
    return [
        Change(
            id=r.id,
            scn=r.scn,
            source_schema=r.source_schema,
            source_table=r.source_table,
            op=r.op,
            pk=dict(r.pk_json),
            before=dict(r.before_json) if r.before_json else None,
            after=dict(r.after_json) if r.after_json else None,
            committed_at=r.committed_at,
        )
        for r in q.all()
    ]


# ─── Apply-result bookkeeping ────────────────────────────────────────


def mark_applied(
    db: Session, change_ids: Iterable[int], *, applied_at: datetime | None = None
) -> int:
    """Stamp these changes as applied. Clears any prior apply_error
    (retry semantics: a previously-failed row that now succeeds
    becomes healthy again)."""
    from ...utils.time import utc_now

    ids = list(change_ids)
    if not ids:
        return 0
    stamp = applied_at or utc_now()
    updated = (
        db.query(MigrationCdcChange)
        .filter(MigrationCdcChange.id.in_(ids))
        .update(
            {"applied_at": stamp, "apply_error": None},
            synchronize_session=False,
        )
    )
    db.commit()
    return updated


def mark_failed(
    db: Session, change_id: int, error: str
) -> None:
    """Record an apply failure on a single change. applied_at stays
    NULL so the next fetch picks it back up (the apply logic is
    idempotent, so re-applying a transiently-failed row is safe)."""
    (
        db.query(MigrationCdcChange)
        .filter(MigrationCdcChange.id == change_id)
        .update(
            {"apply_error": error[:4000]},  # hard cap for runaway driver messages
            synchronize_session=False,
        )
    )
    db.commit()


# ─── Status counts ───────────────────────────────────────────────────


@dataclass
class QueueStatus:
    pending_count: int
    applied_count: int
    failed_count: int


def queue_status(db: Session, migration_id: uuid.UUID) -> QueueStatus:
    """Cheap read for the status endpoint. `failed_count` is the
    subset of pending (applied_at IS NULL) that also has an
    apply_error — those are stuck; operator should investigate."""
    pending = (
        db.query(MigrationCdcChange)
        .filter(
            MigrationCdcChange.migration_id == migration_id,
            MigrationCdcChange.applied_at.is_(None),
        )
        .count()
    )
    applied = (
        db.query(MigrationCdcChange)
        .filter(
            MigrationCdcChange.migration_id == migration_id,
            MigrationCdcChange.applied_at.is_not(None),
        )
        .count()
    )
    failed = (
        db.query(MigrationCdcChange)
        .filter(
            MigrationCdcChange.migration_id == migration_id,
            MigrationCdcChange.applied_at.is_(None),
            MigrationCdcChange.apply_error.is_not(None),
        )
        .count()
    )
    return QueueStatus(
        pending_count=pending, applied_count=applied, failed_count=failed
    )
