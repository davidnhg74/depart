"""Apply worker — drain the CDC queue onto the migration's target.

Called periodically by the arq cron tick (see `src/worker.py`) or on
demand via `POST /api/v1/migrations/{id}/cdc/drain`. Pulls unapplied
changes SCN-ordered per source-table, applies them via
``cdc.apply.apply_changes`` using the migration's ``cdc_apply_mode``,
then marks the queue rows applied / failed and advances
``MigrationRecord.last_applied_scn``.

v1 semantics for ``last_applied_scn``: the max SCN of any row with
``applied_at IS NOT NULL``. This may have gaps (failed rows between
applied ones). The status endpoint surfaces ``failed_count`` so
operators can tell whether the watermark is contiguous or not.
Contiguous-watermark tracking is a v2 refinement — enough for cutover
safety when ``failed_count == 0``.
"""

from __future__ import annotations

import logging
import time
import uuid
from dataclasses import dataclass
from typing import Iterable

import psycopg
from sqlalchemy import func
from sqlalchemy.orm import Session

from ...models import MigrationCdcChange, MigrationRecord
from . import apply as apply_module
from . import queue as queue_module


logger = logging.getLogger(__name__)


# Fetch size per (migration, table) in one drain pass. Larger = fewer
# round-trips; smaller = smaller blast radius on a failing atomic
# batch. 500 is the same default the queue module uses; good enough.
DEFAULT_DRAIN_BATCH = 500


@dataclass
class DrainResult:
    """Per-migration summary of one drain call — relayed back through
    the `/cdc/drain` endpoint and logged from the cron tick."""

    drained_count: int
    applied_count: int
    failed_count: int
    duration_ms: int
    new_last_applied_scn: int | None


def drain_migration(
    db: Session,
    migration_id: uuid.UUID,
    *,
    batch_size: int = DEFAULT_DRAIN_BATCH,
) -> DrainResult:
    """Drain every pending change for this migration onto its target.

    One pass — if the queue has more rows than ``batch_size`` for any
    given table, those remaining rows wait for the next tick rather
    than being processed here. This bounds how long a single drain
    can hold the target's connection open."""
    started = time.monotonic()
    rec = db.get(MigrationRecord, migration_id)
    if rec is None:
        raise ValueError(f"migration {migration_id} not found")
    if not rec.target_url:
        raise ValueError(f"migration {migration_id} has no target_url")
    target_schema = rec.target_schema or rec.schema_name
    if not target_schema:
        raise ValueError(
            f"migration {migration_id} has neither target_schema nor schema_name"
        )

    # psycopg connects on the TARGET Postgres. The DSN stored on
    # MigrationRecord is a SQLAlchemy URL — strip the +psycopg prefix.
    target_dsn = rec.target_url.replace("postgresql+psycopg://", "postgresql://")

    # Collect every pending change for this migration up front so
    # atomic mode can wrap them in a single transaction on the
    # target. The order within each table is SCN-strict (because
    # fetch_unapplied orders by SCN), which is what matters for apply
    # correctness — the relative ordering across tables is arbitrary
    # but safe since apply is idempotent per-PK.
    all_changes: list[queue_module.Change] = []
    for table in _tables_with_pending(db, migration_id):
        all_changes.extend(
            queue_module.fetch_unapplied(
                db, migration_id, source_table=table, limit=batch_size,
            )
        )

    if not all_changes:
        return DrainResult(
            drained_count=0,
            applied_count=0,
            failed_count=0,
            duration_ms=int((time.monotonic() - started) * 1000),
            new_last_applied_scn=rec.last_applied_scn,
        )

    new_max_applied_scn: int | None = None
    total_applied = 0
    total_failed = 0

    with psycopg.connect(target_dsn) as pg_conn:
        pg_conn.autocommit = True
        results = apply_module.apply_changes(
            pg_conn,
            target_schema,
            all_changes,
            mode=rec.cdc_apply_mode,  # type: ignore[arg-type]
        )

    applied_ids: list[int] = []
    failed_results: list[tuple[int, str]] = []
    for change, res in zip(all_changes, results):
        if res.ok:
            applied_ids.append(res.change_id)
            total_applied += 1
            if (
                new_max_applied_scn is None
                or change.scn > new_max_applied_scn
            ):
                new_max_applied_scn = change.scn
        else:
            total_failed += 1
            failed_results.append(
                (res.change_id, res.error or "unknown apply error")
            )

    # Single transaction for all bookkeeping: applied stamps, failure
    # records, and watermark advance. Apply itself is already
    # idempotent, but multi-commit bookkeeping previously left a
    # window where a crash could leave `last_applied_scn` lagging
    # behind reality. One commit removes that window.
    queue_module.commit_apply_results(
        db,
        migration_id=migration_id,
        applied_change_ids=applied_ids,
        failed=failed_results,
        new_max_applied_scn=new_max_applied_scn,
    )

    total_drained = len(all_changes)

    return DrainResult(
        drained_count=total_drained,
        applied_count=total_applied,
        failed_count=total_failed,
        duration_ms=int((time.monotonic() - started) * 1000),
        new_last_applied_scn=rec.last_applied_scn,
    )


def _tables_with_pending(
    db: Session, migration_id: uuid.UUID
) -> list[str]:
    """Distinct source_table values that have at least one unapplied
    change for this migration. We drain one table at a time so SCN
    ordering stays strict per-table — intermixing writes across
    tables in a single `apply_changes` call would still be correct
    (idempotent), but the per-table grouping keeps diagnostic logs
    comprehensible."""
    rows = (
        db.query(MigrationCdcChange.source_table)
        .filter(
            MigrationCdcChange.migration_id == migration_id,
            MigrationCdcChange.applied_at.is_(None),
        )
        .distinct()
        .all()
    )
    return [r[0] for r in rows]


def migrations_with_pending(db: Session) -> list[uuid.UUID]:
    """Every migration id that has at least one unapplied change.
    The cron tick walks these and calls drain_migration on each."""
    rows = (
        db.query(MigrationCdcChange.migration_id)
        .filter(MigrationCdcChange.applied_at.is_(None))
        .distinct()
        .all()
    )
    return [r[0] for r in rows]
