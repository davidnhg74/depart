"""Adapter that wires `Runner.checkpoint` callbacks into the existing
CheckpointManager so a crashed run can resume from the last batch.

The runner's callback signature is `(table, last_pk, rows_so_far)`.
The persistent record needs:
  • migration_id (FK to migrations row — the manager owns it)
  • table_name (qualified)
  • rows_processed (= rows_so_far)
  • total_rows (we don't always know — pass 0 and let the manager handle
    progress_percentage divide-by-zero)
  • last_rowid (string-encoded last_pk tuple, JSON for round-tripping)
  • status (in_progress / completed)

Encoding `last_pk` as JSON keeps composite-PK tuples lossless and
human-readable in the audit table; the resume helper decodes it back
to a tuple.
"""

from __future__ import annotations

import json
from typing import Optional

from ..migration.checkpoint import CheckpointManager
from .planner import TableRef


def make_checkpoint_callback(
    manager: CheckpointManager,
    migration_id: str,
    total_rows_by_table: Optional[dict[str, int]] = None,
):
    """Return a `(table, last_pk, rows_so_far) -> None` closure that
    persists each batch boundary via `manager.create_checkpoint`.

    If `total_rows_by_table` is supplied (typically from a one-time
    `SELECT COUNT(*)` per source table), the persisted progress
    percentage is meaningful; otherwise it shows 0 and consumers can
    rely on the per-table absolute counts."""
    totals = total_rows_by_table or {}

    def callback(table: TableRef, last_pk: tuple | None, rows_so_far: int) -> None:
        manager.create_checkpoint(
            migration_id=migration_id,
            table_name=table.qualified(),
            rows_processed=rows_so_far,
            total_rows=totals.get(table.qualified(), 0),
            last_rowid=encode_last_pk(last_pk),
            status="in_progress",
        )

    return callback


def encode_last_pk(last_pk: tuple | None) -> Optional[str]:
    """Tuple → JSON string. Returns None for None so the column stays
    NULL on the very first batch boundary."""
    if last_pk is None:
        return None
    return json.dumps(list(last_pk))


def decode_last_pk(rowid: Optional[str]) -> Optional[tuple]:
    """JSON string → tuple. Inverse of `encode_last_pk`."""
    if rowid is None:
        return None
    return tuple(json.loads(rowid))


def resume_pk(manager: CheckpointManager, migration_id: str, table: TableRef) -> Optional[tuple]:
    """Convenience: look up the last persisted PK for `table`. The
    runner can use this to skip ahead on a resumed run instead of
    re-reading rows it already loaded."""
    record = manager.get_latest_checkpoint(migration_id, table.qualified())
    if record is None:
        return None
    return decode_last_pk(record.last_rowid)


def make_resume_callback(manager: CheckpointManager, migration_id: str):
    """Return a `(table) -> Optional[tuple]` closure the runner can wire
    into `Runner.resume`. Mirrors `make_checkpoint_callback` so the
    runner stays oblivious to both the manager and the migration id."""

    def callback(table: TableRef) -> Optional[tuple]:
        return resume_pk(manager, migration_id, table)

    return callback
