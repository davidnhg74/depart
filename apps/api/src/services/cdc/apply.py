"""Apply captured changes to the target Postgres database.

Consumes ``Change`` records from the CDC queue (see ``cdc.queue``)
and writes them to the target via idempotent UPSERTs (INSERT/UPDATE)
or straight DELETEs (DELETE). Idempotency is the central guarantee:
applying the same change twice is a no-op, so worker crashes, retries,
and replays are safe.

Two modes, operator-configurable per migration:

* ``per_row`` — each change in its own transaction. A failing row
  gets its error recorded; the rest still land. Forward progress
  through transient failures. Default.
* ``atomic`` — the whole batch runs in one transaction. Any failure
  rolls back every change in the batch. Strict audit semantics for
  customers who need "all or nothing" per LogMiner batch.

The caller passes a psycopg connection (v3). For ``atomic`` mode the
connection should be non-autocommit; for ``per_row`` either works
(each row gets its own explicit transaction either way).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Literal

import psycopg
from psycopg import sql

from .queue import Change


logger = logging.getLogger(__name__)


ApplyMode = Literal["per_row", "atomic"]


@dataclass
class ApplyResult:
    """Per-change outcome. The apply worker relays these back to
    ``cdc.queue.mark_applied`` / ``mark_failed`` so the DB state
    matches what actually landed on the target."""

    change_id: int
    ok: bool
    error: str | None = None


# ─── Public entry point ──────────────────────────────────────────────


def apply_changes(
    pg_conn: psycopg.Connection,
    target_schema: str,
    changes: list[Change],
    *,
    mode: ApplyMode = "per_row",
) -> list[ApplyResult]:
    """Write each change to the target. Returns a result per input
    change in the same order. Never raises on apply errors — those
    come back in the result list."""
    if not changes:
        return []
    # Precondition — these must be persisted rows from the queue, not
    # freshly-built test objects. Treating a missing `.id` as a soft
    # row-level error would mask a real programming bug (the apply
    # worker is supposed to always fetch via the queue first).
    missing = [c for c in changes if c.id is None]
    if missing:
        raise ValueError(
            f"{len(missing)} Change(s) have no .id — apply_changes() "
            "requires rows fetched via cdc.queue.fetch_unapplied"
        )
    if mode == "atomic":
        return _apply_atomic(pg_conn, target_schema, changes)
    return _apply_per_row(pg_conn, target_schema, changes)


# ─── Atomic mode ─────────────────────────────────────────────────────


def _apply_atomic(
    pg_conn: psycopg.Connection,
    target_schema: str,
    changes: list[Change],
) -> list[ApplyResult]:
    try:
        with pg_conn.transaction():
            with pg_conn.cursor() as cur:
                for c in changes:
                    _apply_one(cur, target_schema, c)
        return [ApplyResult(change_id=_cid(c), ok=True) for c in changes]
    except Exception as exc:  # noqa: BLE001 — need to capture any driver error
        msg = _fmt_error(exc)
        logger.warning("CDC atomic batch failed: %s", msg)
        return [ApplyResult(change_id=_cid(c), ok=False, error=msg) for c in changes]


# ─── Per-row mode ────────────────────────────────────────────────────


def _apply_per_row(
    pg_conn: psycopg.Connection,
    target_schema: str,
    changes: list[Change],
) -> list[ApplyResult]:
    results: list[ApplyResult] = []
    for c in changes:
        try:
            with pg_conn.transaction():
                with pg_conn.cursor() as cur:
                    _apply_one(cur, target_schema, c)
            results.append(ApplyResult(change_id=_cid(c), ok=True))
        except Exception as exc:  # noqa: BLE001
            msg = _fmt_error(exc)
            logger.info(
                "CDC per-row apply failed for change %s (%s.%s, SCN %d): %s",
                _cid(c),
                c.source_schema,
                c.source_table,
                c.scn,
                msg,
            )
            results.append(
                ApplyResult(change_id=_cid(c), ok=False, error=msg)
            )
    return results


# ─── Per-change SQL builders ─────────────────────────────────────────


def _apply_one(cur: psycopg.Cursor, target_schema: str, c: Change) -> None:
    """Build + execute one UPSERT / DELETE for a single change.
    Raises on DB error — callers handle that for their mode."""
    if c.op == "D":
        _execute_delete(cur, target_schema, c)
    elif c.op in ("I", "U"):
        _execute_upsert(cur, target_schema, c)
    else:
        raise ValueError(f"unknown CDC op {c.op!r}")


def _execute_delete(
    cur: psycopg.Cursor, target_schema: str, c: Change
) -> None:
    pk_cols = list(c.pk.keys())
    query = sql.SQL(
        "DELETE FROM {schema}.{table} WHERE {where}"
    ).format(
        schema=sql.Identifier(target_schema),
        table=sql.Identifier(c.source_table.lower()),
        where=sql.SQL(" AND ").join(
            sql.SQL("{} = %s").format(sql.Identifier(col.lower()))
            for col in pk_cols
        ),
    )
    cur.execute(query, [c.pk[col] for col in pk_cols])


def _execute_upsert(
    cur: psycopg.Cursor, target_schema: str, c: Change
) -> None:
    """INSERT ... ON CONFLICT (pk) DO UPDATE SET <non-pk cols>. Uses
    the full row from ``c.after`` — that's what LogMiner gives us for
    both INSERT and UPDATE ops."""
    if c.after is None:
        raise ValueError(
            f"CDC {c.op} change at SCN {c.scn} has no 'after' payload"
        )
    cols = list(c.after.keys())
    pk_cols = list(c.pk.keys())
    non_pk_cols = [col for col in cols if col not in pk_cols]

    col_idents = [sql.Identifier(col.lower()) for col in cols]
    placeholders = [sql.Placeholder() for _ in cols]

    if non_pk_cols:
        update_clause = sql.SQL(", ").join(
            sql.SQL("{c} = EXCLUDED.{c}").format(c=sql.Identifier(col.lower()))
            for col in non_pk_cols
        )
        on_conflict = sql.SQL(
            "ON CONFLICT ({pk}) DO UPDATE SET {set}"
        ).format(
            pk=sql.SQL(", ").join(
                sql.Identifier(col.lower()) for col in pk_cols
            ),
            set=update_clause,
        )
    else:
        # Table is all-PK (junction tables, etc.) — INSERT with DO NOTHING
        # keeps apply idempotent.
        on_conflict = sql.SQL("ON CONFLICT ({pk}) DO NOTHING").format(
            pk=sql.SQL(", ").join(
                sql.Identifier(col.lower()) for col in pk_cols
            ),
        )

    query = sql.SQL(
        "INSERT INTO {schema}.{table} ({cols}) VALUES ({vals}) {conflict}"
    ).format(
        schema=sql.Identifier(target_schema),
        table=sql.Identifier(c.source_table.lower()),
        cols=sql.SQL(", ").join(col_idents),
        vals=sql.SQL(", ").join(placeholders),
        conflict=on_conflict,
    )
    cur.execute(query, [c.after[col] for col in cols])


# ─── Helpers ─────────────────────────────────────────────────────────


def _cid(c: Change) -> int:
    """Every Change we apply must have come from the queue, so `id`
    is required. Surface the contract violation clearly if a caller
    tries to apply a freshly-built (unpersisted) Change."""
    if c.id is None:
        raise ValueError(
            "Change.id is None — apply_changes() requires persisted rows "
            "(fetched via cdc.queue.fetch_unapplied)"
        )
    return c.id


def _fmt_error(exc: BaseException) -> str:
    return f"{type(exc).__name__}: {exc}"[:4000]
