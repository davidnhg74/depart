"""Run a migration end-to-end from a MigrationRecord.

This is the service layer that the `/api/v1/migrations/{id}/run`
endpoint hands off to (via FastAPI BackgroundTasks). Everything the
CLI at `src/migrate/__main__.py` does, but driven by a DB record
instead of argparse — connect, introspect, optional DDL, plan, run,
catch sequences up, verify, then update the MigrationRecord.

Runs synchronously inside the background task. For a real production
scheduler this would become a queue worker (Celery, RQ, arq); for v1
the FastAPI BackgroundTasks path is sufficient — migrations run
serially on the same process, which is fine for single-tenant
self-hosted.
"""

from __future__ import annotations

import json
import logging
import traceback
from typing import Optional

import psycopg
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from ..migrate.checkpoint_adapter import (
    make_checkpoint_callback,
    make_resume_callback,
)
from ..migrate.ddl import apply_ddl, generate_schema_ddl, map_oracle_type, map_pg_type
from ..migrate.introspect import introspect
from ..migrate.keyset import Dialect
from ..migrate.planner import TableRef, plan_load_order
from ..migrate.runner import Runner
from ..migration.checkpoint import CheckpointManager
from ..models import MigrationRecord
from ..utils.time import utc_now
from . import webhook_service


logger = logging.getLogger(__name__)


def run_migration(db: Session, migration_id: str) -> None:
    """Execute the migration named by `migration_id`.

    Idempotent-on-rerun via the checkpoint system: if the record's
    status is already `in_progress` or `completed` we still re-run;
    the runner's resume callback will pick up at the last checkpointed
    PK for each table. Operators can re-POST to /run to resume a
    crashed migration from where it died.

    Any exception gets captured onto the record (status=failed,
    error_message=...). We never re-raise — this runs inside a FastAPI
    BackgroundTask and an uncaught exception there just vanishes into
    the server log."""
    record: Optional[MigrationRecord] = db.get(MigrationRecord, migration_id)
    if record is None:
        logger.error("migration %s not found — cannot run", migration_id)
        return

    # Basic config sanity. The UI enforces these too, but never trust a
    # client record end-to-end.
    if not record.source_url or not record.target_url:
        _fail(db, record, "source_url and target_url are required")
        return
    if not record.source_schema or not record.target_schema:
        _fail(db, record, "source_schema and target_schema are required")
        return

    record.status = "in_progress"
    record.started_at = utc_now()
    record.error_message = None
    db.commit()

    try:
        _run_inner(db, record)
    except Exception as exc:
        logger.exception("migration %s failed", migration_id)
        # tb summary makes debugging from the UI practical without
        # leaking our server internals.
        _fail(db, record, f"{type(exc).__name__}: {exc}\n{traceback.format_exc(limit=3)}")


def _fail(db: Session, record: MigrationRecord, message: str) -> None:
    record.status = "failed"
    record.error_message = message
    record.completed_at = utc_now()
    db.commit()
    _fire_terminal_webhook(db, record, "migration.failed")


def _migration_event_payload(record: MigrationRecord) -> dict:
    return {
        "migration_id": str(record.id),
        "name": record.name,
        "status": record.status,
        "schema_name": record.schema_name,
        "source_schema": record.source_schema,
        "target_schema": record.target_schema,
        "started_at": record.started_at.isoformat() if record.started_at else None,
        "completed_at": record.completed_at.isoformat() if record.completed_at else None,
        "elapsed_seconds": record.elapsed_seconds,
        "rows_transferred": record.rows_transferred,
        "total_rows": record.total_rows,
        "error_message": record.error_message,
    }


def _fire_terminal_webhook(
    db: Session, record: MigrationRecord, event: str
) -> None:
    try:
        webhook_service.fire_event(db, event, _migration_event_payload(record))
    except Exception:
        # fire_event already swallows per-endpoint errors; this is a
        # belt-and-braces guard for anything raised before the loop
        # (e.g. a DB error loading endpoints). Never propagate.
        logger.exception("webhook dispatch for %s failed", event)


def _run_inner(db: Session, record: MigrationRecord) -> None:
    src_dialect = Dialect.ORACLE if record.source_url.startswith("oracle") else Dialect.POSTGRES

    src_engine = create_engine(record.source_url)
    dst_engine = create_engine(record.target_url)
    SrcSession = sessionmaker(bind=src_engine)
    DstSession = sessionmaker(bind=dst_engine)
    src_session = SrcSession()
    dst_session = DstSession()

    # Raw psycopg conn for binary COPY — autocommit so COPY commits
    # immediately after each batch, matching the CLI's behavior.
    pg_url = record.target_url.replace("postgresql+psycopg://", "postgresql://")
    pg_conn = psycopg.connect(pg_url, autocommit=True)

    try:
        logger.info("introspecting %s.%s", src_dialect.value, record.source_schema)
        schema = introspect(src_session, src_dialect, record.source_schema)

        wanted_tables = _parse_tables(record.tables)
        if wanted_tables is not None:
            schema.tables = [t for t in schema.tables if t.name in wanted_tables]

        specs = schema.build_specs(target_schema=record.target_schema)
        if not specs:
            _fail(
                db,
                record,
                f"no migratable tables in {record.source_schema!r} "
                "(nothing after PK / whitelist filter)",
            )
            return

        plan = plan_load_order(
            [s.target_table for s in specs.values()],
            [
                type(fk)(
                    name=fk.name,
                    from_table=_rewrite_schema(
                        fk.from_table, record.source_schema, record.target_schema
                    ),
                    to_table=_rewrite_schema(
                        fk.to_table, record.source_schema, record.target_schema
                    ),
                    deferrable=fk.deferrable,
                )
                for fk in schema.foreign_keys
            ],
        )

        if record.create_tables:
            _create_target_tables(pg_url, schema, specs, plan, src_dialect)

        # Checkpoint adapter — run's progress flows through here back
        # into the migrations / migration_checkpoints tables, so the UI
        # can poll `/progress` and render per-table state.
        manager = CheckpointManager(db)
        callback = make_checkpoint_callback(manager, str(record.id))
        resume = make_resume_callback(manager, str(record.id))

        runner = Runner(
            source_session=src_session,
            target_session=dst_session,
            target_pg_conn=pg_conn,
            source_dialect=src_dialect,
            batch_size=record.batch_size or 5000,
            checkpoint=callback,
            resume=resume,
        )

        result = runner.execute(plan, specs)

        # Refresh the record because the SQLAlchemy session used by the
        # CheckpointManager may have written to it on this same row —
        # avoid clobbering its updates.
        db.refresh(record)
        record.rows_transferred = result.total_rows
        record.total_rows = result.total_rows
        record.status = "completed" if result.all_verified else "completed_with_warnings"
        record.completed_at = utc_now()
        if not result.all_verified:
            failures = [
                f"{qn}: {r.discrepancy}"
                for qn, r in result.tables.items()
                if not r.verified and r.discrepancy
            ]
            record.error_message = "verification warnings:\n" + "\n".join(failures)
        db.commit()
        logger.info(
            "migration %s finished: status=%s rows=%d",
            record.id,
            record.status,
            record.rows_transferred,
        )
        _fire_terminal_webhook(db, record, "migration.completed")
    finally:
        src_session.close()
        dst_session.close()
        try:
            pg_conn.close()
        except Exception:
            pass
        src_engine.dispose()
        dst_engine.dispose()


def dry_run_plan(record: MigrationRecord) -> dict:
    """Same introspect + plan path as `run_migration`, minus the load.

    Returns a dict the UI can render before the operator hits Run:

        {
            "tables_with_pk":    [qualified_name, ...],
            "tables_skipped":    [qualified_name, ...],    # no PK → unmigratable
            "load_order":        [qualified_name, ...],    # parents first
            "create_table_ddl":  [sql_stmt, ...],          # empty when create_tables=False
            "type_mappings":     [{table, column, oracle_type, pg_type}, ...],
            "deferred_constraints": [fk_name, ...],
        }

    Read-only: opens source + target connections, introspects, asks
    the planner — never touches rows and never writes DDL. Safe to
    call against a production system."""
    if not record.source_url or not record.target_url:
        raise ValueError("source_url and target_url are required")
    if not record.source_schema or not record.target_schema:
        raise ValueError("source_schema and target_schema are required")

    src_dialect = Dialect.ORACLE if record.source_url.startswith("oracle") else Dialect.POSTGRES

    src_engine = create_engine(record.source_url)
    SrcSession = sessionmaker(bind=src_engine)
    src_session = SrcSession()
    try:
        schema = introspect(src_session, src_dialect, record.source_schema)

        wanted = _parse_tables(record.tables)
        if wanted is not None:
            schema.tables = [t for t in schema.tables if t.name in wanted]

        # Split migratable vs skipped for the UI.
        tables_with_pk_ids = set()
        tables_skipped = []
        for t in schema.tables:
            if schema.primary_keys.get(t.qualified()):
                tables_with_pk_ids.add(t.qualified())
            else:
                tables_skipped.append(t.qualified())

        specs = schema.build_specs(target_schema=record.target_schema)
        if not specs:
            return {
                "tables_with_pk": [],
                "tables_skipped": tables_skipped,
                "load_order": [],
                "create_table_ddl": [],
                "type_mappings": [],
                "deferred_constraints": [],
            }

        plan = plan_load_order(
            [s.target_table for s in specs.values()],
            [
                type(fk)(
                    name=fk.name,
                    from_table=_rewrite_schema(
                        fk.from_table, record.source_schema, record.target_schema
                    ),
                    to_table=_rewrite_schema(
                        fk.to_table, record.source_schema, record.target_schema
                    ),
                    deferrable=fk.deferrable,
                )
                for fk in schema.foreign_keys
            ],
        )
        load_order = [t.qualified() for t in plan.flat_tables()]

        create_ddl: list[str] = []
        if record.create_tables:
            map_type = map_oracle_type if src_dialect == Dialect.ORACLE else map_pg_type
            cols_by_target = {
                s.target_table.qualified(): schema.column_metadata[s.source_table.qualified()]
                for s in specs.values()
            }
            pks_by_target = {
                s.target_table.qualified(): s.pk_columns for s in specs.values()
            }
            create_ddl = generate_schema_ddl(
                plan.flat_tables(),
                cols_by_target,
                pks_by_target,
                map_type=map_type,
            )

        # Per-column type mapping for the UI's table-by-table tooltip.
        map_type = map_oracle_type if src_dialect == Dialect.ORACLE else map_pg_type
        type_mappings: list[dict] = []
        for spec in specs.values():
            for col in schema.column_metadata.get(spec.source_table.qualified(), []):
                try:
                    pg = map_type(col)
                except ValueError as e:
                    pg = f"(no mapping: {e})"
                type_mappings.append(
                    {
                        "table": spec.source_table.qualified(),
                        "column": col.name,
                        "source_type": col.data_type,
                        "pg_type": pg,
                    }
                )

        deferred = []
        for g in plan.groups:
            for fk in g.deferred_constraints:
                deferred.append(fk.name)

        return {
            "tables_with_pk": sorted(tables_with_pk_ids),
            "tables_skipped": sorted(tables_skipped),
            "load_order": load_order,
            "create_table_ddl": create_ddl,
            "type_mappings": type_mappings,
            "deferred_constraints": deferred,
        }
    finally:
        src_session.close()
        src_engine.dispose()


def _parse_tables(raw: Optional[str]) -> Optional[set[str]]:
    """Decode the JSON `tables` field. None / empty / "null" = all tables."""
    if not raw:
        return None
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        # Treat a bad value as "all tables" rather than crashing the
        # whole run — a pickier caller can still filter client-side.
        logger.warning("migration tables filter didn't parse as JSON: %r", raw)
        return None
    if not parsed:
        return None
    return {str(t).strip() for t in parsed if str(t).strip()}


def _rewrite_schema(ref, src_schema: str, dst_schema: str):
    """FK rewrite — mirrors the CLI helper so the planner operates in
    the destination namespace."""
    if ref.schema and ref.schema.upper() == src_schema.upper():
        return TableRef(schema=dst_schema, name=ref.name)
    return ref


def _create_target_tables(pg_url, schema, specs, plan, src_dialect) -> None:
    """Generate + apply CREATE TABLE IF NOT EXISTS statements in
    load-plan order. Borrowed shape from the CLI helper."""
    map_type = map_oracle_type if src_dialect == Dialect.ORACLE else map_pg_type

    cols_by_target: dict = {}
    pks_by_target: dict = {}
    for spec in specs.values():
        source_qn = spec.source_table.qualified()
        target_qn = spec.target_table.qualified()
        cols_by_target[target_qn] = schema.column_metadata[source_qn]
        pks_by_target[target_qn] = spec.pk_columns

    stmts = generate_schema_ddl(
        plan.flat_tables(),
        cols_by_target,
        pks_by_target,
        map_type=map_type,
    )
    ddl_conn = psycopg.connect(pg_url)  # non-autocommit → transactional DDL
    try:
        apply_ddl(ddl_conn, stmts)
    finally:
        ddl_conn.close()
