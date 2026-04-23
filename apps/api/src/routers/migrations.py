"""Migrations CRUD + run/progress endpoints.

This is the self-hosted "I want to move rows" API surface. The CLI at
`src/migrate/__main__.py` is still the low-level entry point — this
router just wraps the same engine in a persistent-record, no-auth,
localhost-friendly shape so the web UI can drive it.

Auth model: none. Same philosophy as /assess, /settings, /license —
the operator owns the host, the trust boundary is the firewall. When
the cloud variant wants per-tenant migrations it'll gate this router
behind require_login + mount it conditionally.

Workflow:

    POST /api/v1/migrations         create row (status=pending)
    GET  /api/v1/migrations         list (newest first)
    GET  /api/v1/migrations/{id}    one row + checkpoints
    POST /api/v1/migrations/{id}/run      kicks off BackgroundTask
    GET  /api/v1/migrations/{id}/progress tight polling loop for the UI

The run endpoint returns 202 Accepted + the record (no wait). The UI
polls /progress every second or two until status is `completed`,
`completed_with_warnings`, or `failed`.
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime
from typing import List, Optional

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Request, status
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from ..auth.roles import require_role
from ..db import get_db
from ..models import MigrationCheckpointRecord, MigrationRecord
from ..services.audit import log_event
from ..services.migration_runner import (
    advise_record,
    dry_run_plan,
    quality_check_record,
    run_migration,
)
from ..services.queue import enqueue_migration
from ..utils.time import utc_now


router = APIRouter(prefix="/api/v1/migrations", tags=["migrations"])


# ─── Schemas ─────────────────────────────────────────────────────────────────


class MigrationCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=255)
    source_url: str = Field(..., min_length=1, max_length=2000)
    target_url: str = Field(..., min_length=1, max_length=2000)
    source_schema: str = Field(..., min_length=1, max_length=255)
    target_schema: str = Field(..., min_length=1, max_length=255)
    tables: Optional[List[str]] = None  # null = all
    batch_size: int = Field(default=5000, ge=1, le=500_000)
    create_tables: bool = False


class MigrationSummary(BaseModel):
    """Row shape returned by list + create. The full detail view
    includes checkpoints — kept separate so the list call stays cheap
    for installs with hundreds of historical runs."""

    id: str
    name: Optional[str]
    source_schema: Optional[str]
    target_schema: Optional[str]
    status: str
    rows_transferred: int
    total_rows: int
    started_at: Optional[datetime]
    completed_at: Optional[datetime]
    created_at: datetime


class CheckpointSummary(BaseModel):
    table_name: str
    rows_processed: int
    total_rows: int
    progress_percentage: float
    status: str
    last_rowid: Optional[str]
    error_message: Optional[str]
    updated_at: Optional[datetime]


class MigrationDetail(MigrationSummary):
    source_url: Optional[str]
    target_url: Optional[str]
    tables: Optional[List[str]]
    batch_size: Optional[int]
    create_tables: bool
    error_message: Optional[str]
    checkpoints: List[CheckpointSummary]


# ─── Conversion helpers ──────────────────────────────────────────────────────


def _tables_from_db(raw: Optional[str]) -> Optional[List[str]]:
    if not raw:
        return None
    try:
        return list(json.loads(raw))
    except json.JSONDecodeError:
        return None


def _to_summary(record: MigrationRecord) -> MigrationSummary:
    return MigrationSummary(
        id=str(record.id),
        name=record.name,
        source_schema=record.source_schema,
        target_schema=record.target_schema,
        status=record.status,
        rows_transferred=record.rows_transferred or 0,
        total_rows=record.total_rows or 0,
        started_at=record.started_at,
        completed_at=record.completed_at,
        created_at=record.created_at,
    )


def _to_detail(
    record: MigrationRecord, checkpoints: List[MigrationCheckpointRecord]
) -> MigrationDetail:
    return MigrationDetail(
        id=str(record.id),
        name=record.name,
        source_schema=record.source_schema,
        target_schema=record.target_schema,
        status=record.status,
        rows_transferred=record.rows_transferred or 0,
        total_rows=record.total_rows or 0,
        started_at=record.started_at,
        completed_at=record.completed_at,
        created_at=record.created_at,
        source_url=record.source_url,
        target_url=record.target_url,
        tables=_tables_from_db(record.tables),
        batch_size=record.batch_size,
        create_tables=bool(record.create_tables),
        error_message=record.error_message,
        checkpoints=[
            CheckpointSummary(
                table_name=c.table_name,
                rows_processed=c.rows_processed or 0,
                total_rows=c.total_rows or 0,
                progress_percentage=c.progress_percentage or 0.0,
                status=c.status,
                last_rowid=c.last_rowid,
                error_message=c.error_message,
                updated_at=c.updated_at,
            )
            for c in checkpoints
        ],
    )


# ─── Endpoints ───────────────────────────────────────────────────────────────


class ConnectionTestRequest(BaseModel):
    """Body for the connection tester. `schema` is optional — when
    supplied, we enumerate tables so the UI can surface 'found N
    tables' alongside the plain 'connection OK' message. We accept it
    under the `schema` alias since Pydantic v2 forbids the literal
    attribute name `schema`."""

    url: str = Field(..., min_length=1, max_length=2000)
    schema_: Optional[str] = Field(default=None, max_length=255, alias="schema")


class ConnectionTestResult(BaseModel):
    ok: bool
    dialect: Optional[str]
    message: str
    schema: Optional[str] = None
    tables_found: Optional[int] = None


@router.post("/test-connection", response_model=ConnectionTestResult)
def test_connection(
    body: ConnectionTestRequest,
    db: Session = Depends(get_db),
    _caller=Depends(require_role("admin", "operator")),
) -> ConnectionTestResult:
    """Open the DSN, run a trivial query, optionally introspect a
    schema for a table count. Budget ~10s — beyond that the UI
    shouldn't be blocking the operator.

    Failures are caught and returned as ok=false with a terse message.
    We don't surface full stack traces (leaks internals) but do
    include enough driver detail that 'wrong port' and 'wrong
    password' produce distinct user-facing text."""
    from sqlalchemy import create_engine, inspect
    from sqlalchemy import text as sql_text

    url = body.url.strip()
    dialect = (
        "oracle"
        if url.startswith("oracle")
        else ("postgres" if url.startswith("postgres") else None)
    )
    ping = "SELECT 1 FROM dual" if dialect == "oracle" else "SELECT 1"

    try:
        engine = create_engine(url, pool_pre_ping=True)
        try:
            with engine.connect() as conn:
                conn.execute(sql_text(ping))
                table_count: Optional[int] = None
                if body.schema_:
                    try:
                        tables = inspect(conn).get_table_names(schema=body.schema_)
                        table_count = len(tables)
                    except Exception:
                        # Connection worked but introspect didn't (wrong
                        # schema name / insufficient privilege on the
                        # catalog). Non-fatal for the UI.
                        table_count = None
        finally:
            engine.dispose()
    except Exception as exc:  # noqa: BLE001 — SQLA throws a wide cone
        return ConnectionTestResult(
            ok=False,
            dialect=dialect,
            message=str(exc).split("\n", 1)[0][:300],
            schema=body.schema_,
            tables_found=None,
        )

    return ConnectionTestResult(
        ok=True,
        dialect=dialect,
        message="Connected successfully.",
        schema=body.schema_,
        tables_found=table_count,
    )


@router.post("", response_model=MigrationSummary, status_code=status.HTTP_201_CREATED)
def create_migration(
    body: MigrationCreate,
    request: Request,
    db: Session = Depends(get_db),
    caller=Depends(require_role("admin", "operator")),
) -> MigrationSummary:
    record = MigrationRecord(
        id=uuid.uuid4(),
        name=body.name,
        # schema_name predates source_schema and is NOT NULL in the
        # existing table — mirror source_schema into it so the legacy
        # CheckpointManager.create_migration path keeps working.
        schema_name=body.source_schema,
        source_url=body.source_url,
        target_url=body.target_url,
        source_schema=body.source_schema,
        target_schema=body.target_schema,
        tables=json.dumps(body.tables) if body.tables else None,
        batch_size=body.batch_size,
        create_tables=body.create_tables,
        status="pending",
        created_at=utc_now(),
    )
    db.add(record)
    db.commit()
    db.refresh(record)
    log_event(
        db,
        request=request,
        user=caller,
        action="migration.created",
        resource_type="migration",
        resource_id=str(record.id),
        details={
            "name": record.name,
            "source_schema": record.source_schema,
            "target_schema": record.target_schema,
        },
    )
    return _to_summary(record)


@router.get("", response_model=List[MigrationSummary])
def list_migrations(
    db: Session = Depends(get_db),
    _caller=Depends(require_role("admin", "operator", "viewer")),
) -> List[MigrationSummary]:
    rows = (
        db.query(MigrationRecord)
        .order_by(MigrationRecord.created_at.desc())
        .all()
    )
    return [_to_summary(r) for r in rows]


@router.get("/{migration_id}", response_model=MigrationDetail)
def get_migration(
    migration_id: str,
    db: Session = Depends(get_db),
    _caller=Depends(require_role("admin", "operator", "viewer")),
) -> MigrationDetail:
    record = _load_or_404(db, migration_id)
    checkpoints = (
        db.query(MigrationCheckpointRecord)
        .filter(MigrationCheckpointRecord.migration_id == record.id)
        .order_by(MigrationCheckpointRecord.updated_at.desc())
        .all()
    )
    return _to_detail(record, checkpoints)


@router.post("/{migration_id}/run", response_model=MigrationSummary, status_code=status.HTTP_202_ACCEPTED)
async def run(
    migration_id: str,
    background: BackgroundTasks,
    request: Request,
    db: Session = Depends(get_db),
    caller=Depends(require_role("admin", "operator")),
) -> MigrationSummary:
    """Kick off the migration and return immediately.

    The UI polls `/progress` to follow along. We deliberately accept a
    re-run of an already-running migration and let the checkpoint-
    based resume logic handle it — pressing 'Run' twice is a UX
    hazard, not a correctness one."""
    record = _load_or_404(db, migration_id)

    # Flip to pending→in_progress before the background task starts so
    # the UI sees immediate feedback on the next poll; the runner
    # service itself will then set started_at and continue.
    record.status = "queued"
    db.commit()
    db.refresh(record)

    # Enqueue onto the arq queue (Redis). If Redis is unreachable —
    # say, the operator hasn't started the worker container yet — we
    # fall back to in-process BackgroundTasks so the migration still
    # runs. The audit event records which path we took.
    job_id = await enqueue_migration(str(record.id), background=background)

    log_event(
        db,
        request=request,
        user=caller,
        action="migration.run",
        resource_type="migration",
        resource_id=str(record.id),
        details={"name": record.name, "job_id": job_id},
    )
    return _to_summary(record)


@router.delete("/{migration_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_migration(
    migration_id: str,
    request: Request,
    db: Session = Depends(get_db),
    caller=Depends(require_role("admin")),
) -> None:
    """Admin-only. Removes the migration row and its checkpoints.
    Guard: refuse while the run is active so we don't orphan the
    background task — operators must wait for completion (or mark it
    failed) before clean-up."""
    record = _load_or_404(db, migration_id)
    if record.status in ("queued", "in_progress"):
        raise HTTPException(
            status_code=400,
            detail=f"cannot delete a migration while status={record.status!r}",
        )
    db.query(MigrationCheckpointRecord).filter(
        MigrationCheckpointRecord.migration_id == record.id
    ).delete()
    snapshot = {"name": record.name, "status": record.status}
    db.delete(record)
    db.commit()
    log_event(
        db,
        request=request,
        user=caller,
        action="migration.deleted",
        resource_type="migration",
        resource_id=migration_id,
        details=snapshot,
    )
    return None


class PlanTypeMapping(BaseModel):
    table: str
    column: str
    source_type: str
    pg_type: str


class PlanResponse(BaseModel):
    """Shape of the dry-run plan for the /preview UI. All lists can
    be empty; an empty `load_order` with a non-empty `tables_skipped`
    means nothing in the schema has a primary key."""

    tables_with_pk: List[str]
    tables_skipped: List[str]
    load_order: List[str]
    create_table_ddl: List[str]
    type_mappings: List[PlanTypeMapping]
    deferred_constraints: List[str]


@router.post("/{migration_id}/plan", response_model=PlanResponse)
def preview_plan(
    migration_id: str,
    db: Session = Depends(get_db),
    _caller=Depends(require_role("admin", "operator")),
) -> PlanResponse:
    """Introspect the source + target schemas and return everything
    the Run action *would* do, without doing any of it. Useful for
    'let me read the CREATE TABLEs before I pull the trigger.'"""
    record = _load_or_404(db, migration_id)
    try:
        result = dry_run_plan(record)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:  # noqa: BLE001 — SQLA + connection errors
        # Map connect/introspect failures to 502 so the UI can
        # distinguish "your DSN is wrong" from "bad request".
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"plan failed: {str(exc).split(chr(10))[0][:300]}",
        )

    return PlanResponse(
        tables_with_pk=result["tables_with_pk"],
        tables_skipped=result["tables_skipped"],
        load_order=result["load_order"],
        create_table_ddl=result["create_table_ddl"],
        type_mappings=[PlanTypeMapping(**tm) for tm in result["type_mappings"]],
        deferred_constraints=result["deferred_constraints"],
    )


class TableAdviceItem(BaseModel):
    qualified_name: str
    estimated_row_width_bytes: int
    recommended_batch_size: int
    rationale: str


class AdviceResponse(BaseModel):
    """Per-table batch_size recommendation. The deterministic baseline
    runs always; `used_ai=True` means Claude refined the numbers and
    `notes` carries operator-facing observations (low-cardinality PKs,
    LOB-dominant tables, FK fan-out, etc.)."""

    used_ai: bool
    per_table: List[TableAdviceItem]
    notes: List[str]


@router.post("/{migration_id}/advise", response_model=AdviceResponse)
def advise_plan(
    migration_id: str,
    ai: bool = False,
    db: Session = Depends(get_db),
    _caller=Depends(require_role("admin", "operator")),
) -> AdviceResponse:
    """Recommend a per-table `batch_size` for the migration's source
    schema. Pass `?ai=true` to enrich the deterministic baseline with a
    Claude refinement pass (requires `ANTHROPIC_API_KEY`); without it,
    we return the heuristic-only result instantly with no API cost."""
    record = _load_or_404(db, migration_id)

    ai_client = None
    if ai:
        try:
            from ..ai.client import AIClient

            ai_client = AIClient.fast(feature="migration_advisor")
        except RuntimeError as exc:
            # No API key configured. Surface as 400 so the UI can prompt
            # the operator instead of silently falling back.
            raise HTTPException(status_code=400, detail=str(exc))

    try:
        result = advise_record(record, ai_client=ai_client)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:  # noqa: BLE001 — connect/introspect errors
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"advise failed: {str(exc).split(chr(10))[0][:300]}",
        )

    return AdviceResponse(
        used_ai=result["used_ai"],
        per_table=[TableAdviceItem(**item) for item in result["per_table"]],
        notes=result["notes"],
    )


class QualityFindingItem(BaseModel):
    severity: str  # "info" | "warning" | "error"
    table: str
    column: Optional[str]
    check: str
    message: str


class QualityCheckResponse(BaseModel):
    """Layer 3 quality validation report. `overall_severity` is the
    rollup the UI uses to colour the report banner; `findings` is the
    full table-by-table list, ordered for UI grouping."""

    overall_severity: str  # "ok" | "warning" | "error"
    findings: List[QualityFindingItem]


@router.post(
    "/{migration_id}/quality-check", response_model=QualityCheckResponse
)
def quality_check(
    migration_id: str,
    db: Session = Depends(get_db),
    _caller=Depends(require_role("admin", "operator")),
) -> QualityCheckResponse:
    """Run Layer 3 quality validation against the source (always) and
    the target (when the migration has run). Pre-copy: VARCHAR length
    overflow / near-limit warnings. Post-copy: row count, NULL count,
    MIN/MAX agreement.

    Read-only. Safe to call before, during, or after a migration run."""
    record = _load_or_404(db, migration_id)
    try:
        result = quality_check_record(record)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:  # noqa: BLE001 — connect/introspect surface
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"quality check failed: {str(exc).split(chr(10))[0][:300]}",
        )
    return QualityCheckResponse(
        overall_severity=result["overall_severity"],
        findings=[QualityFindingItem(**f) for f in result["findings"]],
    )


@router.get("/{migration_id}/progress", response_model=MigrationDetail)
def progress(
    migration_id: str,
    db: Session = Depends(get_db),
    _caller=Depends(require_role("admin", "operator", "viewer")),
) -> MigrationDetail:
    """Exact same shape as GET /{id}; named separately so UIs can make
    the polling intent explicit in their trace logs, and so we can
    later split the polling path onto a read-replica if list traffic
    grows."""
    record = _load_or_404(db, migration_id)
    checkpoints = (
        db.query(MigrationCheckpointRecord)
        .filter(MigrationCheckpointRecord.migration_id == record.id)
        .order_by(MigrationCheckpointRecord.updated_at.desc())
        .all()
    )
    return _to_detail(record, checkpoints)


# ─── Internals ───────────────────────────────────────────────────────────────


def _load_or_404(db: Session, migration_id: str) -> MigrationRecord:
    try:
        uid = uuid.UUID(migration_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="invalid migration id format")
    record = db.get(MigrationRecord, uid)
    if record is None:
        raise HTTPException(status_code=404, detail=f"migration {migration_id!r} not found")
    return record


# Previously this module defined `_run_in_fresh_session` for direct
# BackgroundTasks use. Running now goes through `enqueue_migration`
# (arq → Redis, with in-process BackgroundTasks as the Redis-missing
# fallback). The inline fallback lives in src/services/queue.py.
