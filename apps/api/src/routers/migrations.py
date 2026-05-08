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
from dataclasses import asdict
from datetime import datetime
from typing import List, Optional

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Request, status
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from ..auth.roles import require_role
from ..db import get_db
from ..models import (
    CutoverReadinessSnapshot,
    DataSampleResult,
    MigrationCheckpointRecord,
    MigrationRecord,
    ProductionMonitorSnapshot,
)
from ..services.audit import log_event
from ..services.anomaly_service import anomaly_check_record
from ..services.cutover_service import assess_cutover_readiness
from ..services.monitor_service import monitor_migration
from ..services.sampler_service import sample_migration
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
        # Tenant ownership. None in self-hosted single-tenant mode
        # (auth disabled → caller is None); set in cloud mode so the
        # list/get/run filters apply to this row going forward.
        user_id=caller.id if caller is not None else None,
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
    caller=Depends(require_role("admin", "operator", "viewer")),
) -> List[MigrationSummary]:
    rows = (
        _filter_for_user(db.query(MigrationRecord), caller)
        .order_by(MigrationRecord.created_at.desc())
        .all()
    )
    return [_to_summary(r) for r in rows]


@router.get("/{migration_id}", response_model=MigrationDetail)
def get_migration(
    migration_id: str,
    db: Session = Depends(get_db),
    caller=Depends(require_role("admin", "operator", "viewer")),
) -> MigrationDetail:
    record = _load_or_404_for_user(db, migration_id, caller)
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
    record = _load_or_404_for_user(db, migration_id, caller)

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
    record = _load_or_404_for_user(db, migration_id, caller)
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
    caller=Depends(require_role("admin", "operator")),
) -> PlanResponse:
    """Introspect the source + target schemas and return everything
    the Run action *would* do, without doing any of it. Useful for
    'let me read the CREATE TABLEs before I pull the trigger.'"""
    record = _load_or_404_for_user(db, migration_id, caller)
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
    caller=Depends(require_role("admin", "operator")),
) -> AdviceResponse:
    """Recommend a per-table `batch_size` for the migration's source
    schema. Pass `?ai=true` to enrich the deterministic baseline with a
    Claude refinement pass (requires `ANTHROPIC_API_KEY`); without it,
    we return the heuristic-only result instantly with no API cost."""
    record = _load_or_404_for_user(db, migration_id, caller)

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
    caller=Depends(require_role("admin", "operator")),
) -> QualityCheckResponse:
    """Run Layer 3 quality validation against the source (always) and
    the target (when the migration has run). Pre-copy: VARCHAR length
    overflow / near-limit warnings. Post-copy: row count, NULL count,
    MIN/MAX agreement.

    Read-only. Safe to call before, during, or after a migration run."""
    record = _load_or_404_for_user(db, migration_id, caller)
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


class AnomalyFindingItem(BaseModel):
    severity: str
    table: str
    column: Optional[str]
    anomaly_type: str
    message: str
    recommended_action: str


class AnomalyCheckResponse(BaseModel):
    """Layer 6 AI anomaly detection report."""

    overall_severity: str  # "clean" | "info" | "warning" | "error"
    findings: List[AnomalyFindingItem]
    used_ai: bool
    analysis_id: str
    tables_sampled: int


@router.post(
    "/{migration_id}/check-anomalies", response_model=AnomalyCheckResponse
)
def check_anomalies(
    migration_id: str,
    db: Session = Depends(get_db),
    caller=Depends(require_role("admin", "operator")),
) -> AnomalyCheckResponse:
    """Run Layer 6 AI anomaly detection against the target database.

    Samples post-migration data distributions and uses Claude to surface
    anomalies worth reviewing before cutover. Falls back to rule-based
    checks when ANTHROPIC_API_KEY is not configured.

    Safe to call multiple times — each call writes a new AnomalyAnalysis
    row and updates migrations.anomaly_analysis_id to the latest."""
    record = _load_or_404_for_user(db, migration_id, caller)
    if record.status not in ("completed", "completed_with_warnings"):
        raise HTTPException(
            status_code=400,
            detail=(
                f"Anomaly check requires a completed migration; "
                f"current status is '{record.status}'."
            ),
        )
    try:
        result = anomaly_check_record(record, db)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:  # noqa: BLE001 — connect surface
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"anomaly check failed: {str(exc).split(chr(10))[0][:300]}",
        )
    return AnomalyCheckResponse(
        overall_severity=result.overall_severity,
        findings=[AnomalyFindingItem(**f.to_dict()) for f in result.findings],
        used_ai=result.used_ai,
        analysis_id=result.analysis_id,
        tables_sampled=result.tables_sampled,
    )


class MonitorFindingItem(BaseModel):
    severity: str
    check_name: str
    table: Optional[str]
    message: str
    recommended_action: str


class MonitorSnapshotItem(BaseModel):
    snapshot_id: str
    created_at: str
    overall_severity: str
    tables_checked: int
    findings: List[MonitorFindingItem]


class MonitorResponse(BaseModel):
    """Layer 7 production monitor report."""

    overall_severity: str  # "clean" | "info" | "warning" | "error"
    findings: List[MonitorFindingItem]
    snapshot_id: str
    tables_checked: int


@router.post("/{migration_id}/monitor", response_model=MonitorResponse)
def run_monitor_check(
    migration_id: str,
    db: Session = Depends(get_db),
    caller=Depends(require_role("admin", "operator")),
) -> MonitorResponse:
    """Run Layer 7 production monitor against the target database.

    Collects row counts, dead-tuple bloat, and CDC lag. Compares row
    counts against the previous snapshot to detect drift. Persists a
    new snapshot on every call so the next call has an updated baseline.

    Safe to call on any migration with a target_url — not restricted
    to completed status so operators can monitor in-progress migrations too."""
    record = _load_or_404_for_user(db, migration_id, caller)
    if not record.target_url:
        raise HTTPException(
            status_code=400,
            detail="Migration has no target_url — cannot run production monitor.",
        )
    try:
        result = monitor_migration(record, db)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"monitor failed: {str(exc).split(chr(10))[0][:300]}",
        )
    return MonitorResponse(
        overall_severity=result.overall_severity,
        findings=[MonitorFindingItem(**f.to_dict()) for f in result.findings],
        snapshot_id=result.snapshot_id,
        tables_checked=result.tables_checked,
    )


@router.get("/{migration_id}/monitor", response_model=List[MonitorSnapshotItem])
def list_monitor_snapshots(
    migration_id: str,
    db: Session = Depends(get_db),
    caller=Depends(require_role("admin", "operator", "viewer")),
) -> List[MonitorSnapshotItem]:
    """Return the 10 most recent production monitor snapshots for a migration."""
    record = _load_or_404_for_user(db, migration_id, caller)
    snapshots = (
        db.query(ProductionMonitorSnapshot)
        .filter(ProductionMonitorSnapshot.migration_id == record.id)
        .order_by(ProductionMonitorSnapshot.created_at.desc())
        .limit(10)
        .all()
    )
    return [
        MonitorSnapshotItem(
            snapshot_id=str(s.id),
            created_at=s.created_at.isoformat(),
            overall_severity=s.overall_severity,
            tables_checked=s.tables_checked,
            findings=[MonitorFindingItem(**f) for f in (s.findings or [])],
        )
        for s in snapshots
    ]


class SampleMismatchItem(BaseModel):
    table: str
    pk_values: dict
    column: str
    oracle_value: Optional[str]
    pg_value: Optional[str]
    mismatch_type: str  # value_mismatch | missing_in_pg | null_mismatch


class SampleRequest(BaseModel):
    sample_size: int = Field(default=100, ge=1, le=10000)


class SampleResponse(BaseModel):
    """Layer 8 row-level data sampler report."""

    overall_status: str  # "clean" | "mismatches_found"
    result_id: str
    tables_sampled: int
    tables_skipped: int
    mismatch_count: int
    mismatches: List[SampleMismatchItem]


class SampleResultItem(BaseModel):
    result_id: str
    created_at: str
    overall_status: str
    tables_sampled: int
    tables_skipped: int
    mismatch_count: int
    sample_size: int


@router.post("/{migration_id}/sample", response_model=SampleResponse)
def run_sample(
    migration_id: str,
    body: SampleRequest = SampleRequest(),
    db: Session = Depends(get_db),
    caller=Depends(require_role("admin", "operator")),
) -> SampleResponse:
    """Run Layer 8 row-level data sampler.

    Samples up to sample_size rows per table from the Oracle source,
    fetches matching rows from the PG target by primary key, and reports
    column-level value mismatches. Tables without a primary key are skipped.

    Requires both source_url and target_url on the migration record."""
    record = _load_or_404_for_user(db, migration_id, caller)
    if not record.source_url:
        raise HTTPException(
            status_code=400,
            detail="Migration has no source_url — cannot run data sampler.",
        )
    if not record.target_url:
        raise HTTPException(
            status_code=400,
            detail="Migration has no target_url — cannot run data sampler.",
        )
    try:
        result = sample_migration(record, db, sample_size=body.sample_size)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"sampler failed: {str(exc).split(chr(10))[0][:300]}",
        )
    return SampleResponse(
        overall_status=result.overall_status,
        result_id=result.result_id,
        tables_sampled=result.tables_sampled,
        tables_skipped=result.tables_skipped,
        mismatch_count=result.mismatch_count,
        mismatches=[SampleMismatchItem(**m.to_dict()) for m in result.mismatches],
    )


@router.get("/{migration_id}/sample", response_model=List[SampleResultItem])
def list_sample_results(
    migration_id: str,
    db: Session = Depends(get_db),
    caller=Depends(require_role("admin", "operator", "viewer")),
) -> List[SampleResultItem]:
    """Return the 10 most recent data sampling results for a migration."""
    record = _load_or_404_for_user(db, migration_id, caller)
    results = (
        db.query(DataSampleResult)
        .filter(DataSampleResult.migration_id == record.id)
        .order_by(DataSampleResult.created_at.desc())
        .limit(10)
        .all()
    )
    return [
        SampleResultItem(
            result_id=str(r.id),
            created_at=r.created_at.isoformat(),
            overall_status=r.overall_status,
            tables_sampled=r.tables_sampled,
            tables_skipped=r.tables_skipped,
            mismatch_count=r.mismatch_count,
            sample_size=r.sample_size,
        )
        for r in results
    ]


@router.get("/{migration_id}/progress", response_model=MigrationDetail)
def progress(
    migration_id: str,
    db: Session = Depends(get_db),
    caller=Depends(require_role("admin", "operator", "viewer")),
) -> MigrationDetail:
    """Exact same shape as GET /{id}; named separately so UIs can make
    the polling intent explicit in their trace logs, and so we can
    later split the polling path onto a read-replica if list traffic
    grows."""
    record = _load_or_404_for_user(db, migration_id, caller)
    checkpoints = (
        db.query(MigrationCheckpointRecord)
        .filter(MigrationCheckpointRecord.migration_id == record.id)
        .order_by(MigrationCheckpointRecord.updated_at.desc())
        .all()
    )
    return _to_detail(record, checkpoints)


# ─── Layer 9: Cutover readiness ───────────────────────────────────────────────


class ReadinessSignalItem(BaseModel):
    layer: str
    label: str
    status: str  # ok | advisory | blocking | not_run
    summary: str
    detail: Optional[str] = None


class CutoverReadinessResponse(BaseModel):
    """Layer 9 cutover readiness assessment."""

    snapshot_id: str
    ready_to_cut: bool
    score: int            # 0–100
    blocking_count: int
    advisory_count: int
    not_run_count: int
    signals: List[ReadinessSignalItem]


class CutoverReadinessItem(BaseModel):
    """Summary row for the history list."""

    snapshot_id: str
    created_at: str
    ready_to_cut: bool
    score: int
    blocking_count: int
    advisory_count: int


@router.post(
    "/{migration_id}/cutover-readiness",
    response_model=CutoverReadinessResponse,
)
def run_cutover_readiness(
    migration_id: str,
    db: Session = Depends(get_db),
    caller=Depends(require_role("admin", "operator")),
) -> CutoverReadinessResponse:
    """Run Layer 9 cutover readiness assessment.

    Aggregates the latest Layer 6 (anomaly), Layer 7 (monitor), and
    Layer 8 (sampler) results together with migration status and CDC lag
    to produce a go/no-go verdict for cutting over production traffic
    from Oracle to PostgreSQL. Safe to call multiple times; each call
    persists a new snapshot."""
    record = _load_or_404_for_user(db, migration_id, caller)
    try:
        readiness, snap = assess_cutover_readiness(record, db)
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"cutover readiness check failed: {str(exc).split(chr(10))[0][:300]}",
        )
    return CutoverReadinessResponse(
        snapshot_id=str(snap.id),
        ready_to_cut=readiness.ready_to_cut,
        score=readiness.score,
        blocking_count=readiness.blocking_count,
        advisory_count=readiness.advisory_count,
        not_run_count=readiness.not_run_count,
        signals=[ReadinessSignalItem(**asdict(s)) for s in readiness.signals],
    )


@router.get(
    "/{migration_id}/cutover-readiness",
    response_model=List[CutoverReadinessItem],
)
def list_cutover_readiness(
    migration_id: str,
    db: Session = Depends(get_db),
    caller=Depends(require_role("admin", "operator", "viewer")),
) -> List[CutoverReadinessItem]:
    """Return the 10 most recent cutover readiness assessments for a migration."""
    record = _load_or_404_for_user(db, migration_id, caller)
    snaps = (
        db.query(CutoverReadinessSnapshot)
        .filter(CutoverReadinessSnapshot.migration_id == record.id)
        .order_by(CutoverReadinessSnapshot.created_at.desc())
        .limit(10)
        .all()
    )
    return [
        CutoverReadinessItem(
            snapshot_id=str(s.id),
            created_at=s.created_at.isoformat(),
            ready_to_cut=s.ready_to_cut,
            score=s.score,
            blocking_count=s.blocking_count,
            advisory_count=s.advisory_count,
        )
        for s in snaps
    ]


# ─── Internals ───────────────────────────────────────────────────────────────


def _load_or_404_for_user(
    db: Session, migration_id: str, caller
) -> MigrationRecord:
    """Look up a migration with tenant scoping.

    Cloud mode (`caller is not None`): the row must exist AND be
    owned by `caller.id`. A row owned by someone else returns 404 —
    deliberately, so cross-tenant probes can't enumerate IDs by
    distinguishing 404-not-found from 403-forbidden.

    Self-hosted single-tenant mode (`caller is None`, when
    `ENABLE_SELF_HOSTED_AUTH=false` makes `require_role` a no-op):
    no tenant filter applies — the operator owns the entire install,
    sees everything, exactly the legacy single-tenant behavior. The
    `user_id` column on existing rows stays NULL in that mode."""
    try:
        uid = uuid.UUID(migration_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="invalid migration id format")
    record = db.get(MigrationRecord, uid)
    if record is None:
        raise HTTPException(status_code=404, detail=f"migration {migration_id!r} not found")
    if caller is not None and record.user_id != caller.id:
        # Cross-tenant: behave identically to "row doesn't exist".
        raise HTTPException(status_code=404, detail=f"migration {migration_id!r} not found")
    return record


def _filter_for_user(query, caller):
    """Apply the tenant filter to a list query. No-op in single-tenant
    self-hosted mode (caller is None); strict equality in cloud mode."""
    if caller is None:
        return query
    return query.filter(MigrationRecord.user_id == caller.id)


# Previously this module defined `_run_in_fresh_session` for direct
# BackgroundTasks use. Running now goes through `enqueue_migration`
# (arq → Redis, with in-process BackgroundTasks as the Redis-missing
# fallback). The inline fallback lives in src/services/queue.py.
