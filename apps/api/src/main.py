from fastapi import FastAPI, UploadFile, File, Form, Depends, HTTPException, status
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from pydantic import BaseModel, EmailStr
from typing import Optional
import uuid
import os
from pathlib import Path
from datetime import datetime
import zipfile
import logging

logger = logging.getLogger(__name__)

from .db import get_db
from .models import Lead, AnalysisJob, JobStatus, User
from .config import settings
from .analyze.complexity import ComplexityScorer
from .reports.pdf_generator import PDFReportGenerator
from .utils.time import utc_now

try:
    from .rag import ConversionCaseStore, EmbeddingGenerator
except ImportError:
    # RAG features are optional in dev (sentence-transformers is heavy).
    ConversionCaseStore = None
    EmbeddingGenerator = None
    logger.warning("RAG features disabled - sentence_transformers not installed")
from .migration import CheckpointManager
from .connectors import get_connection_manager
from .cost_calculator import CostCalculator, DatabaseSize, DeploymentType
from .analyzers.permission_analyzer import PermissionAnalyzer
from .analyzers.benchmark_analyzer import BenchmarkComparator
from .llm.client import LLMClient
from .models import MigrationWorkflow, BenchmarkCapture as BenchmarkCaptureModel, MigrationReport
from .connectors.connection_pool import get_connection_pool
from .routers import (
    assess,
    audit as audit_router,
    auth,
    auth_local,
    account,
    billing,
    cloud_analyze,
    convert,
    license as license_router,
    masking as masking_router,
    migrations as migrations_router,
    schedules as schedules_router,
    settings as settings_router,
    setup as setup_router,
    sso as sso_router,
    support,
    webhooks as webhooks_router,
)
from .api.routes import app_impact as app_impact_route
from .api.routes import runbook as runbook_route
from .api.routes import usage as usage_route
from .auth.dependencies import get_optional_user
from .services.billing import get_plan_limits

app = FastAPI(title="Hafen API", version="0.2.0")


@app.on_event("startup")
def _auto_bootstrap_admin() -> None:
    """If HAFEN_ADMIN_EMAIL / HAFEN_ADMIN_PASSWORD are set and no
    admin exists yet, create one. Silent no-op otherwise. Runs at
    FastAPI startup, before the first request is served."""
    if not settings.enable_self_hosted_auth:
        return
    try:
        from .db import get_session_factory
        from .routers.setup import maybe_bootstrap_from_env

        session = get_session_factory()()
        try:
            maybe_bootstrap_from_env(session)
        finally:
            session.close()
    except Exception as exc:  # noqa: BLE001 — startup mustn't crash the app
        logger.warning("auto-bootstrap failed: %s", exc)

# CORS middleware
cors_origins = [settings.frontend_url]
if settings.environment == "development":
    cors_origins.extend(["http://localhost:3000", "http://localhost:8000", "http://127.0.0.1:3000"])

app.add_middleware(
    CORSMiddleware,
    allow_origins=cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Pydantic models
class LeadCreate(BaseModel):
    email: EmailStr


class JobResponse(BaseModel):
    id: str
    status: str
    complexity_report: Optional[dict] = None
    created_at: str
    completed_at: Optional[str] = None

    class Config:
        from_attributes = True


# ============================================================================
# Phase 3.3: HITL Workflow, Permissions, and Benchmarking Models
# ============================================================================


class PermissionAnalysisRequest(BaseModel):
    oracle_connection_id: Optional[str] = None
    oracle_privileges_json: Optional[str] = None


class PermissionAnalysisResponse(BaseModel):
    mappings: list[dict]
    unmappable: list[dict]
    grant_sql: list[str]
    overall_risk: str
    analyzed_at: str


class WorkflowCreateRequest(BaseModel):
    name: str
    migration_id: Optional[str] = None


class WorkflowResponse(BaseModel):
    id: str
    name: str
    migration_id: Optional[str]
    current_step: int
    status: str
    dba_notes: dict
    approvals: dict
    settings: dict
    created_at: str
    updated_at: str


class ApprovalRequest(BaseModel):
    approved_by: str
    notes: Optional[str] = None


class RejectionRequest(BaseModel):
    reason: str
    notes: Optional[str] = None


class WorkflowSettingsRequest(BaseModel):
    settings: dict


class BenchmarkCaptureRequest(BaseModel):
    oracle_connection_id: Optional[str] = None
    postgres_connection_id: Optional[str] = None
    migration_id: Optional[str] = None


class BenchmarkComparisonResponse(BaseModel):
    migration_id: Optional[str]
    query_comparisons: list[dict]
    table_comparisons: list[dict]
    overall_assessment: str
    generated_at: str


# ============================================================================
# Phase 3.3: Connection Management Models
# ============================================================================


class ConnectionTestRequest(BaseModel):
    database_type: str  # "oracle" or "postgres"
    host: str
    port: int
    username: str
    password: str
    service_name: Optional[str] = None  # Oracle
    database: Optional[str] = None  # PostgreSQL


class ConnectionListResponse(BaseModel):
    connection_id: str
    database_type: str
    host: str
    port: int
    connected: bool


class ConnectionStatsResponse(BaseModel):
    connection_id: str
    database_type: str
    created_at: str
    last_used: str
    use_count: int
    health_status: str
    response_time_ms: float


# Create uploads directory
UPLOADS_DIR = Path("/tmp/hafen_uploads")
UPLOADS_DIR.mkdir(exist_ok=True)


@app.get("/health")
async def health():
    return {"status": "ok"}


# Register routers
# Self-hosted-always routers — these are the product.
app.include_router(assess.router)
app.include_router(convert.router)
app.include_router(settings_router.router)
app.include_router(license_router.router)
app.include_router(migrations_router.router)
app.include_router(audit_router.router)

# Setup + self-hosted auth always mount. The ENABLE_SELF_HOSTED_AUTH
# flag doesn't gate the routes themselves — it controls whether
# `require_role` enforces them on mutating endpoints. Login itself is
# always available so dev boxes and test harnesses can still exercise
# the auth code path even when enforcement is disabled.
app.include_router(setup_router.router)
app.include_router(auth_local.router)
app.include_router(sso_router.router)
app.include_router(webhooks_router.router)
app.include_router(schedules_router.router)
app.include_router(masking_router.router)
app.include_router(app_impact_route.router)
app.include_router(runbook_route.router)
app.include_router(usage_route.router)

# Cloud-only routers — signup/login, billing, support tickets, per-user API
# keys, and the legacy email-gated /api/v1/analyze zip-upload flow. The
# self-hosted product image ships without these (no Stripe, no email
# dependency, no user accounts, no Lead/Job persistence); hafen.ai
# mounts them via the ENABLE_CLOUD_ROUTES env flag.
if settings.enable_cloud_routes:
    app.include_router(auth.router)
    app.include_router(account.router)
    app.include_router(billing.router)
    app.include_router(support.router)
    app.include_router(cloud_analyze.router)


# The legacy `/api/v1/analyze`, `/api/v1/jobs/{id}`, and `/api/v1/report/{id}/pdf`
# endpoints now live in `src/routers/cloud_analyze.py` and only mount when
# ENABLE_CLOUD_ROUTES=true. The product image uses the newer `/api/v1/assess`
# flow (no email, no Lead, no Job persistence). See that router for how the
# three moved endpoints are re-mounted.


# Conversion endpoints (/api/v2/convert/*) were removed. The previous
# regex-based converters produced unsafe output (e.g., silently dropping
# COMMIT in PROCEDURE bodies, mis-counting BEGIN/END pairs). They will be
# rebuilt on top of the ANTLR-parsed IR + AI-assisted lowering.


# ============================================================================
# Phase 3.1: RAG System Endpoints
# ============================================================================


class StoreConversionCaseRequest(BaseModel):
    construct_type: str
    oracle_code: str
    postgres_code: str
    success: bool = True


class RAGContextRequest(BaseModel):
    code: str
    construct_type: str
    top_k: int = 3


class RAGContextResponse(BaseModel):
    similar_cases: list[dict]
    average_success_rate: float


@app.post("/api/v3/rag/store-case")
async def store_conversion_case(
    request: StoreConversionCaseRequest,
    db: Session = Depends(get_db),
):
    """Store a conversion case for RAG pattern learning."""
    if ConversionCaseStore is None:
        raise HTTPException(status_code=503, detail="RAG features disabled")
    try:
        store = ConversionCaseStore(db)
        case_id = store.store_case(
            construct_type=request.construct_type,
            oracle_code=request.oracle_code,
            postgres_code=request.postgres_code,
            success=request.success,
        )
        return {
            "case_id": case_id,
            "status": "stored",
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/v3/rag/similar-cases")
async def get_similar_cases(
    request: RAGContextRequest,
    db: Session = Depends(get_db),
):
    """Find similar conversion cases to provide context to Claude."""
    if ConversionCaseStore is None:
        raise HTTPException(status_code=503, detail="RAG features disabled")
    try:
        store = ConversionCaseStore(db)
        similar_cases = store.find_similar_cases(
            oracle_code=request.code,
            construct_type=request.construct_type,
            top_k=request.top_k,
        )

        # Return case details
        case_dicts = [
            {
                "oracle_code": case.oracle_code,
                "postgres_code": case.postgres_code,
                "success_rate": case.success_rate,
                "similarity_score": score,
            }
            for case, score in similar_cases
        ]

        avg_success = (
            sum(c["success_rate"] for c in case_dicts) / len(case_dicts) if case_dicts else 0.0
        )

        return RAGContextResponse(
            similar_cases=case_dicts,
            average_success_rate=avg_success,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v3/rag/pattern-stats/{construct_type}")
async def get_pattern_statistics(construct_type: str, db: Session = Depends(get_db)):
    """Get statistics on conversion patterns for a construct type."""
    if ConversionCaseStore is None:
        raise HTTPException(status_code=503, detail="RAG features disabled")
    try:
        store = ConversionCaseStore(db)
        stats = store.get_pattern_stats(construct_type)
        return stats
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Connection management endpoints live in their dedicated section below.

# ============================================================================
# Cost Savings Calculator
# ============================================================================


class CostAnalysisRequest(BaseModel):
    database_size: str  # "small", "medium", "large", "enterprise"
    deployment_type: str  # "onprem", "cloud_aws", "cloud_azure", "cloud_gcp"
    num_databases: int = 1
    num_oracle_cores: int = 4
    num_dba_fte: float = 1.0


@app.post("/api/v3/cost-analysis")
async def analyze_migration_costs(request: CostAnalysisRequest) -> dict:
    """
    Calculate cost savings for Oracle → PostgreSQL migration.
    Shows ROI, payback period, and 5-year savings.
    """
    try:
        calculator = CostCalculator(
            database_size=DatabaseSize(request.database_size),
            deployment_type=DeploymentType(request.deployment_type),
            num_databases=request.num_databases,
            num_oracle_cores=request.num_oracle_cores,
            num_dba_fte=request.num_dba_fte,
        )

        analysis = calculator.analyze()

        return {
            "status": "success",
            "analysis": analysis.dict(),
            "summary": {
                "annual_savings_year1": f"${analysis.annual_savings_year1:,.0f}",
                "annual_savings_year2_plus": f"${analysis.annual_savings_year2_plus:,.0f}",
                "payback_months": f"{analysis.payback_months:.1f}",
                "roi_percent": f"{analysis.roi_percent:.0f}%",
                "five_year_savings": f"${analysis.five_year_savings:,.0f}",
            },
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid parameter: {e}")
    except Exception as e:
        logger.error(f"Cost analysis error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# Semantic Error Detection
# ============================================================================


class SemanticAnalysisRequest(BaseModel):
    oracle_ddl: Optional[str] = None
    pg_ddl: Optional[str] = None
    oracle_connection_id: Optional[str] = None
    pg_connection_id: Optional[str] = None
    schema_name: Optional[str] = None
    table_names: Optional[list[str]] = None


class SemanticIssueResponse(BaseModel):
    severity: str
    issue_type: str
    affected_object: str
    oracle_type: str
    pg_type: str
    description: str
    recommendation: str


class SemanticAnalysisResponse(BaseModel):
    mode: str
    analyzed_objects: int
    issues: list[SemanticIssueResponse]
    summary: dict


@app.post("/api/v3/analyze/semantic")
async def analyze_semantic(request: SemanticAnalysisRequest) -> SemanticAnalysisResponse:
    """
    Detect semantic type-mapping risks in Oracle → PostgreSQL migration.
    Supports both static (DDL text) and live (DB connections) modes.
    """
    from .analyzers.semantic_analyzer import SemanticAnalyzer
    from .llm.client import LLMClient

    try:
        analyzer = SemanticAnalyzer(LLMClient())

        # Determine mode
        if request.oracle_connection_id and request.pg_connection_id:
            manager = get_connection_manager()
            oracle_conn = manager.get_connector(request.oracle_connection_id)
            pg_conn = manager.get_connector(request.pg_connection_id)

            if not oracle_conn or not pg_conn:
                raise HTTPException(
                    status_code=400,
                    detail="One or both connection IDs not found",
                )

            result = analyzer.analyze_live(
                oracle_conn,
                pg_conn,
                schema_name=request.schema_name,
            )
        elif request.oracle_ddl and request.pg_ddl:
            result = analyzer.analyze_static(request.oracle_ddl, request.pg_ddl)
        else:
            raise HTTPException(
                status_code=400,
                detail="Provide either (oracle_ddl + pg_ddl) or (oracle_connection_id + pg_connection_id)",
            )

        # Build severity summary
        from collections import Counter

        counts = Counter(i.severity for i in result.issues)

        return SemanticAnalysisResponse(
            mode=result.mode,
            analyzed_objects=result.analyzed_objects,
            issues=[
                SemanticIssueResponse(
                    severity=i.severity,
                    issue_type=i.issue_type,
                    affected_object=i.affected_object,
                    oracle_type=i.oracle_type,
                    pg_type=i.pg_type,
                    description=i.description,
                    recommendation=i.recommendation,
                )
                for i in result.issues
            ],
            summary={
                "critical": counts.get("CRITICAL", 0),
                "error": counts.get("ERROR", 0),
                "warning": counts.get("WARNING", 0),
                "info": counts.get("INFO", 0),
                "total": len(result.issues),
            },
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Semantic analysis error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# Phase 3.2: Data Migration Orchestration Endpoints
# ============================================================================


class MigrationStatusResponse(BaseModel):
    migration_id: str
    status: str
    progress_percentage: float
    rows_transferred: int
    total_rows: int
    elapsed_seconds: int
    estimated_remaining_seconds: int
    errors: list[str]


# /api/v3/migration/plan and /api/v3/migration/start were removed. The plan
# endpoint generated strategies from placeholder row counts; the start endpoint
# delegated to a broken orchestrator (row-by-row INSERT, ROWNUM BETWEEN that
# never matches, SQLAlchemy text() bound with %s placeholders). They will be
# rebuilt on top of COPY + keyset pagination + Merkle-hash batch verification.


@app.get("/api/v3/migration/status/{migration_id}")
async def get_migration_status(migration_id: str, db: Session = Depends(get_db)):
    """
    Get current migration status and progress.
    Poll this to track migration in real-time.
    """
    try:
        from .models import MigrationRecord

        migration = (
            db.query(MigrationRecord).filter(MigrationRecord.id == uuid.UUID(migration_id)).first()
        )

        if not migration:
            raise HTTPException(status_code=404, detail="Migration not found")

        checkpoint_manager = CheckpointManager(db)
        progress = checkpoint_manager.get_migration_progress(migration_id)

        elapsed = migration.elapsed_seconds
        estimated_remaining = (
            max(0, migration.estimated_duration_seconds - elapsed)
            if migration.estimated_duration_seconds
            else 0
        )

        return MigrationStatusResponse(
            migration_id=migration_id,
            status=migration.status,
            progress_percentage=migration.progress_percentage,
            rows_transferred=migration.rows_transferred,
            total_rows=migration.total_rows,
            elapsed_seconds=elapsed,
            estimated_remaining_seconds=estimated_remaining,
            errors=[],
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v3/migration/{migration_id}/checkpoints")
async def get_migration_checkpoints(migration_id: str, db: Session = Depends(get_db)):
    """Get all checkpoints for a migration (for recovery/debugging)."""
    try:
        checkpoint_manager = CheckpointManager(db)
        progress = checkpoint_manager.get_migration_progress(migration_id)
        return progress
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v3/migration/{migration_id}/report")
async def get_migration_report(migration_id: str, db: Session = Depends(get_db)):
    """Get migration progress report with conversion statistics."""
    try:
        from .models import MigrationRecord, MigrationCheckpointRecord

        migration = (
            db.query(MigrationRecord).filter(MigrationRecord.id == uuid.UUID(migration_id)).first()
        )

        if not migration:
            raise HTTPException(status_code=404, detail="Migration not found")

        # Get all checkpoints for this migration
        checkpoints = (
            db.query(MigrationCheckpointRecord)
            .filter(MigrationCheckpointRecord.migration_id == uuid.UUID(migration_id))
            .all()
        )

        # Calculate conversion statistics
        total_tables = len(checkpoints) if checkpoints else 0
        completed_tables = (
            sum(1 for cp in checkpoints if cp.status == "completed") if checkpoints else 0
        )
        conversion_percentage = (completed_tables / total_tables * 100) if total_tables > 0 else 0.0

        # Count tests generated (assuming 1 test per completed checkpoint for now)
        tests_generated = completed_tables

        # Build risk breakdown (placeholder: all tables as "low" risk for now)
        risk_breakdown = {
            "high": 0,
            "medium": 0,
            "low": total_tables,
        }

        # Collect blockers from checkpoint error messages
        blockers = []
        for cp in checkpoints:
            if cp.error_message:
                blockers.append(
                    {
                        "name": cp.table_name,
                        "reason": cp.error_message,
                    }
                )

        return MigrationReport(
            migration_id=migration_id,
            total_objects=total_tables,
            converted_count=completed_tables,
            tests_generated=tests_generated,
            conversion_percentage=conversion_percentage,
            risk_breakdown=risk_breakdown,
            blockers=blockers,
            generated_at=utc_now().isoformat(),
        )
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid migration ID format")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error generating migration report: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# Phase 3.3: Permission Analysis Endpoints
# ============================================================================


@app.post("/api/v3/analyze/permissions")
async def analyze_permissions(request: PermissionAnalysisRequest, db: Session = Depends(get_db)):
    """
    Analyze Oracle permissions and map to PostgreSQL GRANT statements.
    Accepts either oracle_connection_id or oracle_privileges_json.
    """
    try:
        llm_client = LLMClient()
        analyzer = PermissionAnalyzer(llm_client)

        if request.oracle_privileges_json:
            result = analyzer.analyze_from_json(request.oracle_privileges_json)
        elif request.oracle_connection_id:
            # TODO: Get connection from connection manager
            raise HTTPException(
                status_code=501, detail="Direct connection analysis not yet implemented"
            )
        else:
            raise HTTPException(
                status_code=400,
                detail="Either oracle_connection_id or oracle_privileges_json required",
            )

        return PermissionAnalysisResponse(
            mappings=[
                {
                    "oracle_privilege": m.oracle_privilege,
                    "pg_equivalent": m.pg_equivalent,
                    "risk_level": m.risk_level,
                    "recommendation": m.recommendation,
                    "grant_sql": m.grant_sql,
                }
                for m in result.mappings
            ],
            unmappable=[
                {
                    "oracle_privilege": u.oracle_privilege,
                    "reason": u.reason,
                    "workaround": u.workaround,
                    "risk_level": u.risk_level,
                }
                for u in result.unmappable
            ],
            grant_sql=result.grant_sql,
            overall_risk=result.overall_risk,
            analyzed_at=result.analyzed_at,
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error analyzing permissions: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# Phase 3.3: Migration Workflow Endpoints
# ============================================================================


@app.post("/api/v3/workflow/create")
async def create_workflow(request: WorkflowCreateRequest, db: Session = Depends(get_db)):
    """Create a new migration workflow for HITL orchestration."""
    try:
        workflow = MigrationWorkflow(
            name=request.name,
            migration_id=uuid.UUID(request.migration_id) if request.migration_id else None,
            current_step=1,
            status="running",
            dba_notes={},
            approvals={},
            settings={},
        )
        db.add(workflow)
        db.commit()
        db.refresh(workflow)

        return WorkflowResponse(
            id=str(workflow.id),
            name=workflow.name,
            migration_id=str(workflow.migration_id) if workflow.migration_id else None,
            current_step=workflow.current_step,
            status=workflow.status,
            dba_notes=workflow.dba_notes,
            approvals=workflow.approvals,
            settings=workflow.settings,
            created_at=workflow.created_at.isoformat(),
            updated_at=workflow.updated_at.isoformat(),
        )
    except Exception as e:
        logger.error(f"Error creating workflow: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v3/workflow/{workflow_id}")
async def get_workflow(workflow_id: str, db: Session = Depends(get_db)):
    """Get workflow details and current status."""
    try:
        workflow = (
            db.query(MigrationWorkflow)
            .filter(MigrationWorkflow.id == uuid.UUID(workflow_id))
            .first()
        )

        if not workflow:
            raise HTTPException(status_code=404, detail="Workflow not found")

        return WorkflowResponse(
            id=str(workflow.id),
            name=workflow.name,
            migration_id=str(workflow.migration_id) if workflow.migration_id else None,
            current_step=workflow.current_step,
            status=workflow.status,
            dba_notes=workflow.dba_notes,
            approvals=workflow.approvals,
            settings=workflow.settings,
            created_at=workflow.created_at.isoformat(),
            updated_at=workflow.updated_at.isoformat(),
        )
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid workflow ID format")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error retrieving workflow: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/v3/workflow/{workflow_id}/approve/{step}")
async def approve_workflow_step(
    workflow_id: str,
    step: int,
    request: ApprovalRequest,
    db: Session = Depends(get_db),
):
    """Approve a DBA review step and advance workflow."""
    try:
        workflow = (
            db.query(MigrationWorkflow)
            .filter(MigrationWorkflow.id == uuid.UUID(workflow_id))
            .first()
        )

        if not workflow:
            raise HTTPException(status_code=404, detail="Workflow not found")

        # Record approval
        approvals = workflow.approvals or {}
        approvals[str(step)] = {
            "approved_by": request.approved_by,
            "approved_at": utc_now().isoformat(),
            "notes": request.notes or "",
        }
        workflow.approvals = approvals

        # Advance to next step if this is the current step
        if workflow.current_step == step:
            workflow.current_step += 1

        workflow.updated_at = utc_now()
        db.commit()
        db.refresh(workflow)

        return WorkflowResponse(
            id=str(workflow.id),
            name=workflow.name,
            migration_id=str(workflow.migration_id) if workflow.migration_id else None,
            current_step=workflow.current_step,
            status=workflow.status,
            dba_notes=workflow.dba_notes,
            approvals=workflow.approvals,
            settings=workflow.settings,
            created_at=workflow.created_at.isoformat(),
            updated_at=workflow.updated_at.isoformat(),
        )
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid workflow ID format")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error approving workflow step: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/v3/workflow/{workflow_id}/reject/{step}")
async def reject_workflow_step(
    workflow_id: str,
    step: int,
    request: RejectionRequest,
    db: Session = Depends(get_db),
):
    """Reject a workflow step and set status to blocked."""
    try:
        workflow = (
            db.query(MigrationWorkflow)
            .filter(MigrationWorkflow.id == uuid.UUID(workflow_id))
            .first()
        )

        if not workflow:
            raise HTTPException(status_code=404, detail="Workflow not found")

        # Record rejection in dba_notes
        dba_notes = workflow.dba_notes or {}
        dba_notes[f"step_{step}_rejection"] = {
            "reason": request.reason,
            "rejected_at": utc_now().isoformat(),
            "notes": request.notes or "",
        }
        workflow.dba_notes = dba_notes
        workflow.status = "blocked"
        workflow.updated_at = utc_now()

        db.commit()
        db.refresh(workflow)

        return WorkflowResponse(
            id=str(workflow.id),
            name=workflow.name,
            migration_id=str(workflow.migration_id) if workflow.migration_id else None,
            current_step=workflow.current_step,
            status=workflow.status,
            dba_notes=workflow.dba_notes,
            approvals=workflow.approvals,
            settings=workflow.settings,
            created_at=workflow.created_at.isoformat(),
            updated_at=workflow.updated_at.isoformat(),
        )
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid workflow ID format")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error rejecting workflow step: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/v3/workflow/{workflow_id}/settings")
async def update_workflow_settings(
    workflow_id: str,
    request: WorkflowSettingsRequest,
    db: Session = Depends(get_db),
):
    """Update workflow settings."""
    try:
        workflow = (
            db.query(MigrationWorkflow)
            .filter(MigrationWorkflow.id == uuid.UUID(workflow_id))
            .first()
        )

        if not workflow:
            raise HTTPException(status_code=404, detail="Workflow not found")

        workflow.settings = request.settings
        workflow.updated_at = utc_now()
        db.commit()
        db.refresh(workflow)

        return WorkflowResponse(
            id=str(workflow.id),
            name=workflow.name,
            migration_id=str(workflow.migration_id) if workflow.migration_id else None,
            current_step=workflow.current_step,
            status=workflow.status,
            dba_notes=workflow.dba_notes,
            approvals=workflow.approvals,
            settings=workflow.settings,
            created_at=workflow.created_at.isoformat(),
            updated_at=workflow.updated_at.isoformat(),
        )
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid workflow ID format")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating workflow settings: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v3/workflow/{workflow_id}/progress")
async def get_workflow_progress(workflow_id: str, db: Session = Depends(get_db)):
    """Get workflow progress summary."""
    try:
        workflow = (
            db.query(MigrationWorkflow)
            .filter(MigrationWorkflow.id == uuid.UUID(workflow_id))
            .first()
        )

        if not workflow:
            raise HTTPException(status_code=404, detail="Workflow not found")

        return {
            "id": str(workflow.id),
            "current_step": workflow.current_step,
            "total_steps": 20,
            "progress_percentage": (workflow.current_step / 20) * 100,
            "status": workflow.status,
            "approvals_count": len([a for a in workflow.approvals.values() if a]),
            "created_at": workflow.created_at.isoformat(),
            "updated_at": workflow.updated_at.isoformat(),
        }
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid workflow ID format")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting workflow progress: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# Phase 3.3: Connection Management Endpoints
# ============================================================================


@app.post("/api/v3/connections/test")
async def test_connection(request: ConnectionTestRequest):
    """Test database connection without storing credentials."""
    try:
        from .connectors import ConnectionConfig

        config = ConnectionConfig(
            database_type=request.database_type,
            host=request.host,
            port=request.port,
            username=request.username,
            password=request.password,
            service_name=request.service_name,
            database=request.database,
        )

        manager = get_connection_manager()
        result = manager.test_connection(config)

        return result
    except Exception as e:
        logger.error(f"Connection test error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v3/connections/list")
async def list_connections():
    """List all active database connections."""
    try:
        manager = get_connection_manager()
        connections = manager.list_connections()

        return {
            "connections": [
                ConnectionListResponse(
                    connection_id=conn_id,
                    database_type=conn_data["type"],
                    host=conn_data["host"],
                    port=conn_data["port"],
                    connected=conn_data["connected"],
                )
                for conn_id, conn_data in connections.items()
            ]
        }
    except Exception as e:
        logger.error(f"Error listing connections: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v3/connections/{connection_id}/stats")
async def get_connection_stats(connection_id: str):
    """Get statistics for a pooled connection."""
    try:
        pool = get_connection_pool()
        stats = pool.get_stats(connection_id)

        if not stats:
            raise HTTPException(status_code=404, detail="Connection not found in pool")

        return ConnectionStatsResponse(
            connection_id=stats.connection_id,
            database_type=stats.database_type,
            created_at=stats.created_at.isoformat(),
            last_used=stats.last_used.isoformat(),
            use_count=stats.use_count,
            health_status=stats.health_status,
            response_time_ms=stats.response_time_ms,
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting connection stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/v3/connections/{connection_id}/health")
async def check_connection_health(connection_id: str):
    """Check health of a specific connection."""
    try:
        manager = get_connection_manager()
        connector = manager.get_connector(connection_id)

        if not connector:
            raise HTTPException(status_code=404, detail="Connection not found")

        health = connector.health_check()

        return {
            "connection_id": connection_id,
            "status": health.get("status", "unknown"),
            "details": health,
            "checked_at": utc_now().isoformat(),
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Health check error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# Phase 3.3: Benchmark Analysis Endpoints
# ============================================================================


@app.post("/api/v3/benchmark/capture-oracle")
async def capture_oracle_benchmark(request: BenchmarkCaptureRequest, db: Session = Depends(get_db)):
    """Capture Oracle performance baseline from v$sql."""
    try:
        # TODO: Get Oracle connection from connection manager
        raise HTTPException(status_code=501, detail="Oracle benchmark capture not yet implemented")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error capturing Oracle benchmark: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/v3/benchmark/capture-postgres")
async def capture_postgres_benchmark(
    request: BenchmarkCaptureRequest, db: Session = Depends(get_db)
):
    """Capture PostgreSQL performance metrics from pg_stat_statements."""
    try:
        # TODO: Get PostgreSQL connection from connection manager
        raise HTTPException(
            status_code=501, detail="PostgreSQL benchmark capture not yet implemented"
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error capturing PostgreSQL benchmark: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v3/benchmark/compare/{migration_id}")
async def compare_benchmarks(migration_id: str, db: Session = Depends(get_db)):
    """Compare Oracle and PostgreSQL benchmark captures for a migration."""
    try:
        # Query benchmark captures for this migration
        oracle_capture = (
            db.query(BenchmarkCaptureModel)
            .filter(
                BenchmarkCaptureModel.migration_id == uuid.UUID(migration_id),
                BenchmarkCaptureModel.db_type == "oracle",
            )
            .first()
        )

        postgres_capture = (
            db.query(BenchmarkCaptureModel)
            .filter(
                BenchmarkCaptureModel.migration_id == uuid.UUID(migration_id),
                BenchmarkCaptureModel.db_type == "postgres",
            )
            .first()
        )

        if not oracle_capture or not postgres_capture:
            raise HTTPException(status_code=404, detail="Benchmark captures not found")

        # Reconstruct baseline and metrics from stored JSON
        import json
        from .analyzers.benchmark_analyzer import (
            OracleBaseline,
            PostgresMetrics,
            QueryStat,
            TableStat,
        )

        oracle_data = (
            json.loads(oracle_capture.data)
            if isinstance(oracle_capture.data, str)
            else oracle_capture.data
        )
        postgres_data = (
            json.loads(postgres_capture.data)
            if isinstance(postgres_capture.data, str)
            else postgres_capture.data
        )

        oracle_baseline = OracleBaseline(
            captured_at=oracle_data["captured_at"],
            top_queries=[QueryStat(**q) for q in oracle_data.get("top_queries", [])],
            table_stats=[TableStat(**t) for t in oracle_data.get("table_stats", [])],
            migration_id=oracle_data.get("migration_id"),
        )

        pg_metrics = PostgresMetrics(
            captured_at=postgres_data["captured_at"],
            top_queries=[QueryStat(**q) for q in postgres_data.get("top_queries", [])],
            table_stats=[TableStat(**t) for t in postgres_data.get("table_stats", [])],
            migration_id=postgres_data.get("migration_id"),
        )

        # Compare
        llm_client = LLMClient()
        report = BenchmarkComparator.compare(oracle_baseline, pg_metrics, llm_client)

        return BenchmarkComparisonResponse(
            migration_id=str(migration_id),
            query_comparisons=[
                {
                    "sql_text": c.sql_text,
                    "oracle_avg_ms": c.oracle_avg_ms,
                    "pg_avg_ms": c.pg_avg_ms,
                    "speedup_factor": c.speedup_factor,
                    "verdict": c.verdict,
                }
                for c in report.query_comparisons
            ],
            table_comparisons=report.table_comparisons,
            overall_assessment=report.overall_assessment,
            generated_at=report.generated_at,
        )
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid migration ID format")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error comparing benchmarks: {e}")
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host=settings.api_host, port=settings.api_port)
