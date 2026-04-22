from fastapi import FastAPI, UploadFile, File, Form, Depends, HTTPException
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

from .db import get_db, create_tables
from .models import Lead, AnalysisJob, JobStatus
from .config import settings
from .analyzers.complexity_scorer import ComplexityScorer
from .reports.pdf_generator import PDFReportGenerator
from .converters.schema_converter import SchemaConverter
from .converters.plsql_converter import PlSqlConverter
from .converters.oracle_functions import OracleFunctionConverter
from .rag import ConversionCaseStore, EmbeddingGenerator
from .migrations import setup_rag_tables, setup_workflow_tables, setup_benchmark_tables
from .migration import DataMigrator, CheckpointManager
from .migration.tasks import get_migration_manager
from .connectors import ConnectionConfig, get_connection_manager
from .cost_calculator import CostCalculator, DatabaseSize, DeploymentType
from .analyzers.permission_analyzer import PermissionAnalyzer, OraclePrivilegeExtractor
from .analyzers.benchmark_analyzer import BenchmarkCapture, BenchmarkComparator
from .llm.client import LLMClient
from .models import MigrationWorkflow, BenchmarkCapture as BenchmarkCaptureModel

app = FastAPI(title="Depart API", version="0.2.0")

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allow all origins for now
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


# Create uploads directory
UPLOADS_DIR = Path("/tmp/depart_uploads")
UPLOADS_DIR.mkdir(exist_ok=True)


@app.on_event("startup")
def startup():
    create_tables()
    # Initialize RAG system (pgvector extension + conversion_cases table)
    try:
        db = next(get_db())
        setup_rag_tables(db)
        setup_workflow_tables(db)
        setup_benchmark_tables(db)
    except Exception as e:
        print(f"Schema initialization warning: {e}")


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.post("/api/v1/analyze")
async def analyze(
    file: UploadFile = File(...),
    email: str = Form(...),
    rate_per_day: int = Form(default=1000),
    db: Session = Depends(get_db),
):
    """
    Upload a zip file of Oracle DDL/PL-SQL and get complexity analysis.
    Email is used to gate access and store results.
    """
    try:
        # Validate file size
        file_content = await file.read()
        file_size = len(file_content)

        if file_size > settings.max_upload_size:
            raise HTTPException(
                status_code=400,
                detail=f"File too large. Max size: {settings.max_upload_size} bytes"
            )

        # Get or create lead
        lead = db.query(Lead).filter(Lead.email == email).first()
        if not lead:
            lead = Lead(email=email)
            db.add(lead)
            db.commit()
            db.refresh(lead)

        # Create job record
        job = AnalysisJob(lead_id=lead.id, rate_per_day=rate_per_day, status=JobStatus.PROCESSING)
        db.add(job)
        db.commit()
        db.refresh(job)

        # Save uploaded file
        file_path = UPLOADS_DIR / f"{job.id}.zip"
        with open(file_path, "wb") as f:
            f.write(file_content)

        # Extract and analyze
        try:
            all_content = ""
            with zipfile.ZipFile(file_path, 'r') as zip_ref:
                for file_info in zip_ref.filelist:
                    if file_info.filename.endswith(('.sql', '.pls', '.plsql', '.txt')):
                        try:
                            content = zip_ref.read(file_info).decode('utf-8', errors='ignore')
                            all_content += content + "\n"
                        except Exception:
                            pass

            # Run complexity analysis
            scorer = ComplexityScorer()
            report = scorer.analyze(all_content, rate_per_day)

            # Generate PDF
            pdf_generator = PDFReportGenerator()
            pdf_content = pdf_generator.generate(report)

            # Save PDF
            pdf_path = UPLOADS_DIR / f"{job.id}_report.pdf"
            with open(pdf_path, "wb") as f:
                f.write(pdf_content)

            # Update job with results
            job.complexity_report = {
                "score": report.score,
                "total_lines": report.total_lines,
                "auto_convertible_lines": report.auto_convertible_lines,
                "needs_review_lines": report.needs_review_lines,
                "must_rewrite_lines": report.must_rewrite_lines,
                "construct_counts": report.construct_counts,
                "effort_estimate_days": report.effort_estimate_days,
                "estimated_cost": report.estimated_cost,
                "top_10_constructs": report.top_10_constructs,
            }
            job.pdf_path = str(pdf_path)
            job.status = JobStatus.DONE
            job.completed_at = datetime.utcnow()
            db.commit()

            return {
                "job_id": str(job.id),
                "status": job.status.value,
            }

        except zipfile.BadZipFile:
            job.status = JobStatus.ERROR
            job.error_message = "Invalid zip file"
            db.commit()
            raise HTTPException(status_code=400, detail="Invalid zip file")

    except HTTPException:
        raise
    except Exception as e:
        job.status = JobStatus.ERROR
        job.error_message = str(e)
        db.commit()
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/jobs/{job_id}")
async def get_job(job_id: str, db: Session = Depends(get_db)):
    """Get job status and report."""
    try:
        job_uuid = uuid.UUID(job_id)
        job = db.query(AnalysisJob).filter(AnalysisJob.id == job_uuid).first()

        if not job:
            raise HTTPException(status_code=404, detail="Job not found")

        return {
            "id": str(job.id),
            "status": job.status.value,
            "complexity_report": job.complexity_report,
            "created_at": job.created_at.isoformat(),
            "completed_at": job.completed_at.isoformat() if job.completed_at else None,
        }
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid job ID format")


@app.get("/api/v1/report/{job_id}/pdf")
async def get_pdf_report(job_id: str, db: Session = Depends(get_db)):
    """Download PDF report for a completed job."""
    try:
        job_uuid = uuid.UUID(job_id)
        job = db.query(AnalysisJob).filter(AnalysisJob.id == job_uuid).first()

        if not job:
            raise HTTPException(status_code=404, detail="Job not found")

        if job.status != JobStatus.DONE:
            raise HTTPException(status_code=400, detail="Job not completed")

        if not job.pdf_path or not os.path.exists(job.pdf_path):
            raise HTTPException(status_code=404, detail="PDF report not found")

        return FileResponse(
            job.pdf_path,
            media_type="application/pdf",
            filename=f"depart_analysis_{job_id}.pdf"
        )
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid job ID format")


# ============================================================================
# Phase 2: Conversion Endpoints
# ============================================================================

class ConvertRequest(BaseModel):
    code: str
    construct_type: str  # "PROCEDURE", "FUNCTION", "TABLE", "VIEW", "SEQUENCE", "INDEX"


class ConvertResponse(BaseModel):
    original: str
    converted: str
    success: bool
    method: str
    warnings: list
    errors: list


@app.post("/api/v2/convert/plsql")
async def convert_plsql(request: ConvertRequest):
    """Convert PL/SQL procedure/function to PL/pgSQL."""
    try:
        converter = PlSqlConverter(use_llm=bool(settings.anthropic_api_key))

        if request.construct_type.upper() == "FUNCTION":
            result = converter.convert_function(request.code)
        else:
            result = converter.convert_procedure(request.code)

        return ConvertResponse(
            original=result.original,
            converted=result.converted,
            success=result.success,
            method=result.method,
            warnings=result.warnings,
            errors=result.errors,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/v2/convert/schema")
async def convert_schema_ddl(request: ConvertRequest):
    """Convert Oracle DDL (tables, indexes, views, sequences) to PostgreSQL."""
    try:
        converter = SchemaConverter()
        result = converter.convert(request.code)

        return ConvertResponse(
            original=result.original,
            converted=result.converted,
            success=True,
            method="deterministic",
            warnings=result.warnings,
            errors=[],
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/v2/convert/batch")
async def convert_batch(batch: list[ConvertRequest]):
    """Convert multiple PL/SQL items in a batch (for full package conversion)."""
    try:
        results = []
        plsql_converter = PlSqlConverter(use_llm=bool(settings.anthropic_api_key))
        schema_converter = SchemaConverter()

        for item in batch:
            if item.construct_type.upper() in ["PROCEDURE", "FUNCTION"]:
                if item.construct_type.upper() == "FUNCTION":
                    result = plsql_converter.convert_function(item.code)
                else:
                    result = plsql_converter.convert_procedure(item.code)
            else:
                schema_result = schema_converter.convert(item.code)
                result = schema_result

            results.append(
                ConvertResponse(
                    original=result.original if hasattr(result, "original") else item.code,
                    converted=result.converted,
                    success=getattr(result, "success", True),
                    method=getattr(result, "method", "deterministic"),
                    warnings=getattr(result, "warnings", []),
                    errors=getattr(result, "errors", []),
                )
            )

        return {"results": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


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
            sum(c["success_rate"] for c in case_dicts) / len(case_dicts)
            if case_dicts
            else 0.0
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
    try:
        store = ConversionCaseStore(db)
        stats = store.get_pattern_stats(construct_type)
        return stats
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# Database Connectivity Endpoints
# ============================================================================

@app.post("/api/v3/connections/test")
async def test_connection(request: ConnectionConfig) -> dict:
    """
    Test database connection before migration.
    Does not store credentials - purely for validation.
    """
    try:
        manager = get_connection_manager()
        result = manager.test_connection(request)
        return result
    except Exception as e:
        logger.error(f"Connection test error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v3/connections/list")
async def list_connections() -> dict:
    """List all active database connections."""
    try:
        manager = get_connection_manager()
        connections = manager.list_connections()
        return {"connections": connections}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


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
            issues=[SemanticIssueResponse(
                severity=i.severity,
                issue_type=i.issue_type,
                affected_object=i.affected_object,
                oracle_type=i.oracle_type,
                pg_type=i.pg_type,
                description=i.description,
                recommendation=i.recommendation,
            ) for i in result.issues],
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

class MigrationPlanRequest(BaseModel):
    oracle_connection_string: str
    postgres_connection_string: str
    tables: list[str]  # List of table names to migrate
    num_workers: int = 4
    chunk_size: int = 10000


class MigrationStatusResponse(BaseModel):
    migration_id: str
    status: str
    progress_percentage: float
    rows_transferred: int
    total_rows: int
    elapsed_seconds: int
    estimated_remaining_seconds: int
    errors: list[str]


@app.post("/api/v3/migration/plan")
async def plan_migration(request: MigrationPlanRequest):
    """
    Analyze schema and create optimized migration plan.
    Uses Claude to optimize chunk sizes, parallelization, and table order.
    """
    try:
        from .migration.claude_planner import MigrationPlanner

        migration_id = str(uuid.uuid4())

        # Check if Claude API is available
        if settings.anthropic_api_key:
            planner = MigrationPlanner()

            # For MVP: send basic table info to Claude
            # In production: connect to Oracle, get actual row counts/sizes
            claude_tables = [
                {
                    "name": table,
                    "rows": 1_000_000,  # Placeholder
                    "size_gb": 1.0,  # Placeholder
                    "has_fk": True,
                }
                for table in request.tables
            ]

            strategy = planner.analyze_schema(
                tables=claude_tables,
                available_memory_gb=8,
                available_bandwidth_mbps=100,
            )

            plan = {
                "migration_id": migration_id,
                "source": "claude_optimized",
                "tables": [
                    {
                        "name": table,
                        "chunk_size": strategy.get("chunk_size", {}).get(table, request.chunk_size),
                        "order": strategy.get("table_order", request.tables).index(table)
                        if table in strategy.get("table_order", [])
                        else request.tables.index(table),
                    }
                    for table in request.tables
                ],
                "num_workers": strategy.get("num_workers", 4),
                "estimated_duration_seconds": strategy.get("estimated_duration_minutes", 60) * 60,
                "total_tables": len(request.tables),
                "recommendations": strategy.get("optimizations", []),
                "risks": strategy.get("risks", []),
            }
        else:
            # Fallback: basic plan without Claude
            plan = {
                "migration_id": migration_id,
                "source": "default",
                "tables": [
                    {
                        "name": table,
                        "chunk_size": request.chunk_size,
                        "order": i,
                    }
                    for i, table in enumerate(request.tables)
                ],
                "num_workers": request.num_workers,
                "estimated_duration_seconds": 3600,
                "total_tables": len(request.tables),
                "recommendations": [],
                "risks": ["Claude not available - using default strategy"],
            }

        return plan
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/v3/migration/start")
async def start_migration(request: MigrationPlanRequest, db: Session = Depends(get_db)):
    """
    Start a data migration with planned strategy.
    Runs in background, returns migration_id for polling status.
    """
    try:
        migration_id = str(uuid.uuid4())

        # Create migration record
        from .models import MigrationRecord

        migration = MigrationRecord(
            id=uuid.UUID(migration_id),
            schema_name="default",
            status="in_progress",
            started_at=datetime.utcnow(),
        )
        db.add(migration)
        db.commit()

        # Create and start background task
        manager = get_migration_manager()
        task = manager.create_task(
            migration_id=migration_id,
            oracle_connection_string=request.oracle_connection_string,
            postgres_connection_string=request.postgres_connection_string,
            tables=request.tables,
            num_workers=request.num_workers,
            chunk_size=request.chunk_size,
        )

        task.start()

        return {
            "migration_id": migration_id,
            "status": "started",
            "estimated_duration_seconds": 3600,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v3/migration/status/{migration_id}")
async def get_migration_status(migration_id: str, db: Session = Depends(get_db)):
    """
    Get current migration status and progress.
    Poll this to track migration in real-time.
    """
    try:
        from .models import MigrationRecord

        migration = db.query(MigrationRecord).filter(
            MigrationRecord.id == uuid.UUID(migration_id)
        ).first()

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
            raise HTTPException(status_code=501, detail="Direct connection analysis not yet implemented")
        else:
            raise HTTPException(status_code=400, detail="Either oracle_connection_id or oracle_privileges_json required")

        return PermissionAnalysisResponse(
            mappings=[{
                "oracle_privilege": m.oracle_privilege,
                "pg_equivalent": m.pg_equivalent,
                "risk_level": m.risk_level,
                "recommendation": m.recommendation,
                "grant_sql": m.grant_sql,
            } for m in result.mappings],
            unmappable=[{
                "oracle_privilege": u.oracle_privilege,
                "reason": u.reason,
                "workaround": u.workaround,
                "risk_level": u.risk_level,
            } for u in result.unmappable],
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
        workflow = db.query(MigrationWorkflow).filter(
            MigrationWorkflow.id == uuid.UUID(workflow_id)
        ).first()

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
        workflow = db.query(MigrationWorkflow).filter(
            MigrationWorkflow.id == uuid.UUID(workflow_id)
        ).first()

        if not workflow:
            raise HTTPException(status_code=404, detail="Workflow not found")

        # Record approval
        approvals = workflow.approvals or {}
        approvals[str(step)] = {
            "approved_by": request.approved_by,
            "approved_at": datetime.utcnow().isoformat(),
            "notes": request.notes or "",
        }
        workflow.approvals = approvals

        # Advance to next step if this is the current step
        if workflow.current_step == step:
            workflow.current_step += 1

        workflow.updated_at = datetime.utcnow()
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
        workflow = db.query(MigrationWorkflow).filter(
            MigrationWorkflow.id == uuid.UUID(workflow_id)
        ).first()

        if not workflow:
            raise HTTPException(status_code=404, detail="Workflow not found")

        # Record rejection in dba_notes
        dba_notes = workflow.dba_notes or {}
        dba_notes[f"step_{step}_rejection"] = {
            "reason": request.reason,
            "rejected_at": datetime.utcnow().isoformat(),
            "notes": request.notes or "",
        }
        workflow.dba_notes = dba_notes
        workflow.status = "blocked"
        workflow.updated_at = datetime.utcnow()

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
        workflow = db.query(MigrationWorkflow).filter(
            MigrationWorkflow.id == uuid.UUID(workflow_id)
        ).first()

        if not workflow:
            raise HTTPException(status_code=404, detail="Workflow not found")

        workflow.settings = request.settings
        workflow.updated_at = datetime.utcnow()
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
        workflow = db.query(MigrationWorkflow).filter(
            MigrationWorkflow.id == uuid.UUID(workflow_id)
        ).first()

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
async def capture_postgres_benchmark(request: BenchmarkCaptureRequest, db: Session = Depends(get_db)):
    """Capture PostgreSQL performance metrics from pg_stat_statements."""
    try:
        # TODO: Get PostgreSQL connection from connection manager
        raise HTTPException(status_code=501, detail="PostgreSQL benchmark capture not yet implemented")
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
        oracle_capture = db.query(BenchmarkCaptureModel).filter(
            BenchmarkCaptureModel.migration_id == uuid.UUID(migration_id),
            BenchmarkCaptureModel.db_type == "oracle",
        ).first()

        postgres_capture = db.query(BenchmarkCaptureModel).filter(
            BenchmarkCaptureModel.migration_id == uuid.UUID(migration_id),
            BenchmarkCaptureModel.db_type == "postgres",
        ).first()

        if not oracle_capture or not postgres_capture:
            raise HTTPException(status_code=404, detail="Benchmark captures not found")

        # Reconstruct baseline and metrics from stored JSON
        import json
        from .analyzers.benchmark_analyzer import OracleBaseline, PostgresMetrics, QueryStat, TableStat

        oracle_data = json.loads(oracle_capture.data) if isinstance(oracle_capture.data, str) else oracle_capture.data
        postgres_data = json.loads(postgres_capture.data) if isinstance(postgres_capture.data, str) else postgres_capture.data

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
            query_comparisons=[{
                "sql_text": c.sql_text,
                "oracle_avg_ms": c.oracle_avg_ms,
                "pg_avg_ms": c.pg_avg_ms,
                "speedup_factor": c.speedup_factor,
                "verdict": c.verdict,
            } for c in report.query_comparisons],
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
