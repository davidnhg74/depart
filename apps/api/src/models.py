from sqlalchemy import Column, String, Integer, DateTime, JSON, ForeignKey, Text, Enum, ARRAY, Float
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship
from datetime import datetime
import uuid
import enum
from .db import Base


class JobStatus(str, enum.Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    DONE = "done"
    ERROR = "error"


class Lead(Base):
    __tablename__ = "leads"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    email = Column(String(255), unique=True, nullable=False, index=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    jobs = relationship("AnalysisJob", back_populates="lead")


class AnalysisJob(Base):
    __tablename__ = "analysis_jobs"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    lead_id = Column(UUID(as_uuid=True), ForeignKey("leads.id"), nullable=False)
    status = Column(Enum(JobStatus), default=JobStatus.PENDING, nullable=False)
    complexity_report = Column(JSON, nullable=True)
    pdf_path = Column(String(512), nullable=True)
    rate_per_day = Column(Integer, default=1000, nullable=False)
    error_message = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    completed_at = Column(DateTime, nullable=True)

    lead = relationship("Lead", back_populates="jobs")


class ConversionCaseRecord(Base):
    """RAG conversion case storage for pattern learning."""

    __tablename__ = "conversion_cases"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    construct_type = Column(String(50), nullable=False, index=True)  # PROCEDURE, FUNCTION, etc.
    oracle_code = Column(Text, nullable=False)
    postgres_code = Column(Text, nullable=False)
    embedding = Column(ARRAY(Float), nullable=False)  # Vector embedding
    success_count = Column(Integer, default=1, nullable=False)
    fail_count = Column(Integer, default=0, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False, index=True)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    @property
    def success_rate(self) -> float:
        total = self.success_count + self.fail_count
        return self.success_count / total if total > 0 else 0.0


class MigrationRecord(Base):
    """Track data migrations from Oracle to PostgreSQL."""

    __tablename__ = "migrations"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    schema_name = Column(String(255), nullable=False)
    status = Column(String(50), nullable=False, index=True)  # pending, in_progress, completed, failed
    total_rows = Column(Integer, default=0)
    rows_transferred = Column(Integer, default=0)
    total_bytes = Column(Integer, default=0)
    estimated_duration_seconds = Column(Integer)
    started_at = Column(DateTime, nullable=True)
    completed_at = Column(DateTime, nullable=True)
    error_message = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False, index=True)

    @property
    def elapsed_seconds(self) -> int:
        if not self.started_at:
            return 0
        end = self.completed_at or datetime.utcnow()
        return int((end - self.started_at).total_seconds())

    @property
    def progress_percentage(self) -> float:
        if self.total_rows == 0:
            return 0.0
        return (self.rows_transferred / self.total_rows) * 100


class MigrationCheckpointRecord(Base):
    """Store checkpoints for resumable migrations."""

    __tablename__ = "migration_checkpoints"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    migration_id = Column(UUID(as_uuid=True), ForeignKey("migrations.id"), nullable=False, index=True)
    table_name = Column(String(255), nullable=False)
    rows_processed = Column(Integer, default=0)
    total_rows = Column(Integer, default=0)
    progress_percentage = Column(Float, default=0.0)
    last_rowid = Column(String(255), nullable=True)
    status = Column(String(50), nullable=False)  # in_progress, completed, failed
    error_message = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False, index=True)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class MigrationWorkflow(Base):
    """Track Human-In-The-Loop migration workflow with DBA approvals."""

    __tablename__ = "migration_workflows"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name = Column(String(255), nullable=False)
    migration_id = Column(UUID(as_uuid=True), ForeignKey("migrations.id"), nullable=True)
    current_step = Column(Integer, default=1, nullable=False)
    status = Column(String(50), default="running", nullable=False, index=True)
    dba_notes = Column(JSON, default=dict, nullable=False)
    approvals = Column(JSON, default=dict, nullable=False)
    settings = Column(JSON, default=dict, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False, index=True)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class BenchmarkCapture(Base):
    """Store Oracle and PostgreSQL benchmark metrics for comparison."""

    __tablename__ = "benchmark_captures"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    migration_id = Column(UUID(as_uuid=True), ForeignKey("migrations.id"), nullable=True)
    db_type = Column(String(20), nullable=False)  # "oracle" or "postgres"
    captured_at = Column(DateTime, default=datetime.utcnow, nullable=False, index=True)
    data = Column(JSON, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
