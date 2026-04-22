from sqlalchemy import (
    Column,
    String,
    Integer,
    DateTime,
    JSON,
    ForeignKey,
    Text,
    Enum,
    ARRAY,
    Float,
    Boolean,
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship
from datetime import datetime
import uuid
import enum
from pydantic import BaseModel
from typing import Dict, List
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
    lead_id = Column(UUID(as_uuid=True), ForeignKey("leads.id"), nullable=True)
    user_id = Column(UUID(as_uuid=True), ForeignKey("users.id"), nullable=True)
    status = Column(Enum(JobStatus), default=JobStatus.PENDING, nullable=False)
    complexity_report = Column(JSON, nullable=True)
    pdf_path = Column(String(512), nullable=True)
    rate_per_day = Column(Integer, default=1000, nullable=False)
    error_message = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    completed_at = Column(DateTime, nullable=True)

    lead = relationship("Lead", back_populates="jobs")
    user = relationship("User", back_populates="jobs")


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
    status = Column(
        String(50), nullable=False, index=True
    )  # pending, in_progress, completed, failed
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
    migration_id = Column(
        UUID(as_uuid=True), ForeignKey("migrations.id"), nullable=False, index=True
    )
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


# ============================================================================
# Phase 4: SaaS Auth & Billing Models
# ============================================================================


class PlanEnum(str, enum.Enum):
    TRIAL = "trial"
    STARTER = "starter"
    PROFESSIONAL = "professional"
    ENTERPRISE = "enterprise"


class User(Base):
    """Authenticated user with subscription and usage tracking."""

    __tablename__ = "users"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    email = Column(String(255), unique=True, nullable=False, index=True)
    full_name = Column(String(255), nullable=True)
    hashed_password = Column(String(255), nullable=False)
    email_verified = Column(Boolean, default=False, nullable=False)
    email_verify_token = Column(String(255), nullable=True)
    reset_token = Column(String(255), nullable=True)
    reset_token_expires = Column(DateTime, nullable=True)

    plan = Column(Enum(PlanEnum), default=PlanEnum.TRIAL, nullable=False)
    stripe_customer_id = Column(String(255), nullable=True, index=True)
    stripe_subscription_id = Column(String(255), nullable=True)
    subscription_status = Column(String(50), nullable=True)  # active | canceled | past_due

    trial_starts_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    trial_expires_at = Column(DateTime, nullable=False)

    databases_used = Column(Integer, default=0, nullable=False)
    migrations_used_this_month = Column(Integer, default=0, nullable=False)
    llm_conversions_this_month = Column(Integer, default=0, nullable=False)
    usage_reset_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    is_active = Column(Boolean, default=True, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False, index=True)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

    jobs = relationship("AnalysisJob", back_populates="user")
    api_keys = relationship("ApiKey", back_populates="user", cascade="all, delete-orphan")
    subscriptions = relationship(
        "Subscription", back_populates="user", cascade="all, delete-orphan"
    )
    support_tickets = relationship(
        "SupportTicket", back_populates="user", cascade="all, delete-orphan"
    )


class ApiKey(Base):
    """API key for programmatic access."""

    __tablename__ = "api_keys"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(
        UUID(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True
    )
    name = Column(String(255), nullable=False)
    key_hash = Column(String(255), unique=True, nullable=False, index=True)  # SHA-256 hash
    key_prefix = Column(String(8), nullable=False)  # first 8 chars for display
    last_used_at = Column(DateTime, nullable=True)
    is_active = Column(Boolean, default=True, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    user = relationship("User", back_populates="api_keys")


class Subscription(Base):
    """Stripe subscription event log (Stripe is source of truth)."""

    __tablename__ = "subscriptions"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(
        UUID(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True
    )
    stripe_subscription_id = Column(String(255), unique=True, nullable=False, index=True)
    plan = Column(Enum(PlanEnum), nullable=False)
    status = Column(String(50), nullable=False)  # active | canceled | past_due
    current_period_start = Column(DateTime, nullable=False)
    current_period_end = Column(DateTime, nullable=False)
    canceled_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

    user = relationship("User", back_populates="subscriptions")


class SupportTicket(Base):
    """User support ticket."""

    __tablename__ = "support_tickets"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(
        UUID(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=True, index=True
    )
    subject = Column(String(255), nullable=False)
    status = Column(
        String(50), default="open", nullable=False
    )  # open | in_progress | resolved | closed
    priority = Column(
        String(50), default="medium", nullable=False
    )  # low | medium | high | critical
    requester_email = Column(String(255), nullable=False, index=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False, index=True)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

    user = relationship("User", back_populates="support_tickets")
    messages = relationship("TicketMessage", back_populates="ticket", cascade="all, delete-orphan")


class TicketMessage(Base):
    """Message in a support ticket."""

    __tablename__ = "ticket_messages"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    ticket_id = Column(
        UUID(as_uuid=True),
        ForeignKey("support_tickets.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    author_id = Column(UUID(as_uuid=True), ForeignKey("users.id"), nullable=True)
    is_staff = Column(Boolean, default=False, nullable=False)
    body = Column(Text, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    ticket = relationship("SupportTicket", back_populates="messages")
    author = relationship("User")


# Pydantic response models (not ORM)


class MigrationReport(BaseModel):
    """Migration progress report with conversion statistics."""

    migration_id: str
    total_objects: int
    converted_count: int
    tests_generated: int
    conversion_percentage: float
    risk_breakdown: Dict[str, int]
    blockers: List[Dict[str, str]]
    generated_at: str
