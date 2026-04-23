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
from .services.crypto import EncryptedText
from .utils.time import utc_now


class JobStatus(str, enum.Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    DONE = "done"
    ERROR = "error"


class Lead(Base):
    __tablename__ = "leads"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    email = Column(String(255), unique=True, nullable=False, index=True)
    created_at = Column(DateTime, default=utc_now, nullable=False)

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
    created_at = Column(DateTime, default=utc_now, nullable=False)
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
    created_at = Column(DateTime, default=utc_now, nullable=False, index=True)
    updated_at = Column(DateTime, default=utc_now, onupdate=utc_now)

    @property
    def success_rate(self) -> float:
        total = self.success_count + self.fail_count
        return self.success_count / total if total > 0 else 0.0


class MigrationRecord(Base):
    """Track data migrations from Oracle to PostgreSQL.

    Holds both the run-time config the operator supplied (source/target
    DSNs, schemas, batch size, table filter, DDL-gen flag) and the
    resulting status + row counts. For v1 self-hosted we persist the
    full DSNs including passwords — the whole install lives inside the
    operator's trust boundary. Production deployments should layer
    Postgres TDE on the metadata DB if that's a concern; we'll add
    per-field column encryption in a later pass.
    """

    __tablename__ = "migrations"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    # Friendly name shown in the UI list. Distinct from the schema
    # names so two migrations against the same schema have separate
    # rows with different labels.
    name = Column(String(255), nullable=True)
    # Legacy source-only schema column (kept for back-compat with the
    # existing CheckpointManager.create_migration(schema_name) call).
    schema_name = Column(String(255), nullable=False)
    # Runtime config — set when the migration is created, used when
    # /run triggers the actual data-movement loop. DSNs encrypted at
    # rest (they carry the source/target DB passwords); see
    # src/services/crypto.py for the scheme.
    source_url = Column(EncryptedText, nullable=True)
    target_url = Column(EncryptedText, nullable=True)
    source_schema = Column(String(255), nullable=True)
    target_schema = Column(String(255), nullable=True)
    tables = Column(Text, nullable=True)  # JSON list of names, or NULL = all
    # Per-column PII redaction rules applied during data movement.
    # JSON-serialized dict: { "SCHEMA.TABLE": { "COLUMN": {"strategy": ..., ...} } }.
    # NULL = no masking (default). See src/services/masking_service.py.
    masking_rules = Column(Text, nullable=True)
    batch_size = Column(Integer, default=5000)
    create_tables = Column(Boolean, default=False, nullable=False)
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
    created_at = Column(DateTime, default=utc_now, nullable=False, index=True)
    # Populated on clones produced by the scheduler so operators can
    # list "past runs of this schedule" from the migration side.
    spawned_from_schedule_id = Column(
        UUID(as_uuid=True),
        ForeignKey("migration_schedules.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )

    @property
    def elapsed_seconds(self) -> int:
        if not self.started_at:
            return 0
        end = self.completed_at or utc_now()
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
    created_at = Column(DateTime, default=utc_now, nullable=False, index=True)
    updated_at = Column(DateTime, default=utc_now, onupdate=utc_now)


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
    created_at = Column(DateTime, default=utc_now, nullable=False, index=True)
    updated_at = Column(DateTime, default=utc_now, onupdate=utc_now)


class BenchmarkCapture(Base):
    """Store Oracle and PostgreSQL benchmark metrics for comparison."""

    __tablename__ = "benchmark_captures"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    migration_id = Column(UUID(as_uuid=True), ForeignKey("migrations.id"), nullable=True)
    db_type = Column(String(20), nullable=False)  # "oracle" or "postgres"
    captured_at = Column(DateTime, default=utc_now, nullable=False, index=True)
    data = Column(JSON, nullable=False)
    created_at = Column(DateTime, default=utc_now, nullable=False)


# ============================================================================
# Phase 4: SaaS Auth & Billing Models
# ============================================================================


class PlanEnum(str, enum.Enum):
    TRIAL = "trial"
    STARTER = "starter"
    PROFESSIONAL = "professional"
    ENTERPRISE = "enterprise"


class UserRole(str, enum.Enum):
    """Self-hosted access-control role. The UI renders feature sets
    off this; the `require_role` FastAPI dependency enforces it on
    every mutating endpoint. The three-level split is deliberately
    coarse — an ops team with two DBAs and a CTO doesn't need more."""

    ADMIN = "admin"        # manage users, upload licenses, change settings
    OPERATOR = "operator"  # create + run migrations, AI convert
    VIEWER = "viewer"      # read-only — dashboards, reports, audit log


class User(Base):
    """Authenticated user with subscription and usage tracking."""

    __tablename__ = "users"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    email = Column(String(255), unique=True, nullable=False, index=True)
    full_name = Column(String(255), nullable=True)
    hashed_password = Column(String(255), nullable=False)
    # Self-hosted role. Cloud users default to OPERATOR (same rights they
    # had before this column existed); the first user created in a fresh
    # self-hosted install gets ADMIN via the bootstrap flow.
    # `values_callable` forces SQLAlchemy to emit the enum *value*
    # ('admin', 'operator', 'viewer') rather than the Python member
    # name ('ADMIN', ...) which doesn't match the pg enum labels.
    role = Column(
        Enum(
            UserRole,
            name="user_role_enum",
            values_callable=lambda enum_cls: [e.value for e in enum_cls],
        ),
        default=UserRole.OPERATOR,
        nullable=False,
    )
    is_active = Column(Boolean, default=True, nullable=False)
    email_verified = Column(Boolean, default=False, nullable=False)
    email_verify_token = Column(String(255), nullable=True)
    reset_token = Column(String(255), nullable=True)
    reset_token_expires = Column(DateTime, nullable=True)

    plan = Column(
        Enum(
            PlanEnum,
            name="plan_enum",
            values_callable=lambda enum_cls: [e.value for e in enum_cls],
        ),
        default=PlanEnum.TRIAL,
        nullable=False,
    )
    stripe_customer_id = Column(String(255), nullable=True, index=True)
    stripe_subscription_id = Column(String(255), nullable=True)
    subscription_status = Column(String(50), nullable=True)  # active | canceled | past_due

    trial_starts_at = Column(DateTime, default=utc_now, nullable=False)
    trial_expires_at = Column(DateTime, nullable=False)

    databases_used = Column(Integer, default=0, nullable=False)
    migrations_used_this_month = Column(Integer, default=0, nullable=False)
    llm_conversions_this_month = Column(Integer, default=0, nullable=False)
    usage_reset_at = Column(DateTime, default=utc_now, nullable=False)

    is_active = Column(Boolean, default=True, nullable=False)
    created_at = Column(DateTime, default=utc_now, nullable=False, index=True)
    updated_at = Column(DateTime, default=utc_now, onupdate=utc_now, nullable=False)

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
    created_at = Column(DateTime, default=utc_now, nullable=False)

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
    created_at = Column(DateTime, default=utc_now, nullable=False)
    updated_at = Column(DateTime, default=utc_now, onupdate=utc_now, nullable=False)

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
    created_at = Column(DateTime, default=utc_now, nullable=False, index=True)
    updated_at = Column(DateTime, default=utc_now, onupdate=utc_now, nullable=False)

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
    created_at = Column(DateTime, default=utc_now, nullable=False)

    ticket = relationship("SupportTicket", back_populates="messages")
    author = relationship("User")


class AuditEvent(Base):
    """Append-only record of mutating actions on this install.

    Compliance pattern: an enterprise running hafen needs to answer
    "who ran migration X?", "who uploaded the license that unlocked
    feature Y?", "who changed the Anthropic key?" long after the
    events happen. We denormalize `user_email` onto the row so the
    answer survives the user being deleted — that's why `user_id` is
    a nullable FK with ON DELETE SET NULL rather than a hard FK."""

    __tablename__ = "audit_events"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    # FK SET NULL on user deletion so audit rows survive. user_email is
    # the actual compliance artifact — we write whatever email was on
    # the user at the time of the event.
    user_id = Column(
        UUID(as_uuid=True),
        ForeignKey("users.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    user_email = Column(String(255), nullable=True, index=True)
    # Action verb: "user.login", "migration.run", "license.upload", ...
    action = Column(String(64), nullable=False, index=True)
    # What was touched. Both nullable because some actions (login, logout)
    # act on the user themselves and don't have a separate resource.
    resource_type = Column(String(64), nullable=True)
    resource_id = Column(String(128), nullable=True)
    # Structured detail. For login attempts: success/failure + IP. For
    # migration.run: the migration name. For user.update: the field(s)
    # changed. Keep it small — this table gets scanned in the UI.
    details = Column(JSON, nullable=True)
    ip = Column(String(45), nullable=True)  # 45 chars = IPv6 max
    user_agent = Column(String(512), nullable=True)
    created_at = Column(DateTime, default=utc_now, nullable=False, index=True)
    # Hash-chain columns for tamper-evident auditing. `row_hash` =
    # sha256 over (prev_hash || action || user_email || created_at_iso
    # || details_json || resource_type || resource_id). `prev_hash`
    # points at the previous event's row_hash, ordered by created_at+id.
    # See src/services/audit.py for the canonical serialization.
    prev_hash = Column(String(64), nullable=True)
    row_hash = Column(String(64), nullable=True, index=True)


class IdentityProvider(Base):
    """Single-row SSO configuration for this install.

    Supports both OIDC and SAML via the `protocol` column. Only one
    active IdP per install for v1 — the enable flag is global and the
    /login button reads `protocol` to pick the right flow.

    Enforced singleton by id=1 via the service layer."""

    __tablename__ = "identity_providers"

    id = Column(Integer, primary_key=True, default=1)
    enabled = Column(Boolean, default=False, nullable=False)
    # "oidc" | "saml". Column is nullable for pre-SAML rows — the
    # service layer coerces NULL to "oidc" so old installs don't break.
    protocol = Column(String(16), nullable=True)
    # Role assigned when a user signs in via SSO for the first time
    # and there's no matching local record. Default 'viewer' is the
    # safe choice: SSO can't mint admins automatically.
    default_role = Column(
        Enum(
            UserRole,
            name="user_role_enum",
            values_callable=lambda enum_cls: [e.value for e in enum_cls],
        ),
        default=UserRole.VIEWER,
        nullable=False,
    )
    auto_provision = Column(Boolean, default=True, nullable=False)

    # ── OIDC fields ────────────────────────────────────────────────
    # The IdP's issuer URL — we append /.well-known/openid-configuration
    # to discover endpoints at runtime.
    issuer = Column(Text, nullable=True)
    client_id = Column(String(255), nullable=True)
    # Encrypted at rest — this is the OAuth client secret the IdP
    # issued us, one of the most sensitive things the install holds.
    client_secret = Column(EncryptedText, nullable=True)

    # ── SAML fields ────────────────────────────────────────────────
    # IdP entity id (SAML "issuer"). Examples:
    #   https://sts.windows.net/<tenant>/
    #   http://www.okta.com/<okta_org_id>
    saml_entity_id = Column(Text, nullable=True)
    # Single-sign-on URL — where we POST AuthnRequest to.
    saml_sso_url = Column(Text, nullable=True)
    # Base-64 encoded X.509 cert used to verify IdP response
    # signatures. Multi-line PEM is fine; the library strips headers.
    saml_x509_cert = Column(Text, nullable=True)

    updated_at = Column(DateTime, default=utc_now, onupdate=utc_now, nullable=False)


class InstanceSettings(Base):
    """Single-row table holding this install's runtime configuration
    that isn't captured by env vars — the operator's BYOK Anthropic
    key, the uploaded license JWT, and future per-install toggles.

    Kept in Postgres so it survives container restarts and is visible
    to migrations. A single row with `id=1` is enforced via a unique
    partial index equivalent (the service layer reads/upserts id=1)."""

    __tablename__ = "instance_settings"

    id = Column(Integer, primary_key=True, default=1)
    # Anthropic API key used for live AI conversion. Encrypted at rest
    # via EncryptedText. The GET /settings endpoint masks all but the
    # last 4 chars before returning it to the UI. NULL = no BYOK
    # configured, convert-live unavailable.
    anthropic_api_key = Column(EncryptedText, nullable=True)
    # Signed JWT issued at license purchase. Encrypted at rest so an
    # attacker with DB read can't lift the license onto another install.
    # NULL = Community tier.
    license_jwt = Column(EncryptedText, nullable=True)
    updated_at = Column(DateTime, default=utc_now, onupdate=utc_now, nullable=False)


class WebhookEndpoint(Base):
    """Per-install webhook subscription for migration lifecycle events.

    The runner's terminal state transitions call
    webhook_service.fire_event(...), which looks up enabled endpoints
    subscribed to the event and POSTs a signed JSON payload.

    URL is encrypted at rest because Slack-style webhook URLs embed
    the auth token in the path. Secret is encrypted because it's the
    HMAC signing key the subscriber validates with."""

    __tablename__ = "webhook_endpoints"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name = Column(String(255), nullable=False)
    url = Column(EncryptedText, nullable=False)
    secret = Column(EncryptedText, nullable=True)
    events = Column(JSON, nullable=False, default=list)
    enabled = Column(Boolean, nullable=False, default=True)
    created_at = Column(DateTime, default=utc_now, nullable=False)
    updated_at = Column(DateTime, default=utc_now, onupdate=utc_now, nullable=False)
    last_triggered_at = Column(DateTime, nullable=True)
    last_status = Column(Integer, nullable=True)
    last_error = Column(Text, nullable=True)


class MigrationSchedule(Base):
    """Cron-driven recurring execution for a MigrationRecord.

    1:1 with a migration (unique FK) — the migration row is the
    template, each fire clones it into a fresh row (new id, reset
    run state) and enqueues through the existing arq path. If the
    operator wants two schedules for the same data movement, they
    clone the migration.

    `next_run_at` is stored as naive UTC (matching the codebase
    convention) but is *computed* in the schedule's timezone so
    "run at 2am" stays 2am across DST. croniter does the math.
    """

    __tablename__ = "migration_schedules"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    migration_id = Column(
        UUID(as_uuid=True),
        ForeignKey("migrations.id", ondelete="CASCADE"),
        nullable=False,
        unique=True,
    )
    name = Column(String(255), nullable=False)
    cron_expr = Column(String(120), nullable=False)
    timezone = Column(String(64), nullable=False, default="UTC")
    enabled = Column(Boolean, nullable=False, default=True)
    next_run_at = Column(DateTime, nullable=False, index=True)
    last_run_at = Column(DateTime, nullable=True)
    last_run_migration_id = Column(
        UUID(as_uuid=True),
        ForeignKey("migrations.id", ondelete="SET NULL"),
        nullable=True,
    )
    last_run_status = Column(String(50), nullable=True)
    created_at = Column(DateTime, default=utc_now, nullable=False)
    updated_at = Column(DateTime, default=utc_now, onupdate=utc_now, nullable=False)


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
