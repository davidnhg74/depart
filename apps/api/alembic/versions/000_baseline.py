"""baseline schema: leads, analysis jobs, conversions, migrations, workflows, benchmarks

Revision ID: 000_baseline
Revises:
Create Date: 2026-04-22
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql
from pgvector.sqlalchemy import Vector

revision = "000_baseline"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    # `conversion_cases` below uses pgvector's VECTOR(384) type. On a
    # pre-baked dev image the extension is already installed; on a
    # fresh managed Postgres (Fly MPG, Neon, Supabase, etc.) it is
    # not, and alembic-on-container-boot would crash here. Enable it
    # idempotently as the first DDL of the schema.
    op.execute("CREATE EXTENSION IF NOT EXISTS vector")

    op.create_table(
        "leads",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False, server_default=sa.text("gen_random_uuid()")),
        sa.Column("email", sa.String(255), nullable=False, unique=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_leads_email", "leads", ["email"])

    op.create_table(
        "analysis_jobs",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False, server_default=sa.text("gen_random_uuid()")),
        sa.Column("lead_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("status", sa.String(50), nullable=False, server_default="pending"),
        sa.Column("complexity_report", postgresql.JSONB, nullable=True),
        sa.Column("pdf_path", sa.String(512), nullable=True),
        sa.Column("rate_per_day", sa.Integer, nullable=False, server_default="1000"),
        sa.Column("error_message", sa.Text, nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(["lead_id"], ["leads.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_analysis_jobs_lead_id", "analysis_jobs", ["lead_id"])
    op.create_index("ix_analysis_jobs_status", "analysis_jobs", ["status"])

    op.create_table(
        "conversion_cases",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False, server_default=sa.text("gen_random_uuid()")),
        sa.Column("construct_type", sa.String(50), nullable=False),
        sa.Column("oracle_code", sa.Text, nullable=False),
        sa.Column("postgres_code", sa.Text, nullable=False),
        sa.Column("embedding", Vector(384), nullable=False),
        sa.Column("success_count", sa.Integer, nullable=False, server_default="1"),
        sa.Column("fail_count", sa.Integer, nullable=False, server_default="0"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_conversion_cases_construct_type", "conversion_cases", ["construct_type"])
    op.create_index("ix_conversion_cases_created_at", "conversion_cases", ["created_at"])
    op.execute(
        "CREATE INDEX ix_conversion_cases_embedding "
        "ON conversion_cases USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100)"
    )

    op.create_table(
        "migrations",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False, server_default=sa.text("gen_random_uuid()")),
        sa.Column("schema_name", sa.String(255), nullable=False),
        sa.Column("status", sa.String(50), nullable=False),
        sa.Column("total_rows", sa.BigInteger, nullable=False, server_default="0"),
        sa.Column("rows_transferred", sa.BigInteger, nullable=False, server_default="0"),
        sa.Column("total_bytes", sa.BigInteger, nullable=False, server_default="0"),
        sa.Column("estimated_duration_seconds", sa.Integer, nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("error_message", sa.Text, nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_migrations_status", "migrations", ["status"])
    op.create_index("ix_migrations_created_at", "migrations", ["created_at"])

    op.create_table(
        "migration_checkpoints",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False, server_default=sa.text("gen_random_uuid()")),
        sa.Column("migration_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("table_name", sa.String(255), nullable=False),
        sa.Column("rows_processed", sa.BigInteger, nullable=False, server_default="0"),
        sa.Column("total_rows", sa.BigInteger, nullable=False, server_default="0"),
        sa.Column("progress_percentage", sa.Float, nullable=False, server_default="0"),
        sa.Column("last_rowid", sa.String(255), nullable=True),
        sa.Column("status", sa.String(50), nullable=False),
        sa.Column("error_message", sa.Text, nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(["migration_id"], ["migrations.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_migration_checkpoints_migration_id", "migration_checkpoints", ["migration_id"])
    op.create_index("ix_migration_checkpoints_created_at", "migration_checkpoints", ["created_at"])

    op.create_table(
        "migration_workflows",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False, server_default=sa.text("gen_random_uuid()")),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("migration_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("current_step", sa.Integer, nullable=False, server_default="1"),
        sa.Column("status", sa.String(50), nullable=False, server_default="running"),
        sa.Column("dba_notes", postgresql.JSONB, nullable=False, server_default="{}"),
        sa.Column("approvals", postgresql.JSONB, nullable=False, server_default="{}"),
        sa.Column("settings", postgresql.JSONB, nullable=False, server_default="{}"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(["migration_id"], ["migrations.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_migration_workflows_status", "migration_workflows", ["status"])
    op.create_index("ix_migration_workflows_created_at", "migration_workflows", ["created_at"])

    op.create_table(
        "benchmark_captures",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False, server_default=sa.text("gen_random_uuid()")),
        sa.Column("migration_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("db_type", sa.String(20), nullable=False),
        sa.Column("captured_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("data", postgresql.JSONB, nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(["migration_id"], ["migrations.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_benchmark_captures_migration_id", "benchmark_captures", ["migration_id"])
    op.create_index("ix_benchmark_captures_captured_at", "benchmark_captures", ["captured_at"])


def downgrade() -> None:
    op.drop_table("benchmark_captures")
    op.drop_table("migration_workflows")
    op.drop_table("migration_checkpoints")
    op.drop_table("migrations")
    op.execute("DROP INDEX IF EXISTS ix_conversion_cases_embedding")
    op.drop_table("conversion_cases")
    op.drop_table("analysis_jobs")
    op.drop_table("leads")
