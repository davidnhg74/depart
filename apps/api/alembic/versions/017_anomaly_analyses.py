"""Layer 6 — anomaly_analyses table + anomaly_analysis_id FK on migrations.

Creates `anomaly_analyses` to store AI-driven post-migration anomaly
detection results. Adds a nullable FK `anomaly_analysis_id` on `migrations`
pointing at the most recent check for that migration.

Revision ID: 017
Revises: 016
Create Date: 2026-05-08 00:00:00.000000
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "017"
down_revision = "016"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "anomaly_analyses",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
        ),
        sa.Column(
            "user_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("users.id"),
            nullable=True,
            index=True,
        ),
        sa.Column(
            "migration_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("migrations.id"),
            nullable=False,
            index=True,
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            index=True,
        ),
        sa.Column(
            "findings",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default="[]",
        ),
        sa.Column("overall_severity", sa.String(16), nullable=False),
        sa.Column(
            "used_ai",
            sa.Boolean(),
            nullable=False,
            server_default="false",
        ),
        sa.Column(
            "tables_sampled",
            sa.Integer(),
            nullable=False,
            server_default="0",
        ),
    )

    op.add_column(
        "migrations",
        sa.Column(
            "anomaly_analysis_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("anomaly_analyses.id", ondelete="SET NULL"),
            nullable=True,
        ),
    )


def downgrade() -> None:
    op.drop_column("migrations", "anomaly_analysis_id")
    op.drop_table("anomaly_analyses")
