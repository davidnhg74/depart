"""Layer 8 — data_sample_results table.

Stores row-level data sampling results: mismatches between Oracle source
and PostgreSQL target rows detected before cutover.

Revision ID: 019
Revises: 018
Create Date: 2026-05-07 00:00:00.000000
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "019"
down_revision = "018"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "data_sample_results",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
        ),
        sa.Column(
            "migration_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("migrations.id", ondelete="CASCADE"),
            nullable=False,
            index=True,
        ),
        sa.Column(
            "user_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("users.id"),
            nullable=True,
            index=True,
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            index=True,
        ),
        sa.Column(
            "sample_size",
            sa.Integer,
            nullable=False,
        ),
        sa.Column(
            "tables_sampled",
            sa.Integer,
            nullable=False,
            server_default="0",
        ),
        sa.Column(
            "tables_skipped",
            sa.Integer,
            nullable=False,
            server_default="0",
        ),
        sa.Column(
            "mismatch_count",
            sa.Integer,
            nullable=False,
            server_default="0",
        ),
        sa.Column(
            "mismatches",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default="[]",
        ),
        sa.Column(
            "overall_status",
            sa.String(20),
            nullable=False,
        ),
    )


def downgrade() -> None:
    op.drop_table("data_sample_results")
