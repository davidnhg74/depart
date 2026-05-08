"""Layer 9 — cutover_readiness_snapshots table.

Stores per-assessment cutover readiness verdicts: aggregated signal
states (migration_status, cdc_lag, L6, L7, L8), a 0-100 score, and
the go/no-go ready_to_cut flag.

Revision ID: 020
Revises: 019
Create Date: 2026-05-08 00:00:00.000000
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "020"
down_revision = "019"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "cutover_readiness_snapshots",
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
            "signals",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default="[]",
        ),
        sa.Column("blocking_count", sa.Integer, nullable=False, server_default="0"),
        sa.Column("advisory_count", sa.Integer, nullable=False, server_default="0"),
        sa.Column("not_run_count", sa.Integer, nullable=False, server_default="0"),
        sa.Column("ready_to_cut", sa.Boolean, nullable=False),
        sa.Column("score", sa.Integer, nullable=False),
    )


def downgrade() -> None:
    op.drop_table("cutover_readiness_snapshots")
