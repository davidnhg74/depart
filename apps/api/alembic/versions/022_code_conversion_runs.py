"""Layer 11 — code_conversion_runs table.

Stores per-run PL/SQL → PL/pgSQL conversion results: per-object source,
converted code, confidence rating, review notes, and pattern list.

Revision ID: 022
Revises: 021
Create Date: 2026-05-08 00:00:00.000000
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "022"
down_revision = "021"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "code_conversion_runs",
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
        sa.Column("objects_found", sa.Integer, nullable=False, server_default="0"),
        sa.Column("objects_attempted", sa.Integer, nullable=False, server_default="0"),
        sa.Column("objects_converted", sa.Integer, nullable=False, server_default="0"),
        sa.Column("objects_failed", sa.Integer, nullable=False, server_default="0"),
        sa.Column(
            "results",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default="[]",
        ),
    )


def downgrade() -> None:
    op.drop_table("code_conversion_runs")
