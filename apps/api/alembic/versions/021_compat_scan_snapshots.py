"""Layer 10 — compat_scan_snapshots table.

Stores per-scan application SQL compatibility results: construct-level
findings (ROWNUM, CONNECT BY, NVL, etc.) and a 0-100 complexity score.

Revision ID: 021
Revises: 020
Create Date: 2026-05-08 00:00:00.000000
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "021"
down_revision = "020"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "compat_scan_snapshots",
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
        sa.Column("oracle_objects_scanned", sa.Integer, nullable=False, server_default="0"),
        sa.Column("blocking_count", sa.Integer, nullable=False, server_default="0"),
        sa.Column("advisory_count", sa.Integer, nullable=False, server_default="0"),
        sa.Column("info_count", sa.Integer, nullable=False, server_default="0"),
        sa.Column("complexity_score", sa.Integer, nullable=False, server_default="100"),
        sa.Column(
            "findings",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default="[]",
        ),
    )


def downgrade() -> None:
    op.drop_table("compat_scan_snapshots")
