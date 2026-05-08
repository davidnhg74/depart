"""Add 'pilot' value to plan_enum — permanent free tier for schema analysis.

Revision ID: 023
Revises: 022
Create Date: 2026-05-08 00:00:00.000000
"""

from __future__ import annotations

from alembic import op

revision = "023"
down_revision = "022"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("ALTER TYPE plan_enum ADD VALUE IF NOT EXISTS 'pilot'")


def downgrade() -> None:
    # PostgreSQL does not support removing enum values without recreating the type.
    # Downgrade is a no-op; remove 'pilot' rows manually before reverting if needed.
    pass
