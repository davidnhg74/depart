"""Add masking_rules Text column to migrations.

Stores JSON of per-column masking rules for PII redaction during
data movement. Shape:

    {
      "SCHEMA.TABLE": {
        "COLUMN": { "strategy": "hash|null|fixed|partial|regex", ...opts }
      }
    }

Kept as Text (JSON-serialized at the app layer) to match the
existing `tables` column convention — avoids pulling JSONB into a
small feature and keeps migrations predictable.

Revision ID: 012
Revises: 011
Create Date: 2026-04-23 11:30:00.000000
"""

from alembic import op
import sqlalchemy as sa


revision = "012"
down_revision = "011"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "migrations",
        sa.Column("masking_rules", sa.Text(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("migrations", "masking_rules")
