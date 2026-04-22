"""Add instance_settings single-row table.

Holds the operator's BYOK Anthropic key + signed license JWT so the
self-hosted install can be configured at runtime without restarting
the container. A single row at id=1 is enforced by the service
layer, not a DB constraint — keeping the schema simple and not
tying future fields to the singleton pattern.

Revision ID: 002
Revises: 001
Create Date: 2026-04-22 15:00:00.000000
"""

from alembic import op
import sqlalchemy as sa


revision = "002"
down_revision = "001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "instance_settings",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("anthropic_api_key", sa.Text(), nullable=True),
        sa.Column("license_jwt", sa.Text(), nullable=True),
        sa.Column(
            "updated_at",
            sa.DateTime(),
            nullable=False,
            server_default=sa.text("NOW()"),
        ),
    )


def downgrade() -> None:
    op.drop_table("instance_settings")
