"""Add identity_providers single-row table for SSO config.

OIDC-only for now. See src/models.py:IdentityProvider for the rationale
on storing secrets in plain text (same trust boundary as
MigrationRecord DSNs).

Revision ID: 006
Revises: 005
Create Date: 2026-04-22 18:00:00.000000
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "006"
down_revision = "005"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # user_role_enum already exists (created in 004); reuse it via
    # create_type=False.
    role_enum = postgresql.ENUM(
        "admin", "operator", "viewer", name="user_role_enum", create_type=False
    )

    op.create_table(
        "identity_providers",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("enabled", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("issuer", sa.Text(), nullable=True),
        sa.Column("client_id", sa.String(length=255), nullable=True),
        sa.Column("client_secret", sa.Text(), nullable=True),
        sa.Column(
            "default_role",
            role_enum,
            nullable=False,
            server_default="viewer",
        ),
        sa.Column(
            "auto_provision",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("true"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(),
            nullable=False,
            server_default=sa.text("NOW()"),
        ),
    )


def downgrade() -> None:
    op.drop_table("identity_providers")
