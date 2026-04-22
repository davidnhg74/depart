"""Add SAML fields + protocol column to identity_providers.

Existing OIDC-only rows keep working — the service layer coerces
`protocol=NULL` to "oidc". New rows must set it explicitly.

Revision ID: 007
Revises: 006
Create Date: 2026-04-22 19:00:00.000000
"""

from alembic import op
import sqlalchemy as sa


revision = "007"
down_revision = "006"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "identity_providers",
        sa.Column("protocol", sa.String(length=16), nullable=True),
    )
    op.add_column(
        "identity_providers", sa.Column("saml_entity_id", sa.Text(), nullable=True)
    )
    op.add_column(
        "identity_providers", sa.Column("saml_sso_url", sa.Text(), nullable=True)
    )
    op.add_column(
        "identity_providers", sa.Column("saml_x509_cert", sa.Text(), nullable=True)
    )
    # Backfill any pre-existing row to protocol='oidc'.
    op.execute("UPDATE identity_providers SET protocol = 'oidc' WHERE protocol IS NULL")


def downgrade() -> None:
    op.drop_column("identity_providers", "saml_x509_cert")
    op.drop_column("identity_providers", "saml_sso_url")
    op.drop_column("identity_providers", "saml_entity_id")
    op.drop_column("identity_providers", "protocol")
