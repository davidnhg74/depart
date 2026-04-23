"""Branding columns on instance_settings for white-label installs.

Adds five nullable text columns to the singleton settings row. NULL
means "use the bundled Hafen default" — the service layer materializes
the effective value at read time. Writes are gated by the license
carrying the `white_label` feature; the schema doesn't enforce that
because feature gating belongs above the data layer.

Revision ID: 014
Revises: 013
Create Date: 2026-04-23 16:00:00.000000
"""

from alembic import op
import sqlalchemy as sa


revision = "014"
down_revision = "013"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "instance_settings",
        sa.Column("brand_company_name", sa.String(length=255), nullable=True),
    )
    op.add_column(
        "instance_settings",
        sa.Column("brand_product_name", sa.String(length=255), nullable=True),
    )
    op.add_column(
        "instance_settings",
        sa.Column("brand_logo_url", sa.String(length=2000), nullable=True),
    )
    op.add_column(
        "instance_settings",
        sa.Column("brand_primary_color", sa.String(length=7), nullable=True),
    )
    op.add_column(
        "instance_settings",
        sa.Column("brand_support_email", sa.String(length=255), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("instance_settings", "brand_support_email")
    op.drop_column("instance_settings", "brand_primary_color")
    op.drop_column("instance_settings", "brand_logo_url")
    op.drop_column("instance_settings", "brand_product_name")
    op.drop_column("instance_settings", "brand_company_name")
