"""Tenant scoping on migrations.user_id.

Adds a nullable `user_id` FK to `migrations` so cloud-mode installs
can isolate each customer's migrations from each other. Self-hosted
single-tenant installs (where `ENABLE_SELF_HOSTED_AUTH=false` makes
auth a no-op) leave the column NULL — the router applies a
mode-aware filter so existing behavior is preserved.

Nullable on purpose:
  * Existing rows in self-hosted installs have no user concept and
    would fail a NOT NULL backfill.
  * Cloud-mode rows are populated by the create endpoint at write
    time; the router rejects writes without a caller when auth is
    enabled, so no genuinely-orphaned rows can be inserted in cloud
    mode.

Revision ID: 015
Revises: 014
Create Date: 2026-04-23 18:00:00.000000
"""

from alembic import op
import sqlalchemy as sa


revision = "015"
down_revision = "014"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "migrations",
        sa.Column(
            "user_id",
            sa.dialects.postgresql.UUID(as_uuid=True),
            sa.ForeignKey("users.id"),
            nullable=True,
        ),
    )
    op.create_index(
        "ix_migrations_user_id", "migrations", ["user_id"], unique=False
    )


def downgrade() -> None:
    op.drop_index("ix_migrations_user_id", table_name="migrations")
    op.drop_column("migrations", "user_id")
