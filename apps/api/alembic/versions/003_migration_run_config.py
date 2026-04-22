"""Add run-time config columns to `migrations`.

Lets operators persist the source/target DSNs, source/target schemas,
table filter, batch size, and create-tables flag alongside the existing
status + row counts, so the web UI can re-run or resume a migration
without asking the user to re-enter the config every time.

Revision ID: 003
Revises: 002
Create Date: 2026-04-22 16:00:00.000000
"""

from alembic import op
import sqlalchemy as sa


revision = "003"
down_revision = "002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("migrations", sa.Column("name", sa.String(length=255), nullable=True))
    op.add_column("migrations", sa.Column("source_url", sa.Text(), nullable=True))
    op.add_column("migrations", sa.Column("target_url", sa.Text(), nullable=True))
    op.add_column("migrations", sa.Column("source_schema", sa.String(length=255), nullable=True))
    op.add_column("migrations", sa.Column("target_schema", sa.String(length=255), nullable=True))
    op.add_column("migrations", sa.Column("tables", sa.Text(), nullable=True))
    op.add_column(
        "migrations",
        sa.Column("batch_size", sa.Integer(), nullable=True, server_default="5000"),
    )
    op.add_column(
        "migrations",
        sa.Column(
            "create_tables",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("false"),
        ),
    )


def downgrade() -> None:
    op.drop_column("migrations", "create_tables")
    op.drop_column("migrations", "batch_size")
    op.drop_column("migrations", "tables")
    op.drop_column("migrations", "target_schema")
    op.drop_column("migrations", "source_schema")
    op.drop_column("migrations", "target_url")
    op.drop_column("migrations", "source_url")
    op.drop_column("migrations", "name")
