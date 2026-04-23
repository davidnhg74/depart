"""CDC (change-data-capture) queue + SCN tracking on migrations.

Foundational schema for Oracle→Postgres CDC. The capture worker
(LogMiner-driven, lands in a follow-up session) inserts rows into
`migration_cdc_changes`. The apply worker pulls unapplied rows
SCN-ordered and UPSERTs them onto the target.

Two apply modes supported via `migrations.cdc_apply_mode`:
  * per_row — each change in its own txn; failures go to apply_error,
              good changes still land. Default. Forward progress.
  * atomic — whole batch in one txn; any failure rolls back all.
              Stricter audit semantics for regulated customers.

See docs/CDC_DESIGN.md for the full rationale.

Revision ID: 013
Revises: 012
Create Date: 2026-04-23 12:15:00.000000
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "013"
down_revision = "012"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ─── migration_cdc_changes ───
    op.create_table(
        "migration_cdc_changes",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column(
            "migration_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("migrations.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("scn", sa.BigInteger(), nullable=False),
        sa.Column("source_schema", sa.String(length=255), nullable=False),
        sa.Column("source_table", sa.String(length=255), nullable=False),
        sa.Column("op", sa.CHAR(length=1), nullable=False),
        sa.Column("pk_json", postgresql.JSONB(), nullable=False),
        sa.Column("before_json", postgresql.JSONB(), nullable=True),
        sa.Column("after_json", postgresql.JSONB(), nullable=True),
        sa.Column("committed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "captured_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("NOW()"),
        ),
        sa.Column("applied_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("apply_error", sa.Text(), nullable=True),
        sa.CheckConstraint("op IN ('I','U','D')", name="ck_cdc_op_valid"),
    )
    # SCN-ordered fetch by migration. Covers the common "next batch"
    # query plus cutover-time full-scan reads.
    op.create_index(
        "ix_cdc_migration_scn",
        "migration_cdc_changes",
        ["migration_id", "scn"],
    )
    # Hot path: "what's unapplied?" — partial index keeps it small.
    op.create_index(
        "ix_cdc_unapplied",
        "migration_cdc_changes",
        ["migration_id"],
        postgresql_where=sa.text("applied_at IS NULL"),
    )

    # ─── SCN tracking on migrations ───
    op.add_column(
        "migrations",
        sa.Column("last_captured_scn", sa.BigInteger(), nullable=True),
    )
    op.add_column(
        "migrations",
        sa.Column("last_applied_scn", sa.BigInteger(), nullable=True),
    )
    op.add_column(
        "migrations",
        sa.Column(
            "cdc_apply_mode",
            sa.String(length=16),
            nullable=False,
            server_default=sa.text("'per_row'"),
        ),
    )
    op.create_check_constraint(
        "ck_migrations_cdc_apply_mode",
        "migrations",
        "cdc_apply_mode IN ('per_row', 'atomic')",
    )


def downgrade() -> None:
    op.drop_constraint(
        "ck_migrations_cdc_apply_mode", "migrations", type_="check"
    )
    op.drop_column("migrations", "cdc_apply_mode")
    op.drop_column("migrations", "last_applied_scn")
    op.drop_column("migrations", "last_captured_scn")
    op.drop_index("ix_cdc_unapplied", table_name="migration_cdc_changes")
    op.drop_index("ix_cdc_migration_scn", table_name="migration_cdc_changes")
    op.drop_table("migration_cdc_changes")
