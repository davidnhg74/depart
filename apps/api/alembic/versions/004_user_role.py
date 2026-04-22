"""Add a `role` column to users.

Adds `role` (admin | operator | viewer) so the self-hosted product
can enforce authorization properly. Any user that existed before this
migration gets role=admin — the assumption is that if you had a user
on an earlier build, you're the operator of this install and deserve
admin rights.

(`is_active` already exists from an earlier schema so we don't touch
it here.)

Revision ID: 004
Revises: 003
Create Date: 2026-04-22 16:30:00.000000
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "004"
down_revision = "003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    role_enum = postgresql.ENUM(
        "admin", "operator", "viewer", name="user_role_enum", create_type=False
    )
    role_enum.create(op.get_bind(), checkfirst=True)

    # Use server_default='operator' for new rows, then backfill existing
    # rows to admin (see rationale in the module docstring).
    op.add_column(
        "users",
        sa.Column(
            "role",
            role_enum,
            nullable=False,
            server_default="operator",
        ),
    )

    # Backfill: promote every pre-existing user to admin.
    op.execute("UPDATE users SET role = 'admin'")


def downgrade() -> None:
    op.drop_column("users", "role")
    role_enum = postgresql.ENUM("admin", "operator", "viewer", name="user_role_enum")
    role_enum.drop(op.get_bind(), checkfirst=True)
