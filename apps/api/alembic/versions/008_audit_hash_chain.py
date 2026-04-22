"""Add hash-chain columns to audit_events for tamper-evident auditing.

Each row stores a sha256 over (prev_hash || canonical row content).
Existing rows get backfilled by walking them in (created_at, id) order
so the chain starts intact from the first event.

Revision ID: 008
Revises: 007
Create Date: 2026-04-22 20:00:00.000000
"""

import hashlib
import json

from alembic import op
import sqlalchemy as sa
from sqlalchemy.sql import text


revision = "008"
down_revision = "007"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "audit_events", sa.Column("prev_hash", sa.String(length=64), nullable=True)
    )
    op.add_column(
        "audit_events", sa.Column("row_hash", sa.String(length=64), nullable=True)
    )
    op.create_index(
        "ix_audit_events_row_hash", "audit_events", ["row_hash"], unique=False
    )

    # Backfill the chain over any pre-existing rows.
    conn = op.get_bind()
    rows = conn.execute(
        text(
            "SELECT id, action, user_email, created_at, details, "
            "resource_type, resource_id FROM audit_events "
            "ORDER BY created_at ASC, id ASC"
        )
    ).fetchall()
    prev = ""
    for r in rows:
        payload = "|".join(
            [
                prev,
                r.action or "",
                r.user_email or "",
                r.created_at.isoformat() if r.created_at else "",
                json.dumps(r.details or {}, sort_keys=True, default=str),
                r.resource_type or "",
                r.resource_id or "",
            ]
        )
        digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()
        conn.execute(
            text(
                "UPDATE audit_events SET prev_hash = :prev, row_hash = :hash "
                "WHERE id = :id"
            ),
            {"prev": prev or None, "hash": digest, "id": r.id},
        )
        prev = digest


def downgrade() -> None:
    op.drop_index("ix_audit_events_row_hash", table_name="audit_events")
    op.drop_column("audit_events", "row_hash")
    op.drop_column("audit_events", "prev_hash")
