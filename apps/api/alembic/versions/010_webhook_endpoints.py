"""Add webhook_endpoints table.

Self-hosted webhook delivery for migration lifecycle events
(migration.completed, migration.failed). Endpoints are stored
per-install and fire from the runner's terminal state transitions.

URL and secret are stored through EncryptedText — the URL because
Slack-style webhook URLs embed a token in the path, and the secret
because it's the HMAC signing key a subscriber validates requests
with.

Revision ID: 010
Revises: 009
Create Date: 2026-04-23 10:00:00.000000
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "010"
down_revision = "009"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "webhook_endpoints",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
        ),
        sa.Column("name", sa.String(length=255), nullable=False),
        # Stored ciphertext via EncryptedText at the app layer.
        sa.Column("url", sa.Text(), nullable=False),
        sa.Column("secret", sa.Text(), nullable=True),
        # Array of event names this endpoint subscribes to, e.g.
        # ["migration.completed", "migration.failed"].
        sa.Column(
            "events",
            sa.JSON(),
            nullable=False,
            server_default=sa.text("'[]'::json"),
        ),
        sa.Column(
            "enabled",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("true"),
        ),
        sa.Column(
            "created_at",
            sa.DateTime(),
            nullable=False,
            server_default=sa.text("NOW()"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(),
            nullable=False,
            server_default=sa.text("NOW()"),
        ),
        # Last delivery attempt telemetry — surfaced in /settings/webhooks
        # so operators can see which endpoints are healthy.
        sa.Column("last_triggered_at", sa.DateTime(), nullable=True),
        sa.Column("last_status", sa.Integer(), nullable=True),
        sa.Column("last_error", sa.Text(), nullable=True),
    )


def downgrade() -> None:
    op.drop_table("webhook_endpoints")
