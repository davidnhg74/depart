"""Opportunistic encryption of existing sensitive columns.

No schema change — the affected columns stay TEXT. What changes is
that the application now writes ciphertext through `EncryptedText`
(see src/services/crypto.py) and reads through a sentinel-prefixed
decrypt step.

This migration walks the existing rows and re-writes each sensitive
value through the encryption layer *if* DEPART_ENCRYPTION_KEY is
configured at migration time. If no key is configured, the migration
is a no-op — future writes will be plaintext until the operator sets
a key and re-runs via the admin rotation endpoint.

Affected columns:
  * migrations.source_url
  * migrations.target_url
  * instance_settings.anthropic_api_key
  * instance_settings.license_jwt
  * identity_providers.client_secret

Revision ID: 009
Revises: 008
Create Date: 2026-04-22 21:00:00.000000
"""

from __future__ import annotations

from alembic import op
from sqlalchemy.sql import text


revision = "009"
down_revision = "008"
branch_labels = None
depends_on = None


# columns that should get encrypted if a key is configured
_TARGETS = [
    ("migrations", "source_url"),
    ("migrations", "target_url"),
    ("instance_settings", "anthropic_api_key"),
    ("instance_settings", "license_jwt"),
    ("identity_providers", "client_secret"),
]


def upgrade() -> None:
    # Import lazily — alembic shouldn't care whether the app's crypto
    # module is importable just to run the schema migration.
    from src.services.crypto import SENTINEL, encrypt, has_encryption_key

    if not has_encryption_key():
        # No key configured → leave plaintext. The app still works
        # correctly; encryption just doesn't happen yet.
        return

    conn = op.get_bind()
    for table, column in _TARGETS:
        # pk-column differs by table; look it up dynamically
        pk_col = "id"
        rows = conn.execute(
            text(f"SELECT {pk_col}, {column} FROM {table} WHERE {column} IS NOT NULL")
        ).fetchall()
        for row in rows:
            value = row[1]
            if value is None or value == "":
                continue
            if value.startswith(SENTINEL):
                # Already encrypted — skip (defense in depth against
                # re-running the migration, or operators who manually
                # encrypted during rollout).
                continue
            encrypted = encrypt(value)
            conn.execute(
                text(
                    f"UPDATE {table} SET {column} = :v WHERE {pk_col} = :id"
                ),
                {"v": encrypted, "id": row[0]},
            )


def downgrade() -> None:
    # Decryption-in-place would require the same key; if someone
    # downgrades we leave the data encrypted and let them roll
    # forward again. Downgrading past an encryption migration is a
    # manual operation.
    pass
