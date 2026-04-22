"""Tests for application-layer column encryption.

Two layers:
  1. Pure helpers (encrypt / decrypt / has_encryption_key) — fast, no DB.
  2. ORM round-trip — write a MigrationRecord, assert the DB row is
     ciphertext, read it back, assert the Python value is plaintext.
"""

from __future__ import annotations

import os
from unittest.mock import patch

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from src.config import settings as env_settings
from src.models import IdentityProvider, InstanceSettings, MigrationRecord
from src.services import crypto
from src.services.crypto import (
    SENTINEL,
    decrypt,
    encrypt,
    generate_key,
    has_encryption_key,
)


@pytest.fixture
def fresh_key(monkeypatch):
    """Set a throwaway Fernet key in env and reset the crypto cache."""
    key = generate_key()
    monkeypatch.setenv("DEPART_ENCRYPTION_KEY", key)
    monkeypatch.delenv("DEPART_ENCRYPTION_KEYS", raising=False)
    crypto.reset_cache_for_tests()
    yield key
    crypto.reset_cache_for_tests()


@pytest.fixture
def no_key(monkeypatch):
    """Force an unkeyed environment."""
    monkeypatch.delenv("DEPART_ENCRYPTION_KEY", raising=False)
    monkeypatch.delenv("DEPART_ENCRYPTION_KEYS", raising=False)
    crypto.reset_cache_for_tests()
    yield
    crypto.reset_cache_for_tests()


# ─── Pure helpers ────────────────────────────────────────────────────────────


class TestHelpers:
    def test_roundtrip(self, fresh_key):
        assert has_encryption_key() is True
        ct = encrypt("sk-ant-api03-abc123")
        assert ct.startswith(SENTINEL)
        assert decrypt(ct) == "sk-ant-api03-abc123"

    def test_encrypt_is_idempotent(self, fresh_key):
        """Re-binding an already-encrypted value must not double-wrap."""
        ct = encrypt("secret")
        assert encrypt(ct) == ct

    def test_no_key_passes_plaintext_through(self, no_key):
        assert has_encryption_key() is False
        assert encrypt("hello") == "hello"
        assert decrypt("hello") == "hello"

    def test_no_key_raises_on_encrypted_input(self, no_key, fresh_key):
        """Encrypt with a key, then simulate key removal — reads must
        raise loudly rather than silently returning garbage."""
        # Use fresh_key to produce ciphertext, then clear env.
        ct = encrypt("the-secret")
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("DEPART_ENCRYPTION_KEY", None)
            crypto.reset_cache_for_tests()
            assert has_encryption_key() is False
            with pytest.raises(RuntimeError, match="not set"):
                decrypt(ct)

    def test_null_and_empty_pass_through(self, fresh_key):
        assert encrypt(None) is None
        assert encrypt("") == ""
        assert decrypt(None) is None
        assert decrypt("") == ""


# ─── ORM round-trip ──────────────────────────────────────────────────────────


@pytest.fixture
def db():
    engine = create_engine(env_settings.database_url)
    S = sessionmaker(bind=engine)
    s = S()
    s.query(MigrationRecord).delete()
    s.query(InstanceSettings).delete()
    s.query(IdentityProvider).delete()
    s.commit()
    yield s
    s.rollback()
    s.query(MigrationRecord).delete()
    s.query(InstanceSettings).delete()
    s.query(IdentityProvider).delete()
    s.commit()
    s.close()
    engine.dispose()


class TestOrmRoundtrip:
    def test_dsn_is_encrypted_on_disk_and_decrypted_on_read(self, db, fresh_key):
        dsn = "postgresql+psycopg://user:s3cret@host:5432/db"
        m = MigrationRecord(
            name="rt",
            schema_name="public",
            source_url=dsn,
            target_url=dsn,
            source_schema="public",
            target_schema="public",
            status="pending",
        )
        db.add(m)
        db.commit()
        mid = m.id

        # Raw row — bypass the ORM to see what actually hit disk.
        engine = create_engine(env_settings.database_url)
        with engine.connect() as conn:
            raw = conn.execute(
                text("SELECT source_url, target_url FROM migrations WHERE id = :id"),
                {"id": mid},
            ).first()
        engine.dispose()
        assert raw[0].startswith(SENTINEL)
        assert raw[1].startswith(SENTINEL)
        assert "s3cret" not in raw[0]
        assert "s3cret" not in raw[1]

        # ORM read decrypts transparently.
        db.expire_all()
        reread = db.query(MigrationRecord).filter_by(id=mid).one()
        assert reread.source_url == dsn
        assert reread.target_url == dsn

    def test_plaintext_row_still_readable(self, db, fresh_key):
        """Pre-encryption rows (no sentinel prefix) must pass through
        on read even once a key is configured — that's the upgrade
        path for existing installs."""
        # Write plaintext directly, bypassing the TypeDecorator.
        engine = create_engine(env_settings.database_url)
        with engine.begin() as conn:
            conn.execute(
                text(
                    "INSERT INTO migrations "
                    "(id, schema_name, status, source_url, target_url, "
                    " source_schema, target_schema, created_at) "
                    "VALUES (gen_random_uuid(), 'public', 'pending', "
                    "'plain-source', 'plain-target', 'public', 'public', NOW())"
                )
            )
        engine.dispose()

        rows = db.query(MigrationRecord).all()
        assert len(rows) == 1
        assert rows[0].source_url == "plain-source"
        assert rows[0].target_url == "plain-target"

    def test_no_key_stores_plaintext(self, db, no_key):
        """Without a key configured, writes go in as plaintext and the
        app works exactly like before."""
        m = MigrationRecord(
            name="rt-noenc",
            schema_name="public",
            source_url="plain://x",
            target_url="plain://y",
            source_schema="public",
            target_schema="public",
            status="pending",
        )
        db.add(m)
        db.commit()

        engine = create_engine(env_settings.database_url)
        with engine.connect() as conn:
            raw = conn.execute(
                text("SELECT source_url FROM migrations WHERE id = :id"),
                {"id": m.id},
            ).scalar()
        engine.dispose()
        assert raw == "plain://x"
        assert not raw.startswith(SENTINEL)


# ─── Key rotation ────────────────────────────────────────────────────────────


class TestRotation:
    def test_multifernet_decrypts_old_ciphertext_with_new_key(
        self, db, monkeypatch
    ):
        """Write with key A. Add key B to the front of DEPART_ENCRYPTION_KEYS.
        A read should still work (old key still in the list). A rewrite
        should produce ciphertext readable only by new key."""
        key_a = generate_key()
        monkeypatch.setenv("DEPART_ENCRYPTION_KEY", key_a)
        monkeypatch.delenv("DEPART_ENCRYPTION_KEYS", raising=False)
        crypto.reset_cache_for_tests()

        m = MigrationRecord(
            name="rot",
            schema_name="public",
            source_url="initial-secret",
            target_url="t",
            source_schema="public",
            target_schema="public",
            status="pending",
        )
        db.add(m)
        db.commit()
        mid = m.id
        db.close()

        # Flip env: new primary key B, old key A still available.
        key_b = generate_key()
        monkeypatch.delenv("DEPART_ENCRYPTION_KEY", raising=False)
        monkeypatch.setenv("DEPART_ENCRYPTION_KEYS", f"{key_b},{key_a}")
        crypto.reset_cache_for_tests()

        # New session — read must still succeed via MultiFernet fallback.
        engine = create_engine(env_settings.database_url)
        S = sessionmaker(bind=engine)
        s = S()
        reread = s.query(MigrationRecord).filter_by(id=mid).one()
        assert reread.source_url == "initial-secret"
        s.close()
        engine.dispose()
