"""Application-layer encryption for sensitive DB columns.

Background: DSN passwords, client secrets, Anthropic API keys, and
license JWTs all sit in the metadata Postgres as plain TEXT. Anyone
with `psql` access — or who walks off with a database backup — reads
them in cleartext. This module adds encryption-at-rest at the ORM
layer.

Design:

  * **Fernet** (via `cryptography`) — authenticated symmetric encryption
    with built-in timestamping. One dependency we already have.
  * **MultiFernet** for rotation: the active key is used for every
    new write; every configured key is tried on decrypt, so a newly-
    added key can coexist with ciphertexts produced by the previous
    key during rollover.
  * **Sentinel prefix** (`enc:v1:`) on encrypted values. Plaintext and
    encrypted rows coexist in the same column — the decryption helper
    looks at the prefix and either decrypts or passes through. That
    makes `Alembic 009` optional (the app works with a mix of
    encrypted and unencrypted rows) and lets operators set the key
    after the install has been running for a while without breaking.

Key configuration:

  * `DEPART_ENCRYPTION_KEY` — single base64 Fernet key (primary /
    write key).
  * `DEPART_ENCRYPTION_KEYS` — comma-separated list of keys, newest
    first. Overrides the single-key var when present. Use for rotation.

A missing key means encryption is disabled: new writes stay plaintext,
reads of plaintext pass through, and reads of ciphertext raise a clear
error. That's the right failure mode — better to loudly fail than to
silently drop a field.
"""

from __future__ import annotations

import logging
import os
from functools import lru_cache
from typing import Optional

from cryptography.fernet import Fernet, InvalidToken, MultiFernet
from sqlalchemy import Text
from sqlalchemy.types import TypeDecorator


logger = logging.getLogger(__name__)


SENTINEL = "enc:v1:"


# ─── Key loading ─────────────────────────────────────────────────────────────


@lru_cache(maxsize=1)
def _multifernet() -> Optional[MultiFernet]:
    """Load keys from env and return a MultiFernet, or None when no
    key is configured. LRU-cached so we don't re-parse env on every
    read/write; the cache is process-scoped, so changing env vars
    requires a restart (matches how operators manage secrets)."""
    keys_env = os.environ.get("DEPART_ENCRYPTION_KEYS")
    if keys_env:
        raw_keys = [k.strip() for k in keys_env.split(",") if k.strip()]
    else:
        single = os.environ.get("DEPART_ENCRYPTION_KEY")
        raw_keys = [single.strip()] if single and single.strip() else []

    if not raw_keys:
        return None

    try:
        fernets = [Fernet(k) for k in raw_keys]
    except Exception as exc:  # noqa: BLE001 — bad key material
        logger.error(
            "DEPART_ENCRYPTION_KEY(S) invalid (expected Fernet base64 keys): %s",
            exc,
        )
        return None
    return MultiFernet(fernets)


def has_encryption_key() -> bool:
    return _multifernet() is not None


def reset_cache_for_tests() -> None:
    """Drop the LRU cache so tests can flip env between cases."""
    _multifernet.cache_clear()


# ─── Encrypt / decrypt ───────────────────────────────────────────────────────


def encrypt(value: Optional[str]) -> Optional[str]:
    """Encrypt `value` if a key is configured and `value` isn't empty.

    Returns the sentinel-prefixed ciphertext, or the original value
    untouched if no key is configured. Idempotent — re-encrypting an
    already-encrypted value is a no-op (callers that want to rotate
    must decrypt first)."""
    if value is None or value == "":
        return value
    if value.startswith(SENTINEL):
        # Already encrypted — don't double-wrap. Matters for TypeDecorator
        # where the same value may pass through bind multiple times.
        return value
    fernet = _multifernet()
    if fernet is None:
        return value
    token = fernet.encrypt(value.encode("utf-8")).decode("ascii")
    return f"{SENTINEL}{token}"


def decrypt(value: Optional[str]) -> Optional[str]:
    """Decrypt when the value looks encrypted; pass through plaintext.

    Raises RuntimeError on encrypted input when no key is configured
    — better to fail loudly than to silently drop the value. The
    operator should either set DEPART_ENCRYPTION_KEY or restore their
    backup."""
    if value is None:
        return None
    if not value.startswith(SENTINEL):
        return value
    fernet = _multifernet()
    if fernet is None:
        raise RuntimeError(
            "encrypted value found but DEPART_ENCRYPTION_KEY is not set — "
            "cannot decrypt. Configure the key and restart the app."
        )
    token = value[len(SENTINEL):]
    try:
        return fernet.decrypt(token.encode("ascii")).decode("utf-8")
    except InvalidToken:
        raise RuntimeError(
            "encrypted value failed to decrypt — key material may be wrong "
            "or the ciphertext was tampered with."
        )


# ─── SQLAlchemy column type ──────────────────────────────────────────────────


class EncryptedText(TypeDecorator):
    """TEXT column that encrypts on write and decrypts on read.

    The underlying DB column is still TEXT — no schema change needed
    when we swap a column from Text → EncryptedText. Values written
    while no key is configured stay plaintext; values written after
    the key is set are sentinel-prefixed ciphertexts. Both shapes
    coexist peacefully in the same column."""

    impl = Text
    cache_ok = True

    def process_bind_param(self, value, dialect):
        return encrypt(value)

    def process_result_value(self, value, dialect):
        return decrypt(value)


# ─── Key generation helper (admin CLI + tests) ───────────────────────────────


def generate_key() -> str:
    """Convenience wrapper so callers don't have to import Fernet."""
    return Fernet.generate_key().decode("ascii")
