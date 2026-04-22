"""Password hashing and verification.

Uses bcrypt directly. The previous implementation went through passlib's
CryptContext, but passlib 1.7 (the latest release) is incompatible with
bcrypt 5+ — its initialization probe sends a 73-byte test password that
bcrypt 5 refuses to truncate, raising at module load. Calling bcrypt
directly sidesteps the issue and removes a stale dependency.

bcrypt's max input is 72 bytes; we hash longer passwords with sha256
first so they're not silently truncated. This is the same trick passlib
recommends for "long passwords + bcrypt" and matches the standard
Python web-stack pattern.
"""

from __future__ import annotations

import base64
import hashlib

import bcrypt


def _coerce(password: str) -> bytes:
    """Encode + length-protect. bcrypt input is capped at 72 bytes; if the
    user's password exceeds that we sha256 it to a fixed 44-byte digest
    so the entire password contributes to the hash."""
    raw = password.encode("utf-8")
    if len(raw) <= 72:
        return raw
    digest = hashlib.sha256(raw).digest()
    return base64.b64encode(digest)  # 44 bytes


def hash_password(password: str) -> str:
    return bcrypt.hashpw(_coerce(password), bcrypt.gensalt()).decode("utf-8")


def verify_password(plain_password: str, hashed_password: str) -> bool:
    try:
        return bcrypt.checkpw(_coerce(plain_password), hashed_password.encode("utf-8"))
    except (ValueError, TypeError):
        return False
