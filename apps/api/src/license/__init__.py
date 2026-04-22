"""Offline license verification for self-hosted depart.

No network calls. No phone-home. The product image ships with the
public half of depart's license-signing keypair (at
`public_key.pem` alongside this module) and verifies any license JWT
the operator uploads against it.

The private half lives outside this repo — dev signing keys at
`~/.depart-keys/license_private_dev.pem`, production keys managed
out-of-band. Customers never see either.

Public surface:

  get_license_status(session)   — returns LicenseStatus for the current install
  verify(jwt, public_key_pem)   — pure: verify a token, return LicenseStatus
  LicenseStatus                 — {valid, tier, features, expires_at, subject, reason}

The verifier intentionally fails closed: a malformed, expired, or
signature-invalid token returns `LicenseStatus(valid=False, reason=...)`
rather than raising. Callers gate features on `.valid` + feature flags.
"""

from __future__ import annotations

from .verifier import (
    LicenseStatus,
    Tier,
    get_license_status,
    verify,
)

__all__ = [
    "LicenseStatus",
    "Tier",
    "get_license_status",
    "verify",
]
