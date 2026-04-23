"""Offline license JWT verification.

RS256 chosen (not HS256) so the private signing key never needs to
live inside the shipped product image. Operators only need the public
key, which we bundle at import time.

Token shape (claims we care about):

    {
        "sub":      "operator-contact@customer.com",
        "project":  "acme-migration-2026q2",
        "tier":     "pro" | "enterprise",
        "features": ["ai_conversion", "runbook_pdf", "ongoing_cdc", "webhooks"],
        "iat":      <epoch seconds issued>,
        "exp":      <epoch seconds expiry>
    }

Extra claims are ignored; missing required claims surface as
`LicenseStatus(valid=False, reason=...)` instead of raising. The idea
is that a bad token *degrades to Community* rather than crashing the
product — a critical property for an offline license system, where a
support-and-forget failure path is much better than a hard gate.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import List, Optional

from jose import JWTError, jwt
from sqlalchemy.orm import Session


# ─── Bundled public key ──────────────────────────────────────────────────────
#
# The public half of the license-signing keypair. Shipped with the product
# image. Regenerate via `scripts/generate_license_keypair.sh` if/when the
# signing key rotates (coordinate with issued licenses first — rotation
# invalidates every token signed with the old key).

_PUBLIC_KEY_PATH = Path(__file__).parent / "public_key.pem"


def _load_public_key() -> str:
    try:
        return _PUBLIC_KEY_PATH.read_text()
    except FileNotFoundError:
        # Shouldn't happen in a real install — the key ships with the
        # package. Surface a clear error so deployment misconfig is
        # caught immediately rather than silently disabling licensing.
        raise RuntimeError(
            f"license public key missing at {_PUBLIC_KEY_PATH!r} — "
            "did the bundle ship correctly?"
        )


# ─── Public types ────────────────────────────────────────────────────────────


class Tier(str, Enum):
    COMMUNITY = "community"  # no license at all
    PRO = "pro"              # per-project license
    ENTERPRISE = "enterprise"


@dataclass(frozen=True)
class LicenseStatus:
    """Summary of the current install's license state.

    `valid` is the single bit callers gate on. `reason` carries the
    human-readable explanation for the UI / logs when valid=False."""

    valid: bool
    tier: Tier = Tier.COMMUNITY
    features: List[str] = field(default_factory=list)
    expires_at: Optional[datetime] = None
    subject: Optional[str] = None
    project: Optional[str] = None
    reason: Optional[str] = None  # populated when valid=False

    def has_feature(self, feature: str) -> bool:
        return self.valid and feature in self.features


# ─── Verification ────────────────────────────────────────────────────────────


def verify(token: Optional[str], *, public_key_pem: Optional[str] = None) -> LicenseStatus:
    """Verify a license JWT against the bundled (or caller-supplied)
    public key. Returns LicenseStatus; never raises on a bad token."""
    if not token:
        return LicenseStatus(valid=False, reason="no license uploaded")

    try:
        claims = jwt.decode(
            token,
            public_key_pem or _load_public_key(),
            algorithms=["RS256"],
            # `jose` treats 'exp' specially; we don't set audience/issuer yet.
        )
    except JWTError as exc:
        return LicenseStatus(valid=False, reason=f"invalid signature or claims: {exc}")

    tier_raw = str(claims.get("tier") or "").lower()
    try:
        tier = Tier(tier_raw)
    except ValueError:
        return LicenseStatus(valid=False, reason=f"unknown tier: {tier_raw!r}")

    if tier == Tier.COMMUNITY:
        # Defensive: a signed token claiming community has no effect.
        return LicenseStatus(valid=False, reason="community-tier token carries no grants")

    features = list(claims.get("features") or [])
    exp_epoch = claims.get("exp")
    expires_at = datetime.fromtimestamp(exp_epoch, tz=timezone.utc) if exp_epoch else None

    return LicenseStatus(
        valid=True,
        tier=tier,
        features=features,
        expires_at=expires_at,
        subject=claims.get("sub"),
        project=claims.get("project"),
    )


def get_license_status(session: Session) -> LicenseStatus:
    """Read the stored license JWT from InstanceSettings and verify it.
    Returns the Community/no-license status when none is uploaded."""
    # Local import to avoid a circular dep between settings_service
    # (which reads InstanceSettings) and the license package.
    from ..services.settings_service import get_instance_settings

    row = get_instance_settings(session)
    return verify(row.license_jwt)
