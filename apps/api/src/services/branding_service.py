"""White-label branding for self-hosted installs.

The product ships under the Hafen brand. Operators on a license that
carries the `white_label` feature can override the company name, the
product name shown in the UI, the logo URL, the primary brand color,
and the support email surfaced throughout the app.

NULL columns on `InstanceSettings` mean "use the Hafen default" — the
defaults live here, not in the schema, so we can ship a new product
name without a data migration.

Reads are cheap and unauthenticated (the UI bootstraps from
`/api/v1/branding` before the user logs in); writes are admin-only and
license-gated.
"""

from __future__ import annotations

import re
from dataclasses import asdict, dataclass
from typing import Optional

from sqlalchemy.orm import Session

from .settings_service import get_instance_settings


WHITE_LABEL_FEATURE = "white_label"


# ─── Defaults ────────────────────────────────────────────────────────────────


DEFAULT_COMPANY_NAME = "Hafen"
DEFAULT_PRODUCT_NAME = "Hafen"
DEFAULT_LOGO_URL: Optional[str] = None  # UI falls back to text-only header
DEFAULT_PRIMARY_COLOR = "#7C3AED"  # purple-600, matches existing pages
DEFAULT_SUPPORT_EMAIL = "support@hafen.ai"


@dataclass(frozen=True)
class BrandingConfig:
    company_name: str
    product_name: str
    logo_url: Optional[str]
    primary_color: str
    support_email: str

    def to_dict(self) -> dict:
        return asdict(self)


# ─── Validation ──────────────────────────────────────────────────────────────


_HEX_COLOR_RX = re.compile(r"^#[0-9A-Fa-f]{6}$")
_EMAIL_RX = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
_URL_RX = re.compile(r"^https?://", re.IGNORECASE)


class BrandingValidationError(ValueError):
    """Raised by `update_branding` for malformed inputs. Routers map
    this to HTTP 400; nothing else should catch it."""


def _validate_color(value: Optional[str]) -> Optional[str]:
    if value is None or value == "":
        return None
    if not _HEX_COLOR_RX.match(value):
        raise BrandingValidationError(
            f"primary_color must be a 6-digit hex like '#7C3AED'; got {value!r}"
        )
    return value


def _validate_email(value: Optional[str]) -> Optional[str]:
    if value is None or value == "":
        return None
    if not _EMAIL_RX.match(value):
        raise BrandingValidationError(f"support_email is not a valid address: {value!r}")
    return value


def _validate_url(value: Optional[str]) -> Optional[str]:
    if value is None or value == "":
        return None
    if not _URL_RX.match(value):
        raise BrandingValidationError(
            f"logo_url must start with http:// or https://; got {value!r}"
        )
    return value


def _validate_text(value: Optional[str], *, field: str, max_len: int) -> Optional[str]:
    if value is None or value == "":
        return None
    stripped = value.strip()
    if not stripped:
        return None
    if len(stripped) > max_len:
        raise BrandingValidationError(f"{field} exceeds {max_len} characters")
    return stripped


# ─── Read ────────────────────────────────────────────────────────────────────


def get_branding(session: Session) -> BrandingConfig:
    """Return the effective branding for this install.

    Each NULL column falls back to its default. Returning a fully
    populated dataclass means the UI never needs to know about the
    null-vs-set distinction — it just renders whatever it gets."""
    row = get_instance_settings(session)
    return BrandingConfig(
        company_name=row.brand_company_name or DEFAULT_COMPANY_NAME,
        product_name=row.brand_product_name or DEFAULT_PRODUCT_NAME,
        logo_url=row.brand_logo_url or DEFAULT_LOGO_URL,
        primary_color=row.brand_primary_color or DEFAULT_PRIMARY_COLOR,
        support_email=row.brand_support_email or DEFAULT_SUPPORT_EMAIL,
    )


# ─── Write ───────────────────────────────────────────────────────────────────


@dataclass
class BrandingUpdate:
    """Partial update payload. Any field left as `_UNSET` is not touched;
    any field set to `None` clears the override and reverts to the
    default. Pydantic on the router side normalizes user input into one
    of these states."""

    company_name: Optional[str] = None
    product_name: Optional[str] = None
    logo_url: Optional[str] = None
    primary_color: Optional[str] = None
    support_email: Optional[str] = None


def update_branding(
    session: Session,
    update: BrandingUpdate,
) -> BrandingConfig:
    """Apply a branding update. Caller is responsible for license + role
    gating; this function only validates the values themselves and
    persists them. Returns the post-update effective config."""
    row = get_instance_settings(session)

    row.brand_company_name = _validate_text(
        update.company_name, field="company_name", max_len=255
    )
    row.brand_product_name = _validate_text(
        update.product_name, field="product_name", max_len=255
    )
    row.brand_logo_url = _validate_url(update.logo_url)
    row.brand_primary_color = _validate_color(update.primary_color)
    row.brand_support_email = _validate_email(update.support_email)

    session.commit()
    session.refresh(row)

    return get_branding(session)
