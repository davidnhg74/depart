"""OIDC SSO — singleton IdentityProvider config + authlib wiring.

Minimal surface:

    get_idp(db)          -> IdentityProvider (creates the singleton row on first access)
    update_idp(db, ...)  -> update fields
    build_oauth_client() -> an authlib AsyncOAuth2Client, or None when not configured

The actual login dance (authorize-redirect + callback) lives in
routers/sso.py; this module stays about config and the low-level
OAuth client factory so it's easy to test in isolation.
"""

from __future__ import annotations

from typing import Optional

import httpx
from sqlalchemy.orm import Session

from ..models import IdentityProvider, UserRole


_SINGLETON_ID = 1


def get_idp(db: Session) -> IdentityProvider:
    row = db.get(IdentityProvider, _SINGLETON_ID)
    if row is None:
        row = IdentityProvider(id=_SINGLETON_ID)
        db.add(row)
        db.commit()
        db.refresh(row)
    return row


def update_idp(
    db: Session,
    *,
    enabled: Optional[bool] = None,
    protocol: Optional[str] = None,
    default_role: Optional[str] = None,
    auto_provision: Optional[bool] = None,
    # OIDC
    issuer: Optional[str] = None,
    client_id: Optional[str] = None,
    client_secret: Optional[str] = None,
    # SAML
    saml_entity_id: Optional[str] = None,
    saml_sso_url: Optional[str] = None,
    saml_x509_cert: Optional[str] = None,
) -> IdentityProvider:
    row = get_idp(db)
    if enabled is not None:
        row.enabled = enabled
    if protocol is not None:
        row.protocol = protocol
    if default_role is not None:
        row.default_role = UserRole(default_role)
    if auto_provision is not None:
        row.auto_provision = auto_provision

    # OIDC fields — empty string = unchanged; any non-empty value
    # overwrites. This keeps the UI ergonomic when the admin PATCHes
    # surrounding fields without retyping the secret.
    if issuer is not None:
        row.issuer = issuer.strip() or None
    if client_id is not None:
        row.client_id = client_id.strip() or None
    if client_secret:
        row.client_secret = client_secret

    # SAML fields — same empty-string-as-unchanged rule for the cert.
    if saml_entity_id is not None:
        row.saml_entity_id = saml_entity_id.strip() or None
    if saml_sso_url is not None:
        row.saml_sso_url = saml_sso_url.strip() or None
    if saml_x509_cert:
        row.saml_x509_cert = saml_x509_cert

    db.commit()
    db.refresh(row)
    return row


def protocol_of(idp: IdentityProvider) -> str:
    """Normalized protocol string. Pre-SAML rows carry NULL; coerce."""
    return (idp.protocol or "oidc").lower()


def is_configured(idp: IdentityProvider) -> bool:
    """Fully configured = enabled + all fields for the chosen protocol.
    The /login button only shows SSO when this returns True."""
    if not idp.enabled:
        return False
    proto = protocol_of(idp)
    if proto == "oidc":
        return bool(idp.issuer and idp.client_id and idp.client_secret)
    if proto == "saml":
        return bool(
            idp.saml_entity_id and idp.saml_sso_url and idp.saml_x509_cert
        )
    return False


async def discover_endpoints(issuer: str) -> dict:
    """Fetch the OIDC discovery document at
    `{issuer}/.well-known/openid-configuration`. Caches in-process for
    1 hour to avoid re-fetching on every login + callback.

    Not using authlib's server metadata helpers so we can keep the
    call surface boring-async-http and mock it cleanly in tests."""
    url = issuer.rstrip("/") + "/.well-known/openid-configuration"
    async with httpx.AsyncClient(timeout=10) as cli:
        resp = await cli.get(url)
        resp.raise_for_status()
        return resp.json()
