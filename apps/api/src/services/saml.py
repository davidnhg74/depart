"""SAML 2.0 helpers that wrap python3-saml.

python3-saml wants its config as a Python dict keyed on the operator's
configured IdP + "us, the service provider". We build that dict once
per request from the IdentityProvider row + the current Request's URL
so self-hosted installs behind any host / reverse proxy get the right
entity id and ACS URL without extra config.
"""

from __future__ import annotations

import re
from typing import Optional

from fastapi import Request

from ..models import IdentityProvider


def _sp_base_url(request: Request) -> str:
    """Absolute base URL of this install's public face. Honors reverse
    proxy headers the same way src/routers/sso.py does for OIDC."""
    scheme = request.headers.get("x-forwarded-proto") or request.url.scheme
    host = request.headers.get("x-forwarded-host") or request.url.netloc
    return f"{scheme}://{host}"


def build_saml_settings(idp: IdentityProvider, request: Request) -> dict:
    """Construct the nested dict python3-saml's settings loader expects.

    Our SP identity is our own URL; the IdP side is what the admin
    configured in /settings/sso. We sign nothing and encrypt nothing —
    signing a SAML response from the IdP is how we validate it, but we
    don't need to sign our AuthnRequests for most IdPs. If a specific
    IdP requires signed requests we can surface a UI flag later.
    """
    base = _sp_base_url(request)
    return {
        "strict": True,
        "debug": False,
        "sp": {
            # Our entity id is the ACS URL itself — a common pattern
            # when a SP doesn't have a dedicated identifier.
            "entityId": f"{base}/api/v1/auth/saml/metadata",
            "assertionConsumerService": {
                "url": f"{base}/api/v1/auth/saml/acs",
                "binding": "urn:oasis:names:tc:SAML:2.0:bindings:HTTP-POST",
            },
            "NameIDFormat": (
                "urn:oasis:names:tc:SAML:1.1:nameid-format:emailAddress"
            ),
            "x509cert": "",
            "privateKey": "",
        },
        "idp": {
            "entityId": idp.saml_entity_id or "",
            "singleSignOnService": {
                "url": idp.saml_sso_url or "",
                "binding": "urn:oasis:names:tc:SAML:2.0:bindings:HTTP-Redirect",
            },
            "x509cert": _clean_cert(idp.saml_x509_cert or ""),
        },
    }


def _clean_cert(pem: str) -> str:
    """Strip BEGIN/END markers + whitespace so python3-saml accepts a
    PEM-formatted cert the admin pasted in. The library wants just the
    base64 body."""
    if not pem:
        return ""
    body = re.sub(
        r"-----(?:BEGIN|END) CERTIFICATE-----", "", pem, flags=re.IGNORECASE
    )
    return "".join(body.split())


def is_saml_configured(idp: IdentityProvider) -> bool:
    """Fully configured = enabled + protocol='saml' + entity id + SSO
    URL + x509 cert."""
    return bool(
        idp.enabled
        and (idp.protocol or "oidc") == "saml"
        and idp.saml_entity_id
        and idp.saml_sso_url
        and idp.saml_x509_cert
    )


def request_to_saml_dict(request: Request, post_data: Optional[dict] = None) -> dict:
    """python3-saml expects a dict with http_host, script_name, etc.
    Build it from FastAPI's Request + the parsed form body (None for
    GET). For an HTTP-Redirect binding we'd pass query args via
    `get_data`; for HTTP-POST we use `post_data`."""
    scheme = request.headers.get("x-forwarded-proto") or request.url.scheme
    host = (
        request.headers.get("x-forwarded-host")
        or request.url.hostname
        or "localhost"
    )
    return {
        "https": "on" if scheme == "https" else "off",
        "http_host": host,
        "script_name": request.url.path,
        "get_data": dict(request.query_params),
        "post_data": post_data or {},
        # python3-saml validates this as a list — no need to fill it
        # unless the IdP requests a specific server port different
        # from the one in http_host.
        "server_port": str(
            (request.url.port or (443 if scheme == "https" else 80))
        ),
    }
