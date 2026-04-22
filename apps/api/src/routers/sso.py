"""SSO (OIDC) endpoints.

Two public handlers drive the login dance:

    GET  /api/v1/auth/sso/start     → 302 to the IdP authorize URL
    GET  /api/v1/auth/sso/callback  → exchanges code, mints our JWT,
                                      302 to /assess with the token
                                      in a secure cookie

Plus admin-only configuration:

    GET  /api/v1/auth/sso            → public-safe status (is SSO on?)
    PUT  /api/v1/auth/sso/config     → admin-only; set issuer / client /
                                       secret / default_role
    POST /api/v1/auth/sso/test       → admin-only; fetch discovery doc
                                       and surface "it looks valid" vs
                                       a specific error

The callback intentionally does NOT require a CSRF state check
persisted to session storage in v1 — self-hosted runs don't use
server-side sessions, and the inline state cookie we round-trip gives
us the same protection. Tighten to a database-backed nonce table if
we ever need to defend against an attacker with cookie-write access.
"""

from __future__ import annotations

import logging
import secrets
from typing import Optional

import httpx
from authlib.integrations.httpx_client import AsyncOAuth2Client
from fastapi import (
    APIRouter,
    Depends,
    HTTPException,
    Query,
    Request,
    status,
)
from fastapi.responses import RedirectResponse, Response
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from ..auth.jwt import create_access_token, create_refresh_token
from ..auth.password import hash_password
from ..auth.roles import require_role
from ..config import settings
from ..db import get_db
from ..models import User, UserRole
from ..services.audit import log_event
from ..services.saml import (
    build_saml_settings,
    is_saml_configured,
    request_to_saml_dict,
)
from ..services.sso import (
    discover_endpoints,
    get_idp,
    is_configured,
    update_idp,
)
from ..utils.time import utc_now


logger = logging.getLogger(__name__)


router = APIRouter(prefix="/api/v1/auth/sso", tags=["sso"])


# ─── Schemas ─────────────────────────────────────────────────────────────────


class SsoStatusPublic(BaseModel):
    """What anonymous callers see. The /login page uses this to decide
    which 'Log in with SSO' button to render (OIDC vs SAML) — the
    protocol field is safe to leak since the /settings/sso admin UI
    already exposes it. No issuer URL, no client id — those are only
    useful to an admin."""

    enabled: bool
    protocol: Optional[str] = None  # "oidc" | "saml" when enabled


class SsoStatusAdmin(BaseModel):
    enabled: bool
    protocol: str  # "oidc" or "saml" — always populated
    default_role: str
    auto_provision: bool
    # OIDC
    issuer: Optional[str]
    client_id: Optional[str]
    client_secret_set: bool
    # SAML
    saml_entity_id: Optional[str]
    saml_sso_url: Optional[str]
    saml_x509_cert_set: bool


class SsoConfigUpdate(BaseModel):
    enabled: Optional[bool] = None
    protocol: Optional[str] = Field(default=None, pattern="^(oidc|saml)$")
    default_role: Optional[str] = Field(default=None)
    auto_provision: Optional[bool] = None
    # OIDC fields
    issuer: Optional[str] = Field(default=None, max_length=500)
    client_id: Optional[str] = Field(default=None, max_length=255)
    # Empty string = leave unchanged; explicit None = unchanged too.
    client_secret: Optional[str] = Field(default=None, max_length=1000)
    # SAML fields
    saml_entity_id: Optional[str] = Field(default=None, max_length=500)
    saml_sso_url: Optional[str] = Field(default=None, max_length=500)
    # Multi-line PEM. Empty string = leave unchanged.
    saml_x509_cert: Optional[str] = Field(default=None, max_length=10_000)


# ─── Public status ───────────────────────────────────────────────────────────


@router.get("", response_model=SsoStatusPublic)
def sso_status_public(db: Session = Depends(get_db)) -> SsoStatusPublic:
    idp = get_idp(db)
    enabled = is_configured(idp)
    return SsoStatusPublic(
        enabled=enabled,
        protocol=(idp.protocol or "oidc") if enabled else None,
    )


# ─── Admin config ────────────────────────────────────────────────────────────


def _admin_status(idp) -> SsoStatusAdmin:
    return SsoStatusAdmin(
        enabled=bool(idp.enabled),
        protocol=(idp.protocol or "oidc"),
        default_role=idp.default_role.value,
        auto_provision=bool(idp.auto_provision),
        issuer=idp.issuer,
        client_id=idp.client_id,
        client_secret_set=bool(idp.client_secret),
        saml_entity_id=idp.saml_entity_id,
        saml_sso_url=idp.saml_sso_url,
        saml_x509_cert_set=bool(idp.saml_x509_cert),
    )


@router.get("/config", response_model=SsoStatusAdmin)
def sso_config_get(
    db: Session = Depends(get_db),
    _admin=Depends(require_role("admin")),
) -> SsoStatusAdmin:
    return _admin_status(get_idp(db))


@router.put("/config", response_model=SsoStatusAdmin)
def sso_config_put(
    body: SsoConfigUpdate,
    request: Request,
    db: Session = Depends(get_db),
    admin=Depends(require_role("admin")),
) -> SsoStatusAdmin:
    row = update_idp(
        db,
        enabled=body.enabled,
        protocol=body.protocol,
        default_role=body.default_role,
        auto_provision=body.auto_provision,
        issuer=body.issuer,
        client_id=body.client_id,
        client_secret=body.client_secret,
        saml_entity_id=body.saml_entity_id,
        saml_sso_url=body.saml_sso_url,
        saml_x509_cert=body.saml_x509_cert,
    )
    log_event(
        db,
        request=request,
        user=admin,
        action="sso.configured",
        details={
            "enabled": bool(row.enabled),
            "protocol": row.protocol or "oidc",
            "issuer": row.issuer,
            "client_id_set": bool(row.client_id),
            "client_secret_set": bool(row.client_secret),
            "saml_entity_id": row.saml_entity_id,
            "saml_sso_url": row.saml_sso_url,
            "saml_x509_cert_set": bool(row.saml_x509_cert),
        },
    )
    return _admin_status(row)


@router.post("/test")
async def sso_test(
    db: Session = Depends(get_db),
    _admin=Depends(require_role("admin")),
) -> dict:
    """Admin helper — fetch the discovery doc so misconfigured issuers
    surface immediately in the UI rather than at first login."""
    idp = get_idp(db)
    if not idp.issuer:
        raise HTTPException(status_code=400, detail="issuer is not set")
    try:
        meta = await discover_endpoints(idp.issuer)
    except httpx.HTTPError as exc:
        raise HTTPException(
            status_code=502,
            detail=f"discovery failed: {exc}",
        )
    return {
        "ok": True,
        "authorization_endpoint": meta.get("authorization_endpoint"),
        "token_endpoint": meta.get("token_endpoint"),
        "userinfo_endpoint": meta.get("userinfo_endpoint"),
        "issuer": meta.get("issuer"),
    }


# ─── Login dance ─────────────────────────────────────────────────────────────


@router.get("/start")
async def sso_start(
    request: Request,
    db: Session = Depends(get_db),
    next: str = Query(default="/assess"),
) -> RedirectResponse:
    """Step 1 of the authorization-code flow. Builds the authorize URL
    from the discovered IdP metadata and redirects the browser to it.
    Stashes a short-lived `sso_state` cookie with the CSRF token + the
    caller's post-login destination."""
    idp = get_idp(db)
    if not is_configured(idp):
        raise HTTPException(status_code=404, detail="SSO is not configured")
    meta = await discover_endpoints(idp.issuer)
    state = secrets.token_urlsafe(24)

    redirect_uri = _callback_url(request)
    client = AsyncOAuth2Client(
        client_id=idp.client_id,
        client_secret=idp.client_secret,
        scope="openid email profile",
        redirect_uri=redirect_uri,
    )
    authorize_url, _ = client.create_authorization_url(
        meta["authorization_endpoint"], state=state
    )

    resp = RedirectResponse(authorize_url, status_code=302)
    # 10-minute window to complete the flow — enough for MFA prompts,
    # not enough for stale bookmarks.
    resp.set_cookie(
        "sso_state",
        f"{state}|{next}",
        max_age=600,
        httponly=True,
        samesite="lax",
        secure=settings.environment != "development",
    )
    return resp


@router.get("/callback")
async def sso_callback(
    request: Request,
    code: Optional[str] = None,
    state: Optional[str] = None,
    error: Optional[str] = None,
    db: Session = Depends(get_db),
) -> RedirectResponse:
    """Step 2 — IdP redirected back here with `code` + `state`. Validate
    state against the cookie, exchange code for tokens, fetch userinfo,
    match-or-provision the local user, mint our JWT, redirect to /assess
    (or the `next` path that /start stashed)."""
    if error:
        _fail_redirect(request, error)

    raw = request.cookies.get("sso_state")
    if not raw or not state or "|" not in raw:
        _fail_redirect(request, "missing_state")
    expected_state, next_path = raw.split("|", 1)
    if state != expected_state:
        _fail_redirect(request, "state_mismatch")
    if not code:
        _fail_redirect(request, "missing_code")

    idp = get_idp(db)
    if not is_configured(idp):
        _fail_redirect(request, "sso_not_configured")

    meta = await discover_endpoints(idp.issuer)
    redirect_uri = _callback_url(request)
    client = AsyncOAuth2Client(
        client_id=idp.client_id,
        client_secret=idp.client_secret,
        scope="openid email profile",
        redirect_uri=redirect_uri,
    )

    try:
        token = await client.fetch_token(
            meta["token_endpoint"],
            code=code,
            grant_type="authorization_code",
        )
    except Exception as exc:  # noqa: BLE001
        logger.exception("SSO token exchange failed")
        _fail_redirect(request, f"token_exchange_failed:{type(exc).__name__}")

    try:
        userinfo_resp = await client.get(meta["userinfo_endpoint"], token=token)
        userinfo_resp.raise_for_status()
        userinfo = userinfo_resp.json()
    except Exception as exc:  # noqa: BLE001
        logger.exception("SSO userinfo fetch failed")
        _fail_redirect(request, f"userinfo_failed:{type(exc).__name__}")
    finally:
        await client.aclose()

    email = userinfo.get("email")
    if not email:
        _fail_redirect(request, "no_email_in_userinfo")

    user = _match_or_provision(db, idp, email=email, userinfo=userinfo)

    if not user.is_active:
        log_event(
            db,
            request=request,
            user=user,
            action="user.login_failed",
            details={"reason": "sso_deactivated"},
        )
        _fail_redirect(request, "user_deactivated")

    log_event(
        db,
        request=request,
        user=user,
        action="user.login",
        details={"via": "sso"},
    )

    access = create_access_token({"sub": str(user.id)})
    refresh = create_refresh_token({"sub": str(user.id)})

    resp = RedirectResponse(next_path or "/assess", status_code=302)
    # Clear the temp state cookie and set the session tokens. The web
    # app's AuthInitializer reads access_token on mount, calls /me, and
    # hydrates the auth store.
    resp.delete_cookie("sso_state")
    resp.set_cookie(
        "access_token",
        access,
        httponly=False,  # Zustand store reads this from document.cookie
        samesite="lax",
        secure=settings.environment != "development",
    )
    resp.set_cookie(
        "refresh_token",
        refresh,
        httponly=False,
        samesite="lax",
        secure=settings.environment != "development",
    )
    return resp


# ─── Internals ───────────────────────────────────────────────────────────────


def _callback_url(request: Request) -> str:
    """Compute the absolute callback URL the IdP should redirect to.

    We honor X-Forwarded-Host / X-Forwarded-Proto so depart behind a
    reverse proxy builds the right URL without the operator having to
    configure it separately."""
    scheme = request.headers.get("x-forwarded-proto") or request.url.scheme
    host = request.headers.get("x-forwarded-host") or request.url.netloc
    return f"{scheme}://{host}/api/v1/auth/sso/callback"


def _match_or_provision(
    db: Session,
    idp,
    *,
    email: str,
    userinfo: dict,
) -> User:
    """Return the existing User row for `email`, or auto-provision a
    new one when the IdP is configured to allow it. SSO never mints
    admins — the default_role is capped to operator / viewer in the
    config."""
    existing = db.query(User).filter(User.email == email).first()
    if existing:
        return existing

    if not idp.auto_provision:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=(
                "SSO user has no local account and auto-provision is "
                "disabled. Ask an admin to create an account first."
            ),
        )

    now = utc_now()
    # We set a random password the SSO user never sees — it keeps the
    # NOT NULL constraint satisfied and a future password-login attempt
    # just fails silently (no one knows the 32-byte random).
    placeholder_pw = hash_password(secrets.token_urlsafe(24))
    full_name = userinfo.get("name") or userinfo.get("preferred_username")
    role = idp.default_role
    # Defense in depth: refuse to auto-provision as admin even if the
    # DB somehow contains that value.
    if role == UserRole.ADMIN:
        role = UserRole.OPERATOR

    user = User(
        email=email,
        full_name=full_name,
        hashed_password=placeholder_pw,
        role=role,
        email_verified=True,
        is_active=True,
        trial_starts_at=now,
        trial_expires_at=now,
    )
    db.add(user)
    db.commit()
    db.refresh(user)
    return user


def _fail_redirect(request: Request, reason: str) -> RedirectResponse:
    """Bounce to /login with an ?error= param. We raise as an
    HTTPException so the handler's control flow stays linear, rather
    than trying to return a RedirectResponse from deep inside the
    token exchange."""
    raise HTTPException(
        status_code=302,
        headers={"Location": f"/login?error=sso_{reason}"},
    )


# ─── SAML endpoints ──────────────────────────────────────────────────────────
#
# python3-saml drives the AuthnRequest construction and the response /
# assertion validation. We do very light plumbing here: build its
# settings dict from the stored IdentityProvider row, hand it the
# request, ask it what to do next.


def _saml_auth(request: Request, post_data=None):
    """Construct the python3-saml auth helper tied to the current
    request. Imported lazily because the SAML stack has a heavy native
    dep (libxmlsec1) that isn't worth loading for OIDC-only installs."""
    from onelogin.saml2.auth import OneLogin_Saml2_Auth

    from ..services.sso import get_idp

    idp = get_idp(_db_from_request(request))
    if not is_saml_configured(idp):
        raise HTTPException(status_code=404, detail="SAML SSO is not configured")
    settings = build_saml_settings(idp, request)
    req_dict = request_to_saml_dict(request, post_data=post_data)
    return OneLogin_Saml2_Auth(req_dict, settings), idp


def _db_from_request(request: Request):
    """Open a short-lived Session for the python3-saml helper. The
    SAML handlers below already take a Session via Depends, but the
    helper factory needs one too (to reload the IdP row)."""
    from ..db import get_session_factory

    return get_session_factory()()


@router.get("/saml/metadata")
async def saml_metadata(request: Request) -> Response:
    """Serve our SP metadata so admins can paste the URL into their
    IdP's trust configuration (Okta, Entra ID, etc). The IdP uses this
    to learn our entity ID and ACS URL."""
    from onelogin.saml2.settings import OneLogin_Saml2_Settings

    from ..services.sso import get_idp

    db = _db_from_request(request)
    try:
        idp = get_idp(db)
        settings_dict = build_saml_settings(idp, request)
        saml_settings = OneLogin_Saml2_Settings(settings_dict, sp_validation_only=True)
        metadata = saml_settings.get_sp_metadata()
        errors = saml_settings.validate_metadata(metadata)
        if errors:
            raise HTTPException(
                status_code=500, detail=f"SAML metadata invalid: {', '.join(errors)}"
            )
        return Response(content=metadata, media_type="text/xml")
    finally:
        db.close()


@router.get("/saml/login")
async def saml_login(request: Request) -> RedirectResponse:
    """Step 1 — redirect to the IdP with a freshly built AuthnRequest."""
    auth, _idp = _saml_auth(request)
    authorize_url = auth.login()
    return RedirectResponse(authorize_url, status_code=302)


@router.post("/saml/acs")
async def saml_acs(request: Request) -> RedirectResponse:
    """Assertion Consumer Service — IdP POSTs the SAML response here.

    We parse + validate the response (signature, audience, notBefore/
    notOnOrAfter), extract the email from NameID or a matching
    attribute, then match-or-provision the local user and mint our
    session JWTs the same way as the OIDC callback."""
    form = dict((await request.form()).items())
    auth, idp = _saml_auth(request, post_data=form)

    auth.process_response()
    errors = auth.get_errors()
    if errors:
        last = auth.get_last_error_reason() or ""
        logger.warning("SAML response errors: %s (%s)", errors, last)
        _fail_redirect(request, "saml_response_invalid")
    if not auth.is_authenticated():
        _fail_redirect(request, "saml_not_authenticated")

    email = auth.get_nameid()
    attrs = auth.get_attributes() or {}
    # Fall back to common attribute names if the NameID isn't an
    # email (some IdPs use persistent IDs and put the email under
    # http://schemas.xmlsoap.org/ws/2005/05/identity/claims/emailaddress).
    if not email or "@" not in email:
        for key in (
            "email",
            "Email",
            "mail",
            "http://schemas.xmlsoap.org/ws/2005/05/identity/claims/emailaddress",
        ):
            v = attrs.get(key)
            if v:
                email = v[0] if isinstance(v, list) else v
                break
    if not email or "@" not in email:
        _fail_redirect(request, "saml_no_email")

    # match_or_provision needs a Session. Use the background-task-style
    # factory since the request's DB dep wasn't injected for this path.
    db = _db_from_request(request)
    try:
        userinfo = {"email": email, "name": _saml_display_name(attrs)}
        user = _match_or_provision(db, idp, email=email, userinfo=userinfo)
        if not user.is_active:
            log_event(
                db,
                request=request,
                user=user,
                action="user.login_failed",
                details={"reason": "saml_deactivated"},
            )
            _fail_redirect(request, "user_deactivated")

        log_event(
            db,
            request=request,
            user=user,
            action="user.login",
            details={"via": "saml"},
        )

        access = create_access_token({"sub": str(user.id)})
        refresh = create_refresh_token({"sub": str(user.id)})
    finally:
        db.close()

    # RelayState carries the caller's post-login destination. Default
    # to /assess if missing / malformed.
    relay = form.get("RelayState") or "/assess"
    if not relay.startswith("/"):
        relay = "/assess"

    resp = RedirectResponse(relay, status_code=302)
    resp.set_cookie(
        "access_token",
        access,
        httponly=False,
        samesite="lax",
        secure=settings.environment != "development",
    )
    resp.set_cookie(
        "refresh_token",
        refresh,
        httponly=False,
        samesite="lax",
        secure=settings.environment != "development",
    )
    return resp


def _saml_display_name(attrs: dict) -> Optional[str]:
    """Extract a display-name-ish string from SAML attributes."""
    for key in (
        "name",
        "displayName",
        "http://schemas.xmlsoap.org/ws/2005/05/identity/claims/name",
    ):
        v = attrs.get(key)
        if v:
            return v[0] if isinstance(v, list) else v
    first = attrs.get("givenName") or attrs.get(
        "http://schemas.xmlsoap.org/ws/2005/05/identity/claims/givenname"
    )
    last = attrs.get("sn") or attrs.get(
        "http://schemas.xmlsoap.org/ws/2005/05/identity/claims/surname"
    )
    if first and last:
        return f"{first[0] if isinstance(first, list) else first} {last[0] if isinstance(last, list) else last}"
    return None
