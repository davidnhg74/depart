"""Self-hosted authentication router.

Parallel to `src/routers/auth.py` (the cloud signup / email-verify /
password-reset flows), this router carries the endpoints that every
self-hosted install needs regardless of SaaS features: login, logout,
who-am-I, change-password, and admin-gated user CRUD.

The cloud auth router is at /api/v4/auth/*; this one lives at
/api/v1/auth/* so the two coexist without collision when the cloud
flag is also on. (Self-hosted buildings leave the cloud router off.)

No signup endpoint here — admins create users via POST /users. The
only path to the first admin is the /api/v1/setup/bootstrap call.
"""

from __future__ import annotations

from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Request, status
from pydantic import BaseModel, ConfigDict, EmailStr, Field
from sqlalchemy.orm import Session

from ..auth.dependencies import get_current_user
from ..auth.jwt import create_access_token, create_refresh_token
from ..auth.password import hash_password, verify_password
from ..auth.roles import require_role
from ..db import get_db
from ..models import User, UserRole
from ..services.audit import log_event
from ..utils.time import utc_now


router = APIRouter(prefix="/api/v1/auth", tags=["auth"])


# ─── Schemas ─────────────────────────────────────────────────────────────────


class LoginRequest(BaseModel):
    email: EmailStr
    password: str = Field(..., min_length=1, max_length=200)


class TokenResponse(BaseModel):
    access_token: str
    refresh_token: str
    token_type: str = "bearer"


class UserResponse(BaseModel):
    id: str
    email: str
    full_name: Optional[str]
    role: str
    is_active: bool

    model_config = ConfigDict(from_attributes=True)


class ChangePasswordRequest(BaseModel):
    current_password: str = Field(..., min_length=1, max_length=200)
    new_password: str = Field(..., min_length=8, max_length=200)


class UserCreateRequest(BaseModel):
    email: EmailStr
    password: str = Field(..., min_length=8, max_length=200)
    full_name: Optional[str] = Field(default=None, max_length=200)
    role: str = "operator"  # validated against UserRole below


class UserUpdateRequest(BaseModel):
    full_name: Optional[str] = Field(default=None, max_length=200)
    role: Optional[str] = None
    is_active: Optional[bool] = None


def _to_response(user: User) -> UserResponse:
    return UserResponse(
        id=str(user.id),
        email=user.email,
        full_name=user.full_name,
        role=user.role.value,
        is_active=user.is_active,
    )


# ─── Login / logout / me ─────────────────────────────────────────────────────


@router.post("/login", response_model=TokenResponse)
def login(
    body: LoginRequest,
    request: Request,
    db: Session = Depends(get_db),
) -> TokenResponse:
    """Email + password → JWT access + refresh tokens.

    Same JWT plumbing the cloud auth router uses, just without the
    email-verification gate (self-hosted users are created by an
    admin — they're already verified by virtue of existing)."""
    user = db.query(User).filter(User.email == body.email).first()
    if not user or not verify_password(body.password, user.hashed_password):
        log_event(
            db,
            request=request,
            user=None,
            action="user.login_failed",
            user_email_override=body.email,
            details={"reason": "bad_credentials"},
        )
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="invalid email or password",
        )
    if not user.is_active:
        log_event(
            db,
            request=request,
            user=user,
            action="user.login_failed",
            details={"reason": "deactivated"},
        )
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="user account is deactivated",
        )
    access = create_access_token({"sub": str(user.id)})
    refresh = create_refresh_token({"sub": str(user.id)})
    log_event(db, request=request, user=user, action="user.login")
    return TokenResponse(access_token=access, refresh_token=refresh)


@router.post("/logout", status_code=status.HTTP_204_NO_CONTENT)
def logout() -> None:
    """Stateless JWT — nothing to invalidate server-side. The UI drops
    the token from its store and we're done. Returning 204 here keeps
    the client API flow symmetric with login."""
    return None


@router.get("/me", response_model=UserResponse)
def me(user: User = Depends(get_current_user)) -> UserResponse:
    return _to_response(user)


@router.post("/password", status_code=status.HTTP_204_NO_CONTENT)
def change_password(
    body: ChangePasswordRequest,
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user),
) -> None:
    if not verify_password(body.current_password, user.hashed_password):
        log_event(
            db,
            request=request,
            user=user,
            action="user.password_change_failed",
            details={"reason": "bad_current_password"},
        )
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="current password is incorrect",
        )
    user.hashed_password = hash_password(body.new_password)
    db.commit()
    log_event(db, request=request, user=user, action="user.password_changed")
    return None


# ─── Admin user management ───────────────────────────────────────────────────


@router.get("/users", response_model=List[UserResponse])
def list_users(
    db: Session = Depends(get_db),
    _admin: User = Depends(require_role("admin")),
) -> List[UserResponse]:
    rows = db.query(User).order_by(User.email).all()
    return [_to_response(u) for u in rows]


@router.post("/users", response_model=UserResponse, status_code=status.HTTP_201_CREATED)
def create_user(
    body: UserCreateRequest,
    request: Request,
    db: Session = Depends(get_db),
    admin: User = Depends(require_role("admin")),
) -> UserResponse:
    try:
        role = UserRole(body.role)
    except ValueError:
        raise HTTPException(status_code=422, detail=f"unknown role: {body.role!r}")

    if db.query(User).filter(User.email == body.email).first():
        raise HTTPException(status_code=409, detail="email already in use")

    now = utc_now()
    u = User(
        email=body.email,
        full_name=body.full_name,
        hashed_password=hash_password(body.password),
        role=role,
        email_verified=True,
        is_active=True,
        trial_starts_at=now,
        trial_expires_at=now,
    )
    db.add(u)
    db.commit()
    db.refresh(u)
    log_event(
        db,
        request=request,
        user=admin,
        action="user.created",
        resource_type="user",
        resource_id=str(u.id),
        details={"email": u.email, "role": role.value},
    )
    return _to_response(u)


@router.patch("/users/{user_id}", response_model=UserResponse)
def update_user(
    user_id: str,
    body: UserUpdateRequest,
    request: Request,
    db: Session = Depends(get_db),
    admin: User = Depends(require_role("admin")),
) -> UserResponse:
    import uuid as _uuid

    try:
        uid = _uuid.UUID(user_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="invalid user id")

    target = db.query(User).filter(User.id == uid).first()
    if target is None:
        raise HTTPException(status_code=404, detail="user not found")

    if body.full_name is not None:
        target.full_name = body.full_name

    if body.role is not None:
        try:
            new_role = UserRole(body.role)
        except ValueError:
            raise HTTPException(status_code=422, detail=f"unknown role: {body.role!r}")
        # Guard: don't let the last admin demote themselves and lock
        # everyone out.
        if target.id == admin.id and new_role != UserRole.ADMIN:
            admin_count = db.query(User).filter(User.role == UserRole.ADMIN).count()
            if admin_count <= 1:
                raise HTTPException(
                    status_code=400,
                    detail="cannot demote the last admin",
                )
        target.role = new_role

    if body.is_active is not None:
        # Same guard for deactivating the last active admin.
        if target.role == UserRole.ADMIN and body.is_active is False:
            active_admins = (
                db.query(User)
                .filter(User.role == UserRole.ADMIN, User.is_active.is_(True))
                .count()
            )
            if active_admins <= 1:
                raise HTTPException(
                    status_code=400,
                    detail="cannot deactivate the last active admin",
                )
        target.is_active = body.is_active

    db.commit()
    db.refresh(target)

    changed = {}
    if body.full_name is not None:
        changed["full_name"] = body.full_name
    if body.role is not None:
        changed["role"] = target.role.value
    if body.is_active is not None:
        changed["is_active"] = target.is_active
    log_event(
        db,
        request=request,
        user=admin,
        action="user.updated",
        resource_type="user",
        resource_id=str(target.id),
        details={"email": target.email, "changed": changed},
    )
    return _to_response(target)


@router.delete("/users/{user_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_user(
    user_id: str,
    request: Request,
    db: Session = Depends(get_db),
    admin: User = Depends(require_role("admin")),
) -> None:
    import uuid as _uuid

    try:
        uid = _uuid.UUID(user_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="invalid user id")

    target = db.query(User).filter(User.id == uid).first()
    if target is None:
        raise HTTPException(status_code=404, detail="user not found")

    if target.id == admin.id:
        raise HTTPException(status_code=400, detail="cannot delete yourself")

    if target.role == UserRole.ADMIN:
        admin_count = db.query(User).filter(User.role == UserRole.ADMIN).count()
        if admin_count <= 1:
            raise HTTPException(
                status_code=400,
                detail="cannot delete the last admin",
            )

    deleted_email = target.email
    deleted_id = str(target.id)
    db.delete(target)
    db.commit()
    log_event(
        db,
        request=request,
        user=admin,
        action="user.deleted",
        resource_type="user",
        resource_id=deleted_id,
        details={"email": deleted_email},
    )
    return None
