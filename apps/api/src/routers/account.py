"""Account management endpoints: profile, password, API keys, usage."""
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from pydantic import BaseModel, EmailStr
import uuid
import hashlib
import secrets
from datetime import datetime
from ..db import get_db
from ..models import User, ApiKey
from ..auth.password import hash_password, verify_password
from ..auth.dependencies import get_current_user
from ..config import settings

router = APIRouter(prefix="/api/v4/account", tags=["account"])


# Pydantic models
class ProfileUpdateRequest(BaseModel):
    full_name: str
    email: EmailStr


class PasswordChangeRequest(BaseModel):
    current_password: str
    new_password: str


class ApiKeyCreateRequest(BaseModel):
    name: str


class ApiKeyResponse(BaseModel):
    id: str
    name: str
    key_prefix: str
    last_used_at: str = None
    created_at: str

    class Config:
        from_attributes = True


class ApiKeyWithSecret(BaseModel):
    id: str
    name: str
    key: str  # Raw key (shown only once)
    key_prefix: str
    created_at: str


class UsageResponse(BaseModel):
    plan: str
    databases_used: int
    databases_limit: int
    migrations_used_this_month: int
    migrations_limit: int
    llm_conversions_this_month: int
    llm_conversions_limit: int
    trial_expires_at: str = None


def get_plan_limits(plan: str) -> dict:
    """Get limits for a plan tier."""
    limits = {
        "trial": {"databases": 1, "migrations_per_month": 3, "llm_per_month": 10},
        "starter": {"databases": 5, "migrations_per_month": 25, "llm_per_month": 100},
        "professional": {"databases": 20, "migrations_per_month": 100, "llm_per_month": 500},
        "enterprise": {"databases": None, "migrations_per_month": None, "llm_per_month": None},
    }
    return limits.get(plan, limits["trial"])


@router.put("/profile")
async def update_profile(
    request: ProfileUpdateRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Update user profile (name and email)."""
    # Check if new email already exists (if changed)
    if request.email != current_user.email:
        existing = db.query(User).filter(User.email == request.email).first()
        if existing:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Email already in use",
            )
        # Mark email as unverified if changed
        current_user.email_verified = False

    current_user.full_name = request.full_name
    current_user.email = request.email
    db.commit()

    return {"message": "Profile updated successfully"}


@router.put("/password")
async def change_password(
    request: PasswordChangeRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Change user password."""
    if not verify_password(request.current_password, current_user.hashed_password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Current password is incorrect",
        )

    current_user.hashed_password = hash_password(request.new_password)
    db.commit()

    return {"message": "Password changed successfully"}


@router.delete("")
async def delete_account(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Soft-delete user account (mark inactive)."""
    current_user.is_active = False
    db.commit()

    return {"message": "Account deleted successfully. All your data will be retained for 30 days."}


@router.get("/api-keys", response_model=list[ApiKeyResponse])
async def list_api_keys(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """List all API keys for current user."""
    keys = db.query(ApiKey).filter(ApiKey.user_id == current_user.id, ApiKey.is_active == True).all()
    return [
        ApiKeyResponse(
            id=str(key.id),
            name=key.name,
            key_prefix=key.key_prefix,
            last_used_at=key.last_used_at.isoformat() if key.last_used_at else None,
            created_at=key.created_at.isoformat(),
        )
        for key in keys
    ]


@router.post("/api-keys", response_model=ApiKeyWithSecret)
async def create_api_key(
    request: ApiKeyCreateRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Create a new API key (raw key shown only once)."""
    # Generate raw key
    raw_key = f"dp_{secrets.token_urlsafe(32)}"
    key_prefix = raw_key[:8]
    key_hash = hashlib.sha256(raw_key.encode()).hexdigest()

    api_key = ApiKey(
        id=uuid.uuid4(),
        user_id=current_user.id,
        name=request.name,
        key_hash=key_hash,
        key_prefix=key_prefix,
    )

    db.add(api_key)
    db.commit()
    db.refresh(api_key)

    return ApiKeyWithSecret(
        id=str(api_key.id),
        name=api_key.name,
        key=raw_key,  # Only returned here
        key_prefix=key_prefix,
        created_at=api_key.created_at.isoformat(),
    )


@router.delete("/api-keys/{key_id}")
async def revoke_api_key(
    key_id: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Revoke an API key."""
    api_key = db.query(ApiKey).filter(
        ApiKey.id == key_id,
        ApiKey.user_id == current_user.id,
    ).first()

    if not api_key:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="API key not found",
        )

    api_key.is_active = False
    db.commit()

    return {"message": "API key revoked successfully"}


@router.get("/usage", response_model=UsageResponse)
async def get_usage(
    current_user: User = Depends(get_current_user),
):
    """Get current month usage vs plan limits."""
    limits = get_plan_limits(current_user.plan.value)

    return UsageResponse(
        plan=current_user.plan.value,
        databases_used=current_user.databases_used,
        databases_limit=limits["databases"],
        migrations_used_this_month=current_user.migrations_used_this_month,
        migrations_limit=limits["migrations_per_month"],
        llm_conversions_this_month=current_user.llm_conversions_this_month,
        llm_conversions_limit=limits["llm_per_month"],
        trial_expires_at=current_user.trial_expires_at.isoformat() if current_user.plan.value == "trial" else None,
    )
