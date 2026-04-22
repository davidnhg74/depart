"""Authentication endpoints: signup, login, logout, email verification, password reset."""
from datetime import datetime, timedelta
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from pydantic import BaseModel, EmailStr
import uuid
import secrets
from ..db import get_db
from ..models import User, PlanEnum
from ..auth.jwt import create_access_token, create_refresh_token
from ..auth.password import hash_password, verify_password
from ..auth.dependencies import get_current_user
from ..config import settings
from ..services.email import (
    send_verification_email,
    send_password_reset_email,
)

router = APIRouter(prefix="/api/v4/auth", tags=["auth"])


# Pydantic models for request/response
class SignupRequest(BaseModel):
    email: EmailStr
    full_name: str
    password: str


class LoginRequest(BaseModel):
    email: EmailStr
    password: str


class TokenResponse(BaseModel):
    access_token: str
    refresh_token: str
    token_type: str = "bearer"


class UserResponse(BaseModel):
    id: str
    email: str
    full_name: str
    plan: str
    email_verified: bool
    created_at: str

    class Config:
        from_attributes = True


class ForgotPasswordRequest(BaseModel):
    email: EmailStr


class ResetPasswordRequest(BaseModel):
    token: str
    password: str


class VerifyEmailRequest(BaseModel):
    token: str


@router.post("/signup", response_model=TokenResponse, status_code=status.HTTP_201_CREATED)
async def signup(request: SignupRequest, db: Session = Depends(get_db)):
    """Create a new user account and send verification email."""
    # Check if user already exists
    existing = db.query(User).filter(User.email == request.email).first()
    if existing:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Email already registered",
        )

    # Create user
    user = User(
        id=uuid.uuid4(),
        email=request.email,
        full_name=request.full_name,
        hashed_password=hash_password(request.password),
        plan=PlanEnum.TRIAL,
        trial_starts_at=datetime.utcnow(),
        trial_expires_at=datetime.utcnow() + timedelta(days=14),
        usage_reset_at=datetime.utcnow(),
    )

    # Generate email verification token
    user.email_verify_token = secrets.token_urlsafe(32)

    db.add(user)
    db.commit()
    db.refresh(user)

    # Send verification email
    send_verification_email(user.email, user.email_verify_token, settings.frontend_url)

    # Return tokens
    access_token = create_access_token({"sub": str(user.id)})
    refresh_token = create_refresh_token({"sub": str(user.id)})

    return TokenResponse(
        access_token=access_token,
        refresh_token=refresh_token,
    )


@router.post("/login", response_model=TokenResponse)
async def login(request: LoginRequest, db: Session = Depends(get_db)):
    """Authenticate user with email and password."""
    user = db.query(User).filter(User.email == request.email).first()

    if not user or not verify_password(request.password, user.hashed_password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid email or password",
        )

    if not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="User account is inactive",
        )

    access_token = create_access_token({"sub": str(user.id)})
    refresh_token = create_refresh_token({"sub": str(user.id)})

    return TokenResponse(
        access_token=access_token,
        refresh_token=refresh_token,
    )


@router.post("/logout")
async def logout(current_user: User = Depends(get_current_user)):
    """Logout (client should discard tokens)."""
    return {"message": "Logged out successfully"}


@router.get("/me", response_model=UserResponse)
async def get_current_user_info(current_user: User = Depends(get_current_user)):
    """Get current user information."""
    return UserResponse(
        id=str(current_user.id),
        email=current_user.email,
        full_name=current_user.full_name or "",
        plan=current_user.plan.value,
        email_verified=current_user.email_verified,
        created_at=current_user.created_at.isoformat(),
    )


@router.post("/refresh", response_model=TokenResponse)
async def refresh_access_token(current_user: User = Depends(get_current_user)):
    """Issue a new access token using refresh token (client sends refresh token as auth)."""
    access_token = create_access_token({"sub": str(current_user.id)})
    refresh_token = create_refresh_token({"sub": str(current_user.id)})

    return TokenResponse(
        access_token=access_token,
        refresh_token=refresh_token,
    )


@router.post("/verify-email")
async def verify_email(request: VerifyEmailRequest, db: Session = Depends(get_db)):
    """Verify user email with token."""
    user = db.query(User).filter(User.email_verify_token == request.token).first()

    if not user:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid or expired verification token",
        )

    user.email_verified = True
    user.email_verify_token = None
    db.commit()

    return {"message": "Email verified successfully"}


@router.post("/forgot-password")
async def forgot_password(request: ForgotPasswordRequest, db: Session = Depends(get_db)):
    """Request password reset email."""
    user = db.query(User).filter(User.email == request.email).first()

    if user:
        # Generate reset token (1 hour expiry)
        reset_token = secrets.token_urlsafe(32)
        user.reset_token = reset_token
        user.reset_token_expires = datetime.utcnow() + timedelta(hours=1)
        db.commit()

        # Send reset email
        send_password_reset_email(user.email, reset_token, settings.frontend_url)

    # Always return success (for security, don't reveal if email exists)
    return {"message": "If an account exists with that email, a reset link has been sent"}


@router.post("/reset-password")
async def reset_password(request: ResetPasswordRequest, db: Session = Depends(get_db)):
    """Reset password with token."""
    user = db.query(User).filter(User.reset_token == request.token).first()

    if not user or not user.reset_token_expires or user.reset_token_expires < datetime.utcnow():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid or expired reset token",
        )

    user.hashed_password = hash_password(request.password)
    user.reset_token = None
    user.reset_token_expires = None
    db.commit()

    return {"message": "Password reset successfully. Please log in with your new password."}
