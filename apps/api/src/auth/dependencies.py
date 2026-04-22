"""FastAPI dependencies for authentication."""

import uuid
from typing import Optional
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer
from sqlalchemy.orm import Session
from ..db import get_db
from ..models import User
from .jwt import decode_token, verify_token_type


def _coerce_user_id(value: str) -> Optional[uuid.UUID]:
    """JWT carries the user id as a string; ORM `UUID(as_uuid=True)`
    columns expect a uuid.UUID. Returns None on malformed input so
    callers raise 401 instead of 500."""
    try:
        return uuid.UUID(value)
    except (ValueError, TypeError):
        return None


security = HTTPBearer()
security_optional = HTTPBearer(auto_error=False)


async def get_current_user(credentials=Depends(security), db: Session = Depends(get_db)) -> User:
    """Get the current authenticated user from JWT token."""
    token = credentials.credentials
    payload = decode_token(token)

    if not payload:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired token",
            headers={"WWW-Authenticate": "Bearer"},
        )

    if not verify_token_type(payload, "access"):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token type",
            headers={"WWW-Authenticate": "Bearer"},
        )

    user_id = _coerce_user_id(payload.get("sub", ""))
    if user_id is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token missing or malformed user ID",
            headers={"WWW-Authenticate": "Bearer"},
        )

    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User not found",
            headers={"WWW-Authenticate": "Bearer"},
        )

    if not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="User account is inactive",
        )

    return user


async def get_optional_user(
    db: Session = Depends(get_db), credentials=Depends(security_optional)
) -> Optional[User]:
    """Get the current user if authenticated, otherwise return None."""
    if not credentials:
        return None

    token = credentials.credentials
    payload = decode_token(token)

    if not payload or not verify_token_type(payload, "access"):
        return None

    user_id = _coerce_user_id(payload.get("sub", ""))
    if user_id is None:
        return None

    user = db.query(User).filter(User.id == user_id, User.is_active.is_(True)).first()
    return user
