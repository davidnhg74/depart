"""Role-based authorization dependencies.

Sits on top of the existing `get_current_user` so every mutating
route just declares who's allowed:

    @router.post(...)
    def do_thing(
        user: User = Depends(require_role("admin", "operator"))
    ):
        ...

`require_role("admin")` → only admins
`require_role("admin", "operator")` → admins or operators
`require_auth()` → any authenticated user (shorthand for all roles)
"""

from __future__ import annotations

from typing import Callable, Optional

from fastapi import Depends, HTTPException, status

from ..config import settings
from ..models import User, UserRole
from .dependencies import get_optional_user


def require_role(*roles: str) -> Callable[..., Optional[User]]:
    """Return a FastAPI dependency that requires the caller's role to
    be in `roles`.

    Behavior depends on `settings.enable_self_hosted_auth` evaluated
    at *request time* (not import time) so tests can flip the flag
    between cases:

      * auth OFF → returns None; the route runs unguarded. Single-user
        dev boxes and the existing test suite rely on this.
      * auth ON + no token → 401
      * auth ON + wrong role → 403
      * auth ON + right role → returns the authenticated User

    The string form of roles (not UserRole enum) keeps route
    declarations compact:

        Depends(require_role("admin", "operator"))
    """
    allowed = {UserRole(r) for r in roles}

    def dep(user: Optional[User] = Depends(get_optional_user)) -> Optional[User]:
        if not settings.enable_self_hosted_auth:
            return None
        if user is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="authentication required",
                headers={"WWW-Authenticate": "Bearer"},
            )
        if user.role not in allowed:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail={
                    "error": "insufficient_role",
                    "required": sorted(r.value for r in allowed),
                    "current": user.role.value,
                },
            )
        return user

    return dep


def require_auth() -> Callable[..., Optional[User]]:
    """Any authenticated user. When auth is disabled this is a no-op
    and the route runs unguarded."""

    def dep(user: Optional[User] = Depends(get_optional_user)) -> Optional[User]:
        if not settings.enable_self_hosted_auth:
            return None
        if user is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="authentication required",
                headers={"WWW-Authenticate": "Bearer"},
            )
        return user

    return dep
