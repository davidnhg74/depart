"""
Standardized response formatting and error handling.
Improves consistency and user experience across API.
"""

from typing import Any, Dict, Optional, List
from datetime import datetime
from pydantic import BaseModel

from .time import utc_now


class APIResponse(BaseModel):
    """Standard API response wrapper."""

    success: bool
    data: Optional[Any] = None
    error: Optional[str] = None
    error_code: Optional[str] = None
    timestamp: str = None
    request_id: Optional[str] = None

    def __init__(self, **data):
        super().__init__(**data)
        if self.timestamp is None:
            self.timestamp = utc_now().isoformat()


class ErrorResponse(APIResponse):
    """Standard error response."""

    def __init__(self, error: str, error_code: str = "UNKNOWN", status_code: int = 500, **kwargs):
        super().__init__(success=False, error=error, error_code=error_code, **kwargs)
        self.status_code = status_code


class ValidationErrorResponse(ErrorResponse):
    """Validation error response with field details."""

    def __init__(self, message: str, validation_errors: Dict[str, str] = None, **kwargs):
        super().__init__(error=message, error_code="VALIDATION_ERROR", status_code=422, **kwargs)
        self.validation_errors = validation_errors or {}


class RateLimitResponse(ErrorResponse):
    """Rate limit exceeded response."""

    def __init__(self, remaining: int = 0, reset_at: str = None, **kwargs):
        super().__init__(
            error="Rate limit exceeded", error_code="RATE_LIMIT_EXCEEDED", status_code=429, **kwargs
        )
        self.remaining = remaining
        self.reset_at = reset_at


class NotFoundResponse(ErrorResponse):
    """Resource not found response."""

    def __init__(self, resource_type: str, resource_id: str = None, **kwargs):
        message = f"{resource_type} not found"
        if resource_id:
            message += f": {resource_id}"

        super().__init__(error=message, error_code="NOT_FOUND", status_code=404, **kwargs)


class SuccessResponse(APIResponse):
    """Success response with data."""

    def __init__(self, data: Any = None, message: str = "Success", **kwargs):
        super().__init__(success=True, data=data, **kwargs)
        self.message = message


class PaginatedResponse(APIResponse):
    """Paginated data response."""

    def __init__(self, items: List[Any], total: int, page: int = 1, page_size: int = 20, **kwargs):
        super().__init__(
            success=True,
            data={
                "items": items,
                "pagination": {
                    "total": total,
                    "page": page,
                    "page_size": page_size,
                    "total_pages": (total + page_size - 1) // page_size,
                },
            },
            **kwargs,
        )


class OperationResponse(APIResponse):
    """Response for async operations (jobs, migrations, etc.)."""

    def __init__(
        self, operation_id: str, status: str, progress: float = 0.0, message: str = None, **kwargs
    ):
        super().__init__(
            success=True,
            data={
                "operation_id": operation_id,
                "status": status,
                "progress": progress,
                "message": message,
            },
            **kwargs,
        )


class ErrorMessages:
    """Common error messages with context."""

    # Validation errors
    INVALID_INPUT = "Invalid input provided"
    MISSING_FIELD = "Missing required field: {field}"
    INVALID_FORMAT = "Invalid format for {field}: {message}"
    INVALID_UUID = "Invalid UUID format"
    INVALID_EMAIL = "Invalid email format"

    # Database errors
    CONNECTION_FAILED = "Failed to connect to database"
    QUERY_FAILED = "Database query failed"
    OPERATION_FAILED = "Operation failed"

    # Resource errors
    NOT_FOUND = "{resource} not found"
    ALREADY_EXISTS = "{resource} already exists"
    PERMISSION_DENIED = "Permission denied"

    # Service errors
    SERVICE_UNAVAILABLE = "Service temporarily unavailable"
    TIMEOUT = "Request timed out"
    RATE_LIMITED = "Too many requests"

    @staticmethod
    def format_missing_field(field: str) -> str:
        return ErrorMessages.MISSING_FIELD.format(field=field)

    @staticmethod
    def format_invalid_format(field: str, message: str) -> str:
        return ErrorMessages.INVALID_FORMAT.format(field=field, message=message)

    @staticmethod
    def format_not_found(resource: str) -> str:
        return ErrorMessages.NOT_FOUND.format(resource=resource)

    @staticmethod
    def format_already_exists(resource: str) -> str:
        return ErrorMessages.ALREADY_EXISTS.format(resource=resource)


class StatusCodes:
    """HTTP status code constants."""

    # Success
    OK = 200
    CREATED = 201
    ACCEPTED = 202

    # Client errors
    BAD_REQUEST = 400
    UNAUTHORIZED = 401
    FORBIDDEN = 403
    NOT_FOUND = 404
    CONFLICT = 409
    UNPROCESSABLE_ENTITY = 422
    RATE_LIMITED = 429

    # Server errors
    INTERNAL_SERVER_ERROR = 500
    NOT_IMPLEMENTED = 501
    SERVICE_UNAVAILABLE = 503
    GATEWAY_TIMEOUT = 504
