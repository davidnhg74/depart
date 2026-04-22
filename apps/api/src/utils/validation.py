"""
Input validation and sanitization utilities.
Prevents common security issues and improves error messages.
"""

from typing import Any, Dict, Optional
import re
import logging

logger = logging.getLogger(__name__)


class ValidationError(Exception):
    """Custom validation error."""

    pass


class InputValidator:
    """Validates and sanitizes API inputs."""

    # Safe SQL identifier pattern (alphanumeric, underscore, dollar sign)
    SAFE_IDENTIFIER = re.compile(r"^[a-zA-Z_$][a-zA-Z0-9_$]*$")

    # Email pattern
    EMAIL_PATTERN = re.compile(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")

    # UUID pattern
    UUID_PATTERN = re.compile(
        r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
        re.IGNORECASE,
    )

    @staticmethod
    def validate_uuid(value: str) -> bool:
        """Check if value is valid UUID format."""
        return bool(InputValidator.UUID_PATTERN.match(value))

    @staticmethod
    def validate_email(value: str) -> bool:
        """Check if value is valid email."""
        return bool(InputValidator.EMAIL_PATTERN.match(value))

    @staticmethod
    def validate_identifier(value: str) -> bool:
        """Check if value is safe SQL identifier."""
        return bool(InputValidator.SAFE_IDENTIFIER.match(value))

    @staticmethod
    def validate_hostname(value: str) -> bool:
        """Check if value is valid hostname/IP."""
        # Allow IPv4, IPv6, and hostnames
        ipv4_pattern = re.compile(
            r"^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$"
        )
        hostname_pattern = re.compile(r"^(?!-)[a-zA-Z0-9-]{1,63}(?<!-)(?:\.[a-zA-Z]{2,})+$")

        return (
            bool(ipv4_pattern.match(value))
            or bool(hostname_pattern.match(value))
            or value == "localhost"
        )

    @staticmethod
    def validate_port(value: int) -> bool:
        """Check if value is valid port number."""
        return 1 <= value <= 65535

    @staticmethod
    def validate_step_number(value: int) -> bool:
        """Check if value is valid workflow step (1-20)."""
        return 1 <= value <= 20

    @staticmethod
    def validate_json_keys(data: Dict[str, Any], required_keys: list[str]) -> bool:
        """Check if dict has all required keys."""
        return all(key in data for key in required_keys)

    @staticmethod
    def validate_string_length(value: str, min_len: int = 1, max_len: int = 255) -> bool:
        """Check string length."""
        return min_len <= len(value.strip()) <= max_len

    @staticmethod
    def sanitize_string(value: str, max_len: int = 1000) -> str:
        """Sanitize string input."""
        # Strip whitespace
        value = value.strip()

        # Limit length
        if len(value) > max_len:
            value = value[:max_len]

        return value

    @staticmethod
    def validate_connection_config(config: Dict[str, Any]) -> tuple[bool, Optional[str]]:
        """
        Validate database connection configuration.

        Returns:
            (is_valid, error_message)
        """
        required = ["database_type", "host", "port", "username", "password"]

        # Check required fields
        if not InputValidator.validate_json_keys(config, required):
            return False, f"Missing required fields. Need: {required}"

        # Validate database type
        db_type = config.get("database_type", "").lower()
        if db_type not in ["oracle", "postgres", "postgresql"]:
            return False, "database_type must be 'oracle' or 'postgres'"

        # Validate host
        host = config.get("host", "")
        if not InputValidator.validate_hostname(host):
            return False, f"Invalid hostname: {host}"

        # Validate port
        try:
            port = int(config.get("port", 0))
            if not InputValidator.validate_port(port):
                return False, f"Port must be between 1 and 65535, got {port}"
        except (ValueError, TypeError):
            return False, "Port must be an integer"

        # Validate username/password
        username = config.get("username", "")
        password = config.get("password", "")

        if not username or not password:
            return False, "Username and password cannot be empty"

        if len(username) > 128 or len(password) > 512:
            return False, "Username or password too long"

        # Oracle-specific validation
        if db_type == "oracle":
            service_name = config.get("service_name")
            if service_name and not InputValidator.validate_identifier(service_name):
                return False, f"Invalid Oracle service name: {service_name}"

        # PostgreSQL-specific validation
        if db_type in ["postgres", "postgresql"]:
            database = config.get("database")
            if database and not InputValidator.validate_identifier(database):
                return False, f"Invalid PostgreSQL database name: {database}"

        return True, None

    @staticmethod
    def validate_workflow_name(name: str) -> tuple[bool, Optional[str]]:
        """Validate workflow name."""
        if not name or not isinstance(name, str):
            return False, "Workflow name is required and must be a string"

        name = name.strip()
        if len(name) < 3:
            return False, "Workflow name must be at least 3 characters"

        if len(name) > 255:
            return False, "Workflow name must be at most 255 characters"

        # Prevent SQL injection-like patterns
        dangerous_patterns = [";", "--", "/*", "*/", "DROP", "DELETE", "TRUNCATE"]
        for pattern in dangerous_patterns:
            if pattern.lower() in name.lower():
                return False, f"Workflow name cannot contain '{pattern}'"

        return True, None


class RateLimiter:
    """Simple in-memory rate limiter for API endpoints."""

    def __init__(self, max_requests: int = 100, window_seconds: int = 60):
        """
        Initialize rate limiter.

        Args:
            max_requests: Max requests per window
            window_seconds: Time window in seconds
        """
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self.requests: Dict[str, list] = {}

    def is_allowed(self, client_id: str) -> bool:
        """Check if client can make request."""
        import time

        now = time.time()
        cutoff = now - self.window_seconds

        if client_id not in self.requests:
            self.requests[client_id] = []

        # Remove old requests
        self.requests[client_id] = [t for t in self.requests[client_id] if t > cutoff]

        # Check limit
        if len(self.requests[client_id]) >= self.max_requests:
            return False

        # Add this request
        self.requests[client_id].append(now)
        return True

    def get_remaining(self, client_id: str) -> int:
        """Get remaining requests for client."""
        import time

        now = time.time()
        cutoff = now - self.window_seconds

        if client_id not in self.requests:
            return self.max_requests

        valid_requests = [t for t in self.requests[client_id] if t > cutoff]
        return max(0, self.max_requests - len(valid_requests))


# Global rate limiters
_workflow_limiter: Optional[RateLimiter] = None
_benchmark_limiter: Optional[RateLimiter] = None
_analysis_limiter: Optional[RateLimiter] = None


def get_workflow_limiter() -> RateLimiter:
    """Get workflow endpoint rate limiter."""
    global _workflow_limiter
    if _workflow_limiter is None:
        _workflow_limiter = RateLimiter(max_requests=50, window_seconds=60)
    return _workflow_limiter


def get_benchmark_limiter() -> RateLimiter:
    """Get benchmark endpoint rate limiter."""
    global _benchmark_limiter
    if _benchmark_limiter is None:
        _benchmark_limiter = RateLimiter(max_requests=20, window_seconds=60)
    return _benchmark_limiter


def get_analysis_limiter() -> RateLimiter:
    """Get analysis endpoint rate limiter."""
    global _analysis_limiter
    if _analysis_limiter is None:
        _analysis_limiter = RateLimiter(max_requests=30, window_seconds=60)
    return _analysis_limiter
