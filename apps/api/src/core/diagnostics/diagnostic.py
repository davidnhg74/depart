"""Structured diagnostic record.

Every transform/validator/AI service produces Diagnostics rather than raising.
The runbook generator turns ERROR/CRITICAL diagnostics into ordered work items
for the customer; INFO/WARNING become advisory notes in the deliverable PDF.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


class Severity(str, Enum):
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


@dataclass(frozen=True)
class Span:
    """Source location range. start_line/col are 1-indexed; end is exclusive."""

    file: Optional[str]
    start_line: int
    start_col: int
    end_line: int
    end_col: int

    @classmethod
    def unknown(cls) -> "Span":
        return cls(file=None, start_line=0, start_col=0, end_line=0, end_col=0)


@dataclass(frozen=True)
class Diagnostic:
    code: str  # stable machine code, e.g. "ORA.PARSE.UNTERMINATED_STRING"
    severity: Severity
    message: str  # human-facing single-line summary
    span: Span
    suggestion: Optional[str] = None  # plain-text remediation, optional
    details: dict = field(default_factory=dict)  # arbitrary structured context

    def __post_init__(self) -> None:
        if not self.code or "." not in self.code:
            raise ValueError(f"Diagnostic code must be dotted, got: {self.code!r}")
