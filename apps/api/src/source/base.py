"""Source-dialect protocol.

Every source dialect (Oracle today; MySQL, MSSQL tomorrow) implements the
`Parser` protocol. Consumers (analyze, transforms, AI services) depend only
on this interface — they never branch on dialect.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from ..core.ir.nodes import Module


@runtime_checkable
class Parser(Protocol):
    """Parse source text into a canonical IR Module.

    Implementations MUST:
      * Tokenize string and comment literals correctly (no regex on raw text).
      * Attach Span information to every node.
      * Surface parse failures as Diagnostics on the Module, not exceptions.
    """

    dialect: str
    """Lowercase dialect identifier, e.g. 'oracle'."""

    def parse(self, source: str, *, name: str = "<inline>") -> Module: ...
