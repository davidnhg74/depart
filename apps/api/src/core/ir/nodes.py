"""Canonical Intermediate Representation.

Every source dialect parses INTO these node types; every target dialect emits
FROM them. Transforms are IR -> IR.

Design rules:
  * Nodes are dataclasses (immutable where practical, frozen where free).
  * Source-specific concepts that have no canonical equivalent (e.g. Oracle
    `OBJECT TYPE` with methods) are modeled as `UnsupportedConstruct` with
    enough detail that a human or LLM can later refactor them — they are
    NEVER represented as raw strings without a typed wrapper.
  * Every node carries a `span` so diagnostics can point at source.
  * The set of node types is intentionally small at first. Add a node when a
    real consumer (an emitter or transform) needs to distinguish it; do not
    speculate.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import List, Optional

from ..diagnostics.diagnostic import Span


# ─── Top-level ────────────────────────────────────────────────────────────────


@dataclass
class Module:
    """A parse unit — one file, one zip entry, or one inline snippet."""

    name: str
    objects: List["SchemaObject"] = field(default_factory=list)
    span: Span = field(default_factory=Span.unknown)


# ─── Schema objects (nameable, top-level constructs) ──────────────────────────


class ObjectKind(str, Enum):
    TABLE = "TABLE"
    VIEW = "VIEW"
    MATERIALIZED_VIEW = "MATERIALIZED_VIEW"
    INDEX = "INDEX"
    SEQUENCE = "SEQUENCE"
    SYNONYM = "SYNONYM"
    TRIGGER = "TRIGGER"
    PROCEDURE = "PROCEDURE"
    FUNCTION = "FUNCTION"
    PACKAGE = "PACKAGE"
    PACKAGE_BODY = "PACKAGE_BODY"
    TYPE = "TYPE"
    TYPE_BODY = "TYPE_BODY"
    UNKNOWN = "UNKNOWN"


@dataclass
class SchemaObject:
    """A named top-level construct. Subclasses fill in details."""

    kind: ObjectKind
    name: str
    schema: Optional[str] = None
    span: Span = field(default_factory=Span.unknown)
    line_count: int = 0
    raw_source: str = ""  # original text; preserved for re-emission/debug
    diagnostics: List[object] = field(default_factory=list)  # Diagnostic, but avoid circular import


@dataclass
class Table(SchemaObject):
    columns: List["Column"] = field(default_factory=list)
    constraints: List["Constraint"] = field(default_factory=list)
    is_global_temp: bool = False
    is_iot: bool = False
    partitioning: Optional["Partitioning"] = None

    def __post_init__(self) -> None:
        if self.kind == ObjectKind.UNKNOWN:
            self.kind = ObjectKind.TABLE


@dataclass
class View(SchemaObject):
    is_materialized: bool = False
    select_text: str = ""

    def __post_init__(self) -> None:
        if self.kind == ObjectKind.UNKNOWN:
            self.kind = ObjectKind.MATERIALIZED_VIEW if self.is_materialized else ObjectKind.VIEW


@dataclass
class Sequence(SchemaObject):
    start_with: Optional[int] = None
    increment_by: Optional[int] = None
    cycle: bool = False

    def __post_init__(self) -> None:
        if self.kind == ObjectKind.UNKNOWN:
            self.kind = ObjectKind.SEQUENCE


@dataclass
class Index(SchemaObject):
    table: str = ""
    columns: List[str] = field(default_factory=list)
    unique: bool = False

    def __post_init__(self) -> None:
        if self.kind == ObjectKind.UNKNOWN:
            self.kind = ObjectKind.INDEX


@dataclass
class Trigger(SchemaObject):
    table: str = ""
    timing: str = ""  # BEFORE/AFTER/INSTEAD OF
    events: List[str] = field(default_factory=list)  # INSERT/UPDATE/DELETE

    def __post_init__(self) -> None:
        if self.kind == ObjectKind.UNKNOWN:
            self.kind = ObjectKind.TRIGGER


@dataclass
class Subprogram(SchemaObject):
    """PROCEDURE or FUNCTION."""

    parameters: List["Parameter"] = field(default_factory=list)
    return_type: Optional["TypeRef"] = None  # None for procedures
    body: str = ""  # PL/SQL body text; structured later
    referenced_constructs: List["ConstructRef"] = field(default_factory=list)

    def __post_init__(self) -> None:
        if self.kind == ObjectKind.UNKNOWN:
            self.kind = ObjectKind.PROCEDURE


@dataclass
class Package(SchemaObject):
    """Spec or body. The `is_body` flag distinguishes."""

    is_body: bool = False
    subprograms: List[Subprogram] = field(default_factory=list)
    state_variables: List["Parameter"] = field(default_factory=list)  # package-state -> GUC

    def __post_init__(self) -> None:
        if self.kind == ObjectKind.UNKNOWN:
            self.kind = ObjectKind.PACKAGE_BODY if self.is_body else ObjectKind.PACKAGE


@dataclass
class UnsupportedConstruct(SchemaObject):
    """An object the current grammar/emitter doesn't fully model.

    Carries enough context that a human or AI rewrite can address it later.
    Always emits a CRITICAL diagnostic at extraction time.
    """

    reason: str = ""

    def __post_init__(self) -> None:
        if self.kind == ObjectKind.UNKNOWN:
            self.kind = ObjectKind.UNKNOWN


# ─── Sub-elements ─────────────────────────────────────────────────────────────


@dataclass
class TypeRef:
    """A reference to a data type. `name` is the canonical IR type name
    (e.g. NUMBER, VARCHAR2, DATE); precision/scale/byte_or_char as parsed.
    Mapping to target types happens in the type_map transform.
    """

    name: str
    precision: Optional[int] = None
    scale: Optional[int] = None
    byte_or_char: Optional[str] = None  # 'BYTE'/'CHAR' for VARCHAR2 only


@dataclass
class Parameter:
    name: str
    type: TypeRef
    mode: str = "IN"  # IN/OUT/IN OUT
    default: Optional[str] = None


@dataclass
class Column:
    name: str
    type: TypeRef
    nullable: bool = True
    default: Optional[str] = None
    identity: bool = False


@dataclass
class Constraint:
    name: Optional[str]
    kind: str  # PRIMARY KEY / FOREIGN KEY / UNIQUE / CHECK / NOT NULL
    columns: List[str] = field(default_factory=list)
    references_table: Optional[str] = None
    references_columns: List[str] = field(default_factory=list)
    expression: Optional[str] = None  # for CHECK


@dataclass
class Partitioning:
    strategy: str  # RANGE / LIST / HASH / INTERVAL / REF
    columns: List[str] = field(default_factory=list)
    raw_clause: str = ""  # full original clause, for fidelity-emit


# ─── Embedded Oracle/PL-SQL constructs we always want to detect ───────────────
# These are not full statements yet — they are tagged occurrences inside a
# SchemaObject's body/source. Transforms decide how to lower each one.


class ConstructTag(str, Enum):
    CONNECT_BY = "CONNECT_BY"
    MERGE = "MERGE"
    AUTONOMOUS_TXN = "AUTONOMOUS_TXN"
    EXECUTE_IMMEDIATE = "EXECUTE_IMMEDIATE"
    BULK_COLLECT = "BULK_COLLECT"
    FORALL = "FORALL"
    DBMS_OUTPUT = "DBMS_OUTPUT"
    DBMS_SCHEDULER = "DBMS_SCHEDULER"
    DBMS_AQ = "DBMS_AQ"
    DBMS_CRYPTO = "DBMS_CRYPTO"
    UTL_FILE = "UTL_FILE"
    UTL_HTTP = "UTL_HTTP"
    DBLINK = "DBLINK"
    SPATIAL = "SPATIAL"
    ORACLE_TEXT = "ORACLE_TEXT"
    OUTER_JOIN_PLUS = "OUTER_JOIN_PLUS"  # the (+) operator
    PRAGMA_EXCEPTION_INIT = "PRAGMA_EXCEPTION_INIT"
    RAISE_APPLICATION_ERROR = "RAISE_APPLICATION_ERROR"
    HIERARCHICAL_PSEUDOCOLUMN = "HIERARCHICAL_PSEUDOCOLUMN"  # LEVEL, CONNECT_BY_ISLEAF, etc.
    GLOBAL_TEMP_TABLE = "GLOBAL_TEMP_TABLE"
    OBJECT_TYPE = "OBJECT_TYPE"
    NESTED_TABLE = "NESTED_TABLE"
    PIPELINED_FUNCTION = "PIPELINED_FUNCTION"
    EXTERNAL_PROCEDURE = "EXTERNAL_PROCEDURE"
    VPD_POLICY = "VPD_POLICY"
    REF_CURSOR = "REF_CURSOR"
    PERCENT_TYPE = "PERCENT_TYPE"  # %TYPE / %ROWTYPE
    ROWNUM = "ROWNUM"
    ROWID = "ROWID"
    NVL = "NVL"  # NVL/NVL2 — Oracle null-coalescing functions
    DECODE = "DECODE"  # DECODE — Oracle's positional CASE
    INTERVAL = "INTERVAL"  # INTERVAL types + arithmetic


@dataclass(frozen=True)
class ConstructRef:
    """A typed occurrence of a construct inside a SchemaObject's body."""

    tag: ConstructTag
    span: Span
    snippet: str = ""  # short excerpt for display (~80 chars)


# ─── Tier classification (drives complexity scoring + cost estimation) ────────


class Tier(str, Enum):
    A = "A"  # auto-convertible: 1x weight
    B = "B"  # needs review: 5x weight
    C = "C"  # must rewrite: 20x weight


# Source-of-truth tier mapping for tagged constructs. Owned here so the
# complexity scorer, the runbook generator, and the deliverable PDF all
# read from the same table.
TIER_FOR_TAG: dict = {
    # Tier A (deterministic, low risk)
    ConstructTag.DBMS_OUTPUT: Tier.A,
    ConstructTag.PERCENT_TYPE: Tier.A,
    ConstructTag.RAISE_APPLICATION_ERROR: Tier.A,
    # Tier B (mechanical but context-dependent)
    ConstructTag.CONNECT_BY: Tier.B,
    ConstructTag.MERGE: Tier.B,
    ConstructTag.GLOBAL_TEMP_TABLE: Tier.B,
    ConstructTag.EXECUTE_IMMEDIATE: Tier.B,
    ConstructTag.BULK_COLLECT: Tier.B,
    ConstructTag.FORALL: Tier.B,
    ConstructTag.OUTER_JOIN_PLUS: Tier.B,
    ConstructTag.PRAGMA_EXCEPTION_INIT: Tier.B,
    ConstructTag.HIERARCHICAL_PSEUDOCOLUMN: Tier.B,
    ConstructTag.ROWNUM: Tier.B,
    ConstructTag.ROWID: Tier.B,
    ConstructTag.REF_CURSOR: Tier.B,
    # Tier C (requires architectural change or extension install)
    ConstructTag.AUTONOMOUS_TXN: Tier.C,
    ConstructTag.DBMS_SCHEDULER: Tier.C,
    ConstructTag.DBMS_AQ: Tier.C,
    ConstructTag.DBMS_CRYPTO: Tier.C,
    ConstructTag.UTL_FILE: Tier.C,
    ConstructTag.UTL_HTTP: Tier.C,
    ConstructTag.DBLINK: Tier.C,
    ConstructTag.SPATIAL: Tier.C,
    ConstructTag.ORACLE_TEXT: Tier.C,
    ConstructTag.OBJECT_TYPE: Tier.C,
    ConstructTag.NESTED_TABLE: Tier.C,
    ConstructTag.PIPELINED_FUNCTION: Tier.C,
    ConstructTag.EXTERNAL_PROCEDURE: Tier.C,
    ConstructTag.VPD_POLICY: Tier.C,
}
