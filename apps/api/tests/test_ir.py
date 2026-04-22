"""IR contract tests.

The IR types are the most important interface in the codebase — every
source dialect, target dialect, and AI service depends on them. Lock down
behavior here so accidental refactors get caught.
"""

import pytest

from src.core.diagnostics.diagnostic import Diagnostic, Severity, Span
from src.core.ir.nodes import (
    ConstructRef,
    ConstructTag,
    Module,
    ObjectKind,
    Package,
    Sequence,
    Subprogram,
    Table,
    Tier,
    TIER_FOR_TAG,
    View,
)


class TestSpan:
    def test_unknown_span_is_safe(self):
        s = Span.unknown()
        assert s.start_line == 0 and s.end_line == 0

    def test_span_is_frozen(self):
        s = Span(file=None, start_line=1, start_col=1, end_line=1, end_col=2)
        with pytest.raises(Exception):
            s.start_line = 99  # type: ignore[misc]


class TestDiagnostic:
    def test_dotted_code_required(self):
        with pytest.raises(ValueError):
            Diagnostic(code="undotted", severity=Severity.INFO, message="m", span=Span.unknown())

    def test_severity_enum_values(self):
        # Lock the enum values; the runbook generator emits these as JSON.
        assert {s.value for s in Severity} == {"info", "warning", "error", "critical"}

    def test_default_details_are_per_instance(self):
        a = Diagnostic("X.Y", Severity.INFO, "m", Span.unknown())
        b = Diagnostic("X.Y", Severity.INFO, "m", Span.unknown())
        # frozen=True dataclasses with default_factory should still be safe.
        assert a.details is not b.details


class TestSchemaObjects:
    def test_table_default_kind(self):
        t = Table(kind=ObjectKind.UNKNOWN, name="x")
        assert t.kind == ObjectKind.TABLE

    def test_view_materialized_kind(self):
        v = View(kind=ObjectKind.UNKNOWN, name="x", is_materialized=True)
        assert v.kind == ObjectKind.MATERIALIZED_VIEW

    def test_view_default_kind(self):
        v = View(kind=ObjectKind.UNKNOWN, name="x")
        assert v.kind == ObjectKind.VIEW

    def test_sequence_default_kind(self):
        assert Sequence(kind=ObjectKind.UNKNOWN, name="x").kind == ObjectKind.SEQUENCE

    def test_subprogram_default_kind(self):
        assert Subprogram(kind=ObjectKind.UNKNOWN, name="x").kind == ObjectKind.PROCEDURE

    def test_package_body_kind(self):
        p = Package(kind=ObjectKind.UNKNOWN, name="x", is_body=True)
        assert p.kind == ObjectKind.PACKAGE_BODY


class TestTierTable:
    def test_every_construct_has_a_tier(self):
        # If we add a new ConstructTag without a Tier mapping, the runbook
        # generator and complexity scorer would silently drop it.
        for tag in ConstructTag:
            assert tag in TIER_FOR_TAG, f"{tag} missing TIER_FOR_TAG entry"

    def test_critical_constructs_are_tier_c(self):
        # A non-exhaustive sanity floor.
        for tag in (
            ConstructTag.AUTONOMOUS_TXN,
            ConstructTag.DBMS_AQ,
            ConstructTag.DBMS_SCHEDULER,
            ConstructTag.DBMS_CRYPTO,
            ConstructTag.OBJECT_TYPE,
            ConstructTag.SPATIAL,
            ConstructTag.PIPELINED_FUNCTION,
        ):
            assert TIER_FOR_TAG[tag] == Tier.C, tag


class TestModule:
    def test_module_can_be_empty(self):
        m = Module(name="x")
        assert m.objects == []

    def test_subprogram_can_carry_construct_refs(self):
        s = Subprogram(kind=ObjectKind.PROCEDURE, name="p")
        s.referenced_constructs.append(
            ConstructRef(tag=ConstructTag.MERGE, span=Span.unknown(), snippet="MERGE INTO")
        )
        assert s.referenced_constructs[0].tag == ConstructTag.MERGE
