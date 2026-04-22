"""Tests for the IR-based complexity analyzer.

These run against the interim parser today and against the ANTLR-backed
parser as soon as `make grammar` produces it — the assertions are written
against the public ComplexityReport contract, not parser internals.
"""

import pytest

from src.analyze.complexity import ComplexityScorer, analyze
from src.core.ir.nodes import ConstructTag, ObjectKind


class TestComplexityScorer:
    @pytest.fixture
    def scorer(self):
        return ComplexityScorer()

    def test_simple_procedure(self, scorer, simple_procedure):
        report = scorer.analyze(simple_procedure)
        assert 1 <= report.score <= 100
        assert report.total_lines > 0
        assert report.effort_estimate_days > 0
        assert report.objects_by_kind.get(ObjectKind.PROCEDURE.value, 0) >= 1

    def test_hr_schema(self, scorer, hr_schema_content):
        report = scorer.analyze(hr_schema_content)
        # The HR fixture has multiple objects across kinds.
        assert report.total_lines > 50
        kinds = report.objects_by_kind
        assert any(
            k in kinds
            for k in (
                ObjectKind.PROCEDURE.value,
                ObjectKind.FUNCTION.value,
                ObjectKind.PACKAGE.value,
            )
        ), f"expected procedural objects, got {kinds}"

    def test_complex_plsql_detects_tier_constructs(self, scorer, complex_plsql):
        report = scorer.analyze(complex_plsql)
        # MERGE and CONNECT BY are Tier B; PRAGMA AUTONOMOUS_TRANSACTION is Tier C.
        assert ConstructTag.MERGE.value in report.construct_counts
        assert ConstructTag.CONNECT_BY.value in report.construct_counts
        assert ConstructTag.AUTONOMOUS_TXN.value in report.construct_counts
        # And the Tier C presence pushes the score above the trivial range.
        assert report.score > 30

    def test_string_literal_does_not_match_keyword(self):
        """The interim regex parser scored `'CONNECT BY'` inside a string as
        a CONNECT BY use. Verify the new tokenizer does not."""
        report = analyze(
            """
            CREATE OR REPLACE PROCEDURE log_msg AS
            BEGIN
                INSERT INTO audit_log (msg) VALUES ('CONNECT BY in a string');
            END;
        """
        )
        assert ConstructTag.CONNECT_BY.value not in report.construct_counts

    def test_comment_does_not_match_keyword(self):
        """`-- MERGE INTO` in a comment must not register."""
        report = analyze(
            """
            CREATE OR REPLACE PROCEDURE p AS
            BEGIN
                -- MERGE INTO is documented elsewhere
                NULL;
            END;
        """
        )
        assert ConstructTag.MERGE.value not in report.construct_counts

    def test_effort_estimation(self, scorer, hr_schema_content):
        report = scorer.analyze(hr_schema_content, rate_per_day=1000)
        assert report.effort_estimate_days > 0
        assert report.effort_estimate_days < 100
        assert report.estimated_cost == round(report.effort_estimate_days * 1000, 2)

    def test_custom_rate(self, scorer, simple_procedure):
        a = scorer.analyze(simple_procedure, rate_per_day=1000)
        b = scorer.analyze(simple_procedure, rate_per_day=2000)
        assert b.estimated_cost == a.estimated_cost * 2

    def test_empty_content(self, scorer):
        report = scorer.analyze("")
        assert report.score >= 1
        assert report.total_lines == 0
        assert report.effort_estimate_days == 0.5

    def test_score_range(self, scorer, hr_schema_content):
        for _ in range(5):
            assert 1 <= scorer.analyze(hr_schema_content).score <= 100
