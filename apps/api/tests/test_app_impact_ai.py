"""Tests for the AI explanation layer over AppImpactReport.

Mocks the underlying AIClient via dependency injection — no LLM calls.
"""

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from src.analyze.app_impact import AppImpactAnalyzer
from src.ai.services.app_impact import (
    AppImpactExplainer,
    EnrichedAppImpactReport,
    EnrichedFinding,
)
from src.source.oracle.parser import parse


FIXTURES = Path(__file__).parent / "fixtures" / "app_impact"


@pytest.fixture(scope="module")
def schema():
    return parse((FIXTURES / "schema" / "schema.sql").read_text())


@pytest.fixture
def analyzer(schema):
    return AppImpactAnalyzer(schema=schema)


def _ai_response_for(findings):
    """Build a fake LLM response that explains each finding."""
    return {
        "findings": [
            {
                "code": f.code,
                "explanation": f"AI says: rewrite this {f.code} occurrence.",
                "before": f"-- before {f.code}",
                "after": f"-- after {f.code}",
                "caveats": [f"watch out for {f.code} edge case"],
            }
            for f in findings
        ]
    }


class TestExplainerEnrichesFindings:
    def test_each_finding_gets_explanation(self, analyzer, schema):
        report = analyzer.analyze_directory(FIXTURES / "java", languages=["java"])
        all_findings = [f for fi in report.files for f in fi.findings]
        assert all_findings

        mock_client = MagicMock()
        mock_client.complete_json.side_effect = lambda system, user: _ai_response_for(all_findings)

        explainer = AppImpactExplainer(client=mock_client, schema=schema, batch_size=100)
        enriched = explainer.enrich(report)

        assert isinstance(enriched, EnrichedAppImpactReport)
        all_enriched = [ef for fi in enriched.files for ef in fi.findings]
        assert len(all_enriched) == len(all_findings)
        for ef in all_enriched:
            assert ef.explanation.startswith("AI says:")
            assert ef.before and ef.after
            assert ef.caveats and "edge case" in ef.caveats[0]
        assert enriched.explanations_generated == len(all_findings)
        assert enriched.explanations_failed == 0

    def test_batches_split_correctly(self, analyzer, schema):
        report = analyzer.analyze_directory(FIXTURES / "java", languages=["java"])
        mock_client = MagicMock()

        # Each call sees only its own subset of findings; we satisfy whatever
        # we're handed.
        def side_effect(system, user):
            # Pull codes out of the user message line "code: APP.SQL.X"
            codes = [
                line.split("code: ", 1)[1].strip()
                for line in user.splitlines()
                if line.startswith("- code:")
            ]
            return {
                "findings": [
                    {"code": c, "explanation": "ok", "before": "b", "after": "a", "caveats": []}
                    for c in codes
                ]
            }

        mock_client.complete_json.side_effect = side_effect

        explainer = AppImpactExplainer(client=mock_client, schema=schema, batch_size=2)
        enriched = explainer.enrich(report)

        # batch_size=2 with N findings -> ceil(N/2) calls.
        all_findings = [f for fi in report.files for f in fi.findings]
        assert mock_client.complete_json.call_count == (len(all_findings) + 1) // 2

    def test_high_risk_findings_explained_first(self, analyzer, schema):
        """If batches degrade, the highest-risk findings should be the
        ones that succeeded — sort order matters for graceful failure."""
        report = analyzer.analyze_directory(FIXTURES / "java", languages=["java"])
        mock_client = MagicMock()
        # Track the sequence of codes the LLM is asked about.
        seen_codes = []

        def side_effect(system, user):
            codes = [
                line.split("code: ", 1)[1].strip()
                for line in user.splitlines()
                if line.startswith("- code:")
            ]
            seen_codes.extend(codes)
            return {
                "findings": [
                    {"code": c, "explanation": "x", "before": "b", "after": "a", "caveats": []}
                    for c in codes
                ]
            }

        mock_client.complete_json.side_effect = side_effect

        explainer = AppImpactExplainer(client=mock_client, schema=schema, batch_size=2)
        explainer.enrich(report)

        # Risk-rank of CRITICAL > HIGH > MEDIUM > LOW. The first batch
        # (first two seen_codes) should both be CRITICAL.
        from src.analyze.app_impact import RiskLevel

        all_findings = [f for fi in report.files for f in fi.findings]
        by_code = {f.code: f.risk for f in all_findings}
        # Drop dup codes (one finding per code is fine for this assertion).
        first_two_uniq = []
        for c in seen_codes:
            if c not in first_two_uniq:
                first_two_uniq.append(c)
            if len(first_two_uniq) == 2:
                break
        assert all(
            by_code[c] == RiskLevel.CRITICAL for c in first_two_uniq
        ), f"first batch was {first_two_uniq} with risks {[by_code[c] for c in first_two_uniq]}"


class TestGracefulDegradation:
    def test_llm_failure_returns_empty_enrichments(self, analyzer, schema):
        report = analyzer.analyze_directory(FIXTURES / "java", languages=["java"])
        mock_client = MagicMock()
        mock_client.complete_json.side_effect = RuntimeError("API down")

        explainer = AppImpactExplainer(client=mock_client, schema=schema)
        enriched = explainer.enrich(report)

        all_enriched = [ef for fi in enriched.files for ef in fi.findings]
        assert all_enriched, "deterministic findings must survive LLM failure"
        for ef in all_enriched:
            assert ef.explanation == ""
            assert ef.before == ""
            assert ef.after == ""
            assert not ef.has_explanation
        assert enriched.explanations_failed == len(all_enriched)
        assert enriched.explanations_generated == 0

    def test_llm_returns_no_match_for_finding_code(self, analyzer, schema):
        report = analyzer.analyze_directory(FIXTURES / "java", languages=["java"])
        mock_client = MagicMock()
        # LLM returns explanations for unrelated codes — none of our findings match.
        mock_client.complete_json.return_value = {
            "findings": [
                {
                    "code": "APP.SQL.UNRELATED",
                    "explanation": "x",
                    "before": "",
                    "after": "",
                    "caveats": [],
                }
            ]
        }
        explainer = AppImpactExplainer(client=mock_client, schema=schema, batch_size=100)
        enriched = explainer.enrich(report)

        all_enriched = [ef for fi in enriched.files for ef in fi.findings]
        assert all_enriched
        for ef in all_enriched:
            assert ef.explanation == ""  # no AI content matched


class TestSchemaSummary:
    def test_summary_includes_schema_objects(self, schema):
        explainer = AppImpactExplainer(client=MagicMock(), schema=schema)
        summary = explainer._schema_summary()
        assert "TABLE" in summary
        assert "employees" in summary or "EMPLOYEES" in summary

    def test_no_schema_yields_empty_summary(self):
        explainer = AppImpactExplainer(client=MagicMock(), schema=None)
        assert explainer._schema_summary() == ""


class TestEnrichedFindingShape:
    def test_has_explanation_property(self):
        from src.analyze.app_impact import Finding, RiskLevel

        f = Finding(
            code="X.Y",
            risk=RiskLevel.LOW,
            message="m",
            suggestion="s",
            file="f",
            line=1,
            snippet="x",
        )
        ef_with = EnrichedFinding(finding=f, explanation="hi", before="", after="")
        ef_without = EnrichedFinding(finding=f, explanation="", before="", after="")
        assert ef_with.has_explanation
        assert not ef_without.has_explanation
