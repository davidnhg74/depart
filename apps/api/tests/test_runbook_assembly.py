"""Tests for the deterministic runbook assembly logic."""

from pathlib import Path

import pytest

from src.analyze.app_impact import (
    AppImpactAnalyzer,
    RiskLevel,
)
from src.analyze.complexity import analyze as analyze_complexity
from src.projects.runbook import (
    RunbookContext,
    assemble,
)
from src.source.oracle.parser import parse


FIXTURES = Path(__file__).parent / "fixtures" / "app_impact"


# ─── fixtures ────────────────────────────────────────────────────────────────


@pytest.fixture(scope="module")
def schema_module():
    return parse((FIXTURES / "schema" / "schema.sql").read_text())


@pytest.fixture(scope="module")
def complexity():
    src = (FIXTURES / "schema" / "schema.sql").read_text()
    return analyze_complexity(src, rate_per_day=1500)


@pytest.fixture
def app_impact(schema_module):
    analyzer = AppImpactAnalyzer(schema=schema_module)
    return analyzer.analyze_directory(FIXTURES, languages=["java", "python"])


@pytest.fixture
def base_ctx(complexity, app_impact):
    return RunbookContext(
        project_name="ACME Migration",
        customer="ACME Corp",
        source_version="Oracle 19c",
        target_version="PostgreSQL 16",
        cutover_window="2026-06-15 02:00 UTC, 4-hour window",
        rate_per_day=1500,
        complexity=complexity,
        app_impact=app_impact,
    )


# ─── Phase structure ─────────────────────────────────────────────────────────


class TestPhaseStructure:
    def test_six_standard_phases(self, base_ctx):
        rb = assemble(base_ctx)
        titles = [p.title for p in rb.phases]
        assert len(titles) == 6
        assert titles[0].startswith("1.")
        assert "Discovery" in titles[0]
        assert "Schema Conversion" in titles[1]
        assert "Application Refactor" in titles[2]
        assert "Data Migration" in titles[3]
        assert "Cutover" in titles[4]
        assert "Stabilization" in titles[5]

    def test_each_phase_has_required_fields(self, base_ctx):
        rb = assemble(base_ctx)
        for p in rb.phases:
            assert p.title
            assert p.description
            assert isinstance(p.prerequisites, list) and p.prerequisites
            assert isinstance(p.activities, list) and p.activities
            assert isinstance(p.rollback, list) and p.rollback
            assert p.duration_days >= 0
            assert isinstance(p.risk_level, RiskLevel)

    def test_phase_durations_sum_close_to_total_effort(self, base_ctx):
        rb = assemble(base_ctx)
        total = sum(p.duration_days for p in rb.phases)
        # Allow rounding error from per-phase rounding to one decimal.
        assert abs(total - base_ctx.complexity.effort_estimate_days) < 1.0

    def test_app_phase_risk_reflects_findings(self, base_ctx):
        rb = assemble(base_ctx)
        app_phase = next(p for p in rb.phases if "Application" in p.title)
        # The fixture has CRITICAL findings (DBLINK, DBMS_OUTPUT, DUAL, ...).
        assert app_phase.risk_level == RiskLevel.CRITICAL

    def test_schema_phase_risk_reflects_complexity(self, base_ctx):
        rb = assemble(base_ctx)
        schema_phase = next(p for p in rb.phases if "Schema" in p.title)
        cx = base_ctx.complexity
        if cx.must_rewrite_lines > 0:
            assert schema_phase.risk_level == RiskLevel.HIGH
        elif cx.needs_review_lines > 0:
            assert schema_phase.risk_level == RiskLevel.MEDIUM
        else:
            assert schema_phase.risk_level == RiskLevel.LOW


# ─── Activity content references inputs ──────────────────────────────────────


class TestActivityContent:
    def test_schema_activities_reference_complexity_numbers(self, base_ctx):
        rb = assemble(base_ctx)
        schema_phase = next(p for p in rb.phases if "Schema" in p.title)
        joined = " ".join(schema_phase.activities)
        cx = base_ctx.complexity
        # When non-zero, the line counts must appear in the activities.
        if cx.auto_convertible_lines:
            assert str(cx.auto_convertible_lines) in joined
        if cx.needs_review_lines:
            assert str(cx.needs_review_lines) in joined
        if cx.must_rewrite_lines:
            assert str(cx.must_rewrite_lines) in joined

    def test_app_activities_reference_finding_counts(self, base_ctx):
        rb = assemble(base_ctx)
        app_phase = next(p for p in rb.phases if "Application" in p.title)
        joined = " ".join(app_phase.activities)
        ai = base_ctx.app_impact
        assert str(ai.total_findings) in joined or str(ai.total_files_scanned) in joined


# ─── Blockers ────────────────────────────────────────────────────────────────


class TestBlockers:
    def test_critical_findings_become_blockers(self, base_ctx):
        rb = assemble(base_ctx)
        # Every CRITICAL finding in the input becomes a blocker.
        ai = base_ctx.app_impact
        critical = []
        for fi in ai.files:
            for f in fi.findings:
                if f.risk == RiskLevel.CRITICAL:
                    critical.append(f)
        assert len(rb.blockers) == len(critical)
        codes = {b.code for b in rb.blockers}
        assert codes == {f.code for f in critical}

    def test_blockers_sorted_by_file_and_line(self, base_ctx):
        rb = assemble(base_ctx)
        for a, b in zip(rb.blockers, rb.blockers[1:]):
            assert (a.file, a.line) <= (b.file, b.line)

    def test_no_app_impact_means_no_blockers(self, complexity):
        ctx = RunbookContext(
            project_name="x",
            customer="y",
            complexity=complexity,
            app_impact=None,
        )
        rb = assemble(ctx)
        assert rb.blockers == []


# ─── Default narrative + summary ─────────────────────────────────────────────


class TestDefaultNarrative:
    def test_default_summary_cites_numbers(self, base_ctx):
        rb = assemble(base_ctx)
        # Default summary is used when AI is not invoked.
        cx = base_ctx.complexity
        assert str(cx.total_lines) in rb.executive_summary or "lines" in rb.executive_summary
        assert "Oracle" in rb.executive_summary
        assert base_ctx.customer in rb.executive_summary

    def test_default_risk_narrative_mentions_tier_lines(self, base_ctx):
        rb = assemble(base_ctx)
        cx = base_ctx.complexity
        if cx.must_rewrite_lines:
            assert (
                "rewrite" in rb.risk_narrative.lower()
                or str(cx.must_rewrite_lines) in rb.risk_narrative
            )
        if cx.needs_review_lines:
            assert (
                "review" in rb.risk_narrative.lower()
                or str(cx.needs_review_lines) in rb.risk_narrative
            )

    def test_ai_summary_overrides_default_when_provided(self, base_ctx):
        rb = assemble(
            base_ctx,
            executive_summary="AI says so.",
            risk_narrative="AI risk text.",
            prompt_version="v1",
        )
        assert rb.executive_summary == "AI says so."
        assert rb.risk_narrative == "AI risk text."
        assert rb.prompt_version == "v1"

    def test_no_complexity_yields_safe_runbook(self):
        ctx = RunbookContext(project_name="x", customer="y", complexity=None, app_impact=None)
        rb = assemble(ctx)
        assert rb.phases and len(rb.phases) == 6
        assert rb.executive_summary
        assert rb.risk_narrative


class TestSignOffs:
    def test_includes_customer_and_depart_roles(self, base_ctx):
        rb = assemble(base_ctx)
        assert any("Engineering Lead" in s for s in rb.sign_offs)
        assert any("DBA" in s for s in rb.sign_offs)
        assert any("Depart" in s for s in rb.sign_offs)
        assert any(base_ctx.customer in s for s in rb.sign_offs)
