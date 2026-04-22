"""Tests for the runbook PDF renderer.

ReportLab compresses text streams by default; raw byte-substring checks
miss anything outside the document metadata. We use `pypdf` to extract
the actual rendered text and assert on that.
"""

import io
from pathlib import Path

import pytest
from pypdf import PdfReader

from src.analyze.complexity import analyze as analyze_complexity
from src.projects.pdf import render
from src.projects.runbook import RunbookContext, assemble


def _extract_text(pdf_bytes: bytes) -> str:
    reader = PdfReader(io.BytesIO(pdf_bytes))
    return "\n".join(p.extract_text() or "" for p in reader.pages)


FIXTURES = Path(__file__).parent / "fixtures" / "app_impact"


@pytest.fixture
def runbook():
    src = (FIXTURES / "schema" / "schema.sql").read_text()
    cx = analyze_complexity(src, rate_per_day=1500)
    ctx = RunbookContext(
        project_name="ACME Migration",
        customer="Acme Corp",
        source_version="Oracle 19c",
        target_version="PostgreSQL 16",
        cutover_window="2026-06-15 02:00 UTC",
        rate_per_day=1500,
        complexity=cx,
    )
    return assemble(
        ctx,
        executive_summary="Brief AI summary for ACME.",
        risk_narrative="AI risk paragraph one.\n\nAI risk paragraph two.",
        prompt_version="v-test",
    )


class TestRenderShape:
    def test_returns_bytes(self, runbook):
        out = render(runbook)
        assert isinstance(out, bytes)
        assert len(out) > 1000

    def test_pdf_magic_number(self, runbook):
        out = render(runbook)
        assert out.startswith(b"%PDF-")

    def test_includes_customer_name(self, runbook):
        text = _extract_text(render(runbook))
        assert "Acme Corp" in text

    def test_includes_section_headers(self, runbook):
        text = _extract_text(render(runbook))
        for header in (
            "Migration Runbook",
            "Executive Summary",
            "Risk Profile",
            "Migration Phases",
            "Cutover Blockers",
            "Approvals Required",
        ):
            assert header in text, f"PDF missing section: {header!r}"

    def test_phase_titles_appear(self, runbook):
        text = _extract_text(render(runbook))
        for token in ("Discovery", "Schema Conversion", "Cutover", "Stabilization"):
            assert token in text, f"PDF missing phase token: {token!r}"


class TestRenderHandlesSpecialCharacters:
    def test_html_metacharacters_in_content_dont_break_xml(self, runbook):
        runbook.executive_summary = "Tables with 5 < 10 rows & special <chars>."
        runbook.context = runbook.context.__class__(
            project_name="Greg <admin> & Co",
            customer="A & B Corp",
            source_version=runbook.context.source_version,
            target_version=runbook.context.target_version,
            cutover_window=runbook.context.cutover_window,
            rate_per_day=runbook.context.rate_per_day,
            complexity=runbook.context.complexity,
        )
        out = render(runbook)
        assert out.startswith(b"%PDF-")


class TestRenderWithoutComplexity:
    def test_renders_with_no_complexity(self):
        ctx = RunbookContext(
            project_name="Empty",
            customer="None Corp",
            source_version="Oracle 19c",
            target_version="PostgreSQL 16",
            cutover_window="TBD",
            rate_per_day=1500,
            complexity=None,
            app_impact=None,
        )
        rb = assemble(ctx)
        out = render(rb)
        assert out.startswith(b"%PDF-")
        assert "None Corp" in _extract_text(out)
