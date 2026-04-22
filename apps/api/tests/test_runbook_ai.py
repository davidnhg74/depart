"""Tests for the AI runbook narrative generator."""

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from src.ai.services.runbook import RunbookGenerator, generate_deterministic
from src.analyze.complexity import analyze as analyze_complexity
from src.projects.runbook import Runbook, RunbookContext


FIXTURES = Path(__file__).parent / "fixtures" / "app_impact"


@pytest.fixture
def ctx():
    src = (FIXTURES / "schema" / "schema.sql").read_text()
    cx = analyze_complexity(src, rate_per_day=1500)
    return RunbookContext(
        project_name="ACME",
        customer="ACME Corp",
        source_version="Oracle 19c",
        target_version="PostgreSQL 16",
        cutover_window="2026-06-15 02:00 UTC",
        rate_per_day=1500,
        complexity=cx,
    )


class TestRunbookGenerator:
    def test_uses_ai_text_when_call_succeeds(self, ctx):
        client = MagicMock()
        client.complete_json.return_value = {
            "executive_summary": "AI exec summary.",
            "risk_narrative": "AI risk paragraph one.\n\nParagraph two.",
        }
        rb = RunbookGenerator(client=client).generate(ctx)
        assert isinstance(rb, Runbook)
        assert rb.executive_summary == "AI exec summary."
        assert "Paragraph two." in rb.risk_narrative
        assert rb.prompt_version  # populated when AI was used

    def test_falls_back_to_default_on_llm_failure(self, ctx):
        client = MagicMock()
        client.complete_json.side_effect = RuntimeError("API down")
        rb = RunbookGenerator(client=client).generate(ctx)
        # Defaults still populated; prompt_version cleared (no AI was used).
        assert rb.executive_summary
        assert rb.risk_narrative
        assert rb.prompt_version == ""

    def test_partial_ai_response_only_overrides_provided_keys(self, ctx):
        client = MagicMock()
        client.complete_json.return_value = {
            "executive_summary": "Only summary, no risk text.",
        }
        rb = RunbookGenerator(client=client).generate(ctx)
        assert rb.executive_summary == "Only summary, no risk text."
        # risk_narrative was empty in the response — assemble() falls back
        # to the deterministic default.
        assert rb.risk_narrative


class TestGenerateDeterministic:
    def test_no_ai_call_made(self, ctx):
        # Convenience wrapper: no client, no LLM, just deterministic.
        rb = generate_deterministic(ctx)
        assert rb.executive_summary
        assert rb.risk_narrative
        assert rb.prompt_version == ""
