"""AI section generator for the migration runbook.

Wraps `projects.runbook.assemble()` with AI-generated executive summary
and risk narrative. Failures degrade gracefully — if the LLM call fails
or no API key is configured, we still produce a runbook with the
deterministic default sections.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional

from ...projects.runbook import Runbook, RunbookContext, assemble
from ..client import AIClient
from ..prompts.runbook import SYSTEM_PROMPT, VERSION, render_user_message

logger = logging.getLogger(__name__)


@dataclass
class RunbookGenerator:
    """Generates a Runbook with AI-narrated executive sections."""

    client: Optional[AIClient] = None

    def __post_init__(self) -> None:
        if self.client is None:
            self.client = AIClient.smart(feature="runbook")

    def generate(self, ctx: RunbookContext) -> Runbook:
        executive_summary = ""
        risk_narrative = ""
        try:
            user = render_user_message(
                ctx=ctx,
                complexity=ctx.complexity,
                app_impact=ctx.app_impact,
            )
            data = self.client.complete_json(system=SYSTEM_PROMPT, user=user)
            executive_summary = str(data.get("executive_summary", "")).strip()
            risk_narrative = str(data.get("risk_narrative", "")).strip()
        except Exception as e:
            logger.warning("AI runbook narrative failed: %s", e)

        return assemble(
            ctx,
            executive_summary=executive_summary,
            risk_narrative=risk_narrative,
            prompt_version=VERSION if executive_summary or risk_narrative else "",
        )


def generate_deterministic(ctx: RunbookContext) -> Runbook:
    """Convenience: Runbook with no AI sections — for tests and offline use."""
    return assemble(ctx)
