"""CLI entry point for the eval harness.

  python -m src.ai.eval app_impact          # run app_impact suite
  python -m src.ai.eval runbook             # run runbook suite
  python -m src.ai.eval app_impact --dry    # mock LLM (returns "{}") — fast
                                            # smoke test that suite + scorer
                                            # parse without API calls

Real runs require ANTHROPIC_API_KEY. Output is plain text via
runner.format_report(); exit code is 0 on full pass, 1 on any failure.
"""
from __future__ import annotations

import argparse
import json
import sys
from typing import Dict

from .runner import EvalRunner, format_report, load_suite


def main(argv: list = None) -> int:
    p = argparse.ArgumentParser(description="Run an AI prompt eval suite.")
    p.add_argument("prompt_id", help="Suite name (matches src/ai/eval/corpus/<id>/)")
    p.add_argument("--dry", action="store_true", help="Mock the LLM (returns empty JSON)")
    args = p.parse_args(argv)

    suite = load_suite(args.prompt_id)

    if args.dry:
        invoker = lambda inputs: "{}"
        runner = EvalRunner(invoke=invoker, prompt_version="dry", model="dry-run")
    else:
        invoker, version, model = _build_invoker(args.prompt_id)
        runner = EvalRunner(invoke=invoker, prompt_version=version, model=model)

    result = runner.run(suite)
    print(format_report(result))
    return 0 if result.failed == 0 else 1


def _build_invoker(prompt_id: str):
    """Return (invoke_fn, prompt_version, model) for the named prompt."""
    if prompt_id == "app_impact":
        from ..client import AIClient
        from ..prompts.app_impact import SYSTEM_PROMPT, VERSION, render_user_message
        client = AIClient.fast(feature="eval.app_impact")

        def invoke(inputs: Dict) -> str:
            user = render_user_message(
                schema_summary=inputs.get("schema_summary", ""),
                findings=inputs.get("findings", []),
            )
            return client.complete(system=SYSTEM_PROMPT, user=user)

        return invoke, VERSION, client.model

    if prompt_id == "runbook":
        from types import SimpleNamespace
        from ..client import AIClient
        from ..prompts.runbook import SYSTEM_PROMPT, VERSION, render_user_message
        client = AIClient.smart(feature="eval.runbook")

        def invoke(inputs: Dict) -> str:
            ctx = SimpleNamespace(**{
                k: inputs.get(k) for k in (
                    "project_name", "customer", "source_version", "target_version",
                    "cutover_window", "rate_per_day",
                )
            })
            cx = SimpleNamespace(**inputs["complexity"]) if inputs.get("complexity") else None
            ai = SimpleNamespace(**inputs["app_impact"]) if inputs.get("app_impact") else None
            user = render_user_message(ctx=ctx, complexity=cx, app_impact=ai)
            return client.complete(system=SYSTEM_PROMPT, user=user)

        return invoke, VERSION, client.model

    raise ValueError(f"No invoker registered for prompt {prompt_id!r}")


if __name__ == "__main__":
    sys.exit(main())
