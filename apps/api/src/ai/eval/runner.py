"""Eval-suite loader + runner.

`load_suite("app_impact")` reads `src/ai/eval/corpus/app_impact/cases.jsonl`
into an `EvalSuite`. `EvalRunner(callable)` runs each case through a
caller-supplied invocation function (so the same runner exercises both
real LLM calls and mocked ones in unit tests).

Why is invocation injected? The eval harness should be agnostic to which
prompt it's evaluating — `app_impact`, `runbook`, or future prompts each
render their input differently before calling the model. The caller
provides a small adapter `(case.inputs) -> response_str` and the runner
handles the rest (timing, scoring, aggregation).
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Dict, List, Optional

from .scorer import evaluate
from .types import CaseResult, EvalCase, EvalResult, EvalSuite


CORPUS_ROOT = Path(__file__).parent / "corpus"


def load_suite(prompt_id: str, *, root: Optional[Path] = None) -> EvalSuite:
    """Load a suite from `<root>/<prompt_id>/cases.jsonl`."""
    base = (root or CORPUS_ROOT) / prompt_id
    path = base / "cases.jsonl"
    if not path.exists():
        raise FileNotFoundError(f"No corpus at {path}")
    cases: List[EvalCase] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("//"):
            continue
        cases.append(EvalCase.from_dict(json.loads(line)))
    if not cases:
        raise ValueError(f"Corpus {path} has no cases")
    return EvalSuite(prompt_id=prompt_id, cases=cases)


# Adapter type: takes the case's raw `inputs` dict and returns the model's
# response string. The eval harness scores the response against the case's
# rules; the adapter is responsible for prompt rendering + model invocation.
Invoker = Callable[[Dict[str, object]], str]


@dataclass
class EvalRunner:
    invoke: Invoker
    prompt_version: str = ""
    model: str = ""

    def run(self, suite: EvalSuite) -> EvalResult:
        out = EvalResult(
            prompt_id=suite.prompt_id,
            prompt_version=self.prompt_version,
            model=self.model,
        )
        for case in suite.cases:
            out.cases.append(self._run_one(case))
        return out

    def _run_one(self, case: EvalCase) -> CaseResult:
        t0 = time.perf_counter()
        try:
            response = self.invoke(case.inputs)
        except Exception as e:
            return CaseResult(
                case_id=case.id,
                passed=False,
                response="",
                error=f"{type(e).__name__}: {e}",
                latency_ms=(time.perf_counter() - t0) * 1000.0,
            )
        rule_results = evaluate(response, case.rules)
        passed = all(r.passed for r in rule_results)
        return CaseResult(
            case_id=case.id,
            passed=passed,
            response=response,
            rule_results=rule_results,
            latency_ms=(time.perf_counter() - t0) * 1000.0,
        )


def format_report(result: EvalResult) -> str:
    """Plain-text report. Used by the CLI; tests assert on its shape."""
    lines = [
        f"Eval: {result.prompt_id}  version={result.prompt_version!r}  model={result.model!r}",
        f"Pass rate: {result.passed}/{result.total} ({result.pass_rate * 100:.1f}%)",
        "",
    ]
    for c in result.cases:
        flag = "PASS" if c.passed else "FAIL"
        lines.append(f"[{flag}] {c.case_id}  ({c.latency_ms:.0f} ms)")
        if c.error:
            lines.append(f"   error: {c.error}")
        for r in c.rule_results:
            if not r.passed:
                lines.append(f"   - {r.rule.kind}: {r.detail}")
    return "\n".join(lines)
