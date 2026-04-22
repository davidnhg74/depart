"""AI prompt evaluation harness.

Every versioned prompt has a corpus of `(input, expected)` cases and a
list of deterministic scoring rules. The harness runs the prompt against
the corpus, scores responses, and reports pass/fail per case. A failing
score on a kept-stable case is the signal that a prompt edit has
regressed quality — stop the merge.

Layout:
  src/ai/eval/
    types.py        — EvalCase, EvalResult, ScoreRule
    scorer.py       — scoring rule implementations
    runner.py       — load corpus, run prompt, collect scores
    corpus/<id>/    — per-prompt fixtures (cases.jsonl)
    cli.py          — `python -m src.ai.eval <prompt_id>`
"""
from .types import EvalCase, EvalResult, EvalSuite, RuleResult, ScoreRule  # noqa: F401
from .runner import EvalRunner, load_suite  # noqa: F401
