"""Eval harness data types.

A *suite* is the corpus + metadata for one versioned prompt. A *case* is
one (input, expected) tuple with the rules that must pass. A *result* is
what the runner produces per case after invoking the LLM.

Rule shapes (machine-friendly; kept small so corpus JSONL stays readable):

  must_contain:        list[str]   — substrings (case-insensitive)
  must_not_contain:    list[str]   — substrings (case-insensitive)
  json_must_have_keys: list[str]   — top-level keys present in parsed JSON
  json_path_equals:    dict        — { "a.b.c": expected_value, ... }
  json_array_min_len:  dict        — { "a.b": min_int, ... }
  max_chars:           int         — overall response length cap
  min_chars:           int
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass(frozen=True)
class ScoreRule:
    """One assertion against an LLM response."""

    kind: str
    config: Any

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "ScoreRule":
        if len(d) != 1:
            raise ValueError(f"ScoreRule dict must have exactly one key, got {list(d)}")
        kind, config = next(iter(d.items()))
        return cls(kind=kind, config=config)


@dataclass(frozen=True)
class EvalCase:
    """A single (input, expected) pair."""

    id: str
    inputs: Dict[str, Any]  # arguments passed to the prompt's render() function
    rules: List[ScoreRule]
    description: str = ""

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "EvalCase":
        return cls(
            id=d["id"],
            inputs=d.get("inputs", {}),
            rules=[ScoreRule.from_dict(r) for r in d.get("rules", [])],
            description=d.get("description", ""),
        )


@dataclass(frozen=True)
class RuleResult:
    rule: ScoreRule
    passed: bool
    detail: str = ""


@dataclass
class CaseResult:
    case_id: str
    passed: bool
    response: str
    rule_results: List[RuleResult] = field(default_factory=list)
    error: Optional[str] = None
    latency_ms: float = 0.0


@dataclass
class EvalResult:
    """All cases for one suite run."""

    prompt_id: str
    prompt_version: str
    model: str
    cases: List[CaseResult] = field(default_factory=list)

    @property
    def passed(self) -> int:
        return sum(1 for c in self.cases if c.passed)

    @property
    def failed(self) -> int:
        return sum(1 for c in self.cases if not c.passed)

    @property
    def total(self) -> int:
        return len(self.cases)

    @property
    def pass_rate(self) -> float:
        return self.passed / self.total if self.total else 0.0


@dataclass(frozen=True)
class EvalSuite:
    """Loaded corpus for one prompt id."""

    prompt_id: str
    cases: List[EvalCase]
