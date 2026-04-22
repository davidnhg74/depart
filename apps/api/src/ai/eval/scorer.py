"""Deterministic rule evaluation.

Each rule is independent and returns RuleResult(passed, detail). A case
passes only when ALL its rules pass.
"""
from __future__ import annotations

import json
from typing import Any, Dict, List, Tuple

from .types import RuleResult, ScoreRule


def evaluate(response: str, rules: List[ScoreRule]) -> List[RuleResult]:
    return [_eval_one(rule, response) for rule in rules]


def _eval_one(rule: ScoreRule, response: str) -> RuleResult:
    handler = _HANDLERS.get(rule.kind)
    if handler is None:
        return RuleResult(rule, False, f"unknown rule kind: {rule.kind}")
    try:
        passed, detail = handler(rule.config, response)
        return RuleResult(rule, passed, detail)
    except Exception as e:
        return RuleResult(rule, False, f"rule raised {type(e).__name__}: {e}")


# ─── handlers ────────────────────────────────────────────────────────────────


def _must_contain(needles: List[str], response: str) -> Tuple[bool, str]:
    lower = response.lower()
    missing = [n for n in needles if n.lower() not in lower]
    if missing:
        return False, f"missing: {missing}"
    return True, ""


def _must_not_contain(needles: List[str], response: str) -> Tuple[bool, str]:
    lower = response.lower()
    present = [n for n in needles if n.lower() in lower]
    if present:
        return False, f"present (forbidden): {present}"
    return True, ""


def _json_must_have_keys(keys: List[str], response: str) -> Tuple[bool, str]:
    parsed = _parse_json(response)
    if not isinstance(parsed, dict):
        return False, "response is not a JSON object"
    missing = [k for k in keys if k not in parsed]
    if missing:
        return False, f"missing keys: {missing}"
    return True, ""


def _json_path_equals(spec: Dict[str, Any], response: str) -> Tuple[bool, str]:
    parsed = _parse_json(response)
    fails: List[str] = []
    for path, expected in spec.items():
        actual = _walk_path(parsed, path)
        if actual != expected:
            fails.append(f"{path}: expected {expected!r}, got {actual!r}")
    if fails:
        return False, "; ".join(fails)
    return True, ""


def _json_array_min_len(spec: Dict[str, int], response: str) -> Tuple[bool, str]:
    parsed = _parse_json(response)
    fails: List[str] = []
    for path, min_len in spec.items():
        actual = _walk_path(parsed, path)
        if not isinstance(actual, list):
            fails.append(f"{path}: not a list ({type(actual).__name__})")
        elif len(actual) < min_len:
            fails.append(f"{path}: len {len(actual)} < {min_len}")
    if fails:
        return False, "; ".join(fails)
    return True, ""


def _max_chars(limit: int, response: str) -> Tuple[bool, str]:
    if len(response) > limit:
        return False, f"length {len(response)} > {limit}"
    return True, ""


def _min_chars(limit: int, response: str) -> Tuple[bool, str]:
    if len(response) < limit:
        return False, f"length {len(response)} < {limit}"
    return True, ""


_HANDLERS = {
    "must_contain":         _must_contain,
    "must_not_contain":     _must_not_contain,
    "json_must_have_keys":  _json_must_have_keys,
    "json_path_equals":     _json_path_equals,
    "json_array_min_len":   _json_array_min_len,
    "max_chars":            _max_chars,
    "min_chars":            _min_chars,
}


# ─── helpers ─────────────────────────────────────────────────────────────────


def _parse_json(response: str) -> Any:
    """Parse JSON, tolerating ```json fences (mirrors AIClient.complete_json)."""
    stripped = response.strip()
    if stripped.startswith("```"):
        # Drop opening fence (with or without language tag) and closing fence.
        stripped = stripped.split("\n", 1)[1] if "\n" in stripped else stripped[3:]
        if stripped.endswith("```"):
            stripped = stripped[:-3]
        stripped = stripped.strip()
    return json.loads(stripped)


def _walk_path(obj: Any, dotted: str) -> Any:
    """Navigate `a.b.c` through dicts (and `a.0` through lists)."""
    cur = obj
    for part in dotted.split("."):
        if isinstance(cur, dict):
            cur = cur.get(part)
        elif isinstance(cur, list):
            try:
                cur = cur[int(part)]
            except (ValueError, IndexError):
                return None
        else:
            return None
    return cur
