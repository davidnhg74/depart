"""Per-column data masking for migration runs.

Applied between "read batch from source" and "write batch to target"
in the Runner's copy loop. Rules live on the MigrationRecord as
JSON; this module parses, validates, and builds a per-batch
transform function the Runner injects.

Strategies in v1:
  * ``null``     — replace with None
  * ``fixed``    — replace with a constant (default "[REDACTED]")
  * ``hash``     — HMAC-SHA256 hex, deterministic so equal inputs
                   produce equal outputs (FK integrity preserved
                   across tables that share a value)
  * ``partial``  — keep first N + last M chars, mask the middle
  * ``regex``    — pattern + replacement (re.sub semantics)

The hash strategy's HMAC key comes from ``HAFEN_MASKING_KEY`` if set,
else falls back to ``HAFEN_ENCRYPTION_KEY``. If neither is set, the
transform raises at build time — fail-fast is the correct mode for
a "redact PII" operation.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import os
import re
from typing import Any, Callable, Sequence


logger = logging.getLogger(__name__)


# Avoid importing TableSpec at module load — callers pass it in, and
# the runner module imports us. `Any` keeps the contract clear in
# signatures without creating an import cycle.
RowTransform = Callable[[list[tuple], Any], list[tuple]]


VALID_STRATEGIES = {"null", "fixed", "hash", "partial", "regex"}
DEFAULT_HASH_LENGTH = 32
DEFAULT_MASK_CHAR = "*"
DEFAULT_FIXED_VALUE = "[REDACTED]"


# ─── Validation ──────────────────────────────────────────────────────


def validate_rules(rules: dict) -> None:
    """Raise ValueError if `rules` doesn't match the expected shape.
    Called on write so operators get a useful 400 instead of a later
    runtime crash inside the runner."""
    if not isinstance(rules, dict):
        raise ValueError("masking_rules must be an object mapping table→columns")
    for table_key, col_rules in rules.items():
        if not isinstance(table_key, str) or not table_key:
            raise ValueError(f"table key must be non-empty string, got {table_key!r}")
        if not isinstance(col_rules, dict):
            raise ValueError(
                f"rules for {table_key!r} must be an object mapping column→rule"
            )
        for col_name, rule in col_rules.items():
            if not isinstance(col_name, str) or not col_name:
                raise ValueError(
                    f"{table_key}: column key must be non-empty string, got {col_name!r}"
                )
            if not isinstance(rule, dict):
                raise ValueError(
                    f"{table_key}.{col_name}: rule must be an object, got {type(rule).__name__}"
                )
            strategy = rule.get("strategy")
            if strategy not in VALID_STRATEGIES:
                raise ValueError(
                    f"{table_key}.{col_name}: strategy must be one of "
                    f"{sorted(VALID_STRATEGIES)}, got {strategy!r}"
                )
            _validate_strategy_opts(f"{table_key}.{col_name}", strategy, rule)


def _validate_strategy_opts(path: str, strategy: str, rule: dict) -> None:
    if strategy == "partial":
        kf = rule.get("keep_first", 0)
        kl = rule.get("keep_last", 0)
        if not isinstance(kf, int) or kf < 0:
            raise ValueError(f"{path}: keep_first must be a non-negative int")
        if not isinstance(kl, int) or kl < 0:
            raise ValueError(f"{path}: keep_last must be a non-negative int")
    elif strategy == "regex":
        pattern = rule.get("pattern")
        if not isinstance(pattern, str) or not pattern:
            raise ValueError(f"{path}: regex requires a non-empty 'pattern'")
        try:
            re.compile(pattern)
        except re.error as exc:
            raise ValueError(f"{path}: invalid regex pattern: {exc}") from exc
    elif strategy == "hash":
        length = rule.get("length", DEFAULT_HASH_LENGTH)
        if not isinstance(length, int) or not (1 <= length <= 64):
            raise ValueError(f"{path}: hash length must be 1..64, got {length!r}")


# ─── Strategy implementations ────────────────────────────────────────


def _mask_null(_value: Any, _rule: dict, _key: bytes) -> Any:
    return None


def _mask_fixed(_value: Any, rule: dict, _key: bytes) -> Any:
    return rule.get("value", DEFAULT_FIXED_VALUE)


def _mask_hash(value: Any, rule: dict, key: bytes) -> Any:
    if value is None:
        return None
    raw = str(value).encode("utf-8")
    digest = hmac.new(key, raw, hashlib.sha256).hexdigest()
    length = rule.get("length", DEFAULT_HASH_LENGTH)
    return digest[:length]


def _mask_partial(value: Any, rule: dict, _key: bytes) -> Any:
    if value is None:
        return None
    s = str(value)
    kf = rule.get("keep_first", 0)
    kl = rule.get("keep_last", 0)
    mask_char = rule.get("mask_char", DEFAULT_MASK_CHAR) or DEFAULT_MASK_CHAR
    # If the input is shorter than keep_first + keep_last, mask the whole
    # thing — never leak more than the operator asked to keep.
    if len(s) <= kf + kl:
        return mask_char * len(s)
    middle_len = len(s) - kf - kl
    return s[:kf] + (mask_char * middle_len) + (s[-kl:] if kl else "")


def _mask_regex(value: Any, rule: dict, _key: bytes) -> Any:
    if value is None:
        return None
    return re.sub(rule["pattern"], rule.get("replacement", ""), str(value))


_STRATEGY_FNS = {
    "null": _mask_null,
    "fixed": _mask_fixed,
    "hash": _mask_hash,
    "partial": _mask_partial,
    "regex": _mask_regex,
}


# ─── Key resolution ──────────────────────────────────────────────────


def _resolve_hmac_key() -> bytes:
    for var in ("HAFEN_MASKING_KEY", "HAFEN_ENCRYPTION_KEY"):
        raw = os.environ.get(var)
        if raw:
            return raw.encode("utf-8")
    raise RuntimeError(
        "data masking requires HAFEN_MASKING_KEY (preferred) or "
        "HAFEN_ENCRYPTION_KEY to be set — neither is configured"
    )


def _rules_need_hmac(rules: dict) -> bool:
    for col_rules in rules.values():
        for rule in col_rules.values():
            if rule.get("strategy") == "hash":
                return True
    return False


# ─── Transform factory ───────────────────────────────────────────────


def build_row_transform(rules: dict) -> RowTransform:
    """Compile `rules` into a fast per-batch transform function.

    The returned callable takes a batch (list of tuples) and a
    TableSpec; it returns a new batch with masked columns replaced.
    Column lookups and strategy-function dispatch are resolved once
    per spec the first time we see it — subsequent batches for the
    same table skip the lookup.
    """
    validate_rules(rules)
    key = _resolve_hmac_key() if _rules_need_hmac(rules) else b""

    # Cache: qualified_name → list of (col_index, strategy_fn, rule_dict)
    # Populated lazily the first time a spec flows through; invariant
    # across batches of the same table.
    per_table_cache: dict[str, list[tuple[int, Callable, dict]] | None] = {}

    def _prepare(qualified: str, columns: list[str]) -> list[tuple[int, Callable, dict]] | None:
        col_rules = rules.get(qualified)
        if not col_rules:
            return None
        plan: list[tuple[int, Callable, dict]] = []
        for col_name, rule in col_rules.items():
            try:
                idx = columns.index(col_name)
            except ValueError:
                # Rule references a column that doesn't exist in this
                # table — log and skip rather than crash. Common during
                # schema drift; operators can clean up later.
                logger.warning(
                    "masking rule for %s.%s refers to missing column; skipping",
                    qualified,
                    col_name,
                )
                continue
            fn = _STRATEGY_FNS[rule["strategy"]]
            plan.append((idx, fn, rule))
        return plan or None

    def transform(batch: list[tuple], spec) -> list[tuple]:
        qualified = spec.source_table.qualified()
        if qualified not in per_table_cache:
            per_table_cache[qualified] = _prepare(qualified, list(spec.columns))
        plan = per_table_cache[qualified]
        if plan is None:
            return batch

        out: list[tuple] = []
        for row in batch:
            mutable = list(row)
            for idx, fn, rule in plan:
                mutable[idx] = fn(mutable[idx], rule, key)
            out.append(tuple(mutable))
        return out

    return transform


# ─── Persistence helpers ─────────────────────────────────────────────


def load_rules_from_text(raw: str | None) -> dict:
    """Parse the JSON stored on MigrationRecord.masking_rules. Empty
    text → no rules."""
    if not raw:
        return {}
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"masking_rules is not valid JSON: {exc}") from exc
    if not isinstance(data, dict):
        raise ValueError("masking_rules JSON must decode to an object")
    return data


def dump_rules_to_text(rules: dict) -> str:
    return json.dumps(rules, sort_keys=True)
