"""Anthropic client wrapper.

Owns model selection, prompt caching, retries, telemetry, and JSON
output parsing in one place. Every AI feature in the platform routes
through this — no module instantiates `anthropic.Anthropic` directly.

The legacy `src/llm/client.py` continues to work for paths that haven't
migrated; new code MUST use `from src.ai.client import AIClient`.

Model selection:
  * `AIClient(model=...)`  — explicit per-instance.
  * `AIClient.fast()`      — cheap+fast (Haiku) for explanations / classification.
  * `AIClient.smart()`     — high-quality (Opus) for plan generation / refactoring.
  * Default reads `ANTHROPIC_MODEL` env or falls back to a current Sonnet.

Prompt caching:
  * `cacheable=True` on `system` or `messages[].content` blocks attaches
    the cache_control marker the SDK expects. The wrapper inserts the
    block correctly so callers don't have to remember the schema.

Retries:
  * Anthropic SDK retries 5xx + rate-limit errors with exponential
    backoff via `max_retries` (default 5 here).

Token telemetry:
  * Every call records (model, prompt_tokens, completion_tokens, latency_ms)
    on the singleton TokenLedger; the cost calculator and the per-project
    audit trail read from it.
"""

from __future__ import annotations

import json
import logging
import os
import re
import time
from dataclasses import dataclass
from threading import Lock
from typing import Any, Dict, List, Optional

from anthropic import Anthropic, APIError, APIStatusError

from ..config import settings

logger = logging.getLogger(__name__)


# Current generation defaults — bumped here when newer models ship.
# Model IDs intentionally hard-coded; per-engagement override via env.
DEFAULT_MODEL = os.getenv("ANTHROPIC_MODEL", "claude-sonnet-4-6")
FAST_MODEL = os.getenv("ANTHROPIC_FAST_MODEL", "claude-haiku-4-5-20251001")
SMART_MODEL = os.getenv("ANTHROPIC_SMART_MODEL", "claude-opus-4-7")
DEFAULT_MAX_TOKENS = 4096
DEFAULT_MAX_RETRIES = 5


@dataclass
class TokenUsage:
    model: str
    input_tokens: int
    output_tokens: int
    cache_read_input_tokens: int = 0
    cache_creation_input_tokens: int = 0
    latency_ms: float = 0.0
    feature: str = ""  # "app_impact", "semantic", ...

    def total(self) -> int:
        return self.input_tokens + self.output_tokens


class TokenLedger:
    """Process-wide token accounting. Thread-safe."""

    def __init__(self) -> None:
        self._lock = Lock()
        self._records: List[TokenUsage] = []

    def record(self, u: TokenUsage) -> None:
        with self._lock:
            self._records.append(u)

    def all(self) -> List[TokenUsage]:
        with self._lock:
            return list(self._records)

    def reset(self) -> None:
        with self._lock:
            self._records.clear()

    def by_feature(self, feature: str) -> List[TokenUsage]:
        with self._lock:
            return [r for r in self._records if r.feature == feature]


_LEDGER = TokenLedger()


def get_ledger() -> TokenLedger:
    return _LEDGER


# ─── Client ──────────────────────────────────────────────────────────────────


@dataclass
class AIClient:
    """Thin wrapper around `anthropic.Anthropic` with our defaults."""

    model: str = DEFAULT_MODEL
    max_tokens: int = DEFAULT_MAX_TOKENS
    max_retries: int = DEFAULT_MAX_RETRIES
    feature: str = "default"  # tags telemetry for cost attribution
    api_key: Optional[str] = None

    def __post_init__(self) -> None:
        key = self.api_key or settings.anthropic_api_key
        if not key:
            raise RuntimeError(
                "ANTHROPIC_API_KEY is not configured. Set it in .env or pass api_key= explicitly."
            )
        self._client = Anthropic(api_key=key, max_retries=self.max_retries)

    # ─── Constructors for common configs ─────────────────────────────────────

    @classmethod
    def fast(cls, *, feature: str = "default", **kwargs) -> "AIClient":
        return cls(model=FAST_MODEL, feature=feature, **kwargs)

    @classmethod
    def smart(cls, *, feature: str = "default", **kwargs) -> "AIClient":
        return cls(model=SMART_MODEL, feature=feature, **kwargs)

    # ─── Calls ───────────────────────────────────────────────────────────────

    def complete(
        self,
        *,
        system: str,
        user: str,
        cache_system: bool = True,
        max_tokens: Optional[int] = None,
    ) -> str:
        """One-shot completion. Returns the assistant's text content.

        `cache_system=True` attaches `cache_control` to the system prompt so
        repeated calls in the same window pay cache-read pricing.
        """
        sys_blocks = self._maybe_cache(system, cacheable=cache_system)
        return self._call(
            system_blocks=sys_blocks, user=user, max_tokens=max_tokens or self.max_tokens
        )

    def complete_json(
        self,
        *,
        system: str,
        user: str,
        cache_system: bool = True,
        max_tokens: Optional[int] = None,
    ) -> Any:
        """Same as `complete` but parses the response as JSON.

        Tolerates surrounding ```json ... ``` fences. Raises ValueError on
        unparseable output rather than retrying with a "fix your JSON"
        round-trip — JSON quality is the prompt's responsibility.
        """
        text = self.complete(
            system=system, user=user, cache_system=cache_system, max_tokens=max_tokens
        )
        return _parse_json(text)

    # ─── Internals ───────────────────────────────────────────────────────────

    def _call(self, *, system_blocks: List[Dict[str, Any]], user: str, max_tokens: int) -> str:
        t0 = time.perf_counter()
        try:
            msg = self._client.messages.create(
                model=self.model,
                max_tokens=max_tokens,
                system=system_blocks,
                messages=[{"role": "user", "content": user}],
            )
        except APIStatusError as e:
            logger.error("Anthropic API error %s: %s", e.status_code, e.message)
            raise
        except APIError as e:
            logger.error("Anthropic API error: %s", e)
            raise
        latency = (time.perf_counter() - t0) * 1000.0
        usage = getattr(msg, "usage", None)
        _LEDGER.record(
            TokenUsage(
                model=self.model,
                input_tokens=getattr(usage, "input_tokens", 0) or 0,
                output_tokens=getattr(usage, "output_tokens", 0) or 0,
                cache_read_input_tokens=getattr(usage, "cache_read_input_tokens", 0) or 0,
                cache_creation_input_tokens=getattr(usage, "cache_creation_input_tokens", 0) or 0,
                latency_ms=latency,
                feature=self.feature,
            )
        )
        # `msg.content` is a list of content blocks; we expect text.
        for block in msg.content:
            if getattr(block, "type", None) == "text":
                return block.text
        return ""

    @staticmethod
    def _maybe_cache(text: str, *, cacheable: bool) -> List[Dict[str, Any]]:
        block: Dict[str, Any] = {"type": "text", "text": text}
        if cacheable:
            block["cache_control"] = {"type": "ephemeral"}
        return [block]


# ─── helpers ─────────────────────────────────────────────────────────────────


_FENCE_RX = re.compile(r"^```(?:json)?\s*\n?(.*?)\n?```\s*$", re.DOTALL)


def _parse_json(text: str) -> Any:
    """Strip ```json fences and parse. Raises ValueError on bad JSON."""
    if not text:
        raise ValueError("Empty model response")
    stripped = text.strip()
    m = _FENCE_RX.match(stripped)
    if m:
        stripped = m.group(1).strip()
    try:
        return json.loads(stripped)
    except json.JSONDecodeError as e:
        raise ValueError(f"Model returned non-JSON output: {e}\nText was: {text[:500]}")
