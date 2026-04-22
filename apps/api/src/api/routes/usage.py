"""Token usage / cost endpoint.

GET /api/v3/usage/summary
  Returns aggregated token usage from the in-process TokenLedger plus a
  cost estimate using current per-model Anthropic pricing. Resets when
  the API process restarts — persistence to the migrations DB is the
  next step (one project = one engagement rolls up cleanly there).

Pricing constants are kept here, not in the LLM client, because the
client cares about correctness while this module cares about $$. Bump
PRICING when Anthropic publishes new rates.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional

from fastapi import APIRouter
from pydantic import BaseModel, Field

from ...ai.client import TokenUsage, get_ledger


router = APIRouter(prefix="/api/v3/usage", tags=["usage"])


# ─── Pricing (USD per million tokens, as of 2026-04) ─────────────────────────
#
# Updated when Anthropic publishes new rates. Prefix-matched so we don't
# need an exact-version table — `claude-sonnet-4-6-20251015` matches
# `claude-sonnet-4-6`. Cache-read is ~10% of base input; cache-write is
# 25% above base input.

@dataclass(frozen=True)
class Rates:
    input_per_million: float
    output_per_million: float
    cache_read_per_million: float
    cache_write_per_million: float


PRICING: List[tuple] = [
    # (model-id-prefix, Rates)
    ("claude-haiku-4",  Rates(0.25,  1.25,  0.025,  0.30)),
    ("claude-sonnet-4", Rates(3.00, 15.00,  0.30,   3.75)),
    ("claude-opus-4",   Rates(15.00, 75.00, 1.50,   18.75)),
]
FALLBACK_RATES = Rates(3.00, 15.00, 0.30, 3.75)      # mid-tier guess


def rates_for(model: str) -> Rates:
    for prefix, rates in PRICING:
        if model.startswith(prefix):
            return rates
    return FALLBACK_RATES


def estimate_cost(usage: TokenUsage) -> float:
    """USD cost of one TokenUsage record, rounded to 6 decimals."""
    r = rates_for(usage.model)
    cost = (
        usage.input_tokens          * r.input_per_million          +
        usage.output_tokens         * r.output_per_million         +
        usage.cache_read_input_tokens     * r.cache_read_per_million +
        usage.cache_creation_input_tokens * r.cache_write_per_million
    ) / 1_000_000.0
    return round(cost, 6)


# ─── Response shape ──────────────────────────────────────────────────────────


class FeatureUsageDTO(BaseModel):
    feature: str
    calls: int
    input_tokens: int
    output_tokens: int
    cache_read_input_tokens: int = 0
    cache_creation_input_tokens: int = 0
    avg_latency_ms: float = 0.0
    estimated_cost_usd: float = 0.0


class ModelUsageDTO(BaseModel):
    model: str
    calls: int
    input_tokens: int
    output_tokens: int
    estimated_cost_usd: float = 0.0


class UsageSummaryDTO(BaseModel):
    total_calls: int
    total_input_tokens: int
    total_output_tokens: int
    total_cache_read_tokens: int
    total_cache_creation_tokens: int
    total_estimated_cost_usd: float
    by_feature: List[FeatureUsageDTO] = Field(default_factory=list)
    by_model: List[ModelUsageDTO] = Field(default_factory=list)


# ─── Route ───────────────────────────────────────────────────────────────────


@router.get("/summary", response_model=UsageSummaryDTO)
def usage_summary() -> UsageSummaryDTO:
    return summarize(get_ledger().all())


# ─── Pure aggregation (importable for tests) ─────────────────────────────────


def summarize(records: List[TokenUsage]) -> UsageSummaryDTO:
    total = UsageSummaryDTO(
        total_calls=len(records),
        total_input_tokens=sum(r.input_tokens for r in records),
        total_output_tokens=sum(r.output_tokens for r in records),
        total_cache_read_tokens=sum(r.cache_read_input_tokens for r in records),
        total_cache_creation_tokens=sum(r.cache_creation_input_tokens for r in records),
        total_estimated_cost_usd=round(sum(estimate_cost(r) for r in records), 6),
    )
    total.by_feature = _aggregate_by_feature(records)
    total.by_model = _aggregate_by_model(records)
    return total


def _aggregate_by_feature(records: List[TokenUsage]) -> List[FeatureUsageDTO]:
    buckets: Dict[str, List[TokenUsage]] = {}
    for r in records:
        buckets.setdefault(r.feature or "default", []).append(r)
    out: List[FeatureUsageDTO] = []
    for feature, rs in sorted(buckets.items()):
        total_latency = sum(r.latency_ms for r in rs)
        out.append(FeatureUsageDTO(
            feature=feature,
            calls=len(rs),
            input_tokens=sum(r.input_tokens for r in rs),
            output_tokens=sum(r.output_tokens for r in rs),
            cache_read_input_tokens=sum(r.cache_read_input_tokens for r in rs),
            cache_creation_input_tokens=sum(r.cache_creation_input_tokens for r in rs),
            avg_latency_ms=round(total_latency / len(rs), 2) if rs else 0.0,
            estimated_cost_usd=round(sum(estimate_cost(r) for r in rs), 6),
        ))
    return out


def _aggregate_by_model(records: List[TokenUsage]) -> List[ModelUsageDTO]:
    buckets: Dict[str, List[TokenUsage]] = {}
    for r in records:
        buckets.setdefault(r.model, []).append(r)
    out: List[ModelUsageDTO] = []
    for model, rs in sorted(buckets.items()):
        out.append(ModelUsageDTO(
            model=model,
            calls=len(rs),
            input_tokens=sum(r.input_tokens for r in rs),
            output_tokens=sum(r.output_tokens for r in rs),
            estimated_cost_usd=round(sum(estimate_cost(r) for r in rs), 6),
        ))
    return out
