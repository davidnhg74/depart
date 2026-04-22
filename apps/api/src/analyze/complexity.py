"""Complexity analysis over IR.

Produces a ComplexityReport from a parsed Module. The score, tier counts, and
effort estimate are computed deterministically — no AI in this path. The
report is the analyzer-endpoint output; it also feeds the cost calculator and
the runbook generator.
"""

from __future__ import annotations

import math
from collections import Counter
from dataclasses import dataclass, field
from typing import Dict, List

from ..core.ir.nodes import (
    ConstructRef,
    Module,
    SchemaObject,
    Subprogram,
    Tier,
    TIER_FOR_TAG,
)
from ..source.oracle import parser as oracle_parser


# Lines-of-code weights per tier, used in both the score and the effort
# estimate. Owned here so the cost calculator and the deliverable PDF can
# import the same constants.
TIER_LOC_WEIGHT = {Tier.A: 1, Tier.B: 5, Tier.C: 20}

# Effort calibration (engineer-days per LOC). These are coarse industry
# averages; the cost calculator can override them per-engagement.
EFFORT_DAYS_PER_LOC = {
    Tier.A: 0.0001,  # 0.1 day per 1,000 lines (auto-converted)
    Tier.B: 0.005,  # 0.5 day per 100 lines (review + small fix)
    Tier.C: 0.02,  # 2.0 days per 100 lines (refactor)
}
MIN_EFFORT_DAYS = 0.5

# Heuristic LOC budget per tagged-construct occurrence. Used by the interim
# scorer because the interim parser can't yet attribute a construct to its
# enclosing PL/SQL block. The ANTLR pass replaces this with real per-object
# attribution. These numbers come from internal calibration on HR + OE +
# SH sample schemas — adjust when we have customer data.
LOC_PER_CONSTRUCT = {Tier.B: 10, Tier.C: 50}


@dataclass
class ComplexityReport:
    score: int
    total_lines: int
    auto_convertible_lines: int
    needs_review_lines: int
    must_rewrite_lines: int
    construct_counts: Dict[str, int]
    tier_b_constructs: List[str]
    tier_c_constructs: List[str]
    effort_estimate_days: float
    estimated_cost: float
    top_10_constructs: List[str]
    # Extras introduced with the IR-based scorer:
    objects_by_kind: Dict[str, int] = field(default_factory=dict)
    diagnostics_count: int = 0


def analyze(source: str, *, rate_per_day: int = 1000) -> ComplexityReport:
    """Parse Oracle source and produce a ComplexityReport.

    The legacy public surface (`ComplexityScorer().analyze(content)`) is
    preserved as a thin wrapper at the bottom of this module so existing API
    callers continue to work unchanged.
    """
    module = oracle_parser.parse(source)
    return _score(module, source, rate_per_day=rate_per_day)


def _score(module: Module, source: str, *, rate_per_day: int) -> ComplexityReport:
    total_lines = max(0, source.count("\n") + (1 if source.strip() else 0))

    construct_counter: Counter[str] = Counter()
    tier_b_names: List[str] = []
    tier_c_names: List[str] = []
    diag_count = 0
    tier_b_count = 0
    tier_c_count = 0

    for obj in module.objects:
        diag_count += len(obj.diagnostics)
        for ref in _construct_refs(obj):
            tier = TIER_FOR_TAG.get(ref.tag, Tier.A)
            construct_counter[ref.tag.value] += 1
            if tier == Tier.B:
                tier_b_count += 1
                if ref.tag.value not in tier_b_names:
                    tier_b_names.append(ref.tag.value)
            elif tier == Tier.C:
                tier_c_count += 1
                if ref.tag.value not in tier_c_names:
                    tier_c_names.append(ref.tag.value)

    # Heuristic line attribution: Tier C claims first (it dominates effort and
    # we don't want a single rare Tier C construct to be crowded out by many
    # Tier B's in a small snippet). Capped at total_lines per tier.
    # Replaced with real per-object attribution when ANTLR ships.
    c_lines = min(total_lines, tier_c_count * LOC_PER_CONSTRUCT[Tier.C])
    b_lines = min(max(0, total_lines - c_lines), tier_b_count * LOC_PER_CONSTRUCT[Tier.B])
    a_lines = max(0, total_lines - b_lines - c_lines)
    tier_lines: Dict[Tier, int] = {Tier.A: a_lines, Tier.B: b_lines, Tier.C: c_lines}

    score = _calc_score(total_lines, tier_lines, construct_counter)
    effort_days = _calc_effort_days(tier_lines)

    # Drop the synthetic "<module-constructs>" sentinel from the public count.
    real_objects = [o for o in module.objects if o.name != "<module-constructs>"]
    objects_by_kind = Counter(o.kind.value for o in real_objects)

    # `construct_counts` is the union of object-kind counts and tagged-construct
    # counts. The frontend and PDF generator expect keys like "PROCEDURE" and
    # "MERGE" side by side; keeping them in one map preserves the v1 API shape.
    merged_counts: Counter[str] = Counter()
    merged_counts.update(objects_by_kind)
    merged_counts.update(construct_counter)

    # "Top 10 hardest constructs" — Tier C first, then Tier B, by frequency.
    top_10 = _top_constructs(construct_counter, limit=10)

    return ComplexityReport(
        score=score,
        total_lines=total_lines,
        auto_convertible_lines=tier_lines[Tier.A],
        needs_review_lines=tier_lines[Tier.B],
        must_rewrite_lines=tier_lines[Tier.C],
        construct_counts=dict(merged_counts),
        tier_b_constructs=tier_b_names,
        tier_c_constructs=tier_c_names,
        effort_estimate_days=effort_days,
        estimated_cost=round(effort_days * rate_per_day, 2),
        top_10_constructs=top_10,
        objects_by_kind=dict(objects_by_kind),
        diagnostics_count=diag_count,
    )


def _construct_refs(obj: SchemaObject) -> List[ConstructRef]:
    if isinstance(obj, Subprogram):
        return list(obj.referenced_constructs)
    return []


def _tier_rank(t: Tier) -> int:
    return {Tier.A: 0, Tier.B: 1, Tier.C: 2}[t]


def _calc_score(total_lines: int, tier_lines: Dict[Tier, int], counts: Counter) -> int:
    """Bounded 1..100. Tier C dominates; log-scaled by file size."""
    weighted_loc = sum(tier_lines[t] * TIER_LOC_WEIGHT[t] for t in tier_lines)
    if total_lines <= 0:
        return 1
    density = weighted_loc / total_lines  # 1.0 = pure Tier A; >5 = lots of B; >20 = lots of C
    size_factor = math.log10(max(total_lines, 1)) * 5
    raw = density * 4 + size_factor
    # Per-construct presence boosts: Tier C constructs warrant attention even
    # when they only touch a few lines, because they typically need
    # architectural refactoring (autonomous tx, AQ, scheduler, ...).
    if tier_lines.get(Tier.C, 0) > 0:
        raw += 25
    if tier_lines.get(Tier.B, 0) > 0:
        raw += 5
    return max(1, min(100, int(raw)))


def _calc_effort_days(tier_lines: Dict[Tier, int]) -> float:
    if all(v == 0 for v in tier_lines.values()):
        return MIN_EFFORT_DAYS
    days = sum(tier_lines[t] * EFFORT_DAYS_PER_LOC[t] for t in tier_lines)
    return max(MIN_EFFORT_DAYS, round(days, 1))


def _top_constructs(counts: Counter, *, limit: int) -> List[str]:
    """Tier C first, then Tier B, then anything else, each ordered by count."""
    from ..core.ir.nodes import ConstructTag

    def tier_for(name: str) -> Tier:
        try:
            return TIER_FOR_TAG.get(ConstructTag(name), Tier.A)
        except ValueError:
            return Tier.A

    def sort_key(item):
        name, n = item
        return (-_tier_rank(tier_for(name)), -n, name)

    return [name for name, _ in sorted(counts.items(), key=sort_key)[:limit]]


# ─── Legacy compatibility shim ────────────────────────────────────────────────


class ComplexityScorer:
    """Back-compat wrapper. Existing API call sites use:

        ComplexityScorer().analyze(content, rate_per_day=...)

    Forward to the IR-based `analyze()` above."""

    def analyze(self, content: str, rate_per_day: int = 1000) -> ComplexityReport:
        return analyze(content, rate_per_day=rate_per_day)
