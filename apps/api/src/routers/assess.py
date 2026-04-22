"""Public `/assess` endpoint — the top-of-funnel for depart.cloud.

Deliberately minimal: take raw DDL/PL-SQL as a JSON body, run the
parser + complexity scorer, and return everything the assessment UI
needs in a single response. No auth, no storage, no rate-limiting
(yet) — the whole point is "paste DDL, see a report, no signup."

The existing `POST /api/v1/analyze` endpoint is a heavier legacy flow
(zip upload, Lead record, PDF generation, plan-limit checks). `/assess`
skips all of that so we can hit the EDB-Migration-Portal-killer bar
of time-to-insight < 5 seconds.
"""

from __future__ import annotations

from typing import Dict, List

from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel, Field

from ..analyze.complexity import analyze as analyze_complexity
from ..core.ir.nodes import ConstructTag, TIER_FOR_TAG, Tier


router = APIRouter(prefix="/api/v1", tags=["assess"])


# ─── Request / response types ────────────────────────────────────────────────


class AssessRequest(BaseModel):
    """Single DDL blob. No login. No storage. No email."""

    ddl: str = Field(..., min_length=1, max_length=1_000_000)


class RiskItem(BaseModel):
    """One user-facing risk callout. Produced by grouping construct
    occurrences by tag and prepending a human-readable label + guidance.
    The AI conversion pane (future) hangs off the same `tag`."""

    tag: str
    tier: str  # A | B | C
    label: str
    guidance: str
    count: int


class AssessResponse(BaseModel):
    score: int
    total_lines: int
    auto_convertible_lines: int
    needs_review_lines: int
    must_rewrite_lines: int
    effort_estimate_days: float
    estimated_cost: float
    objects_by_kind: Dict[str, int]
    construct_counts: Dict[str, int]
    top_constructs: List[str]
    risks: List[RiskItem]


# ─── Handler ─────────────────────────────────────────────────────────────────


@router.post("/assess", response_model=AssessResponse)
async def assess(req: AssessRequest) -> AssessResponse:
    """Run the complexity scorer on `ddl` and return a compact report.

    The scorer accepts raw source; it parses, builds an IR, and counts
    tagged constructs. We pass a hard-coded rate (not a body field) so
    the quick-assess UX stays unambiguous — paid tiers can override it
    later through a different endpoint."""
    try:
        report = analyze_complexity(req.ddl, rate_per_day=1500)
    except Exception as exc:  # noqa: BLE001 — parser surface is broad
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"could not parse input: {exc}",
        )

    risks = _build_risks(report.construct_counts)

    return AssessResponse(
        score=report.score,
        total_lines=report.total_lines,
        auto_convertible_lines=report.auto_convertible_lines,
        needs_review_lines=report.needs_review_lines,
        must_rewrite_lines=report.must_rewrite_lines,
        effort_estimate_days=report.effort_estimate_days,
        estimated_cost=report.estimated_cost,
        objects_by_kind=report.objects_by_kind,
        construct_counts=report.construct_counts,
        top_constructs=report.top_10_constructs,
        risks=risks,
    )


# ─── Risk label table ────────────────────────────────────────────────────────
#
# Single source of truth for the user-facing phrasing of each construct.
# Owned here (not in the IR nodes) because this is UI copy — it changes
# on a different cadence than the enum. When the AI pane lands, it'll
# key off the same tag to fetch a full rewrite example from the RAG
# store.

_LABELS: Dict[ConstructTag, tuple[str, str]] = {
    # Tier A — auto-convertible, included for completeness; the UI
    # filters these out of the "risks" list but the labels help the
    # per-construct tooltip.
    ConstructTag.DBMS_OUTPUT: (
        "DBMS_OUTPUT.PUT_LINE calls",
        "Rewrite to RAISE NOTICE. Mechanical 1:1.",
    ),
    ConstructTag.PERCENT_TYPE: (
        "%TYPE / %ROWTYPE references",
        "Supported natively in PL/pgSQL — no change needed.",
    ),
    ConstructTag.RAISE_APPLICATION_ERROR: (
        "RAISE_APPLICATION_ERROR calls",
        "Map to RAISE EXCEPTION ... USING ERRCODE. Mechanical.",
    ),
    # Tier B — review required
    ConstructTag.CONNECT_BY: (
        "CONNECT BY hierarchical queries",
        "Rewrite as WITH RECURSIVE CTEs. Deterministic but query-by-query.",
    ),
    ConstructTag.MERGE: (
        "MERGE statements",
        "Rewrite to INSERT ... ON CONFLICT DO UPDATE. Watch for WHEN NOT MATCHED BY SOURCE — no PG equivalent.",
    ),
    ConstructTag.GLOBAL_TEMP_TABLE: (
        "Global temporary tables",
        "Convert to ON COMMIT DROP temp tables or unlogged tables depending on scope.",
    ),
    ConstructTag.EXECUTE_IMMEDIATE: (
        "EXECUTE IMMEDIATE dynamic SQL",
        "Maps to EXECUTE in PL/pgSQL, but bind semantics differ — review each call site.",
    ),
    ConstructTag.BULK_COLLECT: (
        "BULK COLLECT INTO",
        "Use array aggregation (ARRAY(SELECT ...) or SELECT ... INTO array) — performance profile differs.",
    ),
    ConstructTag.FORALL: (
        "FORALL bulk DML",
        "No direct equivalent. Batch with unnest() or convert to set-based SQL.",
    ),
    ConstructTag.OUTER_JOIN_PLUS: (
        "Legacy (+) outer-join operator",
        "Rewrite to ANSI LEFT/RIGHT JOIN. Mechanical but easy to introduce bugs.",
    ),
    ConstructTag.PRAGMA_EXCEPTION_INIT: (
        "PRAGMA EXCEPTION_INIT",
        "Map to user-defined exceptions with SQLSTATE codes.",
    ),
    ConstructTag.HIERARCHICAL_PSEUDOCOLUMN: (
        "LEVEL / CONNECT_BY_ISLEAF / CONNECT_BY_ROOT",
        "Rewrite alongside the enclosing CONNECT BY → recursive CTE.",
    ),
    ConstructTag.ROWNUM: (
        "ROWNUM filtering",
        "Replace with LIMIT/OFFSET or ROW_NUMBER() OVER (...).",
    ),
    ConstructTag.ROWID: (
        "ROWID references",
        "No direct equivalent. Use ctid (not stable) or add a surrogate key.",
    ),
    ConstructTag.REF_CURSOR: (
        "REF CURSOR return types",
        "Map to REFCURSOR in PL/pgSQL — calling conventions differ; review each usage.",
    ),
    # Tier C — must rewrite or install an extension
    ConstructTag.AUTONOMOUS_TXN: (
        "AUTONOMOUS_TRANSACTION pragma",
        "No direct equivalent. Use dblink or redesign the transaction boundary.",
    ),
    ConstructTag.DBMS_SCHEDULER: (
        "DBMS_SCHEDULER jobs",
        "Move to pg_cron, external scheduler (Airflow, cron), or application layer.",
    ),
    ConstructTag.DBMS_AQ: (
        "Oracle Advanced Queuing",
        "No equivalent. Use pgmq, a message broker (Kafka/RabbitMQ), or LISTEN/NOTIFY.",
    ),
    ConstructTag.DBMS_CRYPTO: (
        "DBMS_CRYPTO calls",
        "Replace with pgcrypto functions. Algorithm names and padding differ.",
    ),
    ConstructTag.UTL_FILE: (
        "UTL_FILE filesystem I/O",
        "Database-side file I/O is discouraged in PG. Move to application layer or COPY.",
    ),
    ConstructTag.UTL_HTTP: (
        "UTL_HTTP outbound requests",
        "Move to application layer or use http extension (requires sysadmin install).",
    ),
    ConstructTag.DBLINK: (
        "Database links",
        "Replace with postgres_fdw (PG target) or oracle_fdw (during cutover).",
    ),
    ConstructTag.SPATIAL: (
        "Oracle Spatial (SDO_GEOMETRY)",
        "Port to PostGIS. Type names, functions, and SRIDs all differ.",
    ),
    ConstructTag.ORACLE_TEXT: (
        "Oracle Text indexes",
        "Port to PG full-text search (tsvector/tsquery) or pg_trgm.",
    ),
    ConstructTag.OBJECT_TYPE: (
        "User-defined OBJECT types",
        "Flatten to composite types or normalize into tables. Architectural work.",
    ),
    ConstructTag.NESTED_TABLE: (
        "Nested tables",
        "Use PG arrays for simple cases; normalize for complex ones.",
    ),
    ConstructTag.PIPELINED_FUNCTION: (
        "Pipelined table functions",
        "Convert to SETOF-returning functions. Semantics differ for streaming.",
    ),
    ConstructTag.EXTERNAL_PROCEDURE: (
        "External C procedures",
        "Rewrite as PG extensions (C) or move logic out of the DB entirely.",
    ),
    ConstructTag.VPD_POLICY: (
        "Virtual Private Database (VPD) policies",
        "Replace with PG row-level security (RLS) policies. Semantics very close.",
    ),
}


def _build_risks(construct_counts: Dict[str, int]) -> List[RiskItem]:
    """Turn the raw construct-count map into a sorted risk list.

    Only Tier B and C appear — Tier A constructs are "no-op" conversions
    that don't represent risk, and showing them would bury the signal
    the user actually needs to see."""
    risks: List[RiskItem] = []
    for tag_name, count in construct_counts.items():
        try:
            tag = ConstructTag(tag_name)
        except ValueError:
            continue  # object-kind keys like "PROCEDURE" share this dict
        tier = TIER_FOR_TAG.get(tag, Tier.A)
        if tier == Tier.A:
            continue
        label, guidance = _LABELS.get(tag, (tag.value, ""))
        risks.append(
            RiskItem(
                tag=tag.value,
                tier=tier.value,
                label=label,
                guidance=guidance,
                count=count,
            )
        )
    # Tier C first, then by count descending.
    risks.sort(key=lambda r: (r.tier != "C", -r.count, r.label))
    return risks
