"""Migration runbook data model + deterministic phase assembly.

The runbook is the customer-deliverable PDF. It is assembled in two
layers:

  1. **Deterministic** (this module): standard migration phases —
     Discovery → Schema Conversion → Application Refactor → Data Move →
     Cutover → Stabilization — populated from the parsed ComplexityReport,
     the EnrichedAppImpactReport, and project metadata. Prerequisites,
     activities, and rollback steps per phase are template-driven so the
     same project produces the same runbook on rerun.

  2. **AI-generated narrative** (src/ai/services/runbook.py): executive
     summary and risk narrative that put the deterministic findings in
     business context. The AI layer is purely additive — if it fails,
     the deterministic runbook still ships.

PDF rendering lives in `pdf.py`; the API route consumes Runbook + the
PDF renderer to deliver a downloadable artifact.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import List, Optional

from ..analyze.app_impact import RiskLevel
from ..analyze.complexity import ComplexityReport


# ─── Inputs ──────────────────────────────────────────────────────────────────


@dataclass
class RunbookContext:
    """Project metadata + computed inputs."""

    project_name: str
    customer: str
    source_version: str = "Oracle 19c"
    target_version: str = "PostgreSQL 16"
    cutover_window: str = "TBD"
    rate_per_day: int = 1500
    complexity: Optional[ComplexityReport] = None
    # EnrichedAppImpactReport-shaped object; we treat it as duck-typed to
    # avoid a hard dependency on the AI service module from this layer.
    app_impact: Optional[object] = None


# ─── Output structures ───────────────────────────────────────────────────────


@dataclass
class RunbookPhase:
    title: str
    description: str
    prerequisites: List[str] = field(default_factory=list)
    activities: List[str] = field(default_factory=list)
    rollback: List[str] = field(default_factory=list)
    duration_days: float = 0.0
    risk_level: RiskLevel = RiskLevel.MEDIUM


@dataclass
class RunbookBlocker:
    """A CRITICAL finding surfaced into the runbook's blockers section."""

    code: str
    message: str
    file: str
    line: int
    suggestion: str
    explanation: str = ""


@dataclass
class Runbook:
    context: RunbookContext
    executive_summary: str
    risk_narrative: str
    phases: List[RunbookPhase]
    blockers: List[RunbookBlocker]
    sign_offs: List[str]
    generated_at: datetime
    prompt_version: str = ""  # set when AI sections are populated


# ─── Deterministic assembly ──────────────────────────────────────────────────


# Standard six-phase template. Per-phase activities reference the inputs
# (complexity, app-impact) when populated; otherwise sensible defaults.
def assemble(
    context: RunbookContext,
    *,
    executive_summary: str = "",
    risk_narrative: str = "",
    prompt_version: str = "",
) -> Runbook:
    """Build a Runbook from context. AI sections optional — pass them in
    if available, otherwise the runbook is deterministic-only and the
    PDF still renders without a Summary/Risk Narrative section."""

    phases = _build_phases(context)
    blockers = _build_blockers(context)
    sign_offs = _build_sign_offs(context)

    return Runbook(
        context=context,
        executive_summary=executive_summary or _default_summary(context),
        risk_narrative=risk_narrative or _default_risk_narrative(context),
        phases=phases,
        blockers=blockers,
        sign_offs=sign_offs,
        generated_at=datetime.now(timezone.utc),
        prompt_version=prompt_version,
    )


def _build_phases(ctx: RunbookContext) -> List[RunbookPhase]:
    cx = ctx.complexity
    total_days = cx.effort_estimate_days if cx else 5.0

    # Coarse budget split across phases — overridable by per-engagement
    # calibration when we have data from real customer runs.
    budget = {
        "discovery": 0.10,
        "schema": 0.20,
        "app": 0.25,
        "data": 0.20,
        "cutover": 0.10,
        "stabilization": 0.15,
    }

    return [
        RunbookPhase(
            title="1. Discovery & Baseline",
            description=(
                "Inventory every Oracle object in scope, capture a runtime "
                f"baseline (top queries, index usage, lock waits) on {ctx.source_version}, "
                "and confirm the conversion plan with stakeholders."
            ),
            prerequisites=[
                "Read access to the source Oracle instance",
                "Approved migration scope (schemas, packages, dblinks)",
                "Customer DBA available for 2–4 hours of walkthroughs",
            ],
            activities=[
                "Run the Depart complexity analyzer against the full DDL",
                "Run the app-impact analyzer against the application repos in scope",
                "Capture v$sql top-50 by elapsed time as the perf baseline",
                "Confirm cutover window and downtime tolerance",
            ],
            rollback=["No rollback — read-only discovery phase."],
            duration_days=round(total_days * budget["discovery"], 1),
            risk_level=RiskLevel.LOW,
        ),
        RunbookPhase(
            title="2. Schema Conversion",
            description=(
                f"Convert {ctx.source_version} DDL to {ctx.target_version}: tables, "
                "constraints, indexes, sequences, views, triggers, and packages. "
                "Tier-A objects auto-convert; Tier-B/C reviewed object-by-object."
            ),
            prerequisites=[
                "Discovery complete and signed off",
                "Empty target PG instance with target version installed",
                "pgvector / pgcrypto / orafce extensions available where needed",
            ],
            activities=_schema_activities(cx),
            rollback=[
                "Drop the target schema and re-create from version control",
                "Schema conversion produces no source-side changes — Oracle is unaffected",
            ],
            duration_days=round(total_days * budget["schema"], 1),
            risk_level=_max_tier_risk(cx),
        ),
        RunbookPhase(
            title="3. Application Refactor",
            description=(
                "Update application code so SQL is portable. Findings ranked by "
                "risk; CRITICAL items block cutover, others can ship in batches."
            ),
            prerequisites=[
                "Schema converted and reviewed",
                "Per-team owner identified for each codebase",
            ],
            activities=_app_activities(ctx.app_impact),
            rollback=[
                "Application changes ship behind a feature flag pointing at the "
                "Oracle vs PostgreSQL connection; flip back instantly if a "
                "regression is detected during shadow-traffic testing.",
            ],
            duration_days=round(total_days * budget["app"], 1),
            risk_level=_app_risk(ctx.app_impact),
        ),
        RunbookPhase(
            title="4. Data Migration",
            description=(
                "Bulk-load reference and transactional tables to PostgreSQL with "
                "row-count + Merkle-hash equality checks. Re-runnable per table "
                "with checkpoint resume."
            ),
            prerequisites=[
                "Target schema applied",
                "Network throughput between source and target validated",
                "Disk space in target = 1.3 × source data size",
            ],
            activities=[
                "Disable target FK constraints + triggers",
                "Bulk-load tables in dependency order using COPY FROM STDIN",
                "Re-enable constraints; reset sequences to MAX(id)+1",
                "Verify row counts + Merkle-hash equality per table",
                "Capture load durations for the cutover dry-run plan",
            ],
            rollback=[
                "Truncate target tables and re-run the load (idempotent)",
                "Source remains read-only authoritative until cutover",
            ],
            duration_days=round(total_days * budget["data"], 1),
            risk_level=RiskLevel.MEDIUM,
        ),
        RunbookPhase(
            title="5. Cutover",
            description=(
                f"Cutover window: {ctx.cutover_window}. Stop writes on Oracle, "
                "run final delta load, switch application connection strings, "
                "smoke-test, and announce."
            ),
            prerequisites=[
                "Data migration dry-run successful within the cutover window",
                "Customer DBA + on-call engineer available for the window",
                "Communications drafted (status page, customer notice)",
            ],
            activities=[
                "Set Oracle to read-only mode (lock writes)",
                "Capture final delta and apply to PostgreSQL",
                "Update application connection strings (DATABASE_URL)",
                "Run smoke tests and synthetic traffic for 15 minutes",
                "Update DNS / load balancer if needed",
                "Announce cutover complete",
            ],
            rollback=[
                "Revert connection strings to Oracle (Oracle was read-only, no data drift)",
                "Re-enable writes on Oracle",
                "Document the abort cause and reschedule",
            ],
            duration_days=round(total_days * budget["cutover"], 1),
            risk_level=RiskLevel.HIGH,
        ),
        RunbookPhase(
            title="6. Stabilization",
            description=(
                "Two weeks of heightened monitoring on PostgreSQL: query "
                "regressions, autovacuum tuning, statistics refresh, error "
                "rate. Post-mortem at the end."
            ),
            prerequisites=["Cutover complete"],
            activities=[
                "Compare PG query plans against the Oracle baseline (top 50 queries)",
                "Tune autovacuum scale factors on hot tables",
                "Run ANALYZE on every table after first 24h of traffic",
                "Review pg_stat_statements daily for the first week",
                "Hold post-mortem and capture lessons learned",
            ],
            rollback=[
                "If a CRITICAL regression appears within 72h and cannot be "
                "remediated, the feature flag from Phase 3 reverts traffic to "
                "Oracle; data drift is bounded by the duration of the regression.",
            ],
            duration_days=round(total_days * budget["stabilization"], 1),
            risk_level=RiskLevel.MEDIUM,
        ),
    ]


def _schema_activities(cx: Optional[ComplexityReport]) -> List[str]:
    if cx is None:
        return [
            "Convert tables, indexes, sequences, views deterministically",
            "Convert procedures/functions/packages with AI-assisted lowering",
            "Run pgTAP/equivalence tests per converted object",
        ]
    base = [
        f"Auto-convert {cx.auto_convertible_lines} lines (Tier A)",
        f"Review + adjust {cx.needs_review_lines} lines (Tier B)",
        f"Refactor {cx.must_rewrite_lines} lines (Tier C — architectural)",
    ]
    if cx.tier_c_constructs:
        base.append(
            "Tier-C constructs requiring architectural attention: "
            + ", ".join(sorted(set(cx.tier_c_constructs)))
        )
    if cx.tier_b_constructs:
        base.append(
            "Tier-B constructs requiring per-call review: "
            + ", ".join(sorted(set(cx.tier_b_constructs)))
        )
    base.append("Validate every converted object against an equivalence test")
    return base


def _app_activities(app_impact) -> List[str]:
    if app_impact is None:
        return [
            "Run the app-impact analyzer (no app-impact data was provided yet)",
            "Triage findings by risk level",
            "Land changes in batches; CRITICAL items must merge before cutover",
        ]
    counts = getattr(app_impact, "findings_by_risk", {}) or {}
    files = getattr(app_impact, "total_files_scanned", 0)
    findings = getattr(app_impact, "total_findings", 0)
    parts = [
        f"Triage {findings} findings across {files} files",
    ]
    if counts.get("critical"):
        parts.append(f"Resolve {counts['critical']} CRITICAL findings before cutover (blockers)")
    if counts.get("high"):
        parts.append(f"Resolve {counts['high']} HIGH findings during the refactor window")
    if counts.get("medium"):
        parts.append(
            f"Address {counts['medium']} MEDIUM findings (mechanical Oracle→PG function swaps)"
        )
    parts += [
        "Code review every change with the file owner",
        "Land changes behind a feature flag on the connection-string pointer",
        "Run shadow traffic against PG for 48h before cutover",
    ]
    return parts


def _app_risk(app_impact) -> RiskLevel:
    if app_impact is None:
        return RiskLevel.MEDIUM
    counts = getattr(app_impact, "findings_by_risk", {}) or {}
    if counts.get("critical"):
        return RiskLevel.CRITICAL
    if counts.get("high"):
        return RiskLevel.HIGH
    if counts.get("medium"):
        return RiskLevel.MEDIUM
    return RiskLevel.LOW


def _max_tier_risk(cx: Optional[ComplexityReport]) -> RiskLevel:
    if cx is None:
        return RiskLevel.MEDIUM
    if cx.must_rewrite_lines > 0:
        return RiskLevel.HIGH
    if cx.needs_review_lines > 0:
        return RiskLevel.MEDIUM
    return RiskLevel.LOW


def _build_blockers(ctx: RunbookContext) -> List[RunbookBlocker]:
    """Surface every CRITICAL finding as a runbook blocker. CRITICAL means
    cutover cannot proceed until resolved."""
    if ctx.app_impact is None:
        return []
    blockers: List[RunbookBlocker] = []
    files = getattr(ctx.app_impact, "files", []) or []
    for fi in files:
        for ef in getattr(fi, "findings", []):
            f = getattr(ef, "finding", ef)
            if getattr(f, "risk", None) == RiskLevel.CRITICAL:
                blockers.append(
                    RunbookBlocker(
                        code=f.code,
                        message=f.message,
                        file=f.file,
                        line=f.line,
                        suggestion=f.suggestion,
                        explanation=getattr(ef, "explanation", "") or "",
                    )
                )
    blockers.sort(key=lambda b: (b.file, b.line))
    return blockers


def _build_sign_offs(ctx: RunbookContext) -> List[str]:
    return [
        f"{ctx.customer} — Engineering Lead",
        f"{ctx.customer} — DBA / Data Platform Lead",
        f"{ctx.customer} — Application Owner(s) (per impacted codebase)",
        "Depart — Migration Engineer",
        "Depart — Engineering Manager (release captain)",
    ]


def _default_summary(ctx: RunbookContext) -> str:
    cx = ctx.complexity
    if cx is None:
        return (
            f"Migration runbook for {ctx.customer} from {ctx.source_version} "
            f"to {ctx.target_version}. Complexity analysis was not provided; "
            "regenerate after running the complexity analyzer."
        )
    return (
        f"Migration runbook for {ctx.customer}: {cx.total_lines:,} lines of "
        f"{ctx.source_version} PL/SQL across {sum(cx.objects_by_kind.values())} "
        f"top-level objects. Estimated effort {cx.effort_estimate_days} engineer-days "
        f"at ${ctx.rate_per_day:,}/day = ${int(cx.effort_estimate_days * ctx.rate_per_day):,}. "
        f"Cutover window: {ctx.cutover_window}."
    )


def _default_risk_narrative(ctx: RunbookContext) -> str:
    cx = ctx.complexity
    if cx is None:
        return "No complexity data — risk profile cannot be assessed."
    parts = []
    if cx.must_rewrite_lines:
        parts.append(
            f"{cx.must_rewrite_lines} lines require architectural rewrite "
            f"({len(cx.tier_c_constructs)} distinct Tier-C constructs)."
        )
    if cx.needs_review_lines:
        parts.append(
            f"{cx.needs_review_lines} lines need per-call review "
            f"({len(cx.tier_b_constructs)} distinct Tier-B constructs)."
        )
    if cx.auto_convertible_lines:
        parts.append(f"{cx.auto_convertible_lines} lines auto-convert deterministically.")
    return " ".join(parts) or "No notable risks detected in the static analysis."
