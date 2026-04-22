"""Versioned prompts for the migration runbook generator."""

VERSION = "2026-04-22.1"


SYSTEM_PROMPT = """\
You are a senior database migration practice lead writing the executive
summary and risk narrative section of a customer-facing migration
runbook. The audience is the customer's VP of Engineering and CTO —
technical, time-poor, skeptical. They will read the first paragraph and
make a go/no-go funding decision.

OUTPUT CONTRACT
  Return ONLY valid JSON, no prose outside, no fences.

  {
    "executive_summary": "<3-5 sentences. Lead with effort+cost+timeline.
                          Mention top-2 risks. Close with the cutover
                          window and required sign-offs.>",
    "risk_narrative":    "<2-3 paragraphs. Group risks by category
                          (architectural, mechanical, data-quality).
                          Reference actual construct names and counts
                          from the inputs — no generic boilerplate.>"
  }

Quality bar:
  * Direct, numeric, specific. No "may", "might", "could potentially".
  * Cite actual numbers from the inputs (lines, days, finding counts).
  * If a Tier-C construct is present (autonomous transactions, AQ,
    Spatial, etc.), name it and explain the architectural implication.
  * No marketing fluff. No "we are excited to". No emojis.
"""


USER_TEMPLATE = """\
Project: {project_name}
Customer: {customer}
Source: {source_version}  →  Target: {target_version}
Cutover window: {cutover_window}
Rate: ${rate_per_day}/engineer-day

COMPLEXITY ANALYSIS
  Total lines:        {total_lines:,}
  Tier A (auto):      {auto_lines:,}
  Tier B (review):    {review_lines:,}
  Tier C (rewrite):   {rewrite_lines:,}
  Effort estimate:    {effort_days} days  (≈ ${cost:,})
  Tier-C constructs:  {tier_c}
  Tier-B constructs:  {tier_b}
  Top constructs:     {top_constructs}

APP IMPACT (if provided)
  Files scanned:      {files_scanned}
  Total findings:     {total_findings}
  Findings by risk:   {findings_by_risk}

Produce the JSON output as specified.
"""


def render_user_message(*, ctx, complexity, app_impact) -> str:
    cx = complexity
    ai = app_impact
    return USER_TEMPLATE.format(
        project_name=ctx.project_name,
        customer=ctx.customer,
        source_version=ctx.source_version,
        target_version=ctx.target_version,
        cutover_window=ctx.cutover_window,
        rate_per_day=ctx.rate_per_day,
        total_lines=cx.total_lines if cx else 0,
        auto_lines=cx.auto_convertible_lines if cx else 0,
        review_lines=cx.needs_review_lines if cx else 0,
        rewrite_lines=cx.must_rewrite_lines if cx else 0,
        effort_days=cx.effort_estimate_days if cx else 0,
        cost=int((cx.effort_estimate_days if cx else 0) * ctx.rate_per_day),
        tier_c=(
            ", ".join(sorted(set(cx.tier_c_constructs)))
            if cx and cx.tier_c_constructs
            else "(none)"
        ),
        tier_b=(
            ", ".join(sorted(set(cx.tier_b_constructs)))
            if cx and cx.tier_b_constructs
            else "(none)"
        ),
        top_constructs=", ".join(cx.top_10_constructs) if cx and cx.top_10_constructs else "(none)",
        files_scanned=getattr(ai, "total_files_scanned", 0) if ai else 0,
        total_findings=getattr(ai, "total_findings", 0) if ai else 0,
        findings_by_risk=getattr(ai, "findings_by_risk", {}) if ai else {},
    )
