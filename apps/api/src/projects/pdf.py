"""PDF rendering for migration runbooks.

Uses ReportLab Platypus flowables. The output is intentionally clean +
boring — black text, sans-serif, two-column page numbers, no logos.
Branded layouts (customer logo, color palette) are a Phase R2 concern.

Public API: `render(runbook) -> bytes`. Caller decides what to do with
the bytes (write to disk, return from FastAPI as application/pdf, ...).
"""

from __future__ import annotations

import io

from reportlab.lib.colors import HexColor
from reportlab.lib.pagesizes import LETTER
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import inch
from reportlab.platypus import (
    HRFlowable,
    PageBreak,
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)

from .runbook import Runbook, RunbookPhase
from ..analyze.app_impact import RiskLevel


# Risk color palette: green / yellow / amber / red.
_RISK_COLORS = {
    RiskLevel.LOW.value: HexColor("#2E7D32"),
    RiskLevel.MEDIUM.value: HexColor("#F9A825"),
    RiskLevel.HIGH.value: HexColor("#EF6C00"),
    RiskLevel.CRITICAL.value: HexColor("#C62828"),
}


def render(runbook: Runbook) -> bytes:
    buf = io.BytesIO()
    doc = SimpleDocTemplate(
        buf,
        pagesize=LETTER,
        leftMargin=0.75 * inch,
        rightMargin=0.75 * inch,
        topMargin=0.75 * inch,
        bottomMargin=0.75 * inch,
        title=f"Migration Runbook — {runbook.context.customer}",
        author="Depart",
    )
    story = list(_build_story(runbook))
    doc.build(story, onFirstPage=_page_chrome, onLaterPages=_page_chrome)
    return buf.getvalue()


# ─── Story assembly ──────────────────────────────────────────────────────────


def _build_story(runbook: Runbook):
    styles = _styles()
    ctx = runbook.context

    # ── Title page
    yield Paragraph("Migration Runbook", styles["Title"])
    yield Spacer(1, 0.2 * inch)
    yield Paragraph(ctx.project_name, styles["Subtitle"])
    yield Paragraph(f"Customer: <b>{ctx.customer}</b>", styles["Body"])
    yield Paragraph(f"Source: {ctx.source_version}", styles["Body"])
    yield Paragraph(f"Target: {ctx.target_version}", styles["Body"])
    yield Paragraph(f"Cutover window: {ctx.cutover_window}", styles["Body"])
    yield Paragraph(
        f"Generated: {runbook.generated_at.strftime('%Y-%m-%d %H:%M UTC')}",
        styles["Footnote"],
    )
    if runbook.prompt_version:
        yield Paragraph(f"AI prompt: {runbook.prompt_version}", styles["Footnote"])
    yield Spacer(1, 0.4 * inch)

    # ── Executive summary
    yield Paragraph("Executive Summary", styles["H1"])
    yield HRFlowable(width="100%", thickness=0.5)
    yield Spacer(1, 0.1 * inch)
    yield Paragraph(_html_escape(runbook.executive_summary), styles["Body"])
    yield Spacer(1, 0.3 * inch)

    # ── Risk narrative
    yield Paragraph("Risk Profile", styles["H1"])
    yield HRFlowable(width="100%", thickness=0.5)
    yield Spacer(1, 0.1 * inch)
    for para in runbook.risk_narrative.split("\n\n"):
        if para.strip():
            yield Paragraph(_html_escape(para.strip()), styles["Body"])
            yield Spacer(1, 0.05 * inch)
    yield Spacer(1, 0.2 * inch)

    # ── Effort + cost summary table
    yield from _effort_summary(runbook, styles)
    yield PageBreak()

    # ── Phases
    yield Paragraph("Migration Phases", styles["H1"])
    yield HRFlowable(width="100%", thickness=0.5)
    yield Spacer(1, 0.1 * inch)
    for phase in runbook.phases:
        yield from _render_phase(phase, styles)
    yield PageBreak()

    # ── Blockers
    yield Paragraph("Cutover Blockers (CRITICAL findings)", styles["H1"])
    yield HRFlowable(width="100%", thickness=0.5)
    yield Spacer(1, 0.1 * inch)
    if not runbook.blockers:
        yield Paragraph("No CRITICAL application-impact findings detected.", styles["Body"])
    else:
        for b in runbook.blockers:
            yield Paragraph(
                f"<b>{_html_escape(b.code)}</b> — {_html_escape(b.file)}:{b.line}",
                styles["Body"],
            )
            yield Paragraph(_html_escape(b.message), styles["Body"])
            yield Paragraph(f"<i>Suggested fix:</i> {_html_escape(b.suggestion)}", styles["Body"])
            if b.explanation:
                yield Paragraph(_html_escape(b.explanation), styles["BodySmall"])
            yield Spacer(1, 0.1 * inch)
    yield Spacer(1, 0.2 * inch)

    # ── Sign-offs
    yield Paragraph("Approvals Required", styles["H1"])
    yield HRFlowable(width="100%", thickness=0.5)
    yield Spacer(1, 0.1 * inch)
    sign_off_data = [["Role", "Name", "Date"]]
    for role in runbook.sign_offs:
        sign_off_data.append([role, "", ""])
    t = Table(sign_off_data, colWidths=[3 * inch, 2.5 * inch, 1.5 * inch])
    t.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), HexColor("#E0E0E0")),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("ALIGN", (0, 0), (-1, -1), "LEFT"),
                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                ("GRID", (0, 0), (-1, -1), 0.25, HexColor("#9E9E9E")),
                ("ROWBACKGROUNDS", (0, 1), (-1, -1), [None, HexColor("#FAFAFA")]),
                ("LEFTPADDING", (0, 0), (-1, -1), 6),
                ("RIGHTPADDING", (0, 0), (-1, -1), 6),
                ("TOPPADDING", (0, 0), (-1, -1), 6),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
            ]
        )
    )
    yield t


def _render_phase(phase: RunbookPhase, styles):
    yield Paragraph(_html_escape(phase.title), styles["H2"])
    chip = _risk_chip(phase.risk_level.value, styles)
    yield Paragraph(
        f"{chip} &nbsp;&nbsp; <i>Estimated duration: " f"{phase.duration_days} engineer-day(s)</i>",
        styles["Footnote"],
    )
    yield Spacer(1, 0.05 * inch)
    yield Paragraph(_html_escape(phase.description), styles["Body"])

    yield Paragraph("<b>Prerequisites</b>", styles["H3"])
    for p in phase.prerequisites:
        yield Paragraph(f"• {_html_escape(p)}", styles["BodySmall"])

    yield Paragraph("<b>Activities</b>", styles["H3"])
    for a in phase.activities:
        yield Paragraph(f"• {_html_escape(a)}", styles["BodySmall"])

    yield Paragraph("<b>Rollback</b>", styles["H3"])
    for r in phase.rollback:
        yield Paragraph(f"• {_html_escape(r)}", styles["BodySmall"])

    yield Spacer(1, 0.2 * inch)


def _effort_summary(runbook: Runbook, styles):
    cx = runbook.context.complexity
    if cx is None:
        yield Paragraph(
            "Complexity analysis not provided — effort estimate unavailable.",
            styles["Body"],
        )
        return
    rate = runbook.context.rate_per_day
    cost = int(cx.effort_estimate_days * rate)
    data = [
        ["Metric", "Value"],
        ["Total lines", f"{cx.total_lines:,}"],
        ["Tier A (auto-convertible)", f"{cx.auto_convertible_lines:,}"],
        ["Tier B (needs review)", f"{cx.needs_review_lines:,}"],
        ["Tier C (must rewrite)", f"{cx.must_rewrite_lines:,}"],
        ["Estimated effort", f"{cx.effort_estimate_days} engineer-days"],
        ["Rate", f"${rate:,}/day"],
        ["Estimated cost", f"${cost:,}"],
    ]
    t = Table(data, colWidths=[3 * inch, 3 * inch])
    t.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), HexColor("#E0E0E0")),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("FONTNAME", (0, -1), (-1, -1), "Helvetica-Bold"),
                ("BACKGROUND", (0, -1), (-1, -1), HexColor("#FFF8E1")),
                ("ALIGN", (1, 0), (1, -1), "RIGHT"),
                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                ("GRID", (0, 0), (-1, -1), 0.25, HexColor("#9E9E9E")),
                ("LEFTPADDING", (0, 0), (-1, -1), 6),
                ("RIGHTPADDING", (0, 0), (-1, -1), 6),
                ("TOPPADDING", (0, 0), (-1, -1), 4),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
            ]
        )
    )
    yield t


def _risk_chip(risk_value: str, styles) -> str:
    color = _RISK_COLORS.get(risk_value, HexColor("#616161"))
    # ReportLab's inline <font color="..."> needs a leading '#'.
    hex6 = color.hexval()[2:].rjust(6, "0")
    return f'<font color="#{hex6}" size="9"><b>' f"{risk_value.upper()} RISK</b></font>"


def _page_chrome(canvas, doc):
    canvas.saveState()
    canvas.setFont("Helvetica", 8)
    canvas.setFillColor(HexColor("#757575"))
    canvas.drawString(0.75 * inch, 0.5 * inch, "Depart Migration Runbook — Confidential")
    canvas.drawRightString(LETTER[0] - 0.75 * inch, 0.5 * inch, f"Page {doc.page}")
    canvas.restoreState()


def _styles():
    base = getSampleStyleSheet()
    return {
        "Title": ParagraphStyle(
            "Title",
            parent=base["Title"],
            fontSize=24,
            textColor=HexColor("#212121"),
            spaceAfter=8,
        ),
        "Subtitle": ParagraphStyle(
            "Subtitle",
            parent=base["Heading2"],
            fontSize=14,
            textColor=HexColor("#424242"),
            spaceAfter=20,
        ),
        "H1": ParagraphStyle(
            "H1",
            parent=base["Heading1"],
            fontSize=16,
            textColor=HexColor("#1565C0"),
            spaceBefore=8,
            spaceAfter=4,
        ),
        "H2": ParagraphStyle(
            "H2",
            parent=base["Heading2"],
            fontSize=13,
            textColor=HexColor("#212121"),
            spaceBefore=8,
            spaceAfter=2,
        ),
        "H3": ParagraphStyle(
            "H3",
            parent=base["Heading3"],
            fontSize=10,
            textColor=HexColor("#424242"),
            spaceBefore=6,
            spaceAfter=2,
        ),
        "Body": ParagraphStyle(
            "Body",
            parent=base["BodyText"],
            fontSize=10,
            leading=14,
        ),
        "BodySmall": ParagraphStyle(
            "BodySmall",
            parent=base["BodyText"],
            fontSize=9,
            leading=12,
            leftIndent=12,
        ),
        "Footnote": ParagraphStyle(
            "Footnote",
            parent=base["BodyText"],
            fontSize=8,
            leading=10,
            textColor=HexColor("#616161"),
        ),
    }


def _html_escape(text: str) -> str:
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
