"""AI-driven log troubleshooter — backend service.

Single entry point used by both:
  * `POST /api/v1/troubleshoot/analyze` (public — paste/upload form)
  * `POST /api/v1/migrations/{id}/diagnose-error` (post-launch follow-up)

Pipeline:
  1. Truncate the raw input down to ~50KB of relevant ERROR/WARN
     windows. Claude's prompt cost is bounded by this — uploading 1GB
     vs 1MB makes essentially no difference to per-call token cost.
  2. Run the input through `services.anonymizer.anonymize` — strips
     DSNs, passwords, API keys, emails, IPs; derives a stable
     signature for the corpus.
  3. Call Claude via `AIClient.smart()` with `feature="error_diagnosis"`.
     JSON output, structured `Diagnosis` shape.
  4. Two-plane write: Plane 1 (TroubleshootAnalysis, per-user) and
     optionally Plane 2 (CorpusEntry, anonymized) — Plane 2 skipped
     for opt-out users / Enterprise tier per the privacy policy.

Pure orchestration; no Flask/FastAPI imports. Routers wrap this with
HTTP concerns (auth, rate limiting, multipart parsing, response
shaping).
"""

from __future__ import annotations

import json
import logging
import re
import uuid
from dataclasses import dataclass, asdict
from typing import Optional

from sqlalchemy.orm import Session

from ..ai.client import AIClient
from ..models import CorpusEntry, TroubleshootAnalysis, User
from ..utils.time import utc_now
from .anonymizer import AnonymizedInput, anonymize


logger = logging.getLogger(__name__)


# ─── Truncation ──────────────────────────────────────────────────────────────


# Cap the prompt input at this many bytes. Smart truncation picks
# ERROR/WARN lines + 5 lines of context around each, so larger inputs
# get distilled rather than dropped wholesale.
_PROMPT_BUDGET_BYTES = 50_000
_CONTEXT_LINES = 5
_INTERESTING = re.compile(
    r"(error|warn|fatal|fail|exception|abort|ORA-\d+|SQLSTATE)",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class TruncationResult:
    text: str
    extracted_line_count: int


def smart_truncate(text: str, budget_bytes: int = _PROMPT_BUDGET_BYTES) -> TruncationResult:
    """Reduce `text` to a Claude-prompt-sized excerpt.

    Strategy: find every line matching `_INTERESTING` and keep
    `_CONTEXT_LINES` lines on each side. If the result still exceeds
    `budget_bytes`, drop the oldest interesting windows until it
    fits — keeping the tail (typically the final stack trace) which
    is usually the most diagnostic part.

    If the input has NO interesting lines at all, return the head
    of the text — better than returning empty when the operator
    pasted something the heuristic doesn't recognize."""
    if len(text.encode("utf-8")) <= budget_bytes:
        return TruncationResult(text=text, extracted_line_count=text.count("\n") + 1)

    lines = text.splitlines()
    keep_indices: set[int] = set()
    for i, line in enumerate(lines):
        if _INTERESTING.search(line):
            for j in range(max(0, i - _CONTEXT_LINES), min(len(lines), i + _CONTEXT_LINES + 1)):
                keep_indices.add(j)

    if not keep_indices:
        # No interesting tokens — return the head of the input, capped.
        head = text.encode("utf-8")[:budget_bytes].decode("utf-8", errors="ignore")
        return TruncationResult(text=head, extracted_line_count=head.count("\n") + 1)

    # Build excerpt in original order. Mark line-skipped gaps with an
    # ellipsis marker so Claude knows lines are missing.
    sorted_indices = sorted(keep_indices)
    chunks: list[str] = []
    last = -2
    for idx in sorted_indices:
        if idx != last + 1 and chunks:
            chunks.append("…")
        chunks.append(lines[idx])
        last = idx
    excerpt = "\n".join(chunks)

    # If still over budget, drop oldest chunks from the head — keep
    # the tail (last error usually most diagnostic).
    encoded = excerpt.encode("utf-8")
    if len(encoded) > budget_bytes:
        # Trim head bytes; decode safely.
        encoded = encoded[-budget_bytes:]
        excerpt = "…\n" + encoded.decode("utf-8", errors="ignore")

    return TruncationResult(
        text=excerpt, extracted_line_count=len(sorted_indices)
    )


# ─── Diagnosis shape ─────────────────────────────────────────────────────────


@dataclass(frozen=True)
class Diagnosis:
    """Structured response surfaced to the operator. The router wraps
    this in a Pydantic model with the same field names; keeping the
    service-layer shape as a dataclass means the AIClient JSON parse
    can roundtrip directly without going through Pydantic validation
    twice."""

    likely_cause: str
    recommended_action: str
    code_suggestion: Optional[str]
    confidence: str  # "high" | "medium" | "needs-review"
    escalate_if: Optional[str]
    # Operator-facing bookkeeping for the "What we analyzed" footer.
    analyzed_bytes: int
    extracted_line_count: int
    used_ai: bool


# ─── Prompts ─────────────────────────────────────────────────────────────────


_SYSTEM_PROMPT = """You are a senior Oracle-to-PostgreSQL migration engineer \
helping a DBA diagnose a failed or stuck migration from a log excerpt.

Given the log excerpt and any context the operator provided, return a JSON \
object with EXACTLY these fields:

  "likely_cause":         one or two sentences naming the most probable
                          root cause. Cite specific error codes / lines
                          when you can.
  "recommended_action":   imperative steps the operator should take next.
                          Concrete and ordered (1., 2., …) when there are
                          multiple. No fluff like "ensure to verify" — say
                          what to verify.
  "code_suggestion":      either null, or a short SQL/CLI snippet the
                          operator can copy-paste. Keep this under 20 lines.
  "confidence":           one of "high" | "medium" | "needs-review".
                          Use "needs-review" when the log is ambiguous
                          or the cause could be one of several things.
  "escalate_if":          either null, or a one-line trigger that means
                          "if you see X next, this is probably a different
                          problem than I think". Helps the operator catch
                          a wrong diagnosis early.

Rules:
  * Output ONLY the JSON object, no prose before or after.
  * Be specific. "Check your network" is unhelpful; "the connect timeout
    suggests the Oracle listener is unreachable from the runner — verify
    the listener.ora SID is exposed and the firewall allows the runner's
    IP" is the level of specificity to aim for.
  * If the log is too thin to say anything meaningful, set confidence to
    "needs-review" and use likely_cause to explain WHAT additional
    information would make the diagnosis possible.
"""


def _build_user_prompt(redacted: str, context: Optional[str], stage: Optional[str]) -> str:
    parts: list[str] = []
    if stage:
        parts.append(f"Migration stage: {stage}")
    if context:
        parts.append(f"Operator-supplied context: {context}")
    parts.append("Log excerpt:")
    parts.append(redacted)
    return "\n\n".join(parts)


# ─── Public entry point ──────────────────────────────────────────────────────


def analyze_logs(
    *,
    db: Session,
    raw_logs: str,
    user: Optional[User],
    context: Optional[str] = None,
    stage: Optional[str] = None,
    ai_client: Optional[AIClient] = None,
    write_corpus: bool = True,
) -> tuple[Diagnosis, uuid.UUID]:
    """Run the full pipeline. Returns (diagnosis, analysis_id).

    Caller decides:
      * `user`: None for anonymous calls (Plane 1 row gets user_id=NULL,
        not surfaced to any tenant); a User for authenticated cloud calls.
      * `ai_client`: defaults to `AIClient.smart(feature="error_diagnosis")`.
        Tests pass a stub.
      * `write_corpus`: False to skip Plane 2 entirely (Enterprise tier
        default, or any user who toggled the opt-out).

    Always writes Plane 1 (the diagnosis is the user's record). Plane 2
    write is gated.
    """
    truncated = smart_truncate(raw_logs)
    anon = anonymize(truncated.text)

    used_ai = True
    try:
        if ai_client is None:
            ai_client = AIClient.smart(feature="error_diagnosis")
        raw = ai_client.complete_json(
            system=_SYSTEM_PROMPT,
            user=_build_user_prompt(anon.redacted_text, context, stage),
        )
    except Exception as exc:  # noqa: BLE001 — Claude/network surface
        logger.warning(
            "troubleshoot AI call failed (%s: %s); returning bare-bones diagnosis",
            type(exc).__name__,
            exc,
        )
        raw = {
            "likely_cause": (
                "AI diagnosis temporarily unavailable. The log was received "
                "and parsed, but our model couldn't be reached. Retry in a "
                "moment, or escalate to support if the problem persists."
            ),
            "recommended_action": "1. Retry the analyze call. 2. If retries fail, file a support ticket with the migration ID.",
            "code_suggestion": None,
            "confidence": "needs-review",
            "escalate_if": "the retry succeeds but returns the same text — that means our backoff is hiding a real outage",
        }
        used_ai = False

    diagnosis = Diagnosis(
        likely_cause=str(raw.get("likely_cause", "")),
        recommended_action=str(raw.get("recommended_action", "")),
        code_suggestion=raw.get("code_suggestion") or None,
        confidence=str(raw.get("confidence", "needs-review")),
        escalate_if=raw.get("escalate_if") or None,
        analyzed_bytes=len(truncated.text.encode("utf-8")),
        extracted_line_count=truncated.extracted_line_count,
        used_ai=used_ai,
    )

    # Plane 1 — per-user record. Always written.
    analysis = TroubleshootAnalysis(
        id=uuid.uuid4(),
        user_id=user.id if user is not None else None,
        created_at=utc_now(),
        input_excerpt=anon.redacted_text,
        input_byte_count=diagnosis.analyzed_bytes,
        extracted_line_count=diagnosis.extracted_line_count,
        context=context,
        stage=stage,
        diagnosis_json=asdict(diagnosis),
        thumbs=None,
    )
    db.add(analysis)

    # Plane 2 — anonymized corpus row. Gated.
    if write_corpus:
        corpus = CorpusEntry(
            id=uuid.uuid4(),
            created_at=utc_now(),
            error_signature_hash=anon.sig_hash,
            error_codes=",".join(anon.error_codes)[:255],
            table_shape_signature=None,  # populated when DDL is in the input — future enhancement
            fix_pattern=diagnosis.confidence,  # coarse first cut; refine when we add categories
            outcome_thumbs=None,
            source_feature="troubleshoot",
        )
        db.add(corpus)

    db.commit()
    db.refresh(analysis)

    return diagnosis, analysis.id
