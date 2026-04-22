"""AI explanation layer for app-impact findings.

Wraps the deterministic AppImpactReport from `analyze.app_impact` with a
per-finding `explanation`, `before`/`after` code change example, and a
`caveats` list. Batches findings into groups of N per LLM call so we pay
the prompt-cache hit on the (large, stable) system prompt and amortize
output tokens.

The AI layer is purely additive — every Finding still carries its
deterministic risk and suggestion. If the LLM call fails, we degrade to
the deterministic output rather than hiding the report.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Iterable, List, Optional

from ...analyze.app_impact import AppImpactReport, Finding
from ...core.ir.nodes import Module, ObjectKind
from ..client import AIClient
from ..prompts.app_impact import SYSTEM_PROMPT, VERSION, render_user_message

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class EnrichedFinding:
    """A Finding plus AI-generated developer-friendly content."""

    finding: Finding
    explanation: str
    before: str
    after: str
    caveats: tuple = ()
    prompt_version: str = VERSION

    @property
    def has_explanation(self) -> bool:
        return bool(self.explanation.strip())


@dataclass
class EnrichedFileImpact:
    file: str
    language: str
    fragments_scanned: int
    findings: List[EnrichedFinding] = field(default_factory=list)


@dataclass
class EnrichedAppImpactReport:
    files: List[EnrichedFileImpact] = field(default_factory=list)
    total_files_scanned: int = 0
    total_fragments: int = 0
    total_findings: int = 0
    findings_by_risk: dict = field(default_factory=dict)
    explanations_generated: int = 0
    explanations_failed: int = 0


# Findings batched per LLM call. Trade-off: smaller batches give better
# attention per finding but cost more system-prompt-prefill tokens; the
# prompt cache softens that. Calibrated against the eval corpus.
DEFAULT_BATCH_SIZE = 6


@dataclass
class AppImpactExplainer:
    """Wraps an AppImpactReport with AI-generated explanations."""

    client: Optional[AIClient] = None
    schema: Optional[Module] = None
    batch_size: int = DEFAULT_BATCH_SIZE

    def __post_init__(self) -> None:
        if self.client is None:
            self.client = AIClient.fast(feature="app_impact")

    def enrich(self, report: AppImpactReport) -> EnrichedAppImpactReport:
        out = EnrichedAppImpactReport(
            total_files_scanned=report.total_files_scanned,
            total_fragments=report.total_fragments,
            total_findings=report.total_findings,
            findings_by_risk=dict(report.findings_by_risk),
        )
        schema_summary = self._schema_summary()
        for fi in report.files:
            efi = EnrichedFileImpact(
                file=fi.file,
                language=fi.language,
                fragments_scanned=fi.fragments_scanned,
            )
            efi.findings = self._enrich_findings(fi.findings, schema_summary)
            out.files.append(efi)
            out.explanations_generated += sum(1 for f in efi.findings if f.has_explanation)
            out.explanations_failed += sum(1 for f in efi.findings if not f.has_explanation)
        return out

    # ─── internals ───────────────────────────────────────────────────────────

    def _enrich_findings(
        self, findings: List[Finding], schema_summary: str
    ) -> List[EnrichedFinding]:
        enriched: List[EnrichedFinding] = []
        # Sort by risk desc so the first batches contain the most important
        # findings — failures degrade gracefully on the lower-risk tail.
        from ...analyze.app_impact import _rank

        sorted_findings = sorted(findings, key=lambda f: -_rank(f.risk))
        for batch in _chunks(sorted_findings, self.batch_size):
            enriched.extend(self._enrich_batch(batch, schema_summary))
        return enriched

    def _enrich_batch(self, batch: List[Finding], schema_summary: str) -> List[EnrichedFinding]:
        finding_dicts = [
            {
                "code": f.code,
                "file": f.file,
                "line": f.line,
                "snippet": f.snippet,
                "suggestion": f.suggestion,
            }
            for f in batch
        ]
        user = render_user_message(schema_summary=schema_summary, findings=finding_dicts)
        try:
            data = self.client.complete_json(system=SYSTEM_PROMPT, user=user)
        except Exception as e:
            logger.warning("AI enrichment failed for batch of %d: %s", len(batch), e)
            return [_empty_enrichment(f) for f in batch]

        by_code = {d.get("code"): d for d in data.get("findings", []) if "code" in d}
        out: List[EnrichedFinding] = []
        for f in batch:
            d = by_code.get(f.code)
            if not d:
                out.append(_empty_enrichment(f))
                continue
            out.append(
                EnrichedFinding(
                    finding=f,
                    explanation=str(d.get("explanation", "")).strip(),
                    before=str(d.get("before", "")).strip(),
                    after=str(d.get("after", "")).strip(),
                    caveats=tuple(str(c).strip() for c in d.get("caveats", []) if c),
                )
            )
        return out

    def _schema_summary(self) -> str:
        """Compact, model-friendly digest of the parsed schema. Names + kinds
        + first column for each table (helps the model anchor explanations
        in real identifiers without overflowing context)."""
        if self.schema is None:
            return ""
        from ...core.ir.nodes import Table

        lines: List[str] = []
        for o in self.schema.objects:
            if o.kind in {ObjectKind.TABLE, ObjectKind.VIEW, ObjectKind.MATERIALIZED_VIEW}:
                cols = ""
                if isinstance(o, Table) and o.columns:
                    cols = (
                        " ("
                        + ", ".join(c.name for c in o.columns[:6])
                        + (", ..." if len(o.columns) > 6 else "")
                        + ")"
                    )
                lines.append(f"{o.kind.value} {o.name}{cols}")
            elif o.kind in {
                ObjectKind.PROCEDURE,
                ObjectKind.FUNCTION,
                ObjectKind.PACKAGE,
                ObjectKind.PACKAGE_BODY,
                ObjectKind.SEQUENCE,
                ObjectKind.INDEX,
                ObjectKind.TRIGGER,
            }:
                lines.append(f"{o.kind.value} {o.name}")
        return "\n".join(lines[:200])


def _empty_enrichment(f: Finding) -> EnrichedFinding:
    return EnrichedFinding(
        finding=f,
        explanation="",
        before="",
        after="",
        caveats=(),
    )


def _chunks(seq: List, n: int) -> Iterable[List]:
    for i in range(0, len(seq), n):
        yield seq[i : i + n]
