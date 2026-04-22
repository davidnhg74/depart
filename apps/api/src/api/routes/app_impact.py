"""HTTP route for the app-impact analyzer.

POST /api/v3/analyze/app-impact
  multipart/form-data:
    schema_zip:  Oracle DDL zip (CREATE TABLE, packages, etc.)
    source_zip:  customer application source zip (.java, .py, .cs, .xml)
    explain:     bool (default false) — call the AI explanation layer
    languages:   comma-separated list to restrict scan (default: all)

Response: 200 with EnrichedAppImpactReport (schema-typed below). On AI
failures we still return the deterministic report; the response includes
counts so the caller can see how many enrichments succeeded.
"""

from __future__ import annotations

import logging
import tempfile
import zipfile
from pathlib import Path
from typing import List, Optional

from fastapi import APIRouter, File, Form, HTTPException, UploadFile
from pydantic import BaseModel, Field

from ...analyze.app_impact import AppImpactAnalyzer
from ...source.oracle.parser import parse as parse_oracle
from ...ai.services.app_impact import AppImpactExplainer

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/v3/analyze", tags=["analyze"])


# ─── Response schema (Pydantic, JSON-serializable) ───────────────────────────


class FindingDTO(BaseModel):
    code: str
    risk: str
    message: str
    suggestion: str
    file: str
    line: int
    snippet: str
    schema_objects: List[str] = Field(default_factory=list)
    construct_tags: List[str] = Field(default_factory=list)
    explanation: Optional[str] = None
    before: Optional[str] = None
    after: Optional[str] = None
    caveats: List[str] = Field(default_factory=list)


class FileImpactDTO(BaseModel):
    file: str
    language: str
    fragments_scanned: int
    findings: List[FindingDTO]
    max_risk: str


class AppImpactResponse(BaseModel):
    files: List[FileImpactDTO]
    total_files_scanned: int
    total_fragments: int
    total_findings: int
    findings_by_risk: dict
    schema_objects_scanned: int
    explained: bool
    explanations_generated: int = 0
    explanations_failed: int = 0


# ─── Route ───────────────────────────────────────────────────────────────────


MAX_ZIP_BYTES = 100 * 1024 * 1024  # 100 MB hard cap on either zip


@router.post("/app-impact", response_model=AppImpactResponse)
async def analyze_app_impact(
    schema_zip: UploadFile = File(..., description="Zip of Oracle DDL"),
    source_zip: UploadFile = File(..., description="Zip of application source"),
    explain: bool = Form(default=False),
    languages: Optional[str] = Form(default=None),
):
    lang_list = _parse_languages(languages)

    with tempfile.TemporaryDirectory(prefix="depart_appimpact_") as tmpdir:
        tmp = Path(tmpdir)
        schema_dir = await _unzip_to(schema_zip, tmp / "schema")
        source_dir = await _unzip_to(source_zip, tmp / "source")

        schema_module = _parse_schema_dir(schema_dir)
        analyzer = AppImpactAnalyzer(schema=schema_module)
        report = analyzer.analyze_directory(source_dir, languages=lang_list)

        if explain:
            try:
                explainer = AppImpactExplainer(schema=schema_module)
                enriched = explainer.enrich(report)
                return _serialize_enriched(enriched, schema_module)
            except RuntimeError as e:
                # E.g. ANTHROPIC_API_KEY missing — fall through to non-AI response.
                logger.warning("AI enrichment unavailable: %s", e)

        return _serialize_deterministic(report, schema_module)


# ─── helpers ─────────────────────────────────────────────────────────────────


async def _unzip_to(upload: UploadFile, dest: Path) -> Path:
    dest.mkdir(parents=True, exist_ok=True)
    data = await upload.read()
    if not data:
        raise HTTPException(400, f"{upload.filename or 'upload'} is empty")
    if len(data) > MAX_ZIP_BYTES:
        raise HTTPException(
            413, f"{upload.filename} exceeds {MAX_ZIP_BYTES // (1024 * 1024)} MB cap"
        )
    tmp_zip = dest.parent / f"{dest.name}.zip"
    tmp_zip.write_bytes(data)
    try:
        with zipfile.ZipFile(tmp_zip) as zf:
            zf.extractall(dest)
    except zipfile.BadZipFile:
        raise HTTPException(400, f"{upload.filename} is not a valid zip")
    finally:
        tmp_zip.unlink(missing_ok=True)
    return dest


def _parse_schema_dir(schema_dir: Path):
    """Concatenate every .sql file in the schema dir and parse as one Module."""
    sql_text = "\n\n".join(
        p.read_text(encoding="utf-8", errors="replace") for p in sorted(schema_dir.rglob("*.sql"))
    )
    if not sql_text.strip():
        return None
    return parse_oracle(sql_text, name="<schema>")


def _parse_languages(raw: Optional[str]) -> Optional[List[str]]:
    if not raw:
        return None
    return [t.strip() for t in raw.split(",") if t.strip()]


def _serialize_deterministic(report, schema_module) -> AppImpactResponse:
    return AppImpactResponse(
        files=[
            FileImpactDTO(
                file=fi.file,
                language=fi.language,
                fragments_scanned=fi.fragments_scanned,
                max_risk=fi.max_risk.value,
                findings=[_finding_dto(f) for f in fi.findings],
            )
            for fi in report.files
        ],
        total_files_scanned=report.total_files_scanned,
        total_fragments=report.total_fragments,
        total_findings=report.total_findings,
        findings_by_risk=dict(report.findings_by_risk),
        schema_objects_scanned=_count_schema_objects(schema_module),
        explained=False,
    )


def _serialize_enriched(enriched, schema_module) -> AppImpactResponse:
    return AppImpactResponse(
        files=[
            FileImpactDTO(
                file=efi.file,
                language=efi.language,
                fragments_scanned=efi.fragments_scanned,
                max_risk=_max_risk_str(efi.findings),
                findings=[_enriched_finding_dto(ef) for ef in efi.findings],
            )
            for efi in enriched.files
        ],
        total_files_scanned=enriched.total_files_scanned,
        total_fragments=enriched.total_fragments,
        total_findings=enriched.total_findings,
        findings_by_risk=dict(enriched.findings_by_risk),
        schema_objects_scanned=_count_schema_objects(schema_module),
        explained=True,
        explanations_generated=enriched.explanations_generated,
        explanations_failed=enriched.explanations_failed,
    )


def _max_risk_str(enriched_findings) -> str:
    from ...analyze.app_impact import RiskLevel, _rank

    if not enriched_findings:
        return RiskLevel.LOW.value
    return max((ef.finding for ef in enriched_findings), key=lambda f: _rank(f.risk)).risk.value


def _finding_dto(f) -> FindingDTO:
    return FindingDTO(
        code=f.code,
        risk=f.risk.value,
        message=f.message,
        suggestion=f.suggestion,
        file=f.file,
        line=f.line,
        snippet=f.snippet,
        schema_objects=list(f.schema_objects),
        construct_tags=[t.value for t in f.construct_tags],
    )


def _enriched_finding_dto(ef) -> FindingDTO:
    base = _finding_dto(ef.finding)
    base.explanation = ef.explanation or None
    base.before = ef.before or None
    base.after = ef.after or None
    base.caveats = list(ef.caveats)
    return base


def _count_schema_objects(module) -> int:
    if module is None:
        return 0
    return sum(1 for o in module.objects if o.name != "<module-constructs>")
