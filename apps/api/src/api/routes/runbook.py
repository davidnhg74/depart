"""HTTP route for the migration runbook generator.

POST /api/v3/projects/runbook
  multipart/form-data:
    schema_zip:    Oracle DDL zip
    source_zip:    application source zip (optional)
    project_name:  required string
    customer:      required string
    source_version, target_version, cutover_window: optional
    rate_per_day:  optional int (default 1500)
    explain:       bool (default false) — adds AI exec summary + risk narrative
    format:        'pdf' (default) or 'json' for the structured runbook

Returns either application/pdf or a JSON serialization of the Runbook.
"""

from __future__ import annotations

import logging
import tempfile
import zipfile
from pathlib import Path
from typing import List, Optional

from fastapi import APIRouter, File, Form, HTTPException, UploadFile
from fastapi.responses import Response
from pydantic import BaseModel

from ...analyze.app_impact import AppImpactAnalyzer
from ...analyze.complexity import analyze as analyze_complexity
from ...source.oracle.parser import parse as parse_oracle
from ...projects.pdf import render as render_pdf
from ...projects.runbook import RunbookContext, assemble
from ...ai.services.runbook import RunbookGenerator
from ...ai.services.app_impact import AppImpactExplainer

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/v3/projects", tags=["projects"])


MAX_ZIP_BYTES = 100 * 1024 * 1024


# ─── Response shape (when format=json) ───────────────────────────────────────


class PhaseDTO(BaseModel):
    title: str
    description: str
    prerequisites: List[str]
    activities: List[str]
    rollback: List[str]
    duration_days: float
    risk_level: str


class BlockerDTO(BaseModel):
    code: str
    message: str
    file: str
    line: int
    suggestion: str
    explanation: str = ""


class RunbookDTO(BaseModel):
    project_name: str
    customer: str
    source_version: str
    target_version: str
    cutover_window: str
    rate_per_day: int
    executive_summary: str
    risk_narrative: str
    phases: List[PhaseDTO]
    blockers: List[BlockerDTO]
    sign_offs: List[str]
    generated_at: str
    prompt_version: str = ""
    explained: bool = False


# ─── Route ───────────────────────────────────────────────────────────────────


@router.post("/runbook")
async def generate_runbook(
    project_name: str = Form(...),
    customer: str = Form(...),
    schema_zip: UploadFile = File(...),
    source_zip: Optional[UploadFile] = File(default=None),
    source_version: str = Form(default="Oracle 19c"),
    target_version: str = Form(default="PostgreSQL 16"),
    cutover_window: str = Form(default="TBD"),
    rate_per_day: int = Form(default=1500),
    explain: bool = Form(default=False),
    format: str = Form(default="pdf"),
):
    if format not in ("pdf", "json"):
        raise HTTPException(400, "format must be 'pdf' or 'json'")

    with tempfile.TemporaryDirectory(prefix="depart_runbook_") as tmpdir:
        tmp = Path(tmpdir)

        schema_dir = await _unzip(schema_zip, tmp / "schema")
        sql_text = _read_sql(schema_dir)
        if not sql_text.strip():
            raise HTTPException(400, "schema_zip contains no .sql files")
        complexity = analyze_complexity(sql_text, rate_per_day=rate_per_day)
        schema_module = parse_oracle(sql_text, name="<schema>")

        app_impact = None
        if source_zip is not None:
            source_dir = await _unzip(source_zip, tmp / "source")
            analyzer = AppImpactAnalyzer(schema=schema_module)
            report = analyzer.analyze_directory(source_dir)
            if explain:
                try:
                    app_impact = AppImpactExplainer(schema=schema_module).enrich(report)
                except RuntimeError as e:
                    logger.warning("AI app-impact enrichment unavailable: %s", e)
                    app_impact = report
            else:
                app_impact = report

        ctx = RunbookContext(
            project_name=project_name,
            customer=customer,
            source_version=source_version,
            target_version=target_version,
            cutover_window=cutover_window,
            rate_per_day=rate_per_day,
            complexity=complexity,
            app_impact=app_impact,
        )

        if explain:
            try:
                runbook = RunbookGenerator().generate(ctx)
            except RuntimeError as e:
                logger.warning("AI runbook narrative unavailable: %s", e)
                runbook = assemble(ctx)
        else:
            runbook = assemble(ctx)

    if format == "pdf":
        pdf_bytes = render_pdf(runbook)
        return Response(
            content=pdf_bytes,
            media_type="application/pdf",
            headers={
                "Content-Disposition": f'attachment; filename="runbook-{customer.replace(" ", "_")}.pdf"',
            },
        )

    return _to_dto(runbook, explained=bool(runbook.prompt_version))


# ─── helpers ─────────────────────────────────────────────────────────────────


async def _unzip(upload: UploadFile, dest: Path) -> Path:
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


def _read_sql(schema_dir: Path) -> str:
    return "\n\n".join(
        p.read_text(encoding="utf-8", errors="replace") for p in sorted(schema_dir.rglob("*.sql"))
    )


def _to_dto(runbook, *, explained: bool) -> RunbookDTO:
    return RunbookDTO(
        project_name=runbook.context.project_name,
        customer=runbook.context.customer,
        source_version=runbook.context.source_version,
        target_version=runbook.context.target_version,
        cutover_window=runbook.context.cutover_window,
        rate_per_day=runbook.context.rate_per_day,
        executive_summary=runbook.executive_summary,
        risk_narrative=runbook.risk_narrative,
        phases=[
            PhaseDTO(
                title=p.title,
                description=p.description,
                prerequisites=list(p.prerequisites),
                activities=list(p.activities),
                rollback=list(p.rollback),
                duration_days=p.duration_days,
                risk_level=p.risk_level.value,
            )
            for p in runbook.phases
        ],
        blockers=[
            BlockerDTO(
                code=b.code,
                message=b.message,
                file=b.file,
                line=b.line,
                suggestion=b.suggestion,
                explanation=b.explanation,
            )
            for b in runbook.blockers
        ],
        sign_offs=list(runbook.sign_offs),
        generated_at=runbook.generated_at.isoformat(),
        prompt_version=runbook.prompt_version,
        explained=explained,
    )
