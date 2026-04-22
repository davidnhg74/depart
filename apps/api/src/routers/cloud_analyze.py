"""Cloud-only legacy analyze + job endpoints.

These three endpoints — `/api/v1/analyze`, `/api/v1/jobs/{id}`, and
`/api/v1/report/{id}/pdf` — belong to the email-gated SaaS flow that
predates the modern `/api/v1/assess` path. They persist a `Lead` + an
`AnalysisJob` in Postgres, write the uploaded zip to disk, and gate on
per-plan migration quotas.

Self-hosted installs don't need (and shouldn't run) any of this:

  * no email gate — operators own the box
  * no Lead / CRM concept — nobody to market to
  * no plan limits — local license is already feature-gated
  * no persistent job table — `/assess` runs in-memory

So this router only mounts when `settings.enable_cloud_routes` is True.
The marketing/purchase site at depart.cloud flips that on; the product
image keeps it off.

(The endpoints were previously inlined in `main.py`; moving them here
keeps `main.py` focused on bootstrap + router wiring and makes the
"what's cloud-only?" decision grep-able.)
"""

from __future__ import annotations

import os
import uuid
import zipfile

from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile, status
from fastapi.responses import FileResponse
from sqlalchemy.orm import Session

from ..analyze.complexity import ComplexityScorer
from ..auth.dependencies import get_optional_user
from ..config import settings
from ..db import get_db
from ..models import AnalysisJob, JobStatus, Lead, User
from ..reports.pdf_generator import PDFReportGenerator
from ..services.billing import get_plan_limits
from ..utils.time import utc_now


router = APIRouter(tags=["cloud-analyze"])


# Uploads directory — same location the old inline handler used so
# existing PDFs remain reachable across the refactor.
UPLOADS_DIR = os.environ.get("UPLOADS_DIR", "/tmp/depart_uploads")
os.makedirs(UPLOADS_DIR, exist_ok=True)


@router.post("/api/v1/analyze")
async def analyze(
    file: UploadFile = File(...),
    email: str = Form(...),
    rate_per_day: int = Form(default=1000),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_optional_user),
):
    """Upload a zip of Oracle DDL/PL-SQL and get a complexity report.

    Email is required for the Lead record (legacy SaaS CRM). Authenticated
    users are subject to their plan's monthly quota."""
    try:
        if current_user:
            limits = get_plan_limits(current_user.plan.value)
            if limits.get("migrations_per_month") is not None:
                if current_user.migrations_used_this_month >= limits["migrations_per_month"]:
                    raise HTTPException(
                        status_code=status.HTTP_402_PAYMENT_REQUIRED,
                        detail=f"Monthly migration limit reached ({limits['migrations_per_month']})",
                        headers={"X-Upgrade-URL": f"{settings.frontend_url}/billing"},
                    )

        file_content = await file.read()
        if len(file_content) > settings.max_upload_size:
            raise HTTPException(
                status_code=400,
                detail=f"File too large. Max size: {settings.max_upload_size} bytes",
            )

        lead = db.query(Lead).filter(Lead.email == email).first()
        if not lead:
            lead = Lead(email=email)
            db.add(lead)
            db.commit()
            db.refresh(lead)

        job = AnalysisJob(
            lead_id=lead.id, rate_per_day=rate_per_day, status=JobStatus.PROCESSING
        )
        db.add(job)
        db.commit()
        db.refresh(job)

        file_path = os.path.join(UPLOADS_DIR, f"{job.id}.zip")
        with open(file_path, "wb") as f:
            f.write(file_content)

        try:
            all_content = ""
            with zipfile.ZipFile(file_path, "r") as zip_ref:
                for file_info in zip_ref.filelist:
                    if file_info.filename.endswith((".sql", ".pls", ".plsql", ".txt")):
                        try:
                            all_content += zip_ref.read(file_info).decode(
                                "utf-8", errors="ignore"
                            )
                            all_content += "\n"
                        except Exception:
                            pass

            scorer = ComplexityScorer()
            report = scorer.analyze(all_content, rate_per_day)

            pdf_generator = PDFReportGenerator()
            pdf_content = pdf_generator.generate(report)

            pdf_path = os.path.join(UPLOADS_DIR, f"{job.id}_report.pdf")
            with open(pdf_path, "wb") as f:
                f.write(pdf_content)

            job.complexity_report = {
                "score": report.score,
                "total_lines": report.total_lines,
                "auto_convertible_lines": report.auto_convertible_lines,
                "needs_review_lines": report.needs_review_lines,
                "must_rewrite_lines": report.must_rewrite_lines,
                "construct_counts": report.construct_counts,
                "effort_estimate_days": report.effort_estimate_days,
                "estimated_cost": report.estimated_cost,
                "top_10_constructs": report.top_10_constructs,
            }
            job.pdf_path = str(pdf_path)
            job.status = JobStatus.DONE
            job.completed_at = utc_now()

            if current_user:
                current_user.migrations_used_this_month += 1

            db.commit()

            return {
                "job_id": str(job.id),
                "status": job.status.value,
                "complexity_report": job.complexity_report,
            }

        except zipfile.BadZipFile:
            job.status = JobStatus.ERROR
            job.error_message = "Invalid zip file"
            db.commit()
            raise HTTPException(status_code=400, detail="Invalid zip file")

    except HTTPException:
        raise
    except Exception as e:
        if "job" in locals():
            job.status = JobStatus.ERROR
            job.error_message = str(e)
            db.commit()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/api/v1/jobs/{job_id}")
async def get_job(job_id: str, db: Session = Depends(get_db)):
    """Get job status and report."""
    try:
        job_uuid = uuid.UUID(job_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid job ID format")

    job = db.query(AnalysisJob).filter(AnalysisJob.id == job_uuid).first()
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    return {
        "id": str(job.id),
        "status": job.status.value,
        "complexity_report": job.complexity_report,
        "created_at": job.created_at.isoformat(),
        "completed_at": job.completed_at.isoformat() if job.completed_at else None,
    }


@router.get("/api/v1/report/{job_id}/pdf")
async def get_pdf_report(job_id: str, db: Session = Depends(get_db)):
    """Download PDF report for a completed job."""
    try:
        job_uuid = uuid.UUID(job_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid job ID format")

    job = db.query(AnalysisJob).filter(AnalysisJob.id == job_uuid).first()
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    if job.status != JobStatus.DONE:
        raise HTTPException(status_code=400, detail="Job not completed")
    if not job.pdf_path or not os.path.exists(job.pdf_path):
        raise HTTPException(status_code=404, detail="PDF report not found")

    return FileResponse(
        job.pdf_path, media_type="application/pdf", filename=f"depart_analysis_{job_id}.pdf"
    )
