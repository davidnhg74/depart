"""Tests for the /api/v3/analyze/app-impact endpoint.

We mount just the router on a fresh FastAPI app per test — avoids
importing the full src.main module (which has heavier dependencies)
and isolates each test from session state.
"""

import io
import zipfile
from pathlib import Path
from unittest.mock import patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient


FIXTURES = Path(__file__).parent / "fixtures" / "app_impact"


@pytest.fixture
def client():
    from src.api.routes.app_impact import router

    app = FastAPI()
    app.include_router(router)
    return TestClient(app)


def _zip_dir(path: Path) -> bytes:
    """Pack every regular file under `path` into an in-memory zip."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for p in sorted(path.rglob("*")):
            if p.is_file():
                zf.write(p, arcname=str(p.relative_to(path)))
    return buf.getvalue()


def _make_zip(files: dict) -> bytes:
    """Build an in-memory zip from a dict of {arcname: bytes-or-str}."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for name, content in files.items():
            data = content.encode("utf-8") if isinstance(content, str) else content
            zf.writestr(name, data)
    return buf.getvalue()


# ─── Happy path ──────────────────────────────────────────────────────────────


class TestDeterministicResponse:
    def test_returns_200_with_findings(self, client):
        schema_zip = _zip_dir(FIXTURES / "schema")
        source_zip = _zip_dir(FIXTURES / "java")

        resp = client.post(
            "/api/v3/analyze/app-impact",
            files={
                "schema_zip": ("schema.zip", schema_zip, "application/zip"),
                "source_zip": ("source.zip", source_zip, "application/zip"),
            },
            data={"explain": "false", "languages": "java"},
        )
        assert resp.status_code == 200, resp.text
        body = resp.json()

        assert body["explained"] is False
        assert body["total_files_scanned"] == 1
        assert body["total_findings"] >= 6
        assert body["schema_objects_scanned"] >= 3
        codes = {f["code"] for fi in body["files"] for f in fi["findings"]}
        # The Java fixture's representative Oracle patterns must surface.
        for must_have in (
            "APP.SQL.ROWNUM",
            "APP.SQL.MERGE",
            "APP.SQL.DBMS_OUTPUT",
            "APP.SQL.SYSREF.DUAL",
            "APP.SCHEMA.UNKNOWN_OBJECT",
            "APP.SQL.FN.NVL",
            "APP.SQL.FN.SYSDATE",
        ):
            assert must_have in codes, f"missing {must_have} in {sorted(codes)}"

    def test_default_explain_false(self, client):
        # Omit `explain` entirely — must default to no AI call.
        resp = client.post(
            "/api/v3/analyze/app-impact",
            files={
                "schema_zip": ("s.zip", _zip_dir(FIXTURES / "schema"), "application/zip"),
                "source_zip": ("c.zip", _zip_dir(FIXTURES / "java"), "application/zip"),
            },
        )
        assert resp.status_code == 200
        assert resp.json()["explained"] is False

    def test_languages_filter_applied(self, client):
        schema_zip = _zip_dir(FIXTURES / "schema")
        # Build a zip mixing Java + Python; ask for python only.
        mixed = _make_zip(
            {
                "Repo.java": '"SELECT * FROM employees WHERE ROWNUM <= 10"',
                "repo.py": "x = 'SELECT * FROM employees WHERE ROWNUM <= 10'",
            }
        )
        resp = client.post(
            "/api/v3/analyze/app-impact",
            files={
                "schema_zip": ("s.zip", schema_zip, "application/zip"),
                "source_zip": ("c.zip", mixed, "application/zip"),
            },
            data={"languages": "python"},
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["total_files_scanned"] == 1
        assert body["files"][0]["language"] == "python"


# ─── AI explanation path ─────────────────────────────────────────────────────


class TestExplainPath:
    @patch("src.api.routes.app_impact.AppImpactExplainer")
    def test_explain_true_invokes_explainer(self, MockExplainer, client):
        from src.analyze.app_impact import RiskLevel
        from src.ai.services.app_impact import (
            EnrichedAppImpactReport,
            EnrichedFileImpact,
            EnrichedFinding,
        )
        from src.analyze.app_impact import Finding

        # Build a fake enriched report with one finding so the route serializes
        # the explained branch.
        fake_finding = Finding(
            code="APP.SQL.NVL",
            risk=RiskLevel.MEDIUM,
            message="m",
            suggestion="s",
            file="x.java",
            line=1,
            snippet="NVL(...)",
        )
        ef = EnrichedFinding(
            finding=fake_finding,
            explanation="AI explains NVL.",
            before="-- NVL(x, y)",
            after="COALESCE(x, y)",
            caveats=("watch nulls",),
        )
        MockExplainer.return_value.enrich.return_value = EnrichedAppImpactReport(
            files=[
                EnrichedFileImpact(
                    file="x.java",
                    language="java",
                    fragments_scanned=1,
                    findings=[ef],
                )
            ],
            total_files_scanned=1,
            total_fragments=1,
            total_findings=1,
            findings_by_risk={"medium": 1},
            explanations_generated=1,
            explanations_failed=0,
        )

        resp = client.post(
            "/api/v3/analyze/app-impact",
            files={
                "schema_zip": ("s.zip", _zip_dir(FIXTURES / "schema"), "application/zip"),
                "source_zip": ("c.zip", _zip_dir(FIXTURES / "java"), "application/zip"),
            },
            data={"explain": "true"},
        )
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["explained"] is True
        assert body["explanations_generated"] == 1
        finding = body["files"][0]["findings"][0]
        assert finding["explanation"] == "AI explains NVL."
        assert finding["after"] == "COALESCE(x, y)"
        assert finding["caveats"] == ["watch nulls"]

    @patch("src.api.routes.app_impact.AppImpactExplainer")
    def test_ai_failure_falls_back_to_deterministic(self, MockExplainer, client):
        # If the explainer raises (e.g. missing API key), the route should
        # log + return the deterministic report rather than 500.
        MockExplainer.side_effect = RuntimeError("ANTHROPIC_API_KEY is not configured.")

        resp = client.post(
            "/api/v3/analyze/app-impact",
            files={
                "schema_zip": ("s.zip", _zip_dir(FIXTURES / "schema"), "application/zip"),
                "source_zip": ("c.zip", _zip_dir(FIXTURES / "java"), "application/zip"),
            },
            data={"explain": "true"},
        )
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["explained"] is False  # deterministic fallback


# ─── Error paths ─────────────────────────────────────────────────────────────


class TestErrors:
    def test_empty_schema_zip(self, client):
        resp = client.post(
            "/api/v3/analyze/app-impact",
            files={
                "schema_zip": ("s.zip", b"", "application/zip"),
                "source_zip": ("c.zip", _zip_dir(FIXTURES / "java"), "application/zip"),
            },
        )
        assert resp.status_code == 400
        assert "empty" in resp.json()["detail"].lower()

    def test_invalid_zip(self, client):
        resp = client.post(
            "/api/v3/analyze/app-impact",
            files={
                "schema_zip": ("s.zip", b"not a zip", "application/zip"),
                "source_zip": ("c.zip", _zip_dir(FIXTURES / "java"), "application/zip"),
            },
        )
        assert resp.status_code == 400
        assert "valid zip" in resp.json()["detail"].lower()

    def test_missing_schema_zip(self, client):
        resp = client.post(
            "/api/v3/analyze/app-impact",
            files={"source_zip": ("c.zip", _zip_dir(FIXTURES / "java"), "application/zip")},
        )
        assert resp.status_code == 422  # FastAPI validation error
