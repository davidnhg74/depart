"""Tests for /api/v3/projects/runbook."""

import io
import zipfile
from pathlib import Path
from unittest.mock import patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pypdf import PdfReader


def _pdf_text(pdf_bytes: bytes) -> str:
    reader = PdfReader(io.BytesIO(pdf_bytes))
    return "\n".join(p.extract_text() or "" for p in reader.pages)


FIXTURES = Path(__file__).parent / "fixtures" / "app_impact"


@pytest.fixture
def client():
    from src.api.routes.runbook import router

    app = FastAPI()
    app.include_router(router)
    return TestClient(app)


def _zip_dir(path: Path) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for p in sorted(path.rglob("*")):
            if p.is_file():
                zf.write(p, arcname=str(p.relative_to(path)))
    return buf.getvalue()


# ─── PDF format ──────────────────────────────────────────────────────────────


class TestPdfResponse:
    def test_returns_pdf_with_attachment(self, client):
        resp = client.post(
            "/api/v3/projects/runbook",
            files={
                "schema_zip": ("s.zip", _zip_dir(FIXTURES / "schema"), "application/zip"),
            },
            data={
                "project_name": "ACME Migration",
                "customer": "ACME Corp",
                "format": "pdf",
            },
        )
        assert resp.status_code == 200, resp.text
        assert resp.headers["content-type"] == "application/pdf"
        assert "attachment" in resp.headers["content-disposition"]
        assert resp.content.startswith(b"%PDF-")
        text = _pdf_text(resp.content)
        assert "ACME Corp" in text

    def test_includes_app_impact_in_pdf_when_source_provided(self, client):
        resp = client.post(
            "/api/v3/projects/runbook",
            files={
                "schema_zip": ("s.zip", _zip_dir(FIXTURES / "schema"), "application/zip"),
                "source_zip": ("c.zip", _zip_dir(FIXTURES / "java"), "application/zip"),
            },
            data={
                "project_name": "ACME Migration",
                "customer": "ACME Corp",
                "format": "pdf",
            },
        )
        assert resp.status_code == 200
        text = _pdf_text(resp.content)
        # The fixture has CRITICAL findings that show up under "Cutover Blockers".
        assert "DBLINK" in text or "DBMS_OUTPUT" in text


# ─── JSON format ─────────────────────────────────────────────────────────────


class TestJsonResponse:
    def test_returns_structured_runbook(self, client):
        resp = client.post(
            "/api/v3/projects/runbook",
            files={
                "schema_zip": ("s.zip", _zip_dir(FIXTURES / "schema"), "application/zip"),
            },
            data={
                "project_name": "ACME",
                "customer": "ACME Corp",
                "format": "json",
            },
        )
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["customer"] == "ACME Corp"
        assert body["project_name"] == "ACME"
        assert len(body["phases"]) == 6
        for p in body["phases"]:
            assert p["title"]
            assert p["activities"]
            assert p["rollback"]
            assert p["risk_level"] in {"low", "medium", "high", "critical"}
        assert isinstance(body["sign_offs"], list) and len(body["sign_offs"]) >= 4
        assert body["explained"] is False

    def test_app_impact_blockers_in_json(self, client):
        resp = client.post(
            "/api/v3/projects/runbook",
            files={
                "schema_zip": ("s.zip", _zip_dir(FIXTURES / "schema"), "application/zip"),
                "source_zip": ("c.zip", _zip_dir(FIXTURES / "java"), "application/zip"),
            },
            data={
                "project_name": "ACME",
                "customer": "ACME Corp",
                "format": "json",
            },
        )
        body = resp.json()
        codes = {b["code"] for b in body["blockers"]}
        # CRITICAL Java fixtures: DBMS_OUTPUT, DBLINK, DUAL, UNKNOWN_OBJECT.
        assert "APP.SQL.DBMS_OUTPUT" in codes
        assert "APP.SQL.DBLINK" in codes


# ─── AI explanation path ─────────────────────────────────────────────────────


class TestExplainPath:
    @patch("src.api.routes.runbook.RunbookGenerator")
    @patch("src.api.routes.runbook.AppImpactExplainer")
    def test_explain_true_invokes_ai_layers(self, MockExplainer, MockGen, client):
        from src.projects.runbook import assemble, RunbookContext
        from src.analyze.complexity import analyze as analyze_complexity

        # The runbook generator returns whatever assemble() builds — we
        # short-circuit by having the mock return a runbook with our
        # AI-tagged sections.
        src_text = (FIXTURES / "schema" / "schema.sql").read_text()
        cx = analyze_complexity(src_text)
        ctx = RunbookContext(project_name="X", customer="Y", complexity=cx, app_impact=None)
        rb = assemble(
            ctx, executive_summary="AI EXEC.", risk_narrative="AI RISK.", prompt_version="v-test"
        )
        MockGen.return_value.generate.return_value = rb
        # The app-impact explainer is invoked on the (real) deterministic
        # report; just return whatever the analyzer found, untouched.
        MockExplainer.return_value.enrich.side_effect = lambda r: r

        resp = client.post(
            "/api/v3/projects/runbook",
            files={
                "schema_zip": ("s.zip", _zip_dir(FIXTURES / "schema"), "application/zip"),
                "source_zip": ("c.zip", _zip_dir(FIXTURES / "java"), "application/zip"),
            },
            data={
                "project_name": "X",
                "customer": "Y",
                "explain": "true",
                "format": "json",
            },
        )
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["executive_summary"] == "AI EXEC."
        assert body["risk_narrative"] == "AI RISK."
        assert body["prompt_version"] == "v-test"
        assert body["explained"] is True

    @patch("src.api.routes.runbook.RunbookGenerator")
    def test_ai_runbook_failure_falls_back_to_assemble(self, MockGen, client):
        MockGen.side_effect = RuntimeError("ANTHROPIC_API_KEY is not configured.")

        resp = client.post(
            "/api/v3/projects/runbook",
            files={
                "schema_zip": ("s.zip", _zip_dir(FIXTURES / "schema"), "application/zip"),
            },
            data={
                "project_name": "X",
                "customer": "Y",
                "explain": "true",
                "format": "json",
            },
        )
        assert resp.status_code == 200
        # Deterministic fallback — explained=False because no AI text landed.
        body = resp.json()
        assert body["explained"] is False


# ─── Errors ──────────────────────────────────────────────────────────────────


class TestErrors:
    def test_invalid_format(self, client):
        resp = client.post(
            "/api/v3/projects/runbook",
            files={"schema_zip": ("s.zip", _zip_dir(FIXTURES / "schema"), "application/zip")},
            data={"project_name": "X", "customer": "Y", "format": "xml"},
        )
        assert resp.status_code == 400

    def test_empty_schema(self, client):
        resp = client.post(
            "/api/v3/projects/runbook",
            files={"schema_zip": ("s.zip", b"", "application/zip")},
            data={"project_name": "X", "customer": "Y"},
        )
        assert resp.status_code == 400

    def test_schema_with_no_sql_files(self, client):
        # Zip contains a .txt file but no .sql files — must 400.
        import io
        import zipfile

        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("readme.txt", "no sql here")
        resp = client.post(
            "/api/v3/projects/runbook",
            files={"schema_zip": ("s.zip", buf.getvalue(), "application/zip")},
            data={"project_name": "X", "customer": "Y"},
        )
        assert resp.status_code == 400

    def test_missing_required_fields(self, client):
        # Missing project_name + customer.
        resp = client.post(
            "/api/v3/projects/runbook",
            files={"schema_zip": ("s.zip", _zip_dir(FIXTURES / "schema"), "application/zip")},
        )
        assert resp.status_code == 422
