"""Tests for the public POST /api/v1/troubleshoot endpoints.

Two surfaces:
  * `POST /api/v1/troubleshoot/analyze`         — JSON paste path
  * `POST /api/v1/troubleshoot/analyze/upload`  — multipart upload
                                                  (single + multi file)

Coverage:
  * Anonymous works (no auth header, returns 200 + diagnosis)
  * AIClient is patched so we never call Anthropic in CI
  * Tier-aware caps (50MB anon → 413, larger paid)
  * Multi-file concat with header markers
  * `.gz` auto-decompression
  * Plane 1 + Plane 2 rows actually written
  * Authenticated path stamps user_id correctly
"""

from __future__ import annotations

import gzip
import io
import uuid as _uuid
from datetime import timedelta as _td
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.config import settings as env_settings
from src.main import app
from src.models import (
    CorpusEntry,
    PlanEnum,
    TroubleshootAnalysis,
    User,
    UserRole,
)
from src.utils.time import utc_now

pytestmark = pytest.mark.integration

client = TestClient(app)


_DIAGNOSIS_RESPONSE = {
    "likely_cause": "Wrong listener SID",
    "recommended_action": "1. Verify listener.ora",
    "code_suggestion": "lsnrctl status",
    "confidence": "high",
    "escalate_if": "the listener is up but errors persist",
}


@pytest.fixture(autouse=True)
def patch_claude(monkeypatch):
    """Replace AIClient.complete_json so the test never touches
    Anthropic. AIClient.__init__ is also patched so we don't need
    a real API key in the env."""
    from src.ai.client import AIClient

    def _fake_complete_json(self, *, system, user, **_kw):
        return _DIAGNOSIS_RESPONSE

    monkeypatch.setattr(AIClient, "complete_json", _fake_complete_json)
    monkeypatch.setattr(AIClient, "__init__", lambda self, **kw: None)


@pytest.fixture(autouse=True)
def clean_tables():
    """Wipe Plane 1 + Plane 2 around each test so cross-test
    pollution doesn't make assertions flaky."""
    engine = create_engine(env_settings.database_url)
    S = sessionmaker(bind=engine)
    s = S()

    def wipe():
        s2 = S()
        s2.query(CorpusEntry).delete()
        s2.query(TroubleshootAnalysis).delete()
        s2.commit()
        s2.close()

    wipe()
    yield
    wipe()
    engine.dispose()


# ─── Paste path ─────────────────────────────────────────────────────────────


class TestPaste:
    def test_anonymous_paste_returns_diagnosis(self):
        resp = client.post(
            "/api/v1/troubleshoot/analyze",
            json={"logs": "ORA-01017: invalid credentials"},
        )
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["likely_cause"] == "Wrong listener SID"
        assert body["confidence"] == "high"
        assert body["used_ai"] is True
        assert "analysis_id" in body

    def test_paste_persists_plane_1_and_plane_2(self):
        client.post(
            "/api/v1/troubleshoot/analyze",
            json={"logs": "ORA-12541: TNS no listener"},
        )
        engine = create_engine(env_settings.database_url)
        S = sessionmaker(bind=engine)
        s = S()
        try:
            assert s.query(TroubleshootAnalysis).count() == 1
            # Anonymous → user_id NULL on Plane 1
            row = s.query(TroubleshootAnalysis).first()
            assert row.user_id is None
            # Plane 2 written (anonymous opts in by default).
            corpus = s.query(CorpusEntry).first()
            assert corpus is not None
            assert "ORA-12541" in corpus.error_codes
        finally:
            s.close()
            engine.dispose()

    def test_oversized_paste_returns_413(self):
        # Default trial cap = 50MB. Force a smaller cap by patching the
        # plan limit so we don't actually generate 50MB in the test.
        from src.services import billing

        with patch.dict(
            billing.PLAN_LIMITS["trial"],
            {"troubleshoot_max_upload_bytes": 100},
        ):
            resp = client.post(
                "/api/v1/troubleshoot/analyze",
                json={"logs": "x" * 200},
            )
        assert resp.status_code == 413
        assert "cap" in resp.json()["detail"].lower()


# ─── Upload path ────────────────────────────────────────────────────────────


class TestUpload:
    def test_single_file_upload(self):
        files = {
            "files": ("migration.log", io.BytesIO(b"ORA-00942: table not found"), "text/plain"),
        }
        resp = client.post("/api/v1/troubleshoot/analyze/upload", files=files)
        assert resp.status_code == 200, resp.text
        assert resp.json()["likely_cause"] == "Wrong listener SID"

    def test_multi_file_upload_concatenates_with_headers(self):
        # Send 3 files; assert the AI sees them concatenated with
        # `=== filename ===` markers.
        from src.ai.client import AIClient

        captured = {}

        def _capture(self, *, system, user, **_kw):
            captured["user"] = user
            return _DIAGNOSIS_RESPONSE

        with patch.object(AIClient, "complete_json", _capture):
            files = [
                ("files", ("primary.log", io.BytesIO(b"ORA-01017"), "text/plain")),
                ("files", ("source-db.log", io.BytesIO(b"ORA-12541"), "text/plain")),
                ("files", ("target-db.log", io.BytesIO(b"connection ok"), "text/plain")),
            ]
            resp = client.post("/api/v1/troubleshoot/analyze/upload", files=files)
        assert resp.status_code == 200
        assert "=== primary.log ===" in captured["user"]
        assert "=== source-db.log ===" in captured["user"]
        assert "=== target-db.log ===" in captured["user"]

    def test_gz_uploads_are_decompressed(self):
        raw = b"ORA-01017: gzipped content"
        gz = gzip.compress(raw)
        files = {
            "files": ("alert.log.gz", io.BytesIO(gz), "application/gzip"),
        }
        resp = client.post("/api/v1/troubleshoot/analyze/upload", files=files)
        assert resp.status_code == 200, resp.text

    def test_too_many_files_rejected(self):
        files = [
            ("files", (f"f{i}.log", io.BytesIO(b"x"), "text/plain"))
            for i in range(6)  # >5
        ]
        resp = client.post("/api/v1/troubleshoot/analyze/upload", files=files)
        assert resp.status_code == 400
        assert "5 files" in resp.json()["detail"]

    def test_oversized_upload_returns_413(self):
        from src.services import billing

        big = b"x" * 200
        with patch.dict(
            billing.PLAN_LIMITS["trial"],
            {"troubleshoot_max_upload_bytes": 100},
        ):
            files = {"files": ("big.log", io.BytesIO(big), "text/plain")}
            resp = client.post("/api/v1/troubleshoot/analyze/upload", files=files)
        assert resp.status_code == 413


# ─── Authenticated calls (cloud mode) ──────────────────────────────────────


class TestAuthenticated:
    @pytest.fixture
    def signed_in_user(self, monkeypatch):
        """Persist a real Pro-tier User and override get_optional_user
        to return them. Also enables auth so the override actually
        gets consulted by anything inside the request."""
        from src.auth import dependencies as _auth_deps
        from src.config import settings as cfg

        monkeypatch.setattr(cfg, "enable_self_hosted_auth", True)

        engine = create_engine(env_settings.database_url)
        S = sessionmaker(bind=engine)
        s = S()
        try:
            u = User(
                id=_uuid.uuid4(),
                email=f"{_uuid.uuid4().hex[:8]}@example.com",
                full_name="Pro User",
                hashed_password="x" * 60,
                role=UserRole.OPERATOR,
                is_active=True,
                plan=PlanEnum("professional"),
                trial_expires_at=utc_now() + _td(days=30),
            )
            s.add(u)
            s.commit()
            s.refresh(u)
            uid = u.id
        finally:
            s.close()

        async def _override():
            session = S()
            try:
                return session.get(User, uid)
            finally:
                session.close()

        from src.auth.dependencies import get_optional_user

        app.dependency_overrides[get_optional_user] = _override

        try:
            yield uid
        finally:
            app.dependency_overrides.pop(get_optional_user, None)
            # Wipe analyses referencing this user before deleting
            # the user — same FK ordering issue as in the migrations
            # router tests.
            s2 = S()
            try:
                s2.query(TroubleshootAnalysis).filter(
                    TroubleshootAnalysis.user_id == uid
                ).delete(synchronize_session=False)
                s2.commit()
                row = s2.get(User, uid)
                if row is not None:
                    s2.delete(row)
                    s2.commit()
            finally:
                s2.close()
                engine.dispose()

    def test_authenticated_paste_stamps_user_id(self, signed_in_user):
        uid = signed_in_user
        resp = client.post(
            "/api/v1/troubleshoot/analyze",
            json={"logs": "ORA-00942: table or view does not exist"},
        )
        assert resp.status_code == 200, resp.text

        engine = create_engine(env_settings.database_url)
        S = sessionmaker(bind=engine)
        s = S()
        try:
            row = s.query(TroubleshootAnalysis).first()
            assert row.user_id == uid
        finally:
            s.close()
            engine.dispose()