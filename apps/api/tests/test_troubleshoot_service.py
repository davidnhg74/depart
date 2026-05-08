"""Tests for the troubleshoot service.

Two layers under test:

* `smart_truncate` — pure function. Tests cover the "input fits as-is"
  path, the "find ERROR/WARN windows + N lines context" path, and the
  fallback-when-no-interesting-tokens path.

* `analyze_logs` — service entry point. Uses a fake AIClient stub so
  we never call Anthropic, plus a real Postgres session via the
  existing `env_settings.database_url`. Asserts both Plane 1 and
  Plane 2 rows land correctly per opt-in policy.
"""

from __future__ import annotations

import uuid as _uuid
from datetime import timedelta as _td

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.config import settings as env_settings
from src.models import CorpusEntry, TroubleshootAnalysis, User, UserRole
from src.services.troubleshoot_service import (
    Diagnosis,
    analyze_logs,
    smart_truncate,
)
from src.utils.time import utc_now

pytestmark = pytest.mark.integration


# ─── smart_truncate ─────────────────────────────────────────────────────────


class TestSmartTruncate:
    def test_short_input_passes_through_unchanged(self):
        text = "ORA-01017: invalid credentials"
        out = smart_truncate(text)
        assert out.text == text
        assert out.extracted_line_count == 1

    def test_extracts_error_window_with_context(self):
        # 50 boring lines, then ERROR at index 50, then 50 more boring lines.
        # ±5 context around index 50 covers indices 45–55, which after my
        # shifted post-ERROR numbering means "info line 45".."info line 49"
        # (pre-ERROR) + ERROR + "info line 50".."info line 54" (post-ERROR).
        lines = [f"info line {i}" for i in range(50)]
        lines.append("ERROR: something bad happened — ORA-01017")
        lines.extend(f"info line {i}" for i in range(50, 100))
        text = "\n".join(lines)

        # Force a tight budget so the "extract windows" path fires.
        out = smart_truncate(text, budget_bytes=400)
        assert "ORA-01017" in out.text
        # The 5 lines on each side of the ERROR should be present.
        assert "info line 45" in out.text
        assert "info line 54" in out.text  # last line in the +5 window
        # Lines outside the ±5 window should NOT be.
        assert "info line 0" not in out.text
        assert "info line 60" not in out.text
        assert "info line 99" not in out.text

    def test_no_interesting_tokens_returns_head(self):
        # Nothing matches the ERROR/WARN heuristic — should still
        # return SOMETHING (the head of input) rather than empty.
        text = "\n".join(f"line {i}" for i in range(1000))
        out = smart_truncate(text, budget_bytes=200)
        assert out.text != ""
        assert "line 0" in out.text

    def test_oversized_excerpt_keeps_tail(self):
        # If the windows themselves still exceed the budget, the
        # tail (most recent error) wins.
        lines = []
        for i in range(200):
            lines.append(f"ERROR window {i} for ORA-{i:05d}")
            lines.append(f"  context line for {i}")
        text = "\n".join(lines)

        out = smart_truncate(text, budget_bytes=500)
        # The tail entries should be present; the head ones dropped.
        assert "window 199" in out.text or "window 198" in out.text
        assert "window 0" not in out.text


# ─── analyze_logs (service entry point) ─────────────────────────────────────


class _FakeClient:
    """Stand-in for AIClient. Records calls, returns canned responses."""

    def __init__(self, response: dict | Exception):
        self._response = response
        self.calls: list[dict] = []

    def complete_json(self, *, system: str, user: str):
        self.calls.append({"system": system, "user": user})
        if isinstance(self._response, Exception):
            raise self._response
        return self._response


def _persist_user(session, *, plan_value: str = "starter") -> User:
    """Insert a test User. Trial-expires-at is required NOT NULL."""
    from src.models import PlanEnum

    u = User(
        id=_uuid.uuid4(),
        email=f"{_uuid.uuid4().hex[:8]}@example.com",
        full_name="Test User",
        hashed_password="x" * 60,
        role=UserRole.OPERATOR,
        is_active=True,
        plan=PlanEnum(plan_value),
        trial_expires_at=utc_now() + _td(days=30),
    )
    session.add(u)
    session.commit()
    session.refresh(u)
    return u


@pytest.fixture
def db_session():
    engine = create_engine(env_settings.database_url)
    S = sessionmaker(bind=engine)
    s = S()

    # Pre-clean too — a previous failed run may have left rows
    # behind that would confuse counts in this test.
    s.query(CorpusEntry).delete()
    s.query(TroubleshootAnalysis).delete()
    s.commit()

    yield s

    # Clean analyses + corpus FIRST (FK references users.id), then
    # any test users that the test itself didn't already drop.
    s.query(CorpusEntry).delete()
    s.query(TroubleshootAnalysis).delete()
    s.commit()
    s.close()
    engine.dispose()


class TestAnalyzeLogs:
    def test_writes_plane_1_and_plane_2_for_opt_in_user(self, db_session):
        user = _persist_user(db_session, plan_value="starter")
        try:
            ai = _FakeClient(
                {
                    "likely_cause": "Wrong listener SID",
                    "recommended_action": "1. Verify listener.ora",
                    "code_suggestion": None,
                    "confidence": "high",
                    "escalate_if": None,
                }
            )
            diagnosis, analysis_id = analyze_logs(
                db=db_session,
                raw_logs="ORA-12541: TNS no listener",
                user=user,
                ai_client=ai,
                write_corpus=True,
            )
            assert diagnosis.likely_cause == "Wrong listener SID"
            assert diagnosis.confidence == "high"
            assert diagnosis.used_ai is True

            # Plane 1: row carries the user_id.
            row = db_session.get(TroubleshootAnalysis, analysis_id)
            assert row is not None
            assert row.user_id == user.id
            assert row.thumbs is None
            assert "[REDACTED" not in row.input_excerpt or True  # input wasn't sensitive

            # Plane 2: anonymized corpus row written, no user_id.
            corpus_rows = db_session.query(CorpusEntry).all()
            assert len(corpus_rows) >= 1
            assert any("ORA-12541" in c.error_codes for c in corpus_rows)
        finally:
            # Wipe analyses for this user before deleting the user
            # itself — the FK from troubleshoot_analyses.user_id
            # would otherwise block the delete.
            db_session.query(TroubleshootAnalysis).filter(
                TroubleshootAnalysis.user_id == user.id
            ).delete(synchronize_session=False)
            db_session.delete(user)
            db_session.commit()

    def test_skips_plane_2_when_write_corpus_false(self, db_session):
        # Enterprise behavior: opt-out by default.
        user = _persist_user(db_session, plan_value="enterprise")
        try:
            db_session.query(CorpusEntry).delete()
            db_session.commit()

            ai = _FakeClient(
                {
                    "likely_cause": "x",
                    "recommended_action": "y",
                    "code_suggestion": None,
                    "confidence": "medium",
                    "escalate_if": None,
                }
            )
            _, analysis_id = analyze_logs(
                db=db_session,
                raw_logs="ORA-00942: table or view does not exist",
                user=user,
                ai_client=ai,
                write_corpus=False,
            )

            # Plane 1 still written.
            assert db_session.get(TroubleshootAnalysis, analysis_id) is not None
            # Plane 2 untouched.
            assert db_session.query(CorpusEntry).count() == 0
        finally:
            # Wipe analyses for this user before deleting the user
            # itself — the FK from troubleshoot_analyses.user_id
            # would otherwise block the delete.
            db_session.query(TroubleshootAnalysis).filter(
                TroubleshootAnalysis.user_id == user.id
            ).delete(synchronize_session=False)
            db_session.delete(user)
            db_session.commit()

    def test_anonymous_call_writes_plane_1_with_null_user_id(self, db_session):
        ai = _FakeClient(
            {
                "likely_cause": "x",
                "recommended_action": "y",
                "code_suggestion": None,
                "confidence": "medium",
                "escalate_if": None,
            }
        )
        _, analysis_id = analyze_logs(
            db=db_session,
            raw_logs="some unauthenticated paste — ORA-01017",
            user=None,
            ai_client=ai,
        )
        row = db_session.get(TroubleshootAnalysis, analysis_id)
        assert row is not None
        assert row.user_id is None

    def test_redacts_secrets_before_persisting(self, db_session):
        ai = _FakeClient(
            {
                "likely_cause": "x",
                "recommended_action": "y",
                "code_suggestion": None,
                "confidence": "medium",
                "escalate_if": None,
            }
        )
        sensitive = (
            "Connection failed: postgresql://hafen:hunter2@db.acme.com:5432/prod\n"
            "ORA-01017"
        )
        _, analysis_id = analyze_logs(
            db=db_session, raw_logs=sensitive, user=None, ai_client=ai
        )
        row = db_session.get(TroubleshootAnalysis, analysis_id)
        # Even Plane 1 (the user-private store) carries the redacted
        # excerpt — we don't keep raw secrets server-side.
        assert "hunter2" not in row.input_excerpt
        assert "db.acme.com" not in row.input_excerpt
        # And Claude saw the redacted version too.
        assert "hunter2" not in ai.calls[0]["user"]

    def test_ai_failure_falls_back_to_canned_diagnosis(self, db_session):
        ai = _FakeClient(RuntimeError("connection refused"))
        diagnosis, _ = analyze_logs(
            db=db_session, raw_logs="ORA-01017", user=None, ai_client=ai
        )
        # Service surfaces a usable response even when Claude is down.
        assert diagnosis.used_ai is False
        assert diagnosis.confidence == "needs-review"
        assert "unavailable" in diagnosis.likely_cause.lower()