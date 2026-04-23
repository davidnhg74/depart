"""Tests for the `/api/v1/convert/{tag}` canonical-example endpoint."""

from __future__ import annotations

from fastapi.testclient import TestClient

from src.main import app


client = TestClient(app)


def test_returns_merge_example():
    resp = client.get("/api/v1/convert/MERGE")
    assert resp.status_code == 200
    body = resp.json()
    assert body["tag"] == "MERGE"
    assert "MERGE INTO" in body["oracle"]
    assert "ON CONFLICT" in body["postgres"]
    assert body["confidence"] in ("high", "medium", "needs-review")


def test_returns_connect_by_example():
    resp = client.get("/api/v1/convert/CONNECT_BY")
    assert resp.status_code == 200
    body = resp.json()
    assert "START WITH" in body["oracle"]
    assert "WITH RECURSIVE" in body["postgres"]


def test_autonomous_txn_flagged_needs_review():
    """AUTONOMOUS_TRANSACTION has no clean PG equivalent — the canonical
    example must surface that architectural weight via confidence."""
    resp = client.get("/api/v1/convert/AUTONOMOUS_TXN")
    assert resp.status_code == 200
    body = resp.json()
    assert body["confidence"] == "needs-review"


def test_unknown_tag_returns_404():
    resp = client.get("/api/v1/convert/NOT_A_REAL_TAG")
    assert resp.status_code == 404


def test_known_but_unmapped_tag_returns_404():
    """Tags that are valid enum values but don't have a canonical
    example yet — like DBMS_CRYPTO — return 404 with a helpful message
    rather than a 500."""
    resp = client.get("/api/v1/convert/DBMS_CRYPTO")
    assert resp.status_code == 404
    assert "paid tier" in resp.json()["detail"].lower()


# ─── New canonical examples: NVL / DECODE / INTERVAL ─────────────────────────
#
# These were missing from the table at routers/convert.py:_EXAMPLES,
# so the most common Oracle idioms fell straight to the (unvalidated
# until today) Claude path. Locking them in.


def test_nvl_example_emits_coalesce_and_case():
    resp = client.get("/api/v1/convert/NVL")
    assert resp.status_code == 200
    body = resp.json()
    assert "NVL" in body["oracle"]
    # Both halves must show up: NVL → COALESCE, NVL2 → CASE.
    assert "COALESCE" in body["postgres"]
    assert "CASE WHEN" in body["postgres"]
    # Hand-curated examples are pre-validated; ship empty error list.
    assert body["validation_errors"] == []


def test_decode_example_uses_case_form():
    resp = client.get("/api/v1/convert/DECODE")
    assert resp.status_code == 200
    body = resp.json()
    assert "DECODE" in body["oracle"]
    assert "CASE department_id" in body["postgres"]
    # The reasoning must surface the NULL=NULL gotcha — that's the
    # subtle bug DECODE-to-CASE introduces if not flagged.
    assert "NULL" in body["reasoning"]


def test_interval_example_drops_subtype_qualifier():
    resp = client.get("/api/v1/convert/INTERVAL")
    assert resp.status_code == 200
    body = resp.json()
    # Oracle side has the subtype qualifiers; PG side strips them.
    assert "YEAR TO MONTH" in body["oracle"]
    assert "DAY TO SECOND" in body["oracle"]
    # Confirm the column declarations on the PG side use bare INTERVAL.
    assert "duration    INTERVAL" in body["postgres"]
    assert "notice      INTERVAL" in body["postgres"]
    # No leftover Oracle subtype keywords in the converted column DDL.
    assert "YEAR TO MONTH" not in body["postgres"]
    assert "DAY TO SECOND" not in body["postgres"]


# ─── Live convert (POST) — Claude output validator gating ────────────────────


import pytest

from src.ai.client import AIClient
from src.license import dependencies as license_deps
from src.license.verifier import LicenseStatus, Tier
from src.routers import convert as convert_router
from src.services import settings_service


@pytest.fixture
def stub_live_dependencies(monkeypatch):
    """Patch out the network-touching parts of POST /convert/{tag}:
      * Anthropic key resolution returns a placeholder (so the 412
        early-exit doesn't fire)
      * AIClient.complete_json is overridden per-test via the helper
        below; if no test patches it, the call would hit Anthropic
      * License lookup returns a synthetic Pro status with the
        ai_conversion feature so the require_feature gate passes
        without needing a signed JWT in the test DB
    """
    monkeypatch.setattr(
        settings_service,
        "get_effective_anthropic_key",
        lambda *_args, **_kw: "sk-test-placeholder",
    )
    monkeypatch.setattr(
        convert_router,
        "get_effective_anthropic_key",
        lambda *_args, **_kw: "sk-test-placeholder",
    )
    fake_status = LicenseStatus(
        valid=True,
        tier=Tier.PRO,
        features=["ai_conversion"],
    )
    # Patch the underlying `get_license_status` that every per-call
    # `require_feature(...)` closure walks. Patching the closure itself
    # would require knowing the exact callable identity registered by
    # the route, which `require_feature("ai_conversion")` rebuilds
    # afresh on each invocation.
    monkeypatch.setattr(
        license_deps, "get_license_status", lambda *_a, **_kw: fake_status
    )
    yield monkeypatch


def _patch_claude(monkeypatch, response: dict):
    """Replace AIClient.complete_json so the test never hits Anthropic."""

    def _fake_complete_json(self, *, system, user, **_kw):
        return response

    monkeypatch.setattr(AIClient, "complete_json", _fake_complete_json)
    # AIClient.__init__ calls Anthropic() which validates the API key
    # by trying a session — patch __init__ to a no-op so we don't need
    # a live key.
    monkeypatch.setattr(AIClient, "__init__", lambda self, **kw: None)


def test_live_convert_clean_output_passes_validator(stub_live_dependencies):
    """A well-formed PL/pgSQL response carries empty validation lists
    and keeps Claude's reported confidence intact."""
    _patch_claude(
        stub_live_dependencies,
        {
            "oracle": "BEGIN NULL; END;",
            "postgres": (
                "CREATE OR REPLACE FUNCTION noop() RETURNS void AS $$\n"
                "BEGIN\n"
                "  NULL;\n"
                "END;\n"
                "$$ LANGUAGE plpgsql;"
            ),
            "reasoning": "Wraps the trivial Oracle block as a void PG function.",
            "confidence": "high",
        },
    )
    resp = client.post(
        "/api/v1/convert/MERGE",
        json={"snippet": "BEGIN NULL; END;"},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["validation_errors"] == []
    assert body["confidence"] == "high"


def test_live_convert_invalid_output_forces_needs_review(stub_live_dependencies):
    """If Claude returns syntactically broken PL/pgSQL (here:
    unbalanced parens AND missing LANGUAGE), the validator flags it
    and the endpoint downgrades confidence so the UI bannerizes it."""
    _patch_claude(
        stub_live_dependencies,
        {
            "oracle": "MERGE INTO t USING s ON (t.id = s.id) WHEN MATCHED THEN UPDATE SET x = 1;",
            "postgres": (
                "CREATE OR REPLACE FUNCTION broken( RETURNS void AS $$\n"
                "BEGIN\n"
                "  INSERT INTO t (id) VALUES (1);\n"
                "END;\n"
                "$$"  # missing LANGUAGE clause + unbalanced paren in signature
            ),
            "reasoning": "...",
            "confidence": "high",  # Claude was confident — validator overrides
        },
    )
    resp = client.post(
        "/api/v1/convert/MERGE",
        json={"snippet": "MERGE INTO t USING s ON (t.id = s.id) ..."},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["validation_errors"], "expected validator to flag broken output"
    assert body["confidence"] == "needs-review", (
        "validator must downgrade Claude's claimed confidence on errors"
    )


def test_live_convert_oracle_remnants_become_warnings(stub_live_dependencies):
    """Output that's syntactically valid but still contains DBMS_*
    calls or PRAGMA hints surfaces as a warning. Confidence is
    preserved (warnings are advisory, not blocking)."""
    _patch_claude(
        stub_live_dependencies,
        {
            "oracle": "BEGIN DBMS_OUTPUT.PUT_LINE('x'); END;",
            "postgres": (
                "CREATE OR REPLACE FUNCTION partial() RETURNS void AS $$\n"
                "BEGIN\n"
                "  DBMS_OUTPUT.PUT_LINE('x');\n"  # left in place — Claude punted
                "END;\n"
                "$$ LANGUAGE plpgsql;"
            ),
            "reasoning": "...",
            "confidence": "medium",
        },
    )
    resp = client.post(
        "/api/v1/convert/MERGE",
        json={"snippet": "BEGIN DBMS_OUTPUT.PUT_LINE('x'); END;"},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert any(
        "DBMS_OUTPUT" in w for w in body["validation_warnings"]
    ), f"expected DBMS_OUTPUT warning, got {body['validation_warnings']}"
    assert body["confidence"] == "medium"  # warnings don't downgrade
