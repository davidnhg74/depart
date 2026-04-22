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
