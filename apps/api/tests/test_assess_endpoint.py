"""Tests for the public `/api/v1/assess` endpoint.

Lightweight — uses TestClient so no Postgres dependency for this file.
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from src.main import app


client = TestClient(app)


# ─── Happy paths ─────────────────────────────────────────────────────────────


def test_assess_trivial_ddl_is_low_complexity():
    """A single CREATE TABLE with plain types has no tagged constructs
    at all — the score should sit at the bottom of the range."""
    resp = client.post(
        "/api/v1/assess",
        json={
            "ddl": """
                CREATE TABLE hr.employees (
                    employee_id NUMBER(6) PRIMARY KEY,
                    first_name  VARCHAR2(20),
                    last_name   VARCHAR2(25) NOT NULL
                );
            """
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["score"] < 40  # no risky constructs → low
    assert body["risks"] == []  # no Tier B/C items
    assert body["must_rewrite_lines"] == 0


def test_assess_surfaces_tier_c_as_risk():
    """MERGE is Tier B and AUTONOMOUS_TRANSACTION is Tier C — both
    should show up in the risks list, C first."""
    ddl = """
        CREATE OR REPLACE PROCEDURE audit_trail IS
            PRAGMA AUTONOMOUS_TRANSACTION;
        BEGIN
            MERGE INTO audit_log a
            USING source_rows s ON (a.id = s.id)
            WHEN MATCHED THEN UPDATE SET a.val = s.val
            WHEN NOT MATCHED THEN INSERT (id, val) VALUES (s.id, s.val);
        END;
    """
    resp = client.post("/api/v1/assess", json={"ddl": ddl})
    assert resp.status_code == 200
    body = resp.json()
    tags = [r["tag"] for r in body["risks"]]
    # Tier C (AUTONOMOUS_TXN) must come before Tier B (MERGE).
    assert tags[0] == "AUTONOMOUS_TXN"
    assert "MERGE" in tags


def test_assess_returns_effort_and_cost():
    resp = client.post(
        "/api/v1/assess",
        json={"ddl": "CREATE TABLE t (id NUMBER PRIMARY KEY);"},
    )
    body = resp.json()
    assert body["effort_estimate_days"] >= 0.5  # MIN_EFFORT floor
    # estimated_cost = effort_days * 1500 (the rate baked into the endpoint)
    assert body["estimated_cost"] == pytest.approx(body["effort_estimate_days"] * 1500)


# ─── Validation ──────────────────────────────────────────────────────────────


def test_assess_rejects_empty_ddl():
    resp = client.post("/api/v1/assess", json={"ddl": ""})
    assert resp.status_code == 422  # pydantic min_length


def test_assess_rejects_oversize_ddl():
    # 1 MB + 1 byte — just over the max_length cap
    resp = client.post("/api/v1/assess", json={"ddl": "x" * 1_000_001})
    assert resp.status_code == 422
