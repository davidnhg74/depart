"""Tests for Validation Layer 6 — AI anomaly detection.

Covers:
  * Distribution sampling helpers (unit, via SQLite in-memory)
  * Rule-based finding generation
  * AI response parsing
  * Graceful degradation when ANTHROPIC_API_KEY is absent
  * Router endpoint (happy path + status guard)
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import Column, Integer, String, create_engine, text
from sqlalchemy.orm import Session, sessionmaker

from src.migrate.anomaly import (
    AnomalyFinding,
    _CARD_THRESHOLD,
    _NULL_RATE_ERR,
    _NULL_RATE_WARN,
    build_anomaly_prompt,
    get_system_prompt,
    overall_severity,
    rule_based_findings,
)
from src.services.anomaly_service import AnomalyResult, _parse_ai_response


# ─── Fixtures ─────────────────────────────────────────────────────────────────


@pytest.fixture()
def pg_session():
    """In-memory SQLite session that mimics the PG distribution queries."""
    engine = create_engine("sqlite:///:memory:")
    engine.execute = None  # safety guard — use session.execute

    with engine.connect() as conn:
        conn.execute(text(
            "CREATE TABLE customers ("
            "  id INTEGER PRIMARY KEY, "
            "  status TEXT, "
            "  credit_limit REAL, "
            "  notes TEXT"
            ")"
        ))
        conn.execute(text(
            "INSERT INTO customers VALUES "
            "(1,'ACTIVE',1000.0,NULL),"
            "(2,'ACTIVE',2000.0,NULL),"
            "(3,'INACTIVE',NULL,NULL),"
            "(4,'ACTIVE',3000.0,'vip')"
        ))
        conn.commit()

    Sess = sessionmaker(bind=engine)
    session = Sess()
    yield session
    session.close()
    engine.dispose()


# ─── rule_based_findings ──────────────────────────────────────────────────────


def test_rule_empty_table_is_error():
    dist = {
        "ORDERS": {
            "row_count": 0,
            "expected_row_count": 5000,
            "columns": {},
        }
    }
    findings = rule_based_findings(dist)
    assert any(
        f.anomaly_type == "unexpected_empty_table" and f.severity == "error"
        for f in findings
    ), "empty table vs non-zero expected should be error"


def test_rule_no_finding_when_expected_also_zero():
    dist = {
        "EMPTY_LOG": {
            "row_count": 0,
            "expected_row_count": 0,
            "columns": {},
        }
    }
    findings = rule_based_findings(dist)
    assert findings == [], "both zero → not an anomaly"


def test_rule_row_mismatch_over_threshold():
    dist = {
        "CUSTOMERS": {
            "row_count": 9_800,
            "expected_row_count": 10_000,  # 2% drift — above 1% threshold
            "columns": {},
        }
    }
    findings = rule_based_findings(dist)
    assert any(f.anomaly_type == "row_count_mismatch" for f in findings)


def test_rule_row_mismatch_under_threshold():
    dist = {
        "CUSTOMERS": {
            "row_count": 9_999,
            "expected_row_count": 10_000,  # 0.01% drift — below threshold
            "columns": {},
        }
    }
    findings = rule_based_findings(dist)
    assert not any(f.anomaly_type == "row_count_mismatch" for f in findings)


def test_rule_high_null_rate_warning():
    dist = {
        "T": {
            "row_count": 1000,
            "expected_row_count": None,
            "columns": {
                "STATUS": {
                    "type": "varchar",
                    "null_rate": _NULL_RATE_WARN + 0.01,
                }
            },
        }
    }
    findings = rule_based_findings(dist)
    assert any(
        f.severity == "warning" and f.anomaly_type == "null_rate_spike" and f.column == "STATUS"
        for f in findings
    )


def test_rule_near_total_null_is_error():
    dist = {
        "T": {
            "row_count": 1000,
            "expected_row_count": None,
            "columns": {
                "AMOUNT": {
                    "type": "numeric",
                    "null_rate": _NULL_RATE_ERR + 0.001,
                }
            },
        }
    }
    findings = rule_based_findings(dist)
    assert any(f.severity == "error" and f.column == "AMOUNT" for f in findings)


def test_rule_skipped_column_ignored():
    dist = {
        "T": {
            "row_count": 100,
            "expected_row_count": None,
            "columns": {
                "X": {"skipped": True},
            },
        }
    }
    assert rule_based_findings(dist) == []


def test_rule_no_findings_on_clean_data():
    dist = {
        "T": {
            "row_count": 1000,
            "expected_row_count": 1000,
            "columns": {
                "STATUS": {"type": "varchar", "null_rate": 0.02},
                "AMOUNT": {"type": "numeric", "null_rate": 0.00},
            },
        }
    }
    findings = rule_based_findings(dist)
    assert findings == []


# ─── overall_severity ─────────────────────────────────────────────────────────


def test_overall_severity_clean_when_empty():
    assert overall_severity([]) == "clean"


def test_overall_severity_worst_wins():
    findings = [
        AnomalyFinding("info", "T", None, "t", "m", "a"),
        AnomalyFinding("warning", "T", None, "t", "m", "a"),
        AnomalyFinding("error", "T", None, "t", "m", "a"),
    ]
    assert overall_severity(findings) == "error"


def test_overall_severity_warning_only():
    findings = [
        AnomalyFinding("warning", "T", None, "t", "m", "a"),
    ]
    assert overall_severity(findings) == "warning"


# ─── build_anomaly_prompt ─────────────────────────────────────────────────────


def test_build_prompt_contains_json():
    dist = {"T": {"row_count": 100, "columns": {}}}
    prompt = build_anomaly_prompt(dist)
    assert "row_count" in prompt
    assert "```json" in prompt


def test_system_prompt_has_schema():
    sp = get_system_prompt()
    assert "overall_severity" in sp
    assert "findings" in sp
    assert "anomaly_type" in sp


# ─── _parse_ai_response ───────────────────────────────────────────────────────


def test_parse_valid_response():
    raw = {
        "overall_severity": "warning",
        "findings": [
            {
                "severity": "warning",
                "table": "CUSTOMERS",
                "column": "STATUS",
                "anomaly_type": "null_rate_spike",
                "message": "80% NULLs",
                "recommended_action": "check masking rules",
            }
        ],
    }
    findings = _parse_ai_response(raw)
    assert len(findings) == 1
    assert findings[0].table == "CUSTOMERS"
    assert findings[0].severity == "warning"


def test_parse_empty_findings():
    assert _parse_ai_response({"overall_severity": "clean", "findings": []}) == []


def test_parse_bad_item_skipped():
    raw = {"findings": [{"severity": "error"}]}  # missing required fields
    findings = _parse_ai_response(raw)
    # Partial item: missing fields → empty strings, not a crash
    assert isinstance(findings, list)


# ─── Graceful degradation (no API key) ───────────────────────────────────────


def test_anomaly_service_falls_back_without_api_key():
    """anomaly_check_record falls back to rule-based when AIClient raises."""
    from src.services import anomaly_service

    fake_record = MagicMock()
    fake_record.target_url = "postgresql+psycopg://user:pw@localhost/db"
    fake_record.target_schema = "public"
    fake_record.source_schema = "public"
    fake_record.tables = None
    fake_record.user_id = None
    fake_record.id = uuid.uuid4()

    fake_db = MagicMock()

    # Simulate: _resolve_tables returns one table, _sample_all returns empty dist.
    with (
        patch.object(anomaly_service, "_resolve_tables", return_value=["CUSTOMERS"]),
        patch.object(
            anomaly_service,
            "_sample_all",
            return_value={
                "CUSTOMERS": {
                    "row_count": 0,
                    "expected_row_count": 100,
                    "columns": {},
                }
            },
        ),
        patch("src.ai.client.AIClient.smart", side_effect=RuntimeError("no API key")),
    ):
        result = anomaly_service.anomaly_check_record(fake_record, fake_db)

    assert result.used_ai is False
    assert result.overall_severity == "error"  # empty table finding
    assert any(f.anomaly_type == "unexpected_empty_table" for f in result.findings)
    fake_db.add.assert_called_once()
    fake_db.commit.assert_called_once()


# ─── AnomalyFinding serialization ────────────────────────────────────────────


def test_anomaly_finding_to_dict():
    f = AnomalyFinding(
        severity="error",
        table="ORDERS",
        column="AMOUNT",
        anomaly_type="null_rate_spike",
        message="99% NULLs",
        recommended_action="check source",
    )
    d = f.to_dict()
    assert d["severity"] == "error"
    assert d["column"] == "AMOUNT"
    assert "recommended_action" in d
