"""Tests for Validation Layer 5 — temporal checks.

Uses an in-memory SQLite database.  SQLite doesn't have EXTRACT() or
information_schema so we patch the helpers that issue PG-specific SQL and
test the check functions directly with pre-baked counts.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from src.migrate.temporal import (
    _MIDNIGHT_THRESHOLD,
    _MIN_ROWS,
    _YEAR_MAX,
    _YEAR_MIN,
    _check_date_range,
    _check_far_future,
    _check_midnight_rate,
    check_temporal_table,
)


# ─── Fixtures ──────────────────────────────────────────────────────────────────


def _session(scalar_values: list):
    """Mock session whose execute().scalar() returns values in order."""
    session = MagicMock()
    scalars = iter(scalar_values)
    session.execute.return_value.scalar.side_effect = lambda: next(scalars)
    return session


# ─── _check_date_range ─────────────────────────────────────────────────────────


def test_date_range_no_violations():
    session = _session([0])  # COUNT(*) = 0
    findings = _check_date_range(session, '"s"."t"', "t", "col", '"col"', 1000)
    assert findings == []


def test_date_range_violation_produces_error():
    session = _session([50])  # 50 out-of-range rows
    findings = _check_date_range(session, '"s"."t"', "t", "col", '"col"', 1000)
    assert len(findings) == 1
    f = findings[0]
    assert f.severity == "error"
    assert f.anomaly_type == "date_range_violation"
    assert "col" in f.message
    assert str(_YEAR_MIN) in f.message
    assert str(_YEAR_MAX) in f.message


def test_date_range_pct_in_message():
    session = _session([100])
    findings = _check_date_range(session, '"s"."t"', "t", "col", '"col"', 200)
    assert "50.0%" in findings[0].message


# ─── _check_far_future ─────────────────────────────────────────────────────────


def test_far_future_no_violations():
    session = _session([0])  # COUNT(*) = 0
    findings = _check_far_future(session, '"s"."t"', "t", "col", '"col"', 1000, 2025)
    assert findings == []


def test_far_future_produces_warning():
    # COUNT = 10, max_year = 2029 (within _YEAR_MAX → not already flagged as error)
    session = _session([10, 2029])
    findings = _check_far_future(session, '"s"."t"', "t", "col", '"col"', 1000, 2025)
    assert len(findings) == 1
    assert findings[0].severity == "warning"
    assert findings[0].anomaly_type == "far_future_date"


def test_far_future_skipped_when_already_range_error():
    # max_year > _YEAR_MAX → date_range_violation already flagged; skip
    session = _session([5, _YEAR_MAX + 100])
    findings = _check_far_future(session, '"s"."t"', "t", "col", '"col"', 1000, 2025)
    assert findings == []


def test_far_future_year_in_message():
    session = _session([3, 2029])
    findings = _check_far_future(session, '"s"."t"', "t", "col", '"col"', 1000, 2025)
    assert "2030" in findings[0].message  # 2025 + _FAR_FUTURE_BUFFER(5) = 2030


# ─── _check_midnight_rate ──────────────────────────────────────────────────────


def test_midnight_rate_below_threshold():
    session = _session([500])  # 500 / 1000 = 50%
    findings = _check_midnight_rate(session, '"s"."t"', "t", "col", '"col"', 1000)
    assert findings == []


def test_midnight_rate_above_threshold_produces_info():
    session = _session([980])  # 980 / 1000 = 98%
    findings = _check_midnight_rate(session, '"s"."t"', "t", "col", '"col"', 1000)
    assert len(findings) == 1
    assert findings[0].severity == "info"
    assert findings[0].anomaly_type == "all_midnight_timestamps"
    assert "98.0%" in findings[0].message


def test_midnight_rate_exactly_at_threshold():
    midnight = int(_MIDNIGHT_THRESHOLD * 1000)
    session = _session([midnight])
    findings = _check_midnight_rate(session, '"s"."t"', "t", "col", '"col"', 1000)
    # rate == threshold → flagged (code: `if rate < threshold: return []`)
    assert len(findings) == 1


# ─── check_temporal_table ──────────────────────────────────────────────────────


def test_check_temporal_table_no_temporal_cols():
    session = MagicMock()
    with patch("src.migrate.temporal._temporal_columns", return_value=[]):
        findings = check_temporal_table(session, "public", "orders")
    assert findings == []


def test_check_temporal_table_skips_sparse_col():
    """Columns with fewer than _MIN_ROWS non-NULL values are skipped."""
    session = MagicMock()
    cols = [("created_at", "timestamp without time zone")]
    with (
        patch("src.migrate.temporal._temporal_columns", return_value=cols),
        patch("src.migrate.temporal._non_null_count", return_value=_MIN_ROWS - 1),
    ):
        findings = check_temporal_table(session, "public", "orders")
    assert findings == []


def test_check_temporal_table_midnight_only_for_plain_timestamp():
    """Midnight check skipped for date and timestamptz cols."""
    session = MagicMock()
    cols = [
        ("ts_tz", "timestamp with time zone"),
        ("dt", "date"),
    ]
    with (
        patch("src.migrate.temporal._temporal_columns", return_value=cols),
        patch("src.migrate.temporal._non_null_count", return_value=_MIN_ROWS + 10),
        patch("src.migrate.temporal._check_date_range", return_value=[]),
        patch("src.migrate.temporal._check_far_future", return_value=[]),
        patch("src.migrate.temporal._check_midnight_rate") as mock_midnight,
    ):
        check_temporal_table(session, "public", "orders")
    mock_midnight.assert_not_called()


def test_check_temporal_table_midnight_called_for_plain_timestamp():
    session = MagicMock()
    cols = [("created_at", "timestamp without time zone")]
    with (
        patch("src.migrate.temporal._temporal_columns", return_value=cols),
        patch("src.migrate.temporal._non_null_count", return_value=_MIN_ROWS + 10),
        patch("src.migrate.temporal._check_date_range", return_value=[]),
        patch("src.migrate.temporal._check_far_future", return_value=[]),
        patch("src.migrate.temporal._check_midnight_rate", return_value=[]) as mock_midnight,
    ):
        check_temporal_table(session, "public", "orders")
    mock_midnight.assert_called_once()


def test_check_temporal_table_aggregates_findings():
    from src.migrate.anomaly import AnomalyFinding

    fake_finding = AnomalyFinding("error", "orders", "created_at", "date_range_violation", "m", "a")
    session = MagicMock()
    cols = [("created_at", "timestamp without time zone")]
    with (
        patch("src.migrate.temporal._temporal_columns", return_value=cols),
        patch("src.migrate.temporal._non_null_count", return_value=100),
        patch("src.migrate.temporal._check_date_range", return_value=[fake_finding]),
        patch("src.migrate.temporal._check_far_future", return_value=[]),
        patch("src.migrate.temporal._check_midnight_rate", return_value=[]),
    ):
        findings = check_temporal_table(session, "public", "orders")
    assert len(findings) == 1
    assert findings[0].anomaly_type == "date_range_violation"


def test_check_temporal_table_exception_per_col_is_silent():
    """Exception in one column check should not propagate — just skip."""
    session = MagicMock()
    cols = [("bad_col", "timestamp without time zone")]
    with (
        patch("src.migrate.temporal._temporal_columns", return_value=cols),
        patch("src.migrate.temporal._non_null_count", side_effect=RuntimeError("boom")),
    ):
        findings = check_temporal_table(session, "public", "orders")
    assert findings == []
