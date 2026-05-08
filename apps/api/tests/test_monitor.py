"""Tests for Layer 7 — production monitor.

Covers:
  - monitor.py pure check functions (row drift, bloat, cdc lag)
  - monitor_service.monitor_migration() with mock DB
  - POST /api/v1/migrations/{id}/monitor endpoint
  - GET  /api/v1/migrations/{id}/monitor endpoint
"""

from __future__ import annotations

import uuid
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from src.main import app
from src.migrate.monitor import (
    MonitorFinding,
    check_cdc_lag,
    check_dead_tuple_bloat,
    check_row_drift,
    collect_row_counts,
    overall_severity,
    run_monitor,
)


@pytest.fixture
def client():
    return TestClient(app)


# ─── collect_row_counts ───────────────────────────────────────────────────────


class TestCollectRowCounts:
    def test_returns_dict(self):
        mock_session = MagicMock()
        row = MagicMock()
        row.__getitem__ = lambda self, k: {"table_name": "orders", "row_count": 1000}[k]
        mock_session.execute.return_value.mappings.return_value.all.return_value = [row]
        result = collect_row_counts(mock_session, "myschema")
        assert isinstance(result, dict)

    def test_returns_empty_on_error(self):
        mock_session = MagicMock()
        mock_session.execute.side_effect = Exception("pg down")
        result = collect_row_counts(mock_session, "myschema")
        assert result == {}


# ─── check_row_drift ─────────────────────────────────────────────────────────


class TestCheckRowDrift:
    def test_no_baseline_returns_empty(self):
        findings = check_row_drift({"orders": 1000}, {})
        assert findings == []

    def test_no_drift_returns_empty(self):
        baseline = {"orders": 1000}
        current = {"orders": 1020}  # 2% — below warn threshold
        findings = check_row_drift(current, baseline)
        assert findings == []

    def test_warn_at_five_percent(self):
        baseline = {"orders": 1000}
        current = {"orders": 950}  # 5% shrink
        findings = check_row_drift(current, baseline)
        assert len(findings) == 1
        assert findings[0].severity == "warning"
        assert findings[0].check_name == "row_drift"
        assert findings[0].table == "orders"

    def test_error_at_twenty_percent(self):
        baseline = {"orders": 1000}
        current = {"orders": 790}  # 21% shrink
        findings = check_row_drift(current, baseline)
        assert len(findings) == 1
        assert findings[0].severity == "error"

    def test_growth_also_flagged(self):
        baseline = {"orders": 1000}
        current = {"orders": 1060}  # 6% growth
        findings = check_row_drift(current, baseline)
        assert len(findings) == 1
        assert findings[0].severity == "warning"
        assert "grown" in findings[0].message

    def test_missing_table_flagged_as_error(self):
        baseline = {"orders": 500}
        current = {}  # table gone
        findings = check_row_drift(current, baseline)
        assert len(findings) == 1
        assert findings[0].severity == "error"

    def test_zero_baseline_skipped(self):
        # Can't compute meaningful drift from zero baseline.
        baseline = {"orders": 0}
        current = {"orders": 100}
        findings = check_row_drift(current, baseline)
        assert findings == []

    def test_multiple_tables(self):
        baseline = {"orders": 1000, "products": 200, "users": 5000}
        current = {"orders": 940, "products": 205, "users": 3500}
        # orders: 6% drift → warning
        # products: 2.5% drift → clean
        # users: 30% drift → error
        findings = check_row_drift(current, baseline)
        assert len(findings) == 2
        severities = {f.table: f.severity for f in findings}
        assert severities["orders"] == "warning"
        assert severities["users"] == "error"


# ─── check_dead_tuple_bloat ───────────────────────────────────────────────────


class TestCheckDeadTupleBloat:
    def _make_session(self, rows):
        mock_session = MagicMock()
        mock_rows = []
        for row_data in rows:
            r = MagicMock()
            r.__getitem__ = lambda self, k, rd=row_data: rd[k]
            mock_rows.append(r)
        mock_session.execute.return_value.mappings.return_value.all.return_value = mock_rows
        return mock_session

    def test_no_bloat_returns_empty(self):
        session = self._make_session([])
        findings = check_dead_tuple_bloat(session, "myschema")
        assert findings == []

    def test_warn_at_twenty_percent(self):
        session = self._make_session([
            {"table_name": "orders", "live_rows": 800, "dead_rows": 200},  # 20% dead
        ])
        findings = check_dead_tuple_bloat(session, "myschema")
        assert len(findings) == 1
        assert findings[0].severity == "warning"
        assert findings[0].check_name == "dead_tuple_bloat"

    def test_error_at_fifty_percent(self):
        session = self._make_session([
            {"table_name": "orders", "live_rows": 500, "dead_rows": 500},  # 50% dead
        ])
        findings = check_dead_tuple_bloat(session, "myschema")
        assert len(findings) == 1
        assert findings[0].severity == "error"

    def test_below_threshold_clean(self):
        session = self._make_session([
            {"table_name": "orders", "live_rows": 900, "dead_rows": 100},  # 10% dead
        ])
        findings = check_dead_tuple_bloat(session, "myschema")
        assert findings == []

    def test_error_on_exception(self):
        mock_session = MagicMock()
        mock_session.execute.side_effect = Exception("pg down")
        findings = check_dead_tuple_bloat(mock_session, "myschema")
        assert findings == []


# ─── check_cdc_lag ───────────────────────────────────────────────────────────


class TestCheckCdcLag:
    def test_none_scns_returns_empty(self):
        assert check_cdc_lag(None, None) == []
        assert check_cdc_lag(100, None) == []
        assert check_cdc_lag(None, 100) == []

    def test_small_lag_clean(self):
        findings = check_cdc_lag(1000, 999)  # lag=1
        assert findings == []

    def test_warn_at_ten_thousand(self):
        findings = check_cdc_lag(20000, 10000)  # lag=10000
        assert len(findings) == 1
        assert findings[0].severity == "warning"
        assert findings[0].check_name == "cdc_lag"

    def test_error_at_hundred_thousand(self):
        findings = check_cdc_lag(200000, 100000)  # lag=100000
        assert len(findings) == 1
        assert findings[0].severity == "error"

    def test_negative_lag_ignored(self):
        # applied > captured (impossible in healthy state, logged only)
        findings = check_cdc_lag(100, 200)
        assert findings == []


# ─── overall_severity ────────────────────────────────────────────────────────


class TestOverallSeverity:
    def _f(self, sev: str) -> MonitorFinding:
        return MonitorFinding(
            severity=sev, check_name="test", table=None,
            message="m", recommended_action="r"
        )

    def test_empty_is_clean(self):
        assert overall_severity([]) == "clean"

    def test_info_only(self):
        assert overall_severity([self._f("info")]) == "info"

    def test_warning_beats_info(self):
        assert overall_severity([self._f("info"), self._f("warning")]) == "warning"

    def test_error_beats_warning(self):
        assert overall_severity([self._f("warning"), self._f("error")]) == "error"


# ─── run_monitor ─────────────────────────────────────────────────────────────


class TestRunMonitor:
    def test_runs_all_checks(self):
        mock_session = MagicMock()
        # Both SQL calls succeed but return empty sets.
        mock_session.execute.return_value.mappings.return_value.all.return_value = []

        findings, counts = run_monitor(
            mock_session, "myschema",
            baseline_counts={"orders": 1000},
            captured_scn=50000,
            applied_scn=45000,  # 5000 lag — below warn threshold
        )
        assert isinstance(findings, list)
        assert isinstance(counts, dict)

    def test_returns_cdc_findings_when_lagged(self):
        mock_session = MagicMock()
        mock_session.execute.return_value.mappings.return_value.all.return_value = []

        findings, _ = run_monitor(
            mock_session, "myschema",
            baseline_counts={},
            captured_scn=200000,
            applied_scn=50000,   # 150000 lag → error
        )
        cdc_findings = [f for f in findings if f.check_name == "cdc_lag"]
        assert len(cdc_findings) == 1
        assert cdc_findings[0].severity == "error"


# ─── Endpoint tests ───────────────────────────────────────────────────────────


class TestMonitorEndpoints:
    def _make_migration(self, target_url="postgresql://localhost/test"):
        from src.models import MigrationRecord
        m = MagicMock(spec=MigrationRecord)
        m.id = uuid.uuid4()
        m.user_id = None
        m.target_url = target_url
        m.target_schema = "public"
        m.source_schema = "public"
        m.tables = None
        m.last_captured_scn = None
        m.last_applied_scn = None
        m.status = "completed"
        return m

    def test_run_monitor_no_target_url(self, client):
        """Returns 400 when migration has no target_url."""
        from src.db import get_db_context
        from src.models import MigrationRecord

        with get_db_context() as session:
            record = MigrationRecord(schema_name="mon-test", status="completed")
            session.add(record)
            session.commit()
            mid = str(record.id)

        response = client.post(f"/api/v1/migrations/{mid}/monitor")
        assert response.status_code == 400
        assert "target_url" in response.json()["detail"]

    def test_run_monitor_not_found(self, client):
        fake_id = str(uuid.uuid4())
        response = client.post(f"/api/v1/migrations/{fake_id}/monitor")
        assert response.status_code == 404

    def test_run_monitor_success(self, client):
        """Happy path: mock monitor_migration returns clean result."""
        from src.db import get_db_context
        from src.models import MigrationRecord
        from src.services.monitor_service import MonitorResult

        with get_db_context() as session:
            record = MigrationRecord(
                schema_name="mon-test-2",
                status="completed",
                target_url="postgresql://localhost/fake",
            )
            session.add(record)
            session.commit()
            mid = str(record.id)

        fake_result = MonitorResult(
            findings=[],
            overall_severity="clean",
            snapshot_id=str(uuid.uuid4()),
            tables_checked=3,
            table_row_counts={"orders": 1000},
        )

        with patch("src.routers.migrations.monitor_migration", return_value=fake_result):
            response = client.post(f"/api/v1/migrations/{mid}/monitor")

        assert response.status_code == 200
        data = response.json()
        assert data["overall_severity"] == "clean"
        assert data["tables_checked"] == 3
        assert "snapshot_id" in data
        assert data["findings"] == []

    def test_run_monitor_with_findings(self, client):
        """Monitor returns findings when issues are detected."""
        from src.db import get_db_context
        from src.models import MigrationRecord
        from src.migrate.monitor import MonitorFinding
        from src.services.monitor_service import MonitorResult

        with get_db_context() as session:
            record = MigrationRecord(
                schema_name="mon-test-3",
                status="completed",
                target_url="postgresql://localhost/fake",
            )
            session.add(record)
            session.commit()
            mid = str(record.id)

        fake_result = MonitorResult(
            findings=[
                MonitorFinding(
                    severity="warning",
                    check_name="dead_tuple_bloat",
                    table="orders",
                    message="High bloat on orders",
                    recommended_action="Run VACUUM ANALYZE",
                )
            ],
            overall_severity="warning",
            snapshot_id=str(uuid.uuid4()),
            tables_checked=5,
            table_row_counts={"orders": 2000},
        )

        with patch("src.routers.migrations.monitor_migration", return_value=fake_result):
            response = client.post(f"/api/v1/migrations/{mid}/monitor")

        assert response.status_code == 200
        data = response.json()
        assert data["overall_severity"] == "warning"
        assert len(data["findings"]) == 1
        assert data["findings"][0]["check_name"] == "dead_tuple_bloat"

    def test_list_monitor_snapshots_empty(self, client):
        """Returns empty list when no snapshots exist."""
        from src.db import get_db_context
        from src.models import MigrationRecord

        with get_db_context() as session:
            record = MigrationRecord(schema_name="mon-list-test", status="completed")
            session.add(record)
            session.commit()
            mid = str(record.id)

        response = client.get(f"/api/v1/migrations/{mid}/monitor")
        assert response.status_code == 200
        assert response.json() == []

    def test_list_monitor_snapshots_returns_history(self, client):
        """Returns snapshots ordered newest-first."""
        from src.db import get_db_context
        from src.models import MigrationRecord, ProductionMonitorSnapshot
        from src.utils.time import utc_now
        from datetime import timedelta

        with get_db_context() as session:
            record = MigrationRecord(schema_name="mon-list-test-2", status="completed")
            session.add(record)
            session.commit()
            mid = str(record.id)

            now = utc_now()
            for i in range(3):
                snap = ProductionMonitorSnapshot(
                    migration_id=record.id,
                    table_row_counts={"orders": 1000 + i * 10},
                    findings=[],
                    overall_severity="clean",
                    tables_checked=1,
                    created_at=now - timedelta(hours=i),
                )
                session.add(snap)
            session.commit()

        response = client.get(f"/api/v1/migrations/{mid}/monitor")
        assert response.status_code == 200
        snapshots = response.json()
        assert len(snapshots) == 3
        # Newest first.
        ts = [s["created_at"] for s in snapshots]
        assert ts == sorted(ts, reverse=True)
