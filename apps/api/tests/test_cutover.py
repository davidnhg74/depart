"""Tests for Layer 9 — cutover readiness gate.

Covers:
  - evaluate_readiness() pure function (all scenarios)
  - POST /api/v1/migrations/{id}/cutover-readiness endpoint
  - GET  /api/v1/migrations/{id}/cutover-readiness endpoint
"""

from __future__ import annotations

import uuid
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from src.main import app
from src.migrate.cutover import evaluate_readiness


@pytest.fixture
def client():
    return TestClient(app)


# ─── evaluate_readiness — pure function ───────────────────────────────────────


class TestEvaluateReadiness:
    def _all_clear(self, **overrides):
        """Baseline: all checks clean, migration completed, CDC caught up."""
        defaults = dict(
            migration_status="completed",
            last_captured_scn=5000,
            last_applied_scn=5000,
            anomaly_severity="clean",
            anomaly_tables=3,
            monitor_severity="clean",
            monitor_findings_count=0,
            sample_status="clean",
            sample_mismatch_count=0,
            sample_tables=3,
        )
        defaults.update(overrides)
        return evaluate_readiness(**defaults)

    def test_all_clear_is_ready(self):
        r = self._all_clear()
        assert r.ready_to_cut is True
        assert r.blocking_count == 0
        assert r.advisory_count == 0
        assert r.score == 100

    def test_migration_not_completed_blocks(self):
        r = self._all_clear(migration_status="pending")
        assert r.ready_to_cut is False
        assert r.blocking_count >= 1
        blocking = [s for s in r.signals if s.layer == "migration_status"]
        assert blocking[0].status == "blocking"

    def test_migration_in_progress_blocks(self):
        r = self._all_clear(migration_status="in_progress")
        assert r.ready_to_cut is False
        sig = next(s for s in r.signals if s.layer == "migration_status")
        assert sig.status == "blocking"

    def test_no_cdc_is_not_run(self):
        r = self._all_clear(last_captured_scn=None, last_applied_scn=None)
        sig = next(s for s in r.signals if s.layer == "cdc_lag")
        assert sig.status == "not_run"
        assert r.ready_to_cut is True  # not_run != blocking

    def test_high_cdc_lag_blocks(self):
        r = self._all_clear(last_captured_scn=200_000, last_applied_scn=50_000)
        sig = next(s for s in r.signals if s.layer == "cdc_lag")
        assert sig.status == "blocking"
        assert r.ready_to_cut is False

    def test_moderate_cdc_lag_is_advisory(self):
        r = self._all_clear(last_captured_scn=60_000, last_applied_scn=40_000)
        sig = next(s for s in r.signals if s.layer == "cdc_lag")
        assert sig.status == "advisory"
        assert r.ready_to_cut is True

    def test_small_cdc_lag_is_ok(self):
        r = self._all_clear(last_captured_scn=5_100, last_applied_scn=5_000)
        sig = next(s for s in r.signals if s.layer == "cdc_lag")
        assert sig.status == "ok"

    def test_anomaly_not_run(self):
        r = self._all_clear(anomaly_severity=None)
        sig = next(s for s in r.signals if s.layer == "L6_anomaly")
        assert sig.status == "not_run"
        assert r.ready_to_cut is True

    def test_anomaly_warning_is_advisory(self):
        r = self._all_clear(anomaly_severity="warning")
        sig = next(s for s in r.signals if s.layer == "L6_anomaly")
        assert sig.status == "advisory"
        assert r.ready_to_cut is True

    def test_anomaly_error_blocks(self):
        r = self._all_clear(anomaly_severity="error")
        sig = next(s for s in r.signals if s.layer == "L6_anomaly")
        assert sig.status == "blocking"
        assert r.ready_to_cut is False

    def test_monitor_not_run(self):
        r = self._all_clear(monitor_severity=None)
        sig = next(s for s in r.signals if s.layer == "L7_monitor")
        assert sig.status == "not_run"

    def test_monitor_warning_is_advisory(self):
        r = self._all_clear(monitor_severity="warning", monitor_findings_count=2)
        sig = next(s for s in r.signals if s.layer == "L7_monitor")
        assert sig.status == "advisory"
        assert r.advisory_count >= 1

    def test_monitor_error_blocks(self):
        r = self._all_clear(monitor_severity="error", monitor_findings_count=3)
        sig = next(s for s in r.signals if s.layer == "L7_monitor")
        assert sig.status == "blocking"
        assert r.ready_to_cut is False

    def test_sampler_not_run(self):
        r = self._all_clear(sample_status=None)
        sig = next(s for s in r.signals if s.layer == "L8_sampler")
        assert sig.status == "not_run"

    def test_sampler_mismatches_blocks(self):
        r = self._all_clear(sample_status="mismatches_found", sample_mismatch_count=5)
        sig = next(s for s in r.signals if s.layer == "L8_sampler")
        assert sig.status == "blocking"
        assert r.ready_to_cut is False

    def test_multiple_blocking_lowers_score_to_zero(self):
        r = self._all_clear(
            migration_status="failed",
            last_captured_scn=500_000,
            last_applied_scn=0,
            anomaly_severity="error",
            sample_status="mismatches_found",
            sample_mismatch_count=10,
        )
        assert r.score == 0
        assert r.blocking_count >= 3

    def test_score_100_when_all_clear(self):
        r = self._all_clear()
        assert r.score == 100

    def test_score_decreases_with_advisories(self):
        r_clean = self._all_clear()
        r_advisory = self._all_clear(monitor_severity="warning", monitor_findings_count=1)
        assert r_advisory.score < r_clean.score

    def test_signals_count(self):
        r = self._all_clear()
        assert len(r.signals) == 5  # status, cdc_lag, L6, L7, L8

    def test_not_run_count(self):
        r = evaluate_readiness(
            migration_status="completed",
            last_captured_scn=None,
            last_applied_scn=None,
            anomaly_severity=None,
            monitor_severity=None,
            sample_status=None,
        )
        # cdc_lag, L6_anomaly, L7_monitor, L8_sampler → 4
        assert r.not_run_count == 4

    def test_ready_to_cut_false_when_blocking(self):
        r = self._all_clear(migration_status="pending")
        assert r.ready_to_cut is False

    def test_ready_to_cut_true_with_advisories_and_not_run(self):
        r = self._all_clear(
            monitor_severity="warning",
            monitor_findings_count=1,
            sample_status=None,
        )
        assert r.ready_to_cut is True


# ─── Endpoint tests ───────────────────────────────────────────────────────────


class TestCutoverReadinessEndpoints:
    @pytest.fixture(autouse=True)
    def cleanup(self):
        yield
        app.dependency_overrides.clear()

    def _make_migration(self, *, status="completed"):
        from src.models import MigrationRecord
        m = MagicMock(spec=MigrationRecord)
        m.id = uuid.uuid4()
        m.user_id = None
        m.status = status
        m.last_captured_scn = None
        m.last_applied_scn = None
        m.source_schema = "public"
        m.target_schema = "public"
        return m

    def _override_db(self, migration, *, list_return=None):
        from src.db import get_db
        mock_db = MagicMock()
        mock_db.get.return_value = migration
        mock_db.query.return_value.filter.return_value.order_by.return_value.first.return_value = None
        mock_db.query.return_value.filter.return_value.order_by.return_value.limit.return_value.all.return_value = (
            list_return or []
        )
        app.dependency_overrides[get_db] = lambda: mock_db
        return mock_db

    def test_run_readiness_not_found(self, client):
        from src.db import get_db
        mock_db = MagicMock()
        mock_db.get.return_value = None
        app.dependency_overrides[get_db] = lambda: mock_db

        response = client.post(f"/api/v1/migrations/{uuid.uuid4()}/cutover-readiness")
        assert response.status_code == 404

    def test_run_readiness_success(self, client):
        migration = self._make_migration(status="completed")
        mock_db = self._override_db(migration)

        snap = MagicMock()
        snap.id = uuid.uuid4()

        with patch(
            "src.routers.migrations.assess_cutover_readiness"
        ) as mock_assess:
            from src.migrate.cutover import CutoverReadiness, ReadinessSignal
            fake_readiness = CutoverReadiness(
                signals=[
                    ReadinessSignal(
                        layer="migration_status",
                        label="Migration completed",
                        status="ok",
                        summary="Done",
                    )
                ],
                blocking_count=0,
                advisory_count=0,
                not_run_count=0,
                ready_to_cut=True,
                score=100,
            )
            mock_assess.return_value = (fake_readiness, snap)
            response = client.post(f"/api/v1/migrations/{migration.id}/cutover-readiness")

        assert response.status_code == 200
        data = response.json()
        assert data["ready_to_cut"] is True
        assert data["score"] == 100
        assert data["blocking_count"] == 0
        assert len(data["signals"]) == 1
        assert "snapshot_id" in data

    def test_run_readiness_with_blocking(self, client):
        migration = self._make_migration(status="pending")
        mock_db = self._override_db(migration)

        snap = MagicMock()
        snap.id = uuid.uuid4()

        with patch(
            "src.routers.migrations.assess_cutover_readiness"
        ) as mock_assess:
            from src.migrate.cutover import CutoverReadiness, ReadinessSignal
            fake_readiness = CutoverReadiness(
                signals=[
                    ReadinessSignal(
                        layer="migration_status",
                        label="Migration completed",
                        status="blocking",
                        summary="Not done",
                    )
                ],
                blocking_count=1,
                advisory_count=0,
                not_run_count=0,
                ready_to_cut=False,
                score=70,
            )
            mock_assess.return_value = (fake_readiness, snap)
            response = client.post(f"/api/v1/migrations/{migration.id}/cutover-readiness")

        assert response.status_code == 200
        data = response.json()
        assert data["ready_to_cut"] is False
        assert data["blocking_count"] == 1
        assert data["signals"][0]["status"] == "blocking"

    def test_list_readiness_empty(self, client):
        migration = self._make_migration()
        self._override_db(migration)  # list returns [] by default

        response = client.get(f"/api/v1/migrations/{migration.id}/cutover-readiness")
        assert response.status_code == 200
        assert response.json() == []

    def test_list_readiness_returns_history(self, client):
        from datetime import datetime, timezone, timedelta

        migration = self._make_migration()
        mock_db = self._override_db(migration)

        now = datetime.now(timezone.utc)
        snaps = []
        for i in range(3):
            s = MagicMock()
            s.id = uuid.uuid4()
            s.created_at = now - timedelta(hours=i)
            s.ready_to_cut = i == 0
            s.score = 100 - i * 20
            s.blocking_count = i
            s.advisory_count = 0
            snaps.append(s)

        mock_db.query.return_value.filter.return_value.order_by.return_value.limit.return_value.all.return_value = snaps

        response = client.get(f"/api/v1/migrations/{migration.id}/cutover-readiness")
        assert response.status_code == 200
        items = response.json()
        assert len(items) == 3
        ts = [item["created_at"] for item in items]
        assert ts == sorted(ts, reverse=True)
