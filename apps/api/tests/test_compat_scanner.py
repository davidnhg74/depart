"""Tests for Layer 10 — Application SQL Compatibility Scanner.

Covers:
  - scan_objects() pure function (all severity categories)
  - Scoring logic
  - POST /api/v1/migrations/{id}/compat-scan endpoint
  - GET  /api/v1/migrations/{id}/compat-scan endpoint
"""

from __future__ import annotations

import uuid
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from src.main import app
from src.migrate.compat_scanner import CompatScanResult, scan_objects


@pytest.fixture
def client():
    return TestClient(app)


# ─── Pure scanner tests ───────────────────────────────────────────────────────


class TestCompatScanner:
    def _scan(self, text: str, obj_type: str = "VIEW", name: str = "TEST_OBJ"):
        return scan_objects([{"type": obj_type, "name": name, "text": text}])

    def test_empty_text_is_fully_compatible(self):
        r = self._scan("")
        assert r.complexity_score == 100
        assert r.blocking_count == 0
        assert r.findings == []

    def test_rownum_is_blocking(self):
        r = self._scan("SELECT * FROM t WHERE ROWNUM <= 10")
        finding = next(f for f in r.findings if f.construct == "ROWNUM")
        assert finding.severity == "blocking"
        assert r.blocking_count >= 1
        assert r.complexity_score < 100

    def test_connect_by_is_blocking(self):
        r = self._scan("SELECT id, CONNECT BY PRIOR parent_id = id")
        finding = next(f for f in r.findings if f.construct == "CONNECT_BY")
        assert finding.severity == "blocking"

    def test_start_with_is_blocking(self):
        r = self._scan("SELECT * FROM emp START WITH manager_id IS NULL")
        finding = next(f for f in r.findings if f.construct == "START_WITH")
        assert finding.severity == "blocking"

    def test_outer_join_plus_is_blocking(self):
        r = self._scan("SELECT a.*, b.col FROM a, b WHERE a.id = b.id(+)")
        finding = next(f for f in r.findings if f.construct == "OUTER_JOIN_PLUS")
        assert finding.severity == "blocking"

    def test_minus_is_blocking(self):
        r = self._scan("SELECT id FROM a MINUS SELECT id FROM b")
        finding = next(f for f in r.findings if f.construct == "MINUS_SET_OP")
        assert finding.severity == "blocking"

    def test_package_is_blocking(self):
        r = self._scan("CREATE OR REPLACE PACKAGE my_pkg AS", "PACKAGE", "MY_PKG")
        finding = next(f for f in r.findings if f.construct == "PACKAGE")
        assert finding.severity == "blocking"

    def test_nvl_is_advisory(self):
        r = self._scan("SELECT NVL(col, 0) FROM t")
        finding = next(f for f in r.findings if f.construct == "NVL")
        assert finding.severity == "advisory"
        assert r.advisory_count >= 1

    def test_decode_is_advisory(self):
        r = self._scan("SELECT DECODE(status, 'A', 'Active', 'Inactive') FROM t")
        finding = next(f for f in r.findings if f.construct == "DECODE")
        assert finding.severity == "advisory"

    def test_sysdate_is_advisory(self):
        r = self._scan("SELECT SYSDATE FROM DUAL")
        assert any(f.construct == "SYSDATE" for f in r.findings)
        assert any(f.construct == "DUAL_TABLE" for f in r.findings)

    def test_dual_table_is_advisory(self):
        r = self._scan("SELECT 1 FROM DUAL")
        finding = next(f for f in r.findings if f.construct == "DUAL_TABLE")
        assert finding.severity == "advisory"

    def test_hint_comment_is_info(self):
        r = self._scan("SELECT /*+ INDEX(t idx) */ * FROM t")
        finding = next(f for f in r.findings if f.construct == "HINT_COMMENT")
        assert finding.severity == "info"

    def test_varchar2_is_info(self):
        r = self._scan("col1 VARCHAR2(100)")
        finding = next(f for f in r.findings if f.construct == "VARCHAR2")
        assert finding.severity == "info"

    def test_case_insensitive_detection(self):
        r = self._scan("select rownum from t")
        assert any(f.construct == "ROWNUM" for f in r.findings)

    def test_count_multiple_occurrences(self):
        r = self._scan("SELECT NVL(a, 0), NVL(b, ''), NVL(c, -1) FROM t")
        finding = next(f for f in r.findings if f.construct == "NVL")
        assert finding.count == 3

    def test_location_tracking(self):
        objs = [
            {"type": "VIEW", "name": "V_ORDERS", "text": "SELECT ROWNUM, NVL(col,0) FROM t"},
            {"type": "VIEW", "name": "V_ITEMS", "text": "SELECT * FROM t WHERE ROWNUM < 5"},
        ]
        r = scan_objects(objs)
        rownum = next(f for f in r.findings if f.construct == "ROWNUM")
        assert "VIEW:V_ORDERS" in rownum.locations
        assert "VIEW:V_ITEMS" in rownum.locations
        assert rownum.count == 2

    def test_findings_ordered_blocking_first(self):
        r = self._scan("SELECT NVL(a,0), ROWNUM FROM t")
        severities = [f.severity for f in r.findings]
        # All blocking come before advisory
        seen_advisory = False
        for s in severities:
            if s == "advisory":
                seen_advisory = True
            if seen_advisory:
                assert s != "blocking", "blocking finding appeared after advisory"

    def test_score_100_when_clean(self):
        r = self._scan("SELECT id, name FROM users WHERE active = 1")
        assert r.complexity_score == 100

    def test_score_decreases_with_blocking(self):
        r_clean = self._scan("SELECT id FROM t")
        r_blocked = self._scan("SELECT ROWNUM FROM t")
        assert r_blocked.complexity_score < r_clean.complexity_score

    def test_score_floor_zero(self):
        many_blocks = " ".join([
            "ROWNUM CONNECT BY START WITH PIVOT UNPIVOT MODEL( EXECUTE IMMEDIATE",
            "BULK COLLECT FORALL CREATE PACKAGE pkg_test AS PRAGMA AUTONOMOUS",
        ])
        r = self._scan(many_blocks)
        assert r.complexity_score == 0

    def test_objects_scanned_count(self):
        objs = [
            {"type": "VIEW", "name": "V1", "text": "SELECT 1 FROM DUAL"},
            {"type": "PROCEDURE", "name": "P1", "text": "BEGIN NULL; END;"},
        ]
        r = scan_objects(objs)
        assert r.oracle_objects_scanned == 2

    def test_empty_object_list(self):
        r = scan_objects([])
        assert r.oracle_objects_scanned == 0
        assert r.complexity_score == 100
        assert r.findings == []

    def test_pg_equivalent_populated(self):
        r = self._scan("SELECT ROWNUM FROM t")
        finding = next(f for f in r.findings if f.construct == "ROWNUM")
        assert len(finding.pg_equivalent) > 10  # has actual guidance


# ─── Endpoint tests ───────────────────────────────────────────────────────────


class TestCompatScanEndpoints:
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
        m.source_url = "oracle+cx_oracle://user:pass@host/db"
        m.target_url = None
        m.source_schema = "HR"
        m.schema_name = "HR"
        m.tables = None
        return m

    def _override_db(self, migration, *, list_return=None):
        from src.db import get_db
        mock_db = MagicMock()
        mock_db.get.return_value = migration
        mock_db.query.return_value.filter.return_value.order_by.return_value.limit.return_value.all.return_value = (
            list_return or []
        )
        app.dependency_overrides[get_db] = lambda: mock_db
        return mock_db

    def test_run_compat_scan_not_found(self, client):
        from src.db import get_db
        mock_db = MagicMock()
        mock_db.get.return_value = None
        app.dependency_overrides[get_db] = lambda: mock_db

        response = client.post(f"/api/v1/migrations/{uuid.uuid4()}/compat-scan")
        assert response.status_code == 404

    def test_run_compat_scan_success(self, client):
        migration = self._make_migration()
        mock_db = self._override_db(migration)

        snap = MagicMock()
        snap.id = uuid.uuid4()

        with patch("src.routers.migrations.scan_compat") as mock_scan:
            from src.migrate.compat_scanner import CompatFinding, CompatScanResult
            fake_result = CompatScanResult(
                oracle_objects_scanned=5,
                blocking_count=1,
                advisory_count=2,
                info_count=0,
                findings=[
                    CompatFinding(
                        construct="ROWNUM",
                        severity="blocking",
                        pg_equivalent="Use ROW_NUMBER()",
                        locations=["VIEW:V_TEST"],
                        count=3,
                    )
                ],
                complexity_score=60,
            )
            mock_scan.return_value = (fake_result, snap)
            response = client.post(f"/api/v1/migrations/{migration.id}/compat-scan")

        assert response.status_code == 200
        data = response.json()
        assert data["complexity_score"] == 60
        assert data["blocking_count"] == 1
        assert data["oracle_objects_scanned"] == 5
        assert len(data["findings"]) == 1
        assert data["findings"][0]["construct"] == "ROWNUM"
        assert "snapshot_id" in data

    def test_run_compat_scan_no_source_url(self, client):
        migration = self._make_migration()
        migration.source_url = None
        self._override_db(migration)

        with patch("src.routers.migrations.scan_compat") as mock_scan:
            mock_scan.side_effect = ValueError("Migration has no source_url")
            response = client.post(f"/api/v1/migrations/{migration.id}/compat-scan")

        assert response.status_code == 400

    def test_list_compat_scans_empty(self, client):
        migration = self._make_migration()
        self._override_db(migration)

        response = client.get(f"/api/v1/migrations/{migration.id}/compat-scan")
        assert response.status_code == 200
        assert response.json() == []

    def test_list_compat_scans_history(self, client):
        from datetime import datetime, timezone, timedelta

        migration = self._make_migration()
        mock_db = self._override_db(migration)

        now = datetime.now(timezone.utc)
        snaps = []
        for i in range(3):
            s = MagicMock()
            s.id = uuid.uuid4()
            s.created_at = now - timedelta(hours=i)
            s.oracle_objects_scanned = 10 + i
            s.blocking_count = i
            s.advisory_count = 1
            s.complexity_score = 100 - i * 20
            snaps.append(s)

        mock_db.query.return_value.filter.return_value.order_by.return_value.limit.return_value.all.return_value = snaps

        response = client.get(f"/api/v1/migrations/{migration.id}/compat-scan")
        assert response.status_code == 200
        items = response.json()
        assert len(items) == 3
        ts = [item["created_at"] for item in items]
        assert ts == sorted(ts, reverse=True)
