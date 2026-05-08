"""Tests for Layer 8 — row-level data sampler.

Covers:
  - sampler.py pure functions (normalise, compare_row, run_sampler)
  - sampler_service.sample_migration() with mocked DB connections
  - POST /api/v1/migrations/{id}/sample endpoint
  - GET  /api/v1/migrations/{id}/sample endpoint
"""

from __future__ import annotations

import uuid
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from src.main import app
from src.migrate.sampler import (
    SampleMismatch,
    _normalise,
    compare_row,
    run_sampler,
)


@pytest.fixture
def client():
    return TestClient(app)


# ─── _normalise ──────────────────────────────────────────────────────────────


class TestNormalise:
    def test_none_returns_none(self):
        assert _normalise(None) is None

    def test_empty_string_returns_none(self):
        assert _normalise("") is None

    def test_whitespace_only_returns_none(self):
        assert _normalise("   ") is None

    def test_strips_trailing_whitespace(self):
        assert _normalise("hello   ") == "hello"

    def test_preserves_leading_whitespace(self):
        # Only trailing stripped (CHAR padding)
        assert _normalise("  hello") == "  hello"

    def test_truncates_long_values(self):
        long = "x" * 300
        result = _normalise(long)
        assert result is not None
        assert len(result) <= 210  # 200 + "…"
        assert result.endswith("…")

    def test_numeric_value(self):
        assert _normalise(42) == "42"

    def test_float_value(self):
        assert _normalise(3.14) == "3.14"


# ─── compare_row ─────────────────────────────────────────────────────────────


class TestCompareRow:
    def test_matching_rows_no_mismatches(self):
        oracle = {"ID": 1, "NAME": "Alice", "EMAIL": "alice@example.com"}
        pg = {"id": 1, "name": "Alice", "email": "alice@example.com"}
        mismatches = compare_row("users", ["ID"], oracle, pg, set())
        assert mismatches == []

    def test_missing_in_pg_returns_one_mismatch(self):
        oracle = {"ID": 1, "NAME": "Alice"}
        mismatches = compare_row("users", ["ID"], oracle, None, set())
        assert len(mismatches) == 1
        assert mismatches[0].mismatch_type == "missing_in_pg"
        assert mismatches[0].column == "<row>"

    def test_value_mismatch_detected(self):
        oracle = {"ID": 1, "NAME": "Alice"}
        pg = {"id": 1, "name": "Bob"}
        mismatches = compare_row("users", ["ID"], oracle, pg, set())
        assert len(mismatches) == 1
        assert mismatches[0].mismatch_type == "value_mismatch"
        assert mismatches[0].column == "NAME"
        assert mismatches[0].oracle_value == "Alice"
        assert mismatches[0].pg_value == "Bob"

    def test_null_mismatch_detected(self):
        oracle = {"ID": 1, "NAME": "Alice"}
        pg = {"id": 1, "name": None}
        mismatches = compare_row("users", ["ID"], oracle, pg, set())
        assert len(mismatches) == 1
        assert mismatches[0].mismatch_type == "null_mismatch"

    def test_oracle_empty_string_equals_pg_null(self):
        oracle = {"ID": 1, "NAME": ""}  # Oracle empty string
        pg = {"id": 1, "name": None}   # PG NULL
        mismatches = compare_row("users", ["ID"], oracle, pg, set())
        assert mismatches == []  # Treated as equivalent

    def test_trailing_whitespace_ignored(self):
        oracle = {"ID": 1, "NAME": "Alice   "}  # CHAR padding
        pg = {"id": 1, "name": "Alice"}
        mismatches = compare_row("users", ["ID"], oracle, pg, set())
        assert mismatches == []

    def test_skipped_columns_ignored(self):
        oracle = {"ID": 1, "PHOTO": b"binarydata"}
        pg = {"id": 1, "photo": b"differentdata"}
        mismatches = compare_row("users", ["ID"], oracle, pg, skip_cols={"PHOTO"})
        assert mismatches == []

    def test_pk_values_captured_in_mismatch(self):
        oracle = {"ID": 42, "STATUS": "active"}
        pg = {"id": 42, "status": "inactive"}
        mismatches = compare_row("orders", ["ID"], oracle, pg, set())
        assert len(mismatches) == 1
        assert mismatches[0].pk_values == {"ID": 42}
        assert mismatches[0].table == "orders"

    def test_multiple_mismatches_per_row(self):
        oracle = {"ID": 1, "NAME": "Alice", "STATUS": "active"}
        pg = {"id": 1, "name": "Bob", "status": "inactive"}
        mismatches = compare_row("users", ["ID"], oracle, pg, set())
        assert len(mismatches) == 2

    def test_composite_pk_captured(self):
        oracle = {"ORD_ID": 1, "LINE": 2, "QTY": 10}
        pg = {"ord_id": 1, "line": 2, "qty": 99}
        mismatches = compare_row("order_lines", ["ORD_ID", "LINE"], oracle, pg, set())
        assert len(mismatches) == 1
        assert mismatches[0].pk_values == {"ORD_ID": 1, "LINE": 2}


# ─── run_sampler ─────────────────────────────────────────────────────────────


class TestRunSampler:
    def _make_mock_oracle_session(self, pk_cols=None, rows=None, skip_cols=None):
        mock = MagicMock()
        call_count = [0]

        def execute_side_effect(sql, params=None):
            call_count[0] += 1
            result = MagicMock()
            query = str(sql) if hasattr(sql, '__str__') else sql.text

            if "all_constraints" in str(sql):
                # PK detection query
                pk_rows = [MagicMock(**{"__getitem__": lambda s, k: (pk_cols or ["ID"])[0]})]
                mapping_rows = []
                for col in (pk_cols or ["ID"]):
                    r = MagicMock()
                    r.__getitem__ = lambda s, k, c=col: c
                    mapping_rows.append(r)
                result.mappings.return_value.all.return_value = mapping_rows
            elif "all_tab_columns" in str(sql):
                # Skip columns query
                skip_rows = []
                result.mappings.return_value.all.return_value = skip_rows
            elif "DBMS_RANDOM" in str(sql):
                # Sample rows query
                sample_rows = rows or [{"ID": 1, "NAME": "Alice"}]
                mapping_rows = []
                for row in sample_rows:
                    r = MagicMock()
                    r.__iter__ = lambda s, d=row: iter(d.items())
                    r.keys = lambda d=row: d.keys()
                    r.__getitem__ = lambda s, k, d=row: d[k]
                    mapping_rows.append(dict(row))
                result.mappings.return_value.all.return_value = mapping_rows
            else:
                result.mappings.return_value.all.return_value = []
            return result

        mock.execute.side_effect = execute_side_effect
        return mock

    def test_skips_table_without_pk(self):
        oracle_sess = MagicMock()
        pg_sess = MagicMock()

        # PK query returns empty
        pk_result = MagicMock()
        pk_result.mappings.return_value.all.return_value = []
        oracle_sess.execute.return_value = pk_result

        mismatches, stats = run_sampler(
            oracle_sess, pg_sess, "MYSCHEMA", "public", ["ORDERS"]
        )
        assert stats["skipped_no_pk"] == 1
        assert stats["sampled"] == 0
        assert mismatches == []

    def test_skips_empty_table(self):
        oracle_sess = MagicMock()
        pg_sess = MagicMock()

        call_count = [0]
        def execute_se(sql, params=None):
            call_count[0] += 1
            result = MagicMock()
            if "all_constraints" in str(sql):
                r = MagicMock()
                r.__getitem__ = lambda s, k: "ID"
                result.mappings.return_value.all.return_value = [r]
            else:
                result.mappings.return_value.all.return_value = []
            return result

        oracle_sess.execute.side_effect = execute_se

        mismatches, stats = run_sampler(
            oracle_sess, pg_sess, "MYSCHEMA", "public", ["EMPTY_TABLE"]
        )
        assert stats["skipped_empty"] == 1
        assert stats["sampled"] == 0

    def test_clean_migration_no_mismatches(self):
        oracle_sess = MagicMock()
        pg_sess = MagicMock()

        def oracle_execute(sql, params=None):
            result = MagicMock()
            if "all_constraints" in str(sql):
                r = MagicMock()
                r.__getitem__ = lambda s, k: "ID"
                result.mappings.return_value.all.return_value = [r]
            elif "all_tab_columns" in str(sql):
                result.mappings.return_value.all.return_value = []
            else:
                result.mappings.return_value.all.return_value = [{"ID": 1, "NAME": "Alice"}]
            return result

        oracle_sess.execute.side_effect = oracle_execute

        # PG returns matching row
        pg_result = MagicMock()
        pg_result.mappings.return_value.first.return_value = {"id": 1, "name": "Alice"}
        pg_sess.execute.return_value = pg_result

        mismatches, stats = run_sampler(
            oracle_sess, pg_sess, "MYSCHEMA", "public", ["USERS"]
        )
        assert stats["sampled"] == 1
        assert mismatches == []


# ─── Endpoint tests ──────────────────────────────────────────────────────────


class TestSamplerEndpoints:
    @pytest.fixture(autouse=True)
    def cleanup(self):
        yield
        app.dependency_overrides.clear()

    def _make_migration(self, *, source_url=None, target_url=None):
        from src.models import MigrationRecord
        m = MagicMock(spec=MigrationRecord)
        m.id = uuid.uuid4()
        m.user_id = None
        m.source_url = source_url
        m.target_url = target_url
        m.schema_name = "test"
        m.status = "completed"
        m.tables = None
        m.source_schema = "public"
        m.target_schema = "public"
        return m

    def _override_db(self, migration):
        from src.db import get_db
        mock_db = MagicMock()
        mock_db.get.return_value = migration
        mock_db.query.return_value.filter.return_value.order_by.return_value.limit.return_value.all.return_value = []
        app.dependency_overrides[get_db] = lambda: mock_db
        return mock_db

    def test_run_sample_no_source_url(self, client):
        """Returns 400 when source_url is missing."""
        migration = self._make_migration(target_url="postgresql://localhost/fake")
        self._override_db(migration)

        response = client.post(f"/api/v1/migrations/{migration.id}/sample")
        assert response.status_code == 400
        assert "source_url" in response.json()["detail"]

    def test_run_sample_no_target_url(self, client):
        """Returns 400 when target_url is missing."""
        migration = self._make_migration(
            source_url="oracle+oracledb://user:pass@host:1521/?service_name=ORCL"
        )
        self._override_db(migration)

        response = client.post(f"/api/v1/migrations/{migration.id}/sample")
        assert response.status_code == 400
        assert "target_url" in response.json()["detail"]

    def test_run_sample_not_found(self, client):
        from src.db import get_db
        mock_db = MagicMock()
        mock_db.get.return_value = None
        app.dependency_overrides[get_db] = lambda: mock_db

        response = client.post(f"/api/v1/migrations/{uuid.uuid4()}/sample")
        assert response.status_code == 404

    def test_run_sample_success_clean(self, client):
        """Mock sample_migration returning clean result."""
        from src.services.sampler_service import SamplerResult

        migration = self._make_migration(
            source_url="oracle+oracledb://u:p@h:1521/?service_name=X",
            target_url="postgresql://localhost/fake",
        )
        self._override_db(migration)

        fake_result = SamplerResult(
            mismatches=[],
            overall_status="clean",
            result_id=str(uuid.uuid4()),
            tables_sampled=5,
            tables_skipped=1,
            mismatch_count=0,
        )

        with patch("src.routers.migrations.sample_migration", return_value=fake_result):
            response = client.post(
                f"/api/v1/migrations/{migration.id}/sample", json={"sample_size": 50}
            )

        assert response.status_code == 200
        data = response.json()
        assert data["overall_status"] == "clean"
        assert data["tables_sampled"] == 5
        assert data["tables_skipped"] == 1
        assert data["mismatch_count"] == 0
        assert data["mismatches"] == []

    def test_run_sample_with_mismatches(self, client):
        """Mock result with mismatches returns mismatch details."""
        from src.services.sampler_service import SamplerResult

        migration = self._make_migration(
            source_url="oracle+oracledb://u:p@h:1521/?service_name=X",
            target_url="postgresql://localhost/fake",
        )
        self._override_db(migration)

        mismatch = SampleMismatch(
            table="ORDERS",
            pk_values={"ID": 42},
            column="STATUS",
            oracle_value="ACTIVE",
            pg_value="active",
            mismatch_type="value_mismatch",
        )
        fake_result = SamplerResult(
            mismatches=[mismatch],
            overall_status="mismatches_found",
            result_id=str(uuid.uuid4()),
            tables_sampled=3,
            tables_skipped=0,
            mismatch_count=1,
        )

        with patch("src.routers.migrations.sample_migration", return_value=fake_result):
            response = client.post(f"/api/v1/migrations/{migration.id}/sample")

        assert response.status_code == 200
        data = response.json()
        assert data["overall_status"] == "mismatches_found"
        assert data["mismatch_count"] == 1
        assert len(data["mismatches"]) == 1
        assert data["mismatches"][0]["table"] == "ORDERS"
        assert data["mismatches"][0]["mismatch_type"] == "value_mismatch"

    def test_list_sample_results_empty(self, client):
        migration = self._make_migration()
        self._override_db(migration)  # query chain returns [] by default

        response = client.get(f"/api/v1/migrations/{migration.id}/sample")
        assert response.status_code == 200
        assert response.json() == []

    def test_list_sample_results_returns_history(self, client):
        from datetime import datetime, timezone, timedelta

        migration = self._make_migration()
        mock_db = self._override_db(migration)

        now = datetime.now(timezone.utc)
        rows = []
        for i in range(3):
            r = MagicMock()
            r.id = uuid.uuid4()
            r.created_at = now - timedelta(hours=i)  # newest first
            r.overall_status = "clean" if i == 0 else "mismatches_found"
            r.tables_sampled = 5
            r.tables_skipped = 0
            r.mismatch_count = i
            r.sample_size = 100
            rows.append(r)

        mock_db.query.return_value.filter.return_value.order_by.return_value.limit.return_value.all.return_value = rows

        response = client.get(f"/api/v1/migrations/{migration.id}/sample")
        assert response.status_code == 200
        items = response.json()
        assert len(items) == 3
        ts = [item["created_at"] for item in items]
        assert ts == sorted(ts, reverse=True)

    def test_sample_size_validated(self, client):
        """sample_size must be >= 1; Pydantic rejects before DB is touched."""
        migration = self._make_migration()
        self._override_db(migration)

        response = client.post(
            f"/api/v1/migrations/{migration.id}/sample", json={"sample_size": 0}
        )
        assert response.status_code == 422
