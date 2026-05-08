"""Tests for Layer 11 — PL/SQL → PL/pgSQL Conversion.

Covers:
  - convert_one() pure function (with mocked AIClient)
  - convert_batch() limit behaviour
  - POST /api/v1/migrations/{id}/convert-code — 200, 412 (no key), 400 (no source)
  - GET  /api/v1/migrations/{id}/convert-code — history list
  - GET  /api/v1/migrations/{id}/convert-code/{run_id} — full run results
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from src.main import app


@pytest.fixture
def client():
    return TestClient(app)


# ─── Pure converter tests ─────────────────────────────────────────────────────


class TestConvertOne:
    def _call(self, *, object_type="PROCEDURE", object_name="TEST_PROC",
              oracle_source="BEGIN NULL; END;", api_key="sk-ant-test"):
        from src.migrate.plsql_converter import convert_one
        return convert_one(
            object_type=object_type,
            object_name=object_name,
            oracle_source=oracle_source,
            api_key=api_key,
        )

    def test_empty_source_returns_error(self):
        result = self._call(oracle_source="   ")
        assert result["error"] is not None
        assert result["converted_code"] is None

    def test_passthrough_fields_always_present(self):
        with patch("src.ai.client.AIClient") as mock_cls:
            mock_client = MagicMock()
            mock_cls.smart.return_value = mock_client
            mock_client.complete_json.return_value = {
                "converted_code": "CREATE FUNCTION test() RETURNS void AS $$ BEGIN END; $$ LANGUAGE plpgsql;",
                "confidence": "high",
                "review_notes": "Straightforward conversion.",
                "patterns_applied": [],
            }
            result = self._call()

        assert result["object_type"] == "PROCEDURE"
        assert result["object_name"] == "TEST_PROC"
        assert result["oracle_source"] == "BEGIN NULL; END;"
        assert result["error"] is None

    def test_high_confidence_conversion(self):
        with patch("src.ai.client.AIClient") as mock_cls:
            mock_client = MagicMock()
            mock_cls.smart.return_value = mock_client
            mock_client.complete_json.return_value = {
                "converted_code": "CREATE OR REPLACE FUNCTION hello() RETURNS void ...",
                "confidence": "high",
                "review_notes": "No changes needed.",
                "patterns_applied": ["SYSDATE→NOW()"],
            }
            result = self._call()

        assert result["confidence"] == "high"
        assert result["patterns_applied"] == ["SYSDATE→NOW()"]
        assert "hello" in result["converted_code"]

    def test_low_confidence_on_complex_package(self):
        with patch("src.ai.client.AIClient") as mock_cls:
            mock_client = MagicMock()
            mock_cls.smart.return_value = mock_client
            mock_client.complete_json.return_value = {
                "converted_code": "-- TODO: manual rewrite required",
                "confidence": "low",
                "review_notes": "Package with PRAGMA AUTONOMOUS_TRANSACTION — no PG equivalent.",
                "patterns_applied": [],
            }
            result = self._call(object_type="PACKAGE", object_name="PKG_COMPLEX")

        assert result["confidence"] == "low"
        assert result["error"] is None

    def test_claude_parse_error_sets_error_field(self):
        with patch("src.ai.client.AIClient") as mock_cls:
            mock_client = MagicMock()
            mock_cls.smart.return_value = mock_client
            mock_client.complete_json.side_effect = ValueError("invalid JSON from model")
            result = self._call()

        assert result["error"] is not None
        assert "Parse error" in result["error"]
        assert result["converted_code"] is None

    def test_claude_exception_sets_error_field(self):
        with patch("src.ai.client.AIClient") as mock_cls:
            mock_client = MagicMock()
            mock_cls.smart.return_value = mock_client
            mock_client.complete_json.side_effect = RuntimeError("connection refused")
            result = self._call()

        assert result["error"] is not None
        assert result["converted_code"] is None

    def test_uses_smart_client_with_cache(self):
        with patch("src.ai.client.AIClient") as mock_cls:
            mock_client = MagicMock()
            mock_cls.smart.return_value = mock_client
            mock_client.complete_json.return_value = {
                "converted_code": "...", "confidence": "medium",
                "review_notes": "", "patterns_applied": [],
            }
            self._call(api_key="sk-ant-real-key")

        mock_cls.smart.assert_called_once()
        call_kwargs = mock_cls.smart.call_args.kwargs
        assert call_kwargs["api_key"] == "sk-ant-real-key"
        assert call_kwargs["feature"] == "plsql-convert"

        complete_call_kwargs = mock_client.complete_json.call_args.kwargs
        assert complete_call_kwargs.get("cache_system") is True

    def test_review_notes_preserved(self):
        notes = "Replaced ROWNUM with ROW_NUMBER(). Check ORDER BY assumptions."
        with patch("src.ai.client.AIClient") as mock_cls:
            mock_client = MagicMock()
            mock_cls.smart.return_value = mock_client
            mock_client.complete_json.return_value = {
                "converted_code": "...", "confidence": "medium",
                "review_notes": notes, "patterns_applied": ["ROWNUM→ROW_NUMBER()"],
            }
            result = self._call()

        assert result["review_notes"] == notes
        assert result["patterns_applied"] == ["ROWNUM→ROW_NUMBER()"]


class TestConvertBatch:
    def _objects(self, n=5):
        return [
            {"type": "PROCEDURE", "name": f"PROC_{i}", "text": f"BEGIN NULL; -- {i} END;"}
            for i in range(n)
        ]

    def _patched_convert_one(self, result_override=None):
        default = {
            "object_type": "PROCEDURE", "object_name": "X",
            "oracle_source": "", "converted_code": "...",
            "confidence": "high", "review_notes": "", "patterns_applied": [], "error": None,
        }
        return {**default, **(result_override or {})}

    def test_batch_respects_limit(self):
        from src.migrate.plsql_converter import convert_batch

        with patch("src.migrate.plsql_converter.convert_one") as mock_one:
            mock_one.return_value = self._patched_convert_one()
            results = convert_batch(self._objects(10), api_key="sk-ant-x", limit=3)

        assert len(results) == 3
        assert mock_one.call_count == 3

    def test_batch_converts_all_when_under_limit(self):
        from src.migrate.plsql_converter import convert_batch

        with patch("src.migrate.plsql_converter.convert_one") as mock_one:
            mock_one.return_value = self._patched_convert_one()
            results = convert_batch(self._objects(4), api_key="sk-ant-x", limit=10)

        assert len(results) == 4

    def test_batch_returns_empty_for_no_objects(self):
        from src.migrate.plsql_converter import convert_batch

        with patch("src.migrate.plsql_converter.convert_one") as mock_one:
            results = convert_batch([], api_key="sk-ant-x", limit=10)

        assert results == []
        mock_one.assert_not_called()

    def test_batch_forwards_api_key(self):
        from src.migrate.plsql_converter import convert_batch

        with patch("src.migrate.plsql_converter.convert_one") as mock_one:
            mock_one.return_value = self._patched_convert_one()
            convert_batch(self._objects(1), api_key="sk-ant-mykey", limit=5)

        mock_one.assert_called_once()
        assert mock_one.call_args.kwargs["api_key"] == "sk-ant-mykey"


# ─── Endpoint tests ───────────────────────────────────────────────────────────


class TestCodeConversionEndpoints:
    @pytest.fixture(autouse=True)
    def cleanup(self):
        yield
        app.dependency_overrides.clear()

    def _make_migration(self, *, source_url="oracle+cx_oracle://u:p@host/db"):
        from src.models import MigrationRecord
        m = MagicMock(spec=MigrationRecord)
        m.id = uuid.uuid4()
        m.user_id = None
        m.status = "completed"
        m.source_url = source_url
        m.target_url = None
        m.source_schema = "HR"
        m.schema_name = "HR"
        m.tables = None
        return m

    def _override_db(self, migration, *, runs=None):
        from src.db import get_db
        mock_db = MagicMock()
        mock_db.get.return_value = migration
        mock_db.query.return_value.filter.return_value.order_by.return_value.limit.return_value.all.return_value = (
            runs or []
        )
        app.dependency_overrides[get_db] = lambda: mock_db
        return mock_db

    def _fake_run(self, migration_id, *, n_results=2):
        run = MagicMock()
        run.id = uuid.uuid4()
        run.migration_id = migration_id
        run.objects_found = n_results
        run.objects_attempted = n_results
        run.objects_converted = n_results
        run.objects_failed = 0
        run.results = [
            {
                "object_type": "PROCEDURE",
                "object_name": f"PROC_{i}",
                "oracle_source": f"BEGIN NULL; -- {i} END;",
                "converted_code": f"CREATE OR REPLACE FUNCTION proc_{i}() ...",
                "confidence": "high",
                "review_notes": "Clean conversion.",
                "patterns_applied": [],
                "error": None,
            }
            for i in range(n_results)
        ]
        return run

    # POST /convert-code

    def test_post_404_when_migration_not_found(self, client):
        from src.db import get_db
        mock_db = MagicMock()
        mock_db.get.return_value = None
        app.dependency_overrides[get_db] = lambda: mock_db

        r = client.post(f"/api/v1/migrations/{uuid.uuid4()}/convert-code")
        assert r.status_code == 404

    def test_post_412_when_no_anthropic_key(self, client):
        migration = self._make_migration()
        self._override_db(migration)

        with patch("src.routers.migrations.run_plsql_conversion") as mock_svc:
            mock_svc.side_effect = ValueError(
                "No Anthropic API key configured. Set one in Settings"
            )
            r = client.post(f"/api/v1/migrations/{migration.id}/convert-code")

        assert r.status_code == 412
        assert "Anthropic API key" in r.json()["detail"]

    def test_post_400_when_no_source_url(self, client):
        migration = self._make_migration(source_url=None)
        self._override_db(migration)

        with patch("src.routers.migrations.run_plsql_conversion") as mock_svc:
            mock_svc.side_effect = ValueError("Migration has no source_url")
            r = client.post(f"/api/v1/migrations/{migration.id}/convert-code")

        assert r.status_code == 400

    def test_post_success_returns_conversion_response(self, client):
        migration = self._make_migration()
        mock_db = self._override_db(migration)
        run = self._fake_run(migration.id, n_results=2)

        with patch("src.routers.migrations.run_plsql_conversion") as mock_svc:
            mock_svc.return_value = (run.results, run)
            r = client.post(f"/api/v1/migrations/{migration.id}/convert-code?limit=5")

        assert r.status_code == 200
        data = r.json()
        assert data["objects_found"] == 2
        assert data["objects_converted"] == 2
        assert data["objects_failed"] == 0
        assert len(data["results"]) == 2
        assert data["results"][0]["object_type"] == "PROCEDURE"
        assert data["results"][0]["confidence"] == "high"
        assert "run_id" in data

    def test_post_limit_clamped_to_50(self, client):
        migration = self._make_migration()
        self._override_db(migration)
        run = self._fake_run(migration.id, n_results=1)

        with patch("src.routers.migrations.run_plsql_conversion") as mock_svc:
            mock_svc.return_value = (run.results, run)
            r = client.post(f"/api/v1/migrations/{migration.id}/convert-code?limit=999")

        assert r.status_code == 200
        # Verify limit was clamped (service was called with limit ≤ 50)
        call_kwargs = mock_svc.call_args.kwargs
        assert call_kwargs.get("limit", 999) <= 50

    # GET /convert-code (history)

    def test_get_history_empty(self, client):
        migration = self._make_migration()
        self._override_db(migration, runs=[])

        r = client.get(f"/api/v1/migrations/{migration.id}/convert-code")
        assert r.status_code == 200
        assert r.json() == []

    def test_get_history_returns_list(self, client):
        migration = self._make_migration()
        mock_db = self._override_db(migration)

        now = datetime.now(timezone.utc)
        runs = []
        for i in range(3):
            run = MagicMock()
            run.id = uuid.uuid4()
            run.created_at = now
            run.objects_found = 10
            run.objects_attempted = 5
            run.objects_converted = 4
            run.objects_failed = 1
            runs.append(run)
        mock_db.query.return_value.filter.return_value.order_by.return_value.limit.return_value.all.return_value = runs

        r = client.get(f"/api/v1/migrations/{migration.id}/convert-code")
        assert r.status_code == 200
        items = r.json()
        assert len(items) == 3
        assert items[0]["objects_converted"] == 4
        assert items[0]["objects_failed"] == 1
        assert "run_id" in items[0]

    # GET /convert-code/{run_id}

    def test_get_run_not_found(self, client):
        migration = self._make_migration()
        mock_db = self._override_db(migration)
        # single-run fetch returns None
        mock_db.query.return_value.filter.return_value.first.return_value = None

        r = client.get(f"/api/v1/migrations/{migration.id}/convert-code/{uuid.uuid4()}")
        assert r.status_code == 404

    def test_get_run_invalid_uuid(self, client):
        migration = self._make_migration()
        self._override_db(migration)

        r = client.get(f"/api/v1/migrations/{migration.id}/convert-code/not-a-uuid")
        assert r.status_code == 400

    def test_get_run_returns_full_results(self, client):
        migration = self._make_migration()
        mock_db = self._override_db(migration)
        run = self._fake_run(migration.id, n_results=3)
        mock_db.query.return_value.filter.return_value.first.return_value = run

        r = client.get(f"/api/v1/migrations/{migration.id}/convert-code/{run.id}")
        assert r.status_code == 200
        data = r.json()
        assert data["objects_found"] == 3
        assert len(data["results"]) == 3
        assert data["results"][2]["object_name"] == "PROC_2"
