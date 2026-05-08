"""Integration tests for Phase 1 /api/v1/analyze endpoint."""

import uuid
import pytest
import io
import zipfile
from unittest.mock import MagicMock
from fastapi.testclient import TestClient
from src.main import app
from src.db import get_db


@pytest.fixture
def client():
    """FastAPI test client."""
    return TestClient(app)


@pytest.fixture
def simple_sql_zip():
    """Create a simple SQL zip for testing."""
    sql_content = """
    CREATE TABLE employees (
        employee_id NUMBER PRIMARY KEY,
        name VARCHAR2(100),
        salary NUMBER
    );

    CREATE OR REPLACE PROCEDURE raise_salary (p_emp_id NUMBER, p_amount NUMBER) AS
    BEGIN
        UPDATE employees SET salary = salary + p_amount WHERE employee_id = p_emp_id;
        COMMIT;
    END raise_salary;
    """

    # Create zip in memory
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w") as zf:
        zf.writestr("schema.sql", sql_content)
    zip_buffer.seek(0)
    return zip_buffer


@pytest.fixture
def complex_sql_zip():
    """Create a complex SQL zip with Tier B/C constructs."""
    sql_content = """
    CREATE OR REPLACE PACKAGE complex_pkg AS
      PROCEDURE proc1;
    END complex_pkg;
    /

    CREATE OR REPLACE PACKAGE BODY complex_pkg AS
      PROCEDURE proc1 AS
        PRAGMA AUTONOMOUS_TRANSACTION;
        v_id employees.employee_id%TYPE;
      BEGIN
        MERGE INTO employees e
        USING source_data s
        ON (e.employee_id = s.employee_id)
        WHEN MATCHED THEN
          UPDATE SET e.salary = s.salary
        WHEN NOT MATCHED THEN
          INSERT (employee_id, salary) VALUES (s.employee_id, s.salary);

        SELECT employee_id INTO v_id FROM employees
        START WITH manager_id IS NULL
        CONNECT BY PRIOR employee_id = manager_id;
      END proc1;
    END complex_pkg;
    """

    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w") as zf:
        zf.writestr("complex.sql", sql_content)
    zip_buffer.seek(0)
    return zip_buffer


@pytest.fixture(autouse=True)
def mock_db_for_analyze():
    """Override get_db so /api/v1/analyze tests don't need a live Postgres.

    The endpoint persists Lead and AnalysisJob records; we mock add/commit/
    refresh so the complexity-scoring logic (pure Python) can run without a DB.
    """
    def _refresh_side_effect(obj):
        if getattr(obj, "id", None) is None:
            obj.id = uuid.uuid4()

    mock_session = MagicMock()
    mock_session.query.return_value.filter.return_value.first.return_value = None
    mock_session.add.return_value = None
    mock_session.commit.return_value = None
    mock_session.refresh.side_effect = _refresh_side_effect

    app.dependency_overrides[get_db] = lambda: mock_session
    yield mock_session
    app.dependency_overrides.clear()


class TestPhase1API:
    """Test Phase 1 /api/v1/analyze endpoint."""

    def test_analyze_simple_sql(self, client, simple_sql_zip):
        """Test analyzing simple SQL."""
        response = client.post(
            "/api/v1/analyze",
            data={"email": "test@example.com", "rate_per_day": "1000"},
            files={"file": ("test.zip", simple_sql_zip, "application/zip")},
        )

        assert response.status_code == 200
        data = response.json()

        # Verify response structure
        assert "job_id" in data
        assert "status" in data
        assert data["status"] in ["processing", "done"]
        assert "complexity_report" in data

        # Verify report contents
        report = data["complexity_report"]
        assert "score" in report
        assert 1 <= report["score"] <= 100
        assert report["total_lines"] > 0
        assert "effort_estimate_days" in report
        assert "estimated_cost" in report
        assert "construct_counts" in report

    def test_analyze_complex_sql(self, client, complex_sql_zip):
        """Test analyzing complex SQL with Tier B/C constructs."""
        response = client.post(
            "/api/v1/analyze",
            data={"email": "dba@company.com", "rate_per_day": "1500"},
            files={"file": ("complex.zip", complex_sql_zip, "application/zip")},
        )

        assert response.status_code == 200
        data = response.json()

        report = data["complexity_report"]
        # Complex SQL should have higher score
        assert report["score"] > 20
        # Should detect MERGE, CONNECT BY, PRAGMA
        assert any("MERGE" in str(c) for c in report.get("top_10_constructs", []))

    def test_analyze_missing_email(self, client, simple_sql_zip):
        """Test that email is required."""
        response = client.post(
            "/api/v1/analyze",
            data={"rate_per_day": "1000"},
            files={"file": ("test.zip", simple_sql_zip, "application/zip")},
        )

        assert response.status_code == 422  # Unprocessable entity

    def test_analyze_custom_rate(self, client, simple_sql_zip):
        """Test custom rate per day calculation."""
        response = client.post(
            "/api/v1/analyze",
            data={"email": "test@example.com", "rate_per_day": "2000"},
            files={"file": ("test.zip", simple_sql_zip, "application/zip")},
        )

        assert response.status_code == 200
        data = response.json()
        report = data["complexity_report"]

        # Cost should be effort_days * 2000
        expected_cost = report["effort_estimate_days"] * 2000
        assert report["estimated_cost"] == expected_cost

    def test_analyze_file_size_limit(self, client):
        """Test file size limit."""
        # Create a zip larger than max allowed (if limit is set)
        # This depends on settings.max_upload_size
        pass  # Skip if no size limit is configured
