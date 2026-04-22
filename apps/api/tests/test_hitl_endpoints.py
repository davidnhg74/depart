"""
Tests for Phase 3.3 HITL Endpoints.
Tests workflow, permission analysis, and benchmark endpoints.
"""

import pytest
import json
import uuid
from fastapi.testclient import TestClient
from sqlalchemy.orm import Session
from unittest.mock import Mock

from src.main import app


@pytest.fixture
def client():
    """FastAPI test client."""
    return TestClient(app)


@pytest.fixture
def db_session():
    """Mock database session."""
    session = Mock(spec=Session)
    return session


class TestWorkflowEndpoints:
    """Test workflow HITL endpoints."""

    def test_create_workflow(self, client):
        """Test creating a new workflow."""
        response = client.post("/api/v3/workflow/create", json={"name": "Test Migration Workflow"})

        assert response.status_code == 200
        data = response.json()
        assert data["name"] == "Test Migration Workflow"
        assert data["current_step"] == 1
        assert data["status"] == "running"

    def test_create_workflow_with_migration_id(self, client):
        """Test creating workflow with migration reference."""
        migration_id = str(uuid.uuid4())
        response = client.post(
            "/api/v3/workflow/create", json={"name": "Test", "migration_id": migration_id}
        )

        assert response.status_code == 200
        data = response.json()
        assert data["migration_id"] == migration_id

    def test_get_workflow(self, client):
        """Test retrieving workflow details."""
        # First create a workflow
        create_resp = client.post("/api/v3/workflow/create", json={"name": "Test"})
        workflow_id = create_resp.json()["id"]

        # Then retrieve it
        response = client.get(f"/api/v3/workflow/{workflow_id}")

        assert response.status_code == 200
        data = response.json()
        assert data["id"] == workflow_id
        assert data["name"] == "Test"

    def test_get_workflow_not_found(self, client):
        """Test retrieving non-existent workflow."""
        fake_id = str(uuid.uuid4())
        response = client.get(f"/api/v3/workflow/{fake_id}")

        assert response.status_code == 404

    def test_approve_workflow_step(self, client):
        """Test approving a workflow step."""
        # Create workflow
        create_resp = client.post("/api/v3/workflow/create", json={"name": "Test"})
        workflow_id = create_resp.json()["id"]

        # Approve step 1
        response = client.post(
            f"/api/v3/workflow/{workflow_id}/approve/1",
            json={"approved_by": "John DBA", "notes": "Looks good"},
        )

        assert response.status_code == 200
        data = response.json()
        assert data["current_step"] == 2
        assert "1" in data["approvals"]
        assert data["approvals"]["1"]["approved_by"] == "John DBA"

    def test_reject_workflow_step(self, client):
        """Test rejecting a workflow step."""
        # Create workflow
        create_resp = client.post("/api/v3/workflow/create", json={"name": "Test"})
        workflow_id = create_resp.json()["id"]

        # Reject step 1
        response = client.post(
            f"/api/v3/workflow/{workflow_id}/reject/1",
            json={"reason": "Needs more review", "notes": "Check schema first"},
        )

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "blocked"
        assert "step_1_rejection" in data["dba_notes"]

    def test_update_workflow_settings(self, client):
        """Test updating workflow settings."""
        # Create workflow
        create_resp = client.post("/api/v3/workflow/create", json={"name": "Test"})
        workflow_id = create_resp.json()["id"]

        # Update settings
        response = client.post(
            f"/api/v3/workflow/{workflow_id}/settings",
            json={
                "settings": {"parallel_tables": True, "batch_size": 5000, "timeout_seconds": 3600}
            },
        )

        assert response.status_code == 200
        data = response.json()
        assert data["settings"]["parallel_tables"] is True
        assert data["settings"]["batch_size"] == 5000

    def test_get_workflow_progress(self, client):
        """Test getting workflow progress."""
        # Create workflow
        create_resp = client.post("/api/v3/workflow/create", json={"name": "Test"})
        workflow_id = create_resp.json()["id"]

        # Get progress
        response = client.get(f"/api/v3/workflow/{workflow_id}/progress")

        assert response.status_code == 200
        data = response.json()
        assert data["current_step"] == 1
        assert data["total_steps"] == 20
        assert 0 <= data["progress_percentage"] <= 100

    def test_workflow_progression(self, client):
        """Test workflow advancing through multiple steps."""
        # Create workflow
        create_resp = client.post("/api/v3/workflow/create", json={"name": "Test Progression"})
        workflow_id = create_resp.json()["id"]

        # Progress through steps
        for step in range(1, 6):
            response = client.post(
                f"/api/v3/workflow/{workflow_id}/approve/{step}", json={"approved_by": "DBA"}
            )
            assert response.status_code == 200
            assert response.json()["current_step"] == step + 1


class TestPermissionEndpoints:
    """Test permission analysis endpoints."""

    def test_analyze_permissions_from_json(self, client):
        """Test permission analysis with JSON input."""
        oracle_privs = {
            "system_privs": [
                {"grantee": "SCOTT", "privilege": "CREATE TABLE", "admin_option": "YES"}
            ],
            "object_privs": [
                {
                    "grantee": "SCOTT",
                    "owner": "SYS",
                    "table_name": "V$SQL",
                    "privilege": "SELECT",
                    "grantable": "NO",
                }
            ],
            "role_grants": [],
            "dba_users": [],
            "extracted_as_dba": True,
        }

        response = client.post(
            "/api/v3/analyze/permissions", json={"oracle_privileges_json": json.dumps(oracle_privs)}
        )

        assert response.status_code == 200
        data = response.json()
        assert "mappings" in data
        assert "unmappable" in data
        assert "grant_sql" in data
        assert "overall_risk" in data

    def test_analyze_permissions_missing_input(self, client):
        """Test permission analysis with missing input."""
        response = client.post("/api/v3/analyze/permissions", json={})

        assert response.status_code == 400

    def test_analyze_permissions_via_connection(self, client):
        """Test permission analysis via connection ID (should be 501 Not Implemented for now)."""
        response = client.post(
            "/api/v3/analyze/permissions", json={"oracle_connection_id": "conn-123"}
        )

        assert response.status_code == 501


class TestBenchmarkEndpoints:
    """Test benchmark analysis endpoints."""

    def test_capture_oracle_benchmark(self, client):
        """Test Oracle benchmark capture (should be 501 for now)."""
        response = client.post(
            "/api/v3/benchmark/capture-oracle", json={"oracle_connection_id": "oracle-1"}
        )

        # Not implemented yet
        assert response.status_code == 501

    def test_capture_postgres_benchmark(self, client):
        """Test PostgreSQL benchmark capture (should be 501 for now)."""
        response = client.post(
            "/api/v3/benchmark/capture-postgres", json={"postgres_connection_id": "pg-1"}
        )

        # Not implemented yet
        assert response.status_code == 501

    def test_compare_benchmarks_not_found(self, client):
        """Test benchmark comparison with missing data."""
        migration_id = str(uuid.uuid4())
        response = client.get(f"/api/v3/benchmark/compare/{migration_id}")

        assert response.status_code == 404

    def test_invalid_workflow_id_format(self, client):
        """Test with invalid UUID format."""
        response = client.get("/api/v3/workflow/not-a-uuid")

        assert response.status_code == 400


class TestHealthCheck:
    """Test health endpoint."""

    def test_health_check(self, client):
        """Test API health check."""
        response = client.get("/health")

        assert response.status_code == 200
        assert response.json()["status"] == "ok"


class TestEndpointIntegration:
    """Integration tests for HITL workflow."""

    def test_full_workflow_scenario(self, client):
        """Test a complete workflow scenario."""
        # 1. Create workflow
        workflow_resp = client.post("/api/v3/workflow/create", json={"name": "Full Migration Test"})
        assert workflow_resp.status_code == 200
        workflow_id = workflow_resp.json()["id"]

        # 2. Update settings
        settings_resp = client.post(
            f"/api/v3/workflow/{workflow_id}/settings", json={"settings": {"dry_run": True}}
        )
        assert settings_resp.status_code == 200

        # 3. Get progress
        progress_resp = client.get(f"/api/v3/workflow/{workflow_id}/progress")
        assert progress_resp.status_code == 200
        assert progress_resp.json()["progress_percentage"] == 5  # 1/20

        # 4. Approve first step
        approve_resp = client.post(
            f"/api/v3/workflow/{workflow_id}/approve/1", json={"approved_by": "DBA1"}
        )
        assert approve_resp.status_code == 200
        assert approve_resp.json()["current_step"] == 2

        # 5. Check updated progress
        progress_resp2 = client.get(f"/api/v3/workflow/{workflow_id}/progress")
        assert progress_resp2.json()["current_step"] == 2
