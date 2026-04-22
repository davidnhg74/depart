"""
Tests for checkpoint recovery system (Phase 3.4).
Tests migration checkpoints, resumption, and error recovery.
"""

import pytest
import uuid
from datetime import datetime
from unittest.mock import Mock, MagicMock
from sqlalchemy.orm import Session

from src.migration.checkpoint import MigrationCheckpoint, CheckpointManager
from src.models import MigrationCheckpointRecord, MigrationRecord


class TestMigrationCheckpoint:
    """Test MigrationCheckpoint dataclass."""

    def test_checkpoint_creation(self):
        """Test creating a checkpoint."""
        checkpoint = MigrationCheckpoint(
            migration_id="migration-1",
            table_name="employees",
            rows_processed=1000,
            total_rows=10000,
            last_rowid="emp-999",
            status="in_progress",
        )

        assert checkpoint.table_name == "employees"
        assert checkpoint.rows_processed == 1000
        assert checkpoint.progress_percentage == 10.0
        assert checkpoint.is_complete is False

    def test_checkpoint_progress_percentage(self):
        """Test progress percentage calculation."""
        checkpoint = MigrationCheckpoint(
            migration_id="migration-1",
            table_name="employees",
            rows_processed=5000,
            total_rows=10000,
        )

        assert checkpoint.progress_percentage == 50.0

    def test_checkpoint_complete(self):
        """Test completed checkpoint."""
        checkpoint = MigrationCheckpoint(
            migration_id="migration-1",
            table_name="employees",
            rows_processed=10000,
            total_rows=10000,
            status="completed",
        )

        assert checkpoint.is_complete is True


class TestCheckpointManager:
    """Test checkpoint management."""

    @pytest.fixture
    def mock_db(self):
        """Mock database session."""
        return Mock(spec=Session)

    @pytest.fixture
    def manager(self, mock_db):
        """Checkpoint manager instance."""
        return CheckpointManager(mock_db)

    def test_create_migration(self, manager, mock_db):
        """Test creating a migration record."""
        migration_id = str(uuid.uuid4())
        mock_db.add = Mock()
        mock_db.commit = Mock()

        result = manager.create_migration(migration_id, "public")

        assert result == migration_id
        assert mock_db.add.called
        assert mock_db.commit.called

    def test_create_checkpoint(self, manager, mock_db):
        """Test creating a checkpoint."""
        migration_id = str(uuid.uuid4())
        mock_db.add = Mock()
        mock_db.commit = Mock()

        manager.create_checkpoint(
            migration_id=migration_id,
            table_name="employees",
            rows_processed=100,
            total_rows=1000,
            last_rowid="emp-99",
            status="in_progress",
        )

        assert mock_db.add.called
        assert mock_db.commit.called

    def test_get_latest_checkpoint(self, manager, mock_db):
        """Test retrieving latest checkpoint."""
        migration_id = str(uuid.uuid4())

        mock_checkpoint = Mock()
        mock_checkpoint.table_name = "employees"
        mock_checkpoint.rows_processed = 500
        mock_checkpoint.total_rows = 1000
        mock_checkpoint.status = "in_progress"

        # Mock the query chain
        mock_query = Mock()
        mock_db.query.return_value = mock_query
        mock_query.filter.return_value = mock_query
        mock_query.order_by.return_value = mock_query
        mock_query.first.return_value = mock_checkpoint

        result = manager.get_latest_checkpoint(migration_id, "employees")

        assert result is not None
        assert result.table_name == "employees"

    def test_resume_from_checkpoint_not_started(self, manager, mock_db):
        """Test resuming from non-existent checkpoint (start fresh)."""
        migration_id = str(uuid.uuid4())

        mock_query = Mock()
        mock_db.query.return_value = mock_query
        mock_query.filter.return_value = mock_query
        mock_query.order_by.return_value = mock_query
        mock_query.first.return_value = None

        result = manager.resume_from_checkpoint(migration_id, "employees")

        assert result is None

    def test_resume_from_checkpoint_in_progress(self, manager, mock_db):
        """Test resuming from in-progress checkpoint."""
        migration_id = str(uuid.uuid4())

        mock_checkpoint = Mock()
        mock_checkpoint.rows_processed = 500
        mock_checkpoint.last_rowid = "emp-499"
        mock_checkpoint.status = "in_progress"

        mock_query = Mock()
        mock_db.query.return_value = mock_query
        mock_query.filter.return_value = mock_query
        mock_query.order_by.return_value = mock_query
        mock_query.first.return_value = mock_checkpoint

        result = manager.resume_from_checkpoint(migration_id, "employees")

        assert result is not None
        assert result["rows_processed"] == 500
        assert result["last_rowid"] == "emp-499"

    def test_resume_from_completed_checkpoint(self, manager, mock_db):
        """Test resuming from completed checkpoint (skip table)."""
        migration_id = str(uuid.uuid4())

        mock_checkpoint = Mock()
        mock_checkpoint.rows_processed = 1000
        mock_checkpoint.status = "completed"

        mock_query = Mock()
        mock_db.query.return_value = mock_query
        mock_query.filter.return_value = mock_query
        mock_query.order_by.return_value = mock_query
        mock_query.first.return_value = mock_checkpoint

        result = manager.resume_from_checkpoint(migration_id, "employees")

        assert result is None

    def test_mark_table_complete(self, manager, mock_db):
        """Test marking table as complete."""
        migration_id = str(uuid.uuid4())
        mock_db.add = Mock()
        mock_db.commit = Mock()

        manager.mark_table_complete(migration_id, "employees")

        assert mock_db.add.called
        assert mock_db.commit.called

    def test_mark_migration_complete(self, manager, mock_db):
        """Test marking entire migration as complete."""
        migration_id = str(uuid.uuid4())

        mock_migration = Mock()
        mock_query = Mock()
        mock_db.query.return_value = mock_query
        mock_query.filter.return_value = mock_query
        mock_query.first.return_value = mock_migration

        manager.mark_migration_complete(migration_id)

        assert mock_migration.status == "completed"
        assert mock_migration.completed_at is not None
        assert mock_db.commit.called

    def test_get_migration_progress(self, manager, mock_db):
        """Test getting migration progress."""
        migration_id = str(uuid.uuid4())

        mock_checkpoints = [
            Mock(
                table_name="employees",
                rows_processed=500,
                total_rows=1000,
                progress_percentage=50.0,
                status="in_progress",
            ),
            Mock(
                table_name="departments",
                rows_processed=100,
                total_rows=100,
                progress_percentage=100.0,
                status="completed",
            ),
        ]

        mock_query = Mock()
        mock_db.query.return_value = mock_query
        mock_query.filter.return_value = mock_query
        mock_query.all.return_value = mock_checkpoints

        result = manager.get_migration_progress(migration_id)

        assert result["migration_id"] == migration_id
        assert result["tables_completed"] == 1
        assert result["total_tables"] == 2

    def test_get_failed_tables(self, manager, mock_db):
        """Test getting failed tables for retry."""
        migration_id = str(uuid.uuid4())

        mock_failed = [
            Mock(
                table_name="employees",
                error_message="Connection timeout",
                rows_processed=500,
                last_rowid="emp-499",
                status="failed",
            )
        ]

        mock_query = Mock()
        mock_db.query.return_value = mock_query
        mock_query.filter.return_value = mock_query
        mock_query.all.return_value = mock_failed

        result = manager.get_failed_tables(migration_id)

        assert len(result) == 1
        assert result[0]["table_name"] == "employees"
        assert "Connection timeout" in result[0]["error_message"]

    def test_retry_failed_tables(self, manager, mock_db):
        """Test retrying failed tables."""
        migration_id = str(uuid.uuid4())

        mock_failed_1 = Mock(status="failed", error_message="Error 1")
        mock_failed_2 = Mock(status="failed", error_message="Error 2")

        mock_query = Mock()
        mock_db.query.return_value = mock_query
        mock_query.filter.return_value = mock_query
        mock_query.all.return_value = [mock_failed_1, mock_failed_2]

        count = manager.retry_failed_tables(migration_id)

        assert count == 2
        assert mock_failed_1.status == "in_progress"
        assert mock_failed_1.error_message is None
        assert mock_failed_2.status == "in_progress"
        assert mock_db.commit.called

    def test_mark_table_failed(self, manager, mock_db):
        """Test marking table as failed."""
        migration_id = str(uuid.uuid4())

        mock_checkpoint = Mock(status="in_progress")
        mock_query = Mock()
        mock_db.query.return_value = mock_query
        mock_query.filter.return_value = mock_query
        mock_query.order_by.return_value = mock_query
        mock_query.first.return_value = mock_checkpoint

        manager.mark_table_failed(migration_id, "employees", "Database error")

        assert mock_checkpoint.status == "failed"
        assert mock_checkpoint.error_message == "Database error"
        assert mock_db.commit.called


class TestRecoveryScenarios:
    """Integration tests for recovery scenarios."""

    def test_resume_after_interruption(self):
        """Test resuming migration after interruption."""
        migration_id = str(uuid.uuid4())

        # Scenario: Migration interrupted at 50% for employees table
        # Should be able to resume from last_rowid
        checkpoint = MigrationCheckpoint(
            migration_id=migration_id,
            table_name="employees",
            rows_processed=5000,
            total_rows=10000,
            last_rowid="emp-4999",
            status="in_progress",
        )

        assert checkpoint.progress_percentage == 50.0
        assert checkpoint.last_rowid == "emp-4999"

    def test_partial_failure_recovery(self):
        """Test partial failure and selective retry."""
        migration_id = str(uuid.uuid4())

        # Scenario: 2 tables completed, 1 failed
        checkpoints = [
            MigrationCheckpoint(
                migration_id=migration_id,
                table_name="employees",
                rows_processed=1000,
                total_rows=1000,
                status="completed",
            ),
            MigrationCheckpoint(
                migration_id=migration_id,
                table_name="departments",
                rows_processed=100,
                total_rows=100,
                status="completed",
            ),
            MigrationCheckpoint(
                migration_id=migration_id,
                table_name="projects",
                rows_processed=500,
                total_rows=2000,
                last_rowid="proj-499",
                status="failed",
                error_message="Foreign key violation",
            ),
        ]

        # Only the failed table needs retry
        completed = sum(1 for c in checkpoints if c.status == "completed")
        failed = sum(1 for c in checkpoints if c.status == "failed")

        assert completed == 2
        assert failed == 1
