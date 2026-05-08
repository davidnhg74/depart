"""
Tests for migration checkpoint manager.
Validates save/resume logic and progress tracking.

Sqlite can't host this schema (ARRAY(Float), UUID/JSONB columns).
Requires a running Postgres: make up && alembic upgrade head.
Run with: pytest -m integration
"""

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import uuid

from src.db import Base
from src.models import MigrationRecord, MigrationCheckpointRecord
from src.migration.checkpoint import CheckpointManager

pytestmark = pytest.mark.integration


@pytest.fixture
def db():
    """Postgres-backed test session.

    Sqlite can't host this schema — `ConversionCaseRecord.embedding` is
    `ARRAY(Float)` and several models use UUID/JSONB columns that the
    sqlite dialect doesn't compile. We rely on the migrated dev DB
    (`make up && alembic upgrade head`) and tear down what each test
    inserts via a transactional rollback.
    """
    from src.config import settings
    from src.models import (
        MigrationRecord,
        MigrationCheckpointRecord,
    )

    engine = create_engine(settings.database_url)
    Session = sessionmaker(bind=engine)
    session = Session()
    try:
        yield session
    finally:
        # Tests insert by uuid; clean up rows they created. Using a
        # transactional savepoint would be cleaner, but the manager-
        # under-test commits inside its own session calls so the outer
        # rollback wouldn't help.
        session.rollback()
        session.query(MigrationCheckpointRecord).delete()
        session.query(MigrationRecord).delete()
        session.commit()
        session.close()


class TestCheckpointManager:
    """Test checkpoint save/resume functionality."""

    def test_create_migration(self, db):
        """Verify migration record creation."""
        manager = CheckpointManager(db)
        migration_id = manager.create_migration("test-schema")

        assert migration_id is not None
        migration = (
            db.query(MigrationRecord).filter(MigrationRecord.id == uuid.UUID(migration_id)).first()
        )

        assert migration.schema_name == "test-schema"
        assert migration.status == "pending"

    def test_create_checkpoint(self, db):
        """Verify checkpoint creation."""
        manager = CheckpointManager(db)
        migration_id = manager.create_migration("test-schema")

        manager.create_checkpoint(
            migration_id=migration_id,
            table_name="CUSTOMERS",
            rows_processed=1000,
            total_rows=10000,
            status="in_progress",
        )

        checkpoint = (
            db.query(MigrationCheckpointRecord)
            .filter(
                MigrationCheckpointRecord.migration_id == uuid.UUID(migration_id),
                MigrationCheckpointRecord.table_name == "CUSTOMERS",
            )
            .first()
        )

        assert checkpoint is not None
        assert checkpoint.rows_processed == 1000
        assert checkpoint.progress_percentage == 10.0

    def test_resume_from_checkpoint(self, db):
        """Verify resumption point retrieval."""
        manager = CheckpointManager(db)
        migration_id = manager.create_migration("test-schema")

        # Create checkpoint at 30%
        manager.create_checkpoint(
            migration_id=migration_id,
            table_name="ORDERS",
            rows_processed=3000,
            total_rows=10000,
            last_rowid="12345",
            status="in_progress",
        )

        # Get resumption point
        resume = manager.resume_from_checkpoint(migration_id, "ORDERS")

        assert resume is not None
        assert resume["rows_processed"] == 3000
        assert resume["last_rowid"] == "12345"

    def test_resume_completed_table(self, db):
        """Completed tables should not resume."""
        manager = CheckpointManager(db)
        migration_id = manager.create_migration("test-schema")

        # Mark table complete
        manager.mark_table_complete(migration_id, "CUSTOMERS")

        # Try to resume
        resume = manager.resume_from_checkpoint(migration_id, "CUSTOMERS")

        assert resume is None  # Completed tables don't resume

    def test_get_migration_progress(self, db):
        """Verify overall progress calculation."""
        manager = CheckpointManager(db)
        migration_id = manager.create_migration("test-schema")

        # Add checkpoints for multiple tables
        manager.create_checkpoint(migration_id, "CUSTOMERS", 5000, 10000, status="in_progress")
        manager.create_checkpoint(migration_id, "ORDERS", 2500, 5000, status="in_progress")

        progress = manager.get_migration_progress(migration_id)

        assert progress["total_tables"] == 2
        assert progress["total_rows_processed"] == 7500

    def test_mark_migration_complete(self, db):
        """Verify migration completion marking."""
        manager = CheckpointManager(db)
        migration_id = manager.create_migration("test-schema")

        manager.mark_migration_complete(migration_id)

        migration = (
            db.query(MigrationRecord).filter(MigrationRecord.id == uuid.UUID(migration_id)).first()
        )

        assert migration.status == "completed"
        assert migration.completed_at is not None

    def test_checkpoint_progress_percentage(self, db):
        """Verify progress percentage calculation."""
        manager = CheckpointManager(db)
        migration_id = manager.create_migration("test-schema")

        # Test various progress levels
        test_cases = [
            (0, 1000, 0.0),
            (500, 1000, 50.0),
            (1000, 1000, 100.0),
            (750, 1000, 75.0),
        ]

        for rows_processed, total, expected_pct in test_cases:
            manager.create_checkpoint(
                migration_id, f"TABLE_{rows_processed}", rows_processed, total
            )

        progress = manager.get_migration_progress(migration_id)
        tables = progress["tables"]

        for i, (rows, total, expected) in enumerate(test_cases):
            assert abs(tables[i]["progress_percentage"] - expected) < 0.01

    def test_concurrent_checkpoints(self, db):
        """Verify handling of concurrent table migrations."""
        manager = CheckpointManager(db)
        migration_id = manager.create_migration("test-schema")

        # Simulate concurrent table migrations
        tables = ["CUSTOMERS", "ORDERS", "PRODUCTS"]

        for table in tables:
            manager.create_checkpoint(migration_id, table, 1000 * tables.index(table), 5000)

        progress = manager.get_migration_progress(migration_id)

        assert len(progress["tables"]) == 3
        assert all(t["name"] in tables for t in progress["tables"])


class TestCheckpointResilience:
    """Test checkpoint handling under error conditions."""

    def test_checkpoint_after_error(self, db):
        """Verify checkpoint saved before failure."""
        manager = CheckpointManager(db)
        migration_id = manager.create_migration("test-schema")

        # Save checkpoint
        manager.create_checkpoint(migration_id, "TABLE_A", 1000, 2000, status="in_progress")

        # Simulate error
        try:
            raise Exception("Migration error")
        except Exception:
            pass

        # Checkpoint should still be retrievable
        resume = manager.resume_from_checkpoint(migration_id, "TABLE_A")
        assert resume is not None
        assert resume["rows_processed"] == 1000

    def test_latest_checkpoint_priority(self, db):
        """Newest checkpoint should take priority."""
        manager = CheckpointManager(db)
        migration_id = manager.create_migration("test-schema")

        # Create multiple checkpoints for same table
        manager.create_checkpoint(migration_id, "TABLE_A", 1000, 5000, status="in_progress")
        manager.create_checkpoint(migration_id, "TABLE_A", 2000, 5000, status="in_progress")
        manager.create_checkpoint(migration_id, "TABLE_A", 3000, 5000, status="in_progress")

        # Should get latest
        resume = manager.resume_from_checkpoint(migration_id, "TABLE_A")
        assert resume["rows_processed"] == 3000
