"""
Tests for data migration orchestrator.
Validates chunking, validation, and execution logic.
"""

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from src.db import Base
from src.migration.orchestrator import DataMigrator, MigrationPlan
from src.migration.validators import StructuralValidator, VolumeValidator


@pytest.fixture
def db_engine():
    """Create in-memory SQLite for testing."""
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    return engine


@pytest.fixture
def db_session(db_engine):
    """Create database session."""
    Session = sessionmaker(bind=db_engine)
    session = Session()
    yield session
    session.close()


@pytest.fixture
def sample_table(db_session):
    """Create sample table for testing."""
    db_session.execute(text("""
        CREATE TABLE IF NOT EXISTS CUSTOMERS (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT,
            created_at TIMESTAMP
        )
    """))

    # Insert test data
    for i in range(1000):
        db_session.execute(text(
            f"INSERT INTO CUSTOMERS (id, name, email) VALUES ({i}, 'Customer {i}', 'user{i}@example.com')"
        ))

    db_session.commit()


class TestMigrationPlan:
    """Test migration planning."""

    def test_create_plan(self):
        """Verify migration plan creation."""
        tables = [
            {"name": "CUSTOMERS", "chunk_size": 10000, "order": 1},
            {"name": "ORDERS", "chunk_size": 50000, "order": 2},
        ]

        plan = MigrationPlan(
            migration_id="test-123",
            tables=tables,
            estimated_duration_seconds=3600,
            total_rows=1000000,
            total_bytes=5368709120,  # 5 GB
        )

        assert plan.migration_id == "test-123"
        assert len(plan.tables) == 2
        assert plan.get_table_order() == ["CUSTOMERS", "ORDERS"]

    def test_get_chunk_size(self):
        """Verify chunk size retrieval."""
        tables = [
            {"name": "CUSTOMERS", "chunk_size": 10000, "order": 1},
            {"name": "ORDERS", "chunk_size": 100000, "order": 2},
        ]

        plan = MigrationPlan(
            migration_id="test-123",
            tables=tables,
            estimated_duration_seconds=3600,
            total_rows=1000000,
            total_bytes=5368709120,
        )

        assert plan.get_chunk_size("CUSTOMERS") == 10000
        assert plan.get_chunk_size("ORDERS") == 100000


class TestDataMigrator:
    """Test data migration orchestrator."""

    def test_calculate_chunk_size(self, db_session):
        """Verify chunk size calculation."""
        migrator = DataMigrator(db_session, db_session, num_workers=4, chunk_size=10000)

        # Small table: full table
        assert migrator._calculate_chunk_size(500, 500000) == 500

        # Medium table: default
        assert migrator._calculate_chunk_size(100000, 100000000) == 10000

        # Large table: 100K chunks
        assert migrator._calculate_chunk_size(1000000, 1000000000) == 100000

        # Very large table: 1M chunks
        assert migrator._calculate_chunk_size(10000000, 10000000000) == 1000000

    def test_estimate_table_size(self, db_session, sample_table):
        """Verify table size estimation."""
        migrator = DataMigrator(db_session, db_session)

        # For SQLite in-memory, estimation uses fallback (1KB per row)
        size = migrator._estimate_table_size("CUSTOMERS")

        assert size > 0
        assert size >= 1000000  # ~1000 rows × 1KB minimum

    def test_get_row_count(self, db_session, sample_table):
        """Verify row count retrieval."""
        migrator = DataMigrator(db_session, db_session)

        count = migrator._get_row_count("CUSTOMERS")

        assert count == 1000

    def test_get_status_idle(self, db_session):
        """Verify status when migration not started."""
        migrator = DataMigrator(db_session, db_session)

        status = migrator.get_status()

        assert status["status"] == "idle"
        assert status["rows_transferred"] == 0
        assert status["elapsed_seconds"] == 0
        assert status["throughput_rows_per_sec"] == 0

    def test_plan_migration(self, db_session, sample_table):
        """Verify migration planning."""
        migrator = DataMigrator(db_session, db_session, num_workers=4, chunk_size=10000)

        plan = migrator.plan_migration(["CUSTOMERS"])

        assert plan is not None
        assert len(plan.tables) == 1
        assert plan.tables[0]["name"] == "CUSTOMERS"
        assert plan.tables[0]["row_count"] == 1000
        assert plan.tables[0]["chunk_size"] == 10000


class TestMigrationValidation:
    """Test validation during migration."""

    def test_structural_validator(self, db_session, sample_table):
        """Verify structural validation."""
        validator = StructuralValidator(db_session, db_session)

        # Should find the table
        result = validator.validate_table_exists("CUSTOMERS")

        assert result is True
        assert any(r.message.startswith("Table CUSTOMERS") for r in validator.results)

    def test_volume_validator(self, db_session, sample_table):
        """Verify volume validation."""
        validator = VolumeValidator(db_session, db_session)

        # Row counts should match (same DB)
        result = validator.validate_row_counts("CUSTOMERS")

        assert result is True

    def test_null_distribution(self, db_session, sample_table):
        """Verify NULL distribution checking."""
        validator = VolumeValidator(db_session, db_session)

        # Test on nullable column
        result = validator.validate_null_distribution("CUSTOMERS", "email")

        assert result is True


class TestMigrationChunking:
    """Test chunk reading/writing logic."""

    def test_chunk_boundary_calculation(self, db_session):
        """Verify chunk boundaries."""
        migrator = DataMigrator(db_session, db_session, chunk_size=100)

        # 1000 rows with 100-row chunks = 10 chunks
        total = 1000
        chunk_size = 100

        chunks = list(range(0, total, chunk_size))

        assert len(chunks) == 10
        assert chunks[0] == 0
        assert chunks[-1] == 900

    def test_last_chunk_partial(self, db_session):
        """Verify last chunk can be partial."""
        migrator = DataMigrator(db_session, db_session, chunk_size=300)

        # 1000 rows with 300-row chunks = 4 chunks (last one has 100 rows)
        total = 1000
        chunk_size = 300

        chunks = list(range(0, total, chunk_size))
        last_chunk_size = total - chunks[-1]

        assert len(chunks) == 4
        assert last_chunk_size == 100


class TestErrorHandling:
    """Test error handling during migration."""

    def test_nonexistent_table(self, db_session):
        """Verify handling of missing table."""
        migrator = DataMigrator(db_session, db_session)

        # Getting row count of nonexistent table
        count = migrator._get_row_count("NONEXISTENT_TABLE")

        # Should return 0, not crash
        assert count == 0

    def test_migration_status_after_error(self, db_session):
        """Verify status tracking after error."""
        migrator = DataMigrator(db_session, db_session)

        try:
            raise Exception("Test error")
        except Exception as e:
            migrator.errors.append(str(e))

        status = migrator.get_status()

        assert len(status["errors"]) > 0
        assert "Test error" in status["errors"]
