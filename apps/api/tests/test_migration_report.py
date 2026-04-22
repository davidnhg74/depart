"""
Tests for migration progress reporting.
"""
import pytest
from src.models import MigrationReport


class TestMigrationReport:
    """Test MigrationReport model."""

    def test_migration_report_structure(self):
        """Test that MigrationReport has required fields."""
        report = MigrationReport(
            migration_id="123e4567-e89b-12d3-a456-426614174000",
            total_objects=10,
            converted_count=7,
            tests_generated=7,
            conversion_percentage=70.0,
            risk_breakdown={"high": 1, "medium": 2, "low": 7},
            blockers=[],
            generated_at="2026-04-21T12:00:00Z"
        )

        assert report.migration_id == "123e4567-e89b-12d3-a456-426614174000"
        assert report.total_objects == 10
        assert report.converted_count == 7
        assert report.tests_generated == 7
        assert report.conversion_percentage == 70.0
        assert isinstance(report.risk_breakdown, dict)
        assert isinstance(report.blockers, list)

    def test_conversion_percentage_new_migration(self):
        """Test conversion percentage for new migration."""
        report = MigrationReport(
            migration_id="test-id",
            total_objects=5,
            converted_count=0,
            tests_generated=0,
            conversion_percentage=0.0,
            risk_breakdown={},
            blockers=[],
            generated_at="2026-04-21T12:00:00Z"
        )

        assert report.conversion_percentage == 0.0

    def test_conversion_percentage_complete(self):
        """Test conversion percentage for complete migration."""
        report = MigrationReport(
            migration_id="test-id",
            total_objects=10,
            converted_count=10,
            tests_generated=10,
            conversion_percentage=100.0,
            risk_breakdown={"high": 0, "medium": 0, "low": 10},
            blockers=[],
            generated_at="2026-04-21T12:00:00Z"
        )

        assert report.conversion_percentage == 100.0

    def test_migration_report_with_blockers(self):
        """Test MigrationReport with blockers."""
        blockers = [
            {"name": "calculate_bonus", "reason": "Uses DBMS_SCHEDULER"},
            {"name": "process_audit", "reason": "Uses PRAGMA AUTONOMOUS_TRANSACTION"}
        ]
        report = MigrationReport(
            migration_id="test-id",
            total_objects=5,
            converted_count=3,
            tests_generated=3,
            conversion_percentage=60.0,
            risk_breakdown={"high": 2, "medium": 0, "low": 3},
            blockers=blockers,
            generated_at="2026-04-21T12:00:00Z"
        )

        assert len(report.blockers) == 2
        assert report.blockers[0]["name"] == "calculate_bonus"
        assert "DBMS_SCHEDULER" in report.blockers[0]["reason"]
