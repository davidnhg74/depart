"""
Checkpoint management for resumable migrations.
Saves state every N% to allow recovery from failures.
"""

from sqlalchemy import Column, String, Integer, DateTime, JSON, ForeignKey
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Session
from datetime import datetime
import uuid
import logging

logger = logging.getLogger(__name__)


class MigrationCheckpoint:
    """Model for storing migration checkpoints."""

    def __init__(
        self,
        migration_id: str,
        table_name: str,
        rows_processed: int,
        total_rows: int,
        last_rowid: str = None,
        status: str = "in_progress",
        error_message: str = None,
    ):
        self.migration_id = migration_id
        self.table_name = table_name
        self.rows_processed = rows_processed
        self.total_rows = total_rows
        self.last_rowid = last_rowid
        self.status = status  # in_progress, completed, failed
        self.error_message = error_message
        self.created_at = datetime.utcnow()
        self.updated_at = datetime.utcnow()

    @property
    def progress_percentage(self) -> float:
        """Current progress as percentage."""
        if self.total_rows == 0:
            return 0.0
        return (self.rows_processed / self.total_rows) * 100

    @property
    def is_complete(self) -> bool:
        """Whether table migration is complete."""
        return self.status == "completed"


class CheckpointManager:
    """Manages migration checkpoints for resumption."""

    def __init__(self, db: Session):
        self.db = db

    def create_migration(self, migration_id: str, schema_name: str) -> str:
        """Create a new migration record."""
        from ..models import MigrationRecord

        migration = MigrationRecord(
            id=uuid.UUID(migration_id) if isinstance(migration_id, str) else migration_id,
            schema_name=schema_name,
            status="pending",
            started_at=None,
            completed_at=None,
        )
        self.db.add(migration)
        self.db.commit()
        return str(migration.id)

    def create_checkpoint(
        self,
        migration_id: str,
        table_name: str,
        rows_processed: int,
        total_rows: int,
        last_rowid: str = None,
        status: str = "in_progress",
    ) -> None:
        """Save a checkpoint for a table."""
        from ..models import MigrationCheckpointRecord

        checkpoint = MigrationCheckpointRecord(
            migration_id=uuid.UUID(migration_id) if isinstance(migration_id, str) else migration_id,
            table_name=table_name,
            rows_processed=rows_processed,
            total_rows=total_rows,
            last_rowid=last_rowid,
            status=status,
            progress_percentage=
            (rows_processed / total_rows * 100) if total_rows > 0 else 0,
            created_at=datetime.utcnow(),
        )

        self.db.add(checkpoint)
        self.db.commit()
        logger.info(
            f"Checkpoint: {table_name} {rows_processed}/{total_rows} "
            f"({checkpoint.progress_percentage:.1f}%)"
        )

    def get_latest_checkpoint(
        self, migration_id: str, table_name: str
    ) -> "MigrationCheckpointRecord" or None:
        """Get the most recent checkpoint for a table."""
        from ..models import MigrationCheckpointRecord

        checkpoint = (
            self.db.query(MigrationCheckpointRecord)
            .filter(
                MigrationCheckpointRecord.migration_id
                == uuid.UUID(migration_id) if isinstance(migration_id, str) else migration_id,
                MigrationCheckpointRecord.table_name == table_name,
            )
            .order_by(MigrationCheckpointRecord.created_at.desc())
            .first()
        )

        return checkpoint

    def resume_from_checkpoint(
        self, migration_id: str, table_name: str
    ) -> dict or None:
        """Get resumption point for a table."""
        checkpoint = self.get_latest_checkpoint(migration_id, table_name)

        if not checkpoint or checkpoint.status == "completed":
            return None  # Start from beginning

        return {
            "rows_processed": checkpoint.rows_processed,
            "last_rowid": checkpoint.last_rowid,
            "status": checkpoint.status,
        }

    def mark_table_complete(self, migration_id: str, table_name: str) -> None:
        """Mark a table migration as complete."""
        self.create_checkpoint(
            migration_id=migration_id,
            table_name=table_name,
            rows_processed=0,
            total_rows=1,
            status="completed",
        )

    def mark_migration_complete(self, migration_id: str) -> None:
        """Mark entire migration as complete."""
        from ..models import MigrationRecord

        migration = self.db.query(MigrationRecord).filter(
            MigrationRecord.id == uuid.UUID(migration_id) if isinstance(migration_id, str) else migration_id
        ).first()

        if migration:
            migration.status = "completed"
            migration.completed_at = datetime.utcnow()
            self.db.commit()
            logger.info(f"Migration {migration_id} completed")

    def get_migration_progress(self, migration_id: str) -> dict:
        """Get overall migration progress."""
        from ..models import MigrationCheckpointRecord

        checkpoints = (
            self.db.query(MigrationCheckpointRecord)
            .filter(
                MigrationCheckpointRecord.migration_id
                == uuid.UUID(migration_id) if isinstance(migration_id, str) else migration_id
            )
            .all()
        )

        completed_tables = sum(
            1 for c in checkpoints if c.status == "completed"
        )
        total_tables = len(set(c.table_name for c in checkpoints))
        total_rows_processed = sum(c.rows_processed for c in checkpoints)

        return {
            "migration_id": migration_id,
            "tables_completed": completed_tables,
            "total_tables": total_tables,
            "total_rows_processed": total_rows_processed,
            "tables": [
                {
                    "name": c.table_name,
                    "rows_processed": c.rows_processed,
                    "total_rows": c.total_rows,
                    "progress_percentage": c.progress_percentage,
                    "status": c.status,
                }
                for c in checkpoints
            ],
        }

    def get_failed_tables(self, migration_id: str) -> list:
        """Get list of failed tables for retry."""
        from ..models import MigrationCheckpointRecord

        checkpoints = (
            self.db.query(MigrationCheckpointRecord)
            .filter(
                MigrationCheckpointRecord.migration_id
                == uuid.UUID(migration_id) if isinstance(migration_id, str) else migration_id,
                MigrationCheckpointRecord.status == "failed",
            )
            .all()
        )

        return [
            {
                "table_name": c.table_name,
                "error_message": c.error_message,
                "rows_processed": c.rows_processed,
                "last_rowid": c.last_rowid,
            }
            for c in checkpoints
        ]

    def retry_failed_tables(self, migration_id: str) -> int:
        """Reset failed tables for retry. Returns count of reset tables."""
        from ..models import MigrationCheckpointRecord

        failed_checkpoints = (
            self.db.query(MigrationCheckpointRecord)
            .filter(
                MigrationCheckpointRecord.migration_id
                == uuid.UUID(migration_id) if isinstance(migration_id, str) else migration_id,
                MigrationCheckpointRecord.status == "failed",
            )
            .all()
        )

        count = 0
        for checkpoint in failed_checkpoints:
            checkpoint.status = "in_progress"
            checkpoint.error_message = None
            checkpoint.updated_at = datetime.utcnow()
            count += 1

        self.db.commit()
        logger.info(f"Reset {count} failed tables for migration {migration_id}")
        return count

    def mark_table_failed(self, migration_id: str, table_name: str, error_message: str) -> None:
        """Mark a table as failed with error message."""
        from ..models import MigrationCheckpointRecord

        # Get latest checkpoint
        checkpoint = self.get_latest_checkpoint(migration_id, table_name)

        if checkpoint:
            checkpoint.status = "failed"
            checkpoint.error_message = error_message
            checkpoint.updated_at = datetime.utcnow()
            self.db.commit()
            logger.error(f"Table {table_name} marked as failed: {error_message}")
