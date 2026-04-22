"""
Background migration tasks.
Handles async execution of data migrations.
"""

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from threading import Thread
from typing import Callable
import logging
from datetime import datetime

from .orchestrator import DataMigrator
from .checkpoint import CheckpointManager
from ..models import MigrationRecord

logger = logging.getLogger(__name__)


class MigrationTask:
    """Represents an async migration task."""

    def __init__(
        self,
        migration_id: str,
        oracle_connection_string: str,
        postgres_connection_string: str,
        tables: list,
        num_workers: int = 4,
        chunk_size: int = 10000,
        db_session=None,
    ):
        self.migration_id = migration_id
        self.oracle_conn_str = oracle_connection_string
        self.postgres_conn_str = postgres_connection_string
        self.tables = tables
        self.num_workers = num_workers
        self.chunk_size = chunk_size
        self.db_session = db_session
        self.thread: Thread = None
        self.status = "pending"
        self.result = None
        self.error = None

    def start(self) -> None:
        """Start migration in background thread."""
        self.thread = Thread(target=self._run, daemon=True)
        self.thread.start()
        self.status = "running"
        logger.info(f"Migration {self.migration_id} started")

    def _run(self) -> None:
        """Run migration (called in background thread)."""
        try:
            # Create fresh connections
            oracle_engine = create_engine(self.oracle_conn_str)
            postgres_engine = create_engine(self.postgres_conn_str)

            oracle_session = sessionmaker(bind=oracle_engine)()
            postgres_session = sessionmaker(bind=postgres_engine)()

            # Create migrator
            migrator = DataMigrator(
                oracle_session,
                postgres_session,
                num_workers=self.num_workers,
                chunk_size=self.chunk_size,
            )

            # Plan migration
            plan = migrator.plan_migration(self.tables)
            logger.info(
                f"Migration plan: {plan.total_rows} rows, "
                f"{plan.estimated_duration_seconds} sec estimated"
            )

            # Execute plan
            success = migrator.execute_plan(plan)

            if success:
                self.status = "completed"
                self.result = {
                    "migration_id": self.migration_id,
                    "rows_transferred": migrator.total_rows_transferred,
                    "elapsed_seconds": int(
                        (datetime.utcnow() - migrator.start_time).total_seconds()
                    ),
                    "errors": migrator.errors,
                }
                logger.info(f"✅ Migration {self.migration_id} completed successfully")
            else:
                self.status = "failed"
                self.error = f"Migration failed with errors: {migrator.errors}"
                logger.error(f"❌ Migration {self.migration_id} failed: {self.error}")

        except Exception as e:
            self.status = "failed"
            self.error = str(e)
            logger.error(f"❌ Migration {self.migration_id} exception: {e}", exc_info=True)

    def wait(self, timeout: float = None) -> bool:
        """Wait for migration to complete."""
        if self.thread:
            self.thread.join(timeout=timeout)
            return self.status in ["completed", "failed"]
        return False

    def is_running(self) -> bool:
        """Check if migration is still running."""
        return self.status == "running" and (self.thread and self.thread.is_alive())

    def get_status(self) -> dict:
        """Get migration status."""
        return {
            "migration_id": self.migration_id,
            "status": self.status,
            "result": self.result,
            "error": self.error,
        }


class BackgroundMigrationManager:
    """Manages multiple background migration tasks."""

    def __init__(self):
        self.tasks: dict[str, MigrationTask] = {}

    def create_task(
        self,
        migration_id: str,
        oracle_connection_string: str,
        postgres_connection_string: str,
        tables: list,
        num_workers: int = 4,
        chunk_size: int = 10000,
    ) -> MigrationTask:
        """Create and store a migration task."""
        task = MigrationTask(
            migration_id=migration_id,
            oracle_connection_string=oracle_connection_string,
            postgres_connection_string=postgres_connection_string,
            tables=tables,
            num_workers=num_workers,
            chunk_size=chunk_size,
        )

        self.tasks[migration_id] = task
        return task

    def start_task(self, migration_id: str) -> bool:
        """Start a migration task."""
        task = self.tasks.get(migration_id)
        if task:
            task.start()
            return True
        return False

    def get_task(self, migration_id: str) -> MigrationTask or None:
        """Retrieve a migration task."""
        return self.tasks.get(migration_id)

    def get_task_status(self, migration_id: str) -> dict or None:
        """Get status of a migration task."""
        task = self.tasks.get(migration_id)
        return task.get_status() if task else None

    def list_tasks(self) -> list[dict]:
        """List all migration tasks."""
        return [task.get_status() for task in self.tasks.values()]

    def cleanup_completed(self) -> int:
        """Remove completed tasks (returns count removed)."""
        to_remove = [
            mid for mid, task in self.tasks.items() if task.status in ["completed", "failed"]
        ]

        for mid in to_remove:
            del self.tasks[mid]

        return len(to_remove)


# Global migration manager instance
migration_manager = BackgroundMigrationManager()


def get_migration_manager() -> BackgroundMigrationManager:
    """Get the global migration manager."""
    return migration_manager
