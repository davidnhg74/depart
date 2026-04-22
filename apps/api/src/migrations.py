"""
Database migrations and schema setup for Phase 3 RAG system.
Run this to set up pgvector extension and conversion_cases table.
"""

from sqlalchemy import text
from sqlalchemy.orm import Session
import logging

logger = logging.getLogger(__name__)


def setup_pgvector_extension(db: Session):
    """Enable pgvector extension in PostgreSQL."""
    try:
        db.execute(text("CREATE EXTENSION IF NOT EXISTS vector"))
        db.commit()
        logger.info("pgvector extension enabled")
    except Exception as e:
        logger.warning(f"pgvector extension may already exist: {e}")


def setup_rag_tables(db: Session):
    """Create conversion_cases table with vector support."""
    try:
        # Create extension first
        setup_pgvector_extension(db)

        # Create conversion_cases table
        db.execute(text("""
            CREATE TABLE IF NOT EXISTS conversion_cases (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                construct_type VARCHAR(50) NOT NULL,
                oracle_code TEXT NOT NULL,
                postgres_code TEXT NOT NULL,
                embedding REAL[] NOT NULL,
                success_count INTEGER DEFAULT 1 NOT NULL,
                fail_count INTEGER DEFAULT 0 NOT NULL,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL,
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );

            CREATE INDEX IF NOT EXISTS idx_conversion_cases_construct_type
                ON conversion_cases(construct_type);
            CREATE INDEX IF NOT EXISTS idx_conversion_cases_created_at
                ON conversion_cases(created_at);
        """))
        db.commit()
        logger.info("conversion_cases table created")
    except Exception as e:
        logger.error(f"Error creating conversion_cases table: {e}")
        raise


def setup_workflow_tables(db: Session):
    """Create migration_workflows table for HITL migration orchestration."""
    try:
        db.execute(text("""
            CREATE TABLE IF NOT EXISTS migration_workflows (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                name VARCHAR(255) NOT NULL,
                migration_id UUID REFERENCES migrations(id),
                current_step INTEGER DEFAULT 1 NOT NULL,
                status VARCHAR(50) DEFAULT 'running' NOT NULL,
                dba_notes JSONB DEFAULT '{}' NOT NULL,
                approvals JSONB DEFAULT '{}' NOT NULL,
                settings JSONB DEFAULT '{}' NOT NULL,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL,
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );

            CREATE INDEX IF NOT EXISTS idx_migration_workflows_status
                ON migration_workflows(status);
            CREATE INDEX IF NOT EXISTS idx_migration_workflows_migration_id
                ON migration_workflows(migration_id);
        """))
        db.commit()
        logger.info("migration_workflows table created")
    except Exception as e:
        logger.error(f"Error creating migration_workflows table: {e}")
        raise


def setup_benchmark_tables(db: Session):
    """Create benchmark_captures table for storing benchmark metrics."""
    try:
        db.execute(text("""
            CREATE TABLE IF NOT EXISTS benchmark_captures (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                migration_id UUID REFERENCES migrations(id),
                db_type VARCHAR(20) NOT NULL,
                captured_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL,
                data JSONB NOT NULL,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_benchmark_captures_migration_id
                ON benchmark_captures(migration_id);
            CREATE INDEX IF NOT EXISTS idx_benchmark_captures_db_type
                ON benchmark_captures(db_type);
            CREATE INDEX IF NOT EXISTS idx_benchmark_captures_captured_at
                ON benchmark_captures(captured_at);
        """))
        db.commit()
        logger.info("benchmark_captures table created")
    except Exception as e:
        logger.error(f"Error creating benchmark_captures table: {e}")
        raise
