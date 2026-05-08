"""Service layer for Validation Layer 8 — row-level data sampler.

Orchestrates: Oracle source connection + PG target connection →
row comparison → DB write → result.

Called from:
  * POST /api/v1/migrations/{id}/sample  (on-demand)
"""

from __future__ import annotations

import json
import logging
import uuid
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from ..migrate.sampler import (
    SampleMismatch,
    _DEFAULT_SAMPLE_SIZE,
    run_sampler,
)
from ..models import DataSampleResult, MigrationRecord
from ..utils.time import utc_now

logger = logging.getLogger(__name__)


@dataclass
class SamplerResult:
    mismatches: List[SampleMismatch]
    overall_status: str  # "clean" | "mismatches_found"
    result_id: str
    tables_sampled: int
    tables_skipped: int
    mismatch_count: int

    def to_response_dict(self) -> Dict[str, Any]:
        return {
            "mismatches": [m.to_dict() for m in self.mismatches],
            "overall_status": self.overall_status,
            "result_id": self.result_id,
            "tables_sampled": self.tables_sampled,
            "tables_skipped": self.tables_skipped,
            "mismatch_count": self.mismatch_count,
        }


def sample_migration(
    record: MigrationRecord,
    db: Session,
    sample_size: int = _DEFAULT_SAMPLE_SIZE,
) -> SamplerResult:
    """Run Layer 8 data sampler against a migration.

    Requires both source_url (Oracle) and target_url (PG) on the record.
    """
    if not record.source_url:
        raise ValueError("Migration has no source_url — cannot run data sampler.")
    if not record.target_url:
        raise ValueError("Migration has no target_url — cannot run data sampler.")

    oracle_schema = record.source_schema or record.schema_name
    pg_schema = record.target_schema or record.source_schema or record.schema_name

    # Resolve table list.
    tables: Optional[List[str]] = None
    if record.tables:
        try:
            tables = json.loads(record.tables)
        except (ValueError, TypeError):
            pass

    oracle_engine = create_engine(record.source_url)
    pg_engine = create_engine(record.target_url)
    OracleSess = sessionmaker(bind=oracle_engine)
    PgSess = sessionmaker(bind=pg_engine)
    oracle_session = OracleSess()
    pg_session = PgSess()

    try:
        # If no table list, discover from Oracle schema.
        if not tables:
            from sqlalchemy import text
            rows = oracle_session.execute(
                text(
                    "SELECT table_name FROM all_tables "
                    "WHERE owner = :schema ORDER BY table_name"
                ),
                {"schema": oracle_schema.upper()},
            ).mappings().all()
            tables = [str(r["table_name"]) for r in rows]

        mismatches, stats = run_sampler(
            oracle_session,
            pg_session,
            oracle_schema,
            pg_schema,
            tables,
            sample_size=sample_size,
        )
    finally:
        oracle_session.close()
        pg_session.close()
        oracle_engine.dispose()
        pg_engine.dispose()

    overall_status = "mismatches_found" if mismatches else "clean"
    result_id = str(uuid.uuid4())

    row = DataSampleResult(
        id=uuid.UUID(result_id),
        user_id=record.user_id,
        migration_id=record.id,
        sample_size=sample_size,
        tables_sampled=stats["sampled"],
        tables_skipped=stats["skipped_no_pk"] + stats["skipped_empty"],
        mismatch_count=len(mismatches),
        mismatches=[m.to_dict() for m in mismatches],
        overall_status=overall_status,
        created_at=utc_now(),
    )
    db.add(row)
    db.commit()

    return SamplerResult(
        mismatches=mismatches,
        overall_status=overall_status,
        result_id=result_id,
        tables_sampled=stats["sampled"],
        tables_skipped=stats["skipped_no_pk"] + stats["skipped_empty"],
        mismatch_count=len(mismatches),
    )
