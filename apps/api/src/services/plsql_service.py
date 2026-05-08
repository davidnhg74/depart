"""Service layer for Layer 11 — PL/SQL → PL/pgSQL conversion.

Fetches Oracle code objects, converts each via Claude, persists a
CodeConversionRun snapshot with per-object results, and returns a summary.

Called from:
  * POST /api/v1/migrations/{id}/convert-code  (on-demand)
"""

from __future__ import annotations

import logging
from typing import List

from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker

from ..migrate.plsql_converter import convert_batch
from ..models import CodeConversionRun, MigrationRecord
from ..services.settings_service import get_effective_anthropic_key

logger = logging.getLogger(__name__)

_SUPPORTED_TYPES = ("PROCEDURE", "FUNCTION", "TRIGGER", "PACKAGE", "PACKAGE BODY")


def _fetch_code_objects(oracle_session, schema: str) -> List[dict]:
    """Return procedure/function/trigger/package source from ALL_SOURCE."""
    sql = text(
        """
        SELECT   object_type, name, text
        FROM     all_source
        WHERE    owner = :schema
          AND    object_type IN ('PROCEDURE', 'FUNCTION', 'TRIGGER', 'PACKAGE', 'PACKAGE BODY')
        ORDER    BY object_type, name, line
        """
    )
    rows = oracle_session.execute(sql, {"schema": schema.upper()}).fetchall()

    grouped: dict = {}
    for obj_type, name, line_text in rows:
        key = (obj_type, name)
        if key not in grouped:
            grouped[key] = []
        grouped[key].append(line_text or "")

    return [
        {"type": obj_type, "name": name, "text": "".join(lines)}
        for (obj_type, name), lines in grouped.items()
    ]


def run_plsql_conversion(
    record: MigrationRecord,
    db: Session,
    *,
    limit: int = 10,
) -> tuple[list, CodeConversionRun]:
    """Convert Oracle PL/SQL code objects for a migration.

    Fetches up to *limit* code objects from the Oracle source schema,
    converts each via Claude, persists a CodeConversionRun, and returns
    (results_list, run_orm_row).

    Raises ValueError if source_url is missing or no Anthropic key is configured.
    """
    if not record.source_url:
        raise ValueError("Migration has no source_url — cannot run code conversion.")

    api_key = get_effective_anthropic_key(db)
    if not api_key:
        raise ValueError(
            "No Anthropic API key configured. Set one in Settings or via ANTHROPIC_API_KEY "
            "to enable PL/SQL conversion."
        )

    oracle_schema = record.source_schema or record.schema_name or "PUBLIC"

    oracle_engine = create_engine(record.source_url)
    OracleSess = sessionmaker(bind=oracle_engine)
    oracle_session = OracleSess()
    try:
        objects = _fetch_code_objects(oracle_session, oracle_schema)
    finally:
        oracle_session.close()
        oracle_engine.dispose()

    total_found = len(objects)
    results = convert_batch(objects, api_key=api_key, limit=limit)

    converted_count = sum(1 for r in results if r.get("converted_code") and not r.get("error"))
    failed_count = sum(1 for r in results if r.get("error"))

    run = CodeConversionRun(
        migration_id=record.id,
        user_id=record.user_id,
        objects_found=total_found,
        objects_attempted=len(results),
        objects_converted=converted_count,
        objects_failed=failed_count,
        results=results,
    )
    db.add(run)
    db.commit()
    db.refresh(run)
    return results, run
