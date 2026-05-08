"""Service layer for Layer 10 — Application SQL Compatibility Scanner.

Queries Oracle system views to collect source text for views, stored
procedures, functions, triggers, and packages. Feeds the collected text
to the pure compat_scanner module, persists the result, and returns it.

Called from:
  * POST /api/v1/migrations/{id}/compat-scan  (on-demand)
"""

from __future__ import annotations

import json
import logging
import uuid
from typing import Dict, List, Optional

from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker

from ..migrate.compat_scanner import CompatScanResult, scan_objects
from ..models import CompatScanSnapshot, MigrationRecord
from ..utils.time import utc_now

logger = logging.getLogger(__name__)


# ─── Oracle query helpers ─────────────────────────────────────────────────────


def _fetch_views(oracle_session, schema: str) -> List[Dict[str, str]]:
    """Return all view source texts in *schema* from ALL_VIEWS."""
    sql = text(
        """
        SELECT view_name, text
        FROM   all_views
        WHERE  owner = :schema
        ORDER  BY view_name
        """
    )
    rows = oracle_session.execute(sql, {"schema": schema.upper()}).fetchall()
    return [{"type": "VIEW", "name": row[0], "text": row[1] or ""} for row in rows]


def _fetch_source_objects(oracle_session, schema: str) -> List[Dict[str, str]]:
    """Return procedure/function/trigger/package source from ALL_SOURCE.

    ALL_SOURCE stores source line-by-line; we concatenate lines per
    (object_type, object_name) to reconstruct the full body.
    """
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

    # Group lines by (type, name)
    grouped: Dict[tuple, list] = {}
    for obj_type, name, line_text in rows:
        key = (obj_type, name)
        if key not in grouped:
            grouped[key] = []
        grouped[key].append(line_text or "")

    return [
        {
            "type": obj_type,
            "name": name,
            "text": "".join(lines),
        }
        for (obj_type, name), lines in grouped.items()
    ]


# ─── Main service function ────────────────────────────────────────────────────


def scan_compat(
    record: MigrationRecord,
    db: Session,
) -> tuple[CompatScanResult, CompatScanSnapshot]:
    """Run Layer 10 compat scan against the Oracle source schema.

    Connects to Oracle, collects view + procedure/function/trigger/package
    source, runs the pure scanner, persists a snapshot, and returns both
    the result and the ORM snapshot.

    Only requires source_url — no target PG connection needed.
    """
    if not record.source_url:
        raise ValueError("Migration has no source_url — cannot run compat scan.")

    oracle_schema = record.source_schema or record.schema_name or "PUBLIC"

    oracle_engine = create_engine(record.source_url)
    OracleSess = sessionmaker(bind=oracle_engine)
    oracle_session = OracleSess()

    try:
        views = _fetch_views(oracle_session, oracle_schema)
        source_objs = _fetch_source_objects(oracle_session, oracle_schema)
    finally:
        oracle_session.close()
        oracle_engine.dispose()

    all_objects = views + source_objs
    result = scan_objects(all_objects)

    findings_json = [
        {
            "construct": f.construct,
            "severity": f.severity,
            "pg_equivalent": f.pg_equivalent,
            "locations": f.locations,
            "count": f.count,
        }
        for f in result.findings
    ]

    snap = CompatScanSnapshot(
        migration_id=record.id,
        user_id=record.user_id,
        oracle_objects_scanned=result.oracle_objects_scanned,
        blocking_count=result.blocking_count,
        advisory_count=result.advisory_count,
        info_count=result.info_count,
        complexity_score=result.complexity_score,
        findings=findings_json,
    )
    db.add(snap)
    db.commit()
    db.refresh(snap)
    return result, snap
