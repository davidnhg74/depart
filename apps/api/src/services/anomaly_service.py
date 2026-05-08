"""Service layer for Validation Layer 6 — AI anomaly detection.

Orchestrates: target PG connection → distribution sampling → Claude call
(or rule-based fallback) → DB write → result.

Called from:
  * POST /api/v1/migrations/{id}/check-anomalies  (on-demand)
  * migration_runner._run_inner() when run_anomaly_check=True (auto)
"""

from __future__ import annotations

import json
import logging
import uuid
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker

from ..ai.client import AIClient
from ..migrate.anomaly import (
    AnomalyFinding,
    build_anomaly_prompt,
    get_system_prompt,
    overall_severity,
    rule_based_findings,
    sample_table_distributions,
    _MAX_TABLES,
)
from ..migrate.temporal import check_temporal_table
from ..models import AnomalyAnalysis, MigrationRecord
from ..utils.time import utc_now

logger = logging.getLogger(__name__)

_MAX_TOKENS = 4096


# ─── Result ──────────────────────────────────────────────────────────────────


@dataclass
class AnomalyResult:
    findings: List[AnomalyFinding]
    used_ai: bool
    analysis_id: str
    overall_severity: str
    tables_sampled: int

    def to_response_dict(self) -> Dict[str, Any]:
        return {
            "findings": [f.to_dict() for f in self.findings],
            "used_ai": self.used_ai,
            "analysis_id": self.analysis_id,
            "overall_severity": self.overall_severity,
            "tables_sampled": self.tables_sampled,
        }


# ─── Main entry ──────────────────────────────────────────────────────────────


def anomaly_check_record(record: MigrationRecord, db: Session) -> AnomalyResult:
    """Run Layer 6 anomaly detection against a completed migration.

    Connects to the target PostgreSQL, samples distributions for each
    migrated table (capped at _MAX_TABLES, largest first), calls Claude
    to identify anomalies, falls back to rule-based checks if the API
    key is absent or the call fails, and persists the result.
    """
    if not record.target_url:
        raise ValueError("Migration has no target_url — cannot run anomaly check.")

    target_schema = record.target_schema or record.source_schema

    # Connect to target PG.
    engine = create_engine(record.target_url)
    Sess = sessionmaker(bind=engine)
    dst = Sess()

    try:
        tables = _resolve_tables(dst, target_schema, record)
        distributions = _sample_all(dst, target_schema, tables, record)
        temporal_findings = _collect_temporal(dst, target_schema, tables)
    finally:
        dst.close()
        engine.dispose()

    tables_sampled = len(distributions)
    used_ai = False
    try:
        client = AIClient.smart(feature="anomaly_detection")
        raw = client.complete_json(
            system=get_system_prompt(),
            user=build_anomaly_prompt(distributions),
            cache_system=True,
            max_tokens=_MAX_TOKENS,
        )
        findings = _parse_ai_response(raw)
        used_ai = True
    except Exception as exc:
        logger.warning(
            "anomaly AI call failed (%s: %s); falling back to rule-based checks",
            type(exc).__name__,
            exc,
        )
        findings = rule_based_findings(distributions)

    # Temporal findings (deterministic, Layer 5) always included.
    findings = temporal_findings + findings
    sev = overall_severity(findings)
    analysis_id = str(uuid.uuid4())

    row = AnomalyAnalysis(
        id=uuid.UUID(analysis_id),
        user_id=record.user_id,
        migration_id=record.id,
        findings=[f.to_dict() for f in findings],
        overall_severity=sev,
        used_ai=used_ai,
        tables_sampled=tables_sampled,
        created_at=utc_now(),
    )
    db.add(row)

    # Update the FK on the migration record so GET /{id} can include it.
    record.anomaly_analysis_id = uuid.UUID(analysis_id)
    db.commit()

    return AnomalyResult(
        findings=findings,
        used_ai=used_ai,
        analysis_id=analysis_id,
        overall_severity=sev,
        tables_sampled=tables_sampled,
    )


# ─── Helpers ─────────────────────────────────────────────────────────────────


def _resolve_tables(
    session: Session,
    schema: str,
    record: MigrationRecord,
) -> List[str]:
    """Return table names to sample, capped at _MAX_TABLES largest-first."""
    wanted: Optional[List[str]] = None
    if record.tables:
        try:
            wanted = json.loads(record.tables)
        except (ValueError, TypeError):
            pass

    if wanted:
        candidate_filter = "AND table_name = ANY(:names)"
        params: Dict[str, Any] = {"s": schema.lower(), "names": [t.lower() for t in wanted]}
    else:
        candidate_filter = ""
        params = {"s": schema.lower()}

    rows = session.execute(
        text(
            f"SELECT table_name FROM information_schema.tables "
            f"WHERE table_schema = :s AND table_type = 'BASE TABLE' "
            f"{candidate_filter} "
            f"ORDER BY table_name"
        ),
        params,
    ).fetchall()
    candidates = [r[0] for r in rows]

    if len(candidates) <= _MAX_TABLES:
        return candidates

    # Sort largest-first by a quick reltuples estimate (no full scan).
    sizes = _reltuples(session, schema, candidates)
    candidates.sort(key=lambda t: sizes.get(t, 0), reverse=True)
    return candidates[:_MAX_TABLES]


def _reltuples(session: Session, schema: str, tables: List[str]) -> Dict[str, int]:
    rows = session.execute(
        text(
            "SELECT relname, reltuples::bigint "
            "FROM pg_class c "
            "JOIN pg_namespace n ON n.oid = c.relnamespace "
            "WHERE n.nspname = :s AND relname = ANY(:names)"
        ),
        {"s": schema.lower(), "names": [t.lower() for t in tables]},
    ).fetchall()
    return {r[0]: r[1] for r in rows}


def _sample_all(
    session: Session,
    schema: str,
    tables: List[str],
    record: MigrationRecord,
) -> Dict[str, Any]:
    distributions: Dict[str, Any] = {}
    for table in tables:
        try:
            dist = sample_table_distributions(
                session,
                schema=schema,
                table=table,
                expected_row_count=None,  # Oracle row counts not stored per-table
            )
            distributions[table.upper()] = dist
        except Exception as exc:
            logger.warning("anomaly: failed to sample %s: %s", table, exc)
    return distributions


def _collect_temporal(
    session: Session,
    schema: str,
    tables: List[str],
) -> List[AnomalyFinding]:
    findings: List[AnomalyFinding] = []
    for table in tables:
        try:
            findings.extend(check_temporal_table(session, schema, table))
        except Exception as exc:
            logger.warning("temporal: failed to check %s: %s", table, exc)
    return findings


def _parse_ai_response(raw: Any) -> List[AnomalyFinding]:
    """Convert Claude's JSON response to AnomalyFinding list."""
    findings_raw = raw.get("findings", []) if isinstance(raw, dict) else []
    findings = []
    for item in findings_raw:
        try:
            findings.append(AnomalyFinding(
                severity=item.get("severity", "info"),
                table=item.get("table", ""),
                column=item.get("column"),
                anomaly_type=item.get("anomaly_type", "other"),
                message=item.get("message", ""),
                recommended_action=item.get("recommended_action", ""),
            ))
        except Exception:
            continue
    return findings
