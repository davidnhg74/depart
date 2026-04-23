"""Per-column data-masking endpoints for a migration.

    GET    /api/v1/migrations/{id}/masking          → current rules (or {})
    PUT    /api/v1/migrations/{id}/masking          → upsert rules
    DELETE /api/v1/migrations/{id}/masking          → clear rules
    POST   /api/v1/migrations/{id}/masking/preview  → sampled + masked rows

All admin-gated (operators can *run* masked migrations but only admins
configure the rules) and license-gated by the ``data_masking`` feature.
The preview endpoint reads a small sample from the source DB, applies
the rules, and returns only the masked rows — operators can already
see the originals in their source, and we don't want to round-trip
PII through the product just for a preview.
"""

from __future__ import annotations

import logging
import uuid
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException, Request, status
from pydantic import BaseModel, Field
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from ..auth.roles import require_role
from ..db import get_db
from ..license.dependencies import require_feature
from ..models import MigrationRecord
from ..services import masking_service
from ..services.audit import log_event


logger = logging.getLogger(__name__)


router = APIRouter(prefix="/api/v1/migrations", tags=["masking"])


# ─── Schemas ─────────────────────────────────────────────────────────


class MaskingRulesBody(BaseModel):
    # Intentionally loose: we validate the contents via
    # masking_service.validate_rules so the router and runner share a
    # single definition of "valid rules."
    rules: dict[str, Any] = Field(default_factory=dict)


class MaskingRulesView(BaseModel):
    rules: dict[str, Any]


class PreviewBody(BaseModel):
    sample_size: int = Field(default=5, ge=1, le=50)


class PreviewResult(BaseModel):
    samples: dict[str, list[dict[str, Any]]]
    errors: dict[str, str]


# ─── Helpers ─────────────────────────────────────────────────────────


def _parse_migration_id(raw: str) -> uuid.UUID:
    try:
        return uuid.UUID(raw)
    except (ValueError, AttributeError):
        raise HTTPException(status_code=404, detail="migration not found")


def _load_migration(db: Session, migration_id: str) -> MigrationRecord:
    rec = db.get(MigrationRecord, _parse_migration_id(migration_id))
    if rec is None:
        raise HTTPException(status_code=404, detail="migration not found")
    return rec


# ─── Routes ──────────────────────────────────────────────────────────


@router.get("/{migration_id}/masking", response_model=MaskingRulesView)
def get_masking(
    migration_id: str,
    db: Session = Depends(get_db),
    _admin=Depends(require_role("admin")),
    _license=Depends(require_feature("data_masking")),
) -> MaskingRulesView:
    rec = _load_migration(db, migration_id)
    rules = masking_service.load_rules_from_text(rec.masking_rules)
    return MaskingRulesView(rules=rules)


@router.put("/{migration_id}/masking", response_model=MaskingRulesView)
def put_masking(
    migration_id: str,
    body: MaskingRulesBody,
    request: Request,
    db: Session = Depends(get_db),
    admin=Depends(require_role("admin")),
    _license=Depends(require_feature("data_masking")),
) -> MaskingRulesView:
    rec = _load_migration(db, migration_id)
    try:
        masking_service.validate_rules(body.rules)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    rec.masking_rules = (
        masking_service.dump_rules_to_text(body.rules) if body.rules else None
    )
    db.commit()
    log_event(
        db,
        request=request,
        user=admin,
        action="masking.upserted",
        resource_type="migration",
        resource_id=str(rec.id),
        details={
            "table_count": len(body.rules),
            "column_count": sum(len(v) for v in body.rules.values()),
        },
    )
    return MaskingRulesView(rules=body.rules)


@router.delete(
    "/{migration_id}/masking", status_code=status.HTTP_204_NO_CONTENT
)
def delete_masking(
    migration_id: str,
    request: Request,
    db: Session = Depends(get_db),
    admin=Depends(require_role("admin")),
    _license=Depends(require_feature("data_masking")),
) -> None:
    rec = _load_migration(db, migration_id)
    rec.masking_rules = None
    db.commit()
    log_event(
        db,
        request=request,
        user=admin,
        action="masking.deleted",
        resource_type="migration",
        resource_id=str(rec.id),
    )


@router.post(
    "/{migration_id}/masking/preview", response_model=PreviewResult
)
def preview_masking(
    migration_id: str,
    body: PreviewBody,
    db: Session = Depends(get_db),
    _admin=Depends(require_role("admin")),
    _license=Depends(require_feature("data_masking")),
) -> PreviewResult:
    """Read a small sample from each masked table in the source, apply
    the rules, and return only the masked rows. Errors per-table are
    collected (one bad table shouldn't block previews of the others)."""
    rec = _load_migration(db, migration_id)
    rules = masking_service.load_rules_from_text(rec.masking_rules)
    if not rules:
        raise HTTPException(
            status_code=400,
            detail="no masking rules configured for this migration",
        )
    if not rec.source_url:
        raise HTTPException(
            status_code=400,
            detail="migration has no source URL configured",
        )

    try:
        transform = masking_service.build_row_transform(rules)
    except (ValueError, RuntimeError) as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    is_oracle = rec.source_url.startswith("oracle")
    samples: dict[str, list[dict[str, Any]]] = {}
    errors: dict[str, str] = {}

    try:
        engine = create_engine(rec.source_url, pool_pre_ping=True)
    except Exception as exc:  # DSN parse-time failures
        raise HTTPException(status_code=502, detail=f"source engine error: {exc}")

    # Minimal ad-hoc spec shape — matches what masking_service cares
    # about (source_table.qualified() + columns) without importing
    # the real TableSpec here.
    from types import SimpleNamespace

    try:
        with engine.connect() as conn:
            for qualified in rules.keys():
                try:
                    if is_oracle:
                        q = f"SELECT * FROM {qualified} WHERE ROWNUM <= :n"
                    else:
                        q = f"SELECT * FROM {qualified} LIMIT :n"
                    result = conn.execute(text(q), {"n": body.sample_size})
                    columns = list(result.keys())
                    rows = [tuple(r) for r in result.fetchall()]
                    spec = SimpleNamespace(
                        source_table=SimpleNamespace(
                            qualified=lambda q=qualified: q
                        ),
                        columns=columns,
                    )
                    masked = transform(rows, spec)
                    samples[qualified] = [
                        dict(zip(columns, row)) for row in masked
                    ]
                except Exception as exc:
                    errors[qualified] = f"{type(exc).__name__}: {exc}"[:500]
    finally:
        engine.dispose()

    return PreviewResult(samples=samples, errors=errors)
