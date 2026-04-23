"""Pre-flight advisor: per-table batch_size + optional Claude notes.

The runner ships with `batch_size = 5000` baked in. That number is fine
for narrow tables and terrible for wide ones — a row of CLOB columns at
5000 rows per batch is hundreds of MB of working memory per worker;
a row of three integers at 5000 is ~120 KB and round-trips noisily.

This module produces a per-table recommendation built from cheap signals
the introspector already collected (column widths, PKs, FK fan-out) and,
if an AIClient is supplied, refines them with a Claude pass that also
emits human-readable notes ("the composite PK on ORDER_ITEMS leads with
a low-cardinality column — keyset will be slow").

The advisor is opt-in. Runner does not call it; the planning router and
the CLI do, then optionally fold the recommended batch_size into the
Runner's `batch_size` field per-table (the runner today is single-batch-
size; per-table support is a follow-up).

Pure module: no network, no DB. The AIClient is the only side-effect
channel and it's optional.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from .ddl import ColumnMeta
from .introspect import IntrospectedSchema

logger = logging.getLogger(__name__)


# ─── Public types ────────────────────────────────────────────────────────────


@dataclass
class TableAdvice:
    qualified_name: str
    estimated_row_width_bytes: int
    recommended_batch_size: int
    rationale: str  # "wide row (CLOB-heavy)", "narrow row, large table", etc.


@dataclass
class MigrationAdvice:
    """Output of one advisor run."""

    per_table: Dict[str, TableAdvice] = field(default_factory=dict)
    notes: List[str] = field(default_factory=list)
    used_ai: bool = False  # True when Claude refined the deterministic baseline

    def batch_size(self, qualified_name: str, default: int = 5000) -> int:
        a = self.per_table.get(qualified_name)
        return a.recommended_batch_size if a else default


# ─── Width estimation ────────────────────────────────────────────────────────


# Rough bytes-per-value. Conservative — the goal is a per-batch memory
# budget, not exact storage. Variable-width types (CLOB/BLOB/RAW) get a
# fat default because one row of CLOB easily blows past anything we'd
# estimate from the column metadata alone.
_WIDTH_BY_TYPE: Dict[str, int] = {
    # Numerics
    "INTEGER": 8,
    "INT": 8,
    "BIGINT": 8,
    "SMALLINT": 4,
    "NUMERIC": 16,
    "NUMBER": 16,
    "DECIMAL": 16,
    "REAL": 4,
    "DOUBLE PRECISION": 8,
    "FLOAT": 8,
    # Booleans
    "BOOLEAN": 1,
    # Time
    "DATE": 8,
    "TIMESTAMP": 8,
    "TIMESTAMP WITHOUT TIME ZONE": 8,
    "TIMESTAMP WITH TIME ZONE": 12,
    "INTERVAL": 16,
    # Identity
    "UUID": 16,
    # Variable-width that we treat as fat by default
    "CLOB": 4096,
    "NCLOB": 4096,
    "BLOB": 4096,
    "BFILE": 4096,
    "JSON": 1024,
    "JSONB": 1024,
    "XMLTYPE": 4096,
    "RAW": 256,
    "LONG RAW": 4096,
}

_FAT_TYPES = {"CLOB", "NCLOB", "BLOB", "BFILE", "LONG RAW", "XMLTYPE"}


def estimate_row_width(columns: List[ColumnMeta]) -> int:
    """Sum of per-column width estimates. Bound below by 1 to avoid
    division-by-zero in the batch-size formula."""
    total = 0
    for c in columns:
        dtype = (c.data_type or "").upper().strip()
        if c.length and dtype in {"VARCHAR", "VARCHAR2", "NVARCHAR2", "CHAR", "NCHAR"}:
            # Use declared length — VARCHAR2(4000) really can hit 4000 bytes.
            total += int(c.length)
            continue
        total += _WIDTH_BY_TYPE.get(dtype, 64)
    return max(total, 1)


def has_fat_columns(columns: List[ColumnMeta]) -> bool:
    return any((c.data_type or "").upper().strip() in _FAT_TYPES for c in columns)


# ─── Deterministic baseline ──────────────────────────────────────────────────


# Target ~5 MB per batch. COPY can handle far more, but Oracle source
# reads are the bottleneck and its drivers don't love huge result sets.
_TARGET_BATCH_BYTES = 5_000_000
_MIN_BATCH = 100
_MAX_BATCH = 50_000


def _baseline_batch_size(row_width: int, row_count: Optional[int]) -> tuple[int, str]:
    raw = _TARGET_BATCH_BYTES // row_width
    bounded = max(_MIN_BATCH, min(_MAX_BATCH, raw))

    rationale_bits = [f"~{row_width}B/row → {bounded} rows/batch"]
    # Halve the batch on enormous tables: longer transactions raise the
    # cost of a crash + retry, and the checkpoint cadence improves.
    if row_count is not None and row_count > 100_000_000:
        bounded = max(_MIN_BATCH, bounded // 2)
        rationale_bits.append(f"halved for >100M rows ({row_count:,})")

    return bounded, "; ".join(rationale_bits)


def _baseline(
    schema: IntrospectedSchema, row_counts: Optional[Dict[str, int]]
) -> MigrationAdvice:
    advice = MigrationAdvice()
    row_counts = row_counts or {}
    column_metadata = schema.column_metadata or {}

    for table in schema.tables:
        qn = table.qualified()
        cols = column_metadata.get(qn, [])
        if not cols:
            # No metadata → use the runner's own default.
            advice.per_table[qn] = TableAdvice(
                qualified_name=qn,
                estimated_row_width_bytes=64,
                recommended_batch_size=5000,
                rationale="no column metadata; runner default",
            )
            continue

        width = estimate_row_width(cols)
        batch, rationale = _baseline_batch_size(width, row_counts.get(qn))

        if has_fat_columns(cols):
            rationale += "; LOB-heavy"

        advice.per_table[qn] = TableAdvice(
            qualified_name=qn,
            estimated_row_width_bytes=width,
            recommended_batch_size=batch,
            rationale=rationale,
        )

    return advice


# ─── Claude refinement ───────────────────────────────────────────────────────


_CLAUDE_SYSTEM = """You are a database migration advisor. You receive a JSON \
summary of a schema's tables (row counts, estimated row widths, deterministic \
batch-size recommendations, primary key shape, presence of LOB columns) and \
return refined per-table batch sizes plus operator-facing notes.

Be conservative. Do not change a recommended batch_size unless you have a \
concrete reason. Notes should be specific and actionable — flag things like \
low-cardinality leading PK columns (keyset pagination will scan), tables \
where the LOB ratio dominates transfer time, or FK fan-out that will \
benefit from a pre-load index on the source.

Return JSON only, matching this shape:

{
  "refinements": [
    {"qualified_name": "schema.table", "batch_size": 1234, "reason": "..."}
  ],
  "notes": ["...", "..."]
}

Omit a table from `refinements` if you have no change to recommend."""


def _build_claude_payload(
    schema: IntrospectedSchema,
    advice: MigrationAdvice,
    row_counts: Dict[str, int],
) -> str:
    column_metadata = schema.column_metadata or {}
    items = []
    for table in schema.tables:
        qn = table.qualified()
        ta = advice.per_table.get(qn)
        if ta is None:
            continue
        cols = column_metadata.get(qn, [])
        items.append(
            {
                "qualified_name": qn,
                "row_count": row_counts.get(qn),
                "estimated_row_width_bytes": ta.estimated_row_width_bytes,
                "deterministic_batch_size": ta.recommended_batch_size,
                "primary_key": schema.primary_keys.get(qn, []),
                "has_lob_column": has_fat_columns(cols),
                "column_count": len(cols),
            }
        )
    return json.dumps({"tables": items}, separators=(",", ":"))


def _apply_refinements(advice: MigrationAdvice, data: dict) -> None:
    """Merge a parsed Claude response into `advice` in-place.

    Refinements are clamped to [_MIN_BATCH, _MAX_BATCH]. A bogus or
    out-of-range value falls back to the deterministic baseline rather
    than corrupting the plan."""
    if not isinstance(data, dict):
        logger.warning("advisor: AI response was not a JSON object; ignoring")
        return

    for ref in data.get("refinements", []) or []:
        qn = ref.get("qualified_name")
        new_size = ref.get("batch_size")
        reason = ref.get("reason") or "Claude refinement"
        if not qn or not isinstance(new_size, int):
            continue
        if qn not in advice.per_table:
            continue
        clamped = max(_MIN_BATCH, min(_MAX_BATCH, new_size))
        existing = advice.per_table[qn]
        existing.recommended_batch_size = clamped
        existing.rationale += f"; refined: {reason}"

    for note in data.get("notes", []) or []:
        if isinstance(note, str) and note.strip():
            advice.notes.append(note.strip())


# ─── Public entry point ──────────────────────────────────────────────────────


def advise(
    schema: IntrospectedSchema,
    *,
    row_counts: Optional[Dict[str, int]] = None,
    ai_client=None,  # AIClient | None — typed loose to avoid an import cycle
) -> MigrationAdvice:
    """Produce per-table advice for `schema`.

    Always runs the deterministic baseline. If `ai_client` is provided
    AND there's at least one table with column metadata, asks Claude to
    refine the batch sizes and emit notes. The Claude pass is best-
    effort: any failure (network, parse, schema mismatch) is logged and
    the deterministic result is returned unchanged.

    `row_counts` is optional. When supplied, `qualified_name -> row count`
    lets the advisor halve batches for very large tables and gives Claude
    a concrete signal to reason about. Callers without quick access to
    row counts can pass `None` and accept slightly less-tuned output.
    """
    advice = _baseline(schema, row_counts)

    if ai_client is None or not advice.per_table:
        return advice

    payload = _build_claude_payload(schema, advice, row_counts or {})
    try:
        # We rely on the AIClient.complete_json contract: takes
        # (system=, user=) keyword args, returns a parsed JSON value.
        # Tests can pass a fake with the same signature.
        data = ai_client.complete_json(system=_CLAUDE_SYSTEM, user=payload)
    except Exception as exc:  # noqa: BLE001 — best-effort enrichment
        logger.warning("advisor: AI refinement failed (%s); using baseline", exc)
        return advice

    _apply_refinements(advice, data)
    advice.used_ai = True
    return advice
