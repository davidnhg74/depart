"""Tests for the migration advisor.

The deterministic baseline has no external dependencies, so most tests
construct a tiny IntrospectedSchema by hand and assert on the per-table
TableAdvice. The Claude refinement path is exercised with a fake
AIClient stub (any object with `complete_json(system=, user=) -> dict`).
"""

from __future__ import annotations

import json

import pytest

from src.migrate.advisor import (
    MigrationAdvice,
    TableAdvice,
    advise,
    estimate_row_width,
    has_fat_columns,
)
from src.migrate.ddl import ColumnMeta
from src.migrate.introspect import IntrospectedSchema
from src.migrate.keyset import Dialect
from src.migrate.planner import TableRef


def col(name: str, dtype: str, **kw) -> ColumnMeta:
    return ColumnMeta(name=name, data_type=dtype, **kw)


def schema(
    *,
    tables: list[tuple[str, list[ColumnMeta]]],
    pks: dict[str, list[str]] | None = None,
) -> IntrospectedSchema:
    """Build a minimal IntrospectedSchema. Tables are (qualified_name, columns)."""
    refs = [TableRef.parse(qn) for qn, _ in tables]
    columns = {qn: [c.name for c in cols] for qn, cols in tables}
    column_metadata = {qn: cols for qn, cols in tables}
    primary_keys = pks or {qn: [cols[0].name] for qn, cols in tables}
    return IntrospectedSchema(
        dialect=Dialect.ORACLE,
        schema="HR",
        tables=refs,
        columns=columns,
        primary_keys=primary_keys,
        foreign_keys=[],
        column_metadata=column_metadata,
    )


# ─── Width estimation ────────────────────────────────────────────────────────


class TestRowWidth:
    def test_narrow_integer_row(self):
        cols = [col("id", "INTEGER"), col("amount", "BIGINT")]
        # 8 + 8 = 16
        assert estimate_row_width(cols) == 16

    def test_varchar_uses_declared_length(self):
        cols = [col("id", "INTEGER"), col("name", "VARCHAR2", length=200)]
        # 8 + 200
        assert estimate_row_width(cols) == 208

    def test_unknown_type_falls_back_to_64(self):
        cols = [col("weird", "INTERVAL DAY TO SECOND")]
        assert estimate_row_width(cols) == 64

    def test_clob_is_fat(self):
        cols = [col("body", "CLOB")]
        assert estimate_row_width(cols) == 4096

    def test_minimum_width_is_one(self):
        # Empty row would yield zero, but the formula needs a positive
        # divisor; advisor clamps to 1.
        assert estimate_row_width([]) == 1

    def test_has_fat_columns_detects_clob(self):
        assert has_fat_columns([col("a", "INTEGER"), col("b", "CLOB")])
        assert not has_fat_columns([col("a", "INTEGER"), col("b", "VARCHAR2", length=80)])


# ─── Deterministic baseline ──────────────────────────────────────────────────


class TestBaseline:
    def test_narrow_table_gets_large_batch(self):
        # 16 B/row → 5_000_000 / 16 = 312,500 → clamped to MAX (50,000)
        s = schema(tables=[("HR.EMP", [col("id", "INTEGER"), col("dept", "BIGINT")])])
        adv = advise(s)
        ta = adv.per_table["HR.EMP"]
        assert ta.recommended_batch_size == 50_000
        assert ta.estimated_row_width_bytes == 16
        assert "rows/batch" in ta.rationale

    def test_wide_table_gets_small_batch(self):
        # 4096 B/row → 5_000_000 / 4096 ≈ 1220 → bounded, no clamp
        s = schema(tables=[("HR.DOC", [col("id", "INTEGER"), col("body", "CLOB")])])
        adv = advise(s)
        ta = adv.per_table["HR.DOC"]
        assert 100 < ta.recommended_batch_size < 5000
        assert "LOB-heavy" in ta.rationale

    def test_huge_table_halves_batch(self):
        s = schema(tables=[("HR.EVENTS", [col("id", "BIGINT"), col("ts", "TIMESTAMP")])])
        # Without row count: 5_000_000 / 16 → clamped to 50,000
        small = advise(s).per_table["HR.EVENTS"].recommended_batch_size
        # With >100M rows: halved
        big = advise(s, row_counts={"HR.EVENTS": 200_000_000})
        assert big.per_table["HR.EVENTS"].recommended_batch_size == small // 2
        assert "halved" in big.per_table["HR.EVENTS"].rationale

    def test_no_column_metadata_uses_runner_default(self):
        # Build an IntrospectedSchema where column_metadata is empty for a table.
        s = IntrospectedSchema(
            dialect=Dialect.POSTGRES,
            schema="public",
            tables=[TableRef(schema="public", name="orphans")],
            columns={"public.orphans": []},
            primary_keys={"public.orphans": ["id"]},
            foreign_keys=[],
            column_metadata={"public.orphans": []},
        )
        adv = advise(s)
        ta = adv.per_table["public.orphans"]
        assert ta.recommended_batch_size == 5000
        # No-metadata path uses a width sentinel (64) and the runner default,
        # rather than running the formula on an empty column list.
        assert ta.estimated_row_width_bytes == 64
        assert "no column metadata" in ta.rationale

    def test_used_ai_false_without_client(self):
        s = schema(tables=[("HR.EMP", [col("id", "INTEGER")])])
        adv = advise(s)
        assert adv.used_ai is False
        assert adv.notes == []


# ─── Claude refinement (fake AIClient) ───────────────────────────────────────


class FakeAI:
    """Minimal stand-in for AIClient. Records what it was called with so
    tests can assert on the prompt shape, and returns a canned dict."""

    def __init__(self, response: dict | Exception):
        self._response = response
        self.calls: list[dict] = []

    def complete_json(self, *, system: str, user: str):
        self.calls.append({"system": system, "user": user})
        if isinstance(self._response, Exception):
            raise self._response
        return self._response


class TestClaudeRefinement:
    def test_refinement_overrides_baseline(self):
        s = schema(tables=[("HR.EMP", [col("id", "INTEGER")])])
        ai = FakeAI(
            {
                "refinements": [
                    {"qualified_name": "HR.EMP", "batch_size": 1234, "reason": "tight"}
                ],
                "notes": ["watch the FK fan-out on EMP.manager_id"],
            }
        )
        adv = advise(s, ai_client=ai)
        assert adv.used_ai is True
        assert adv.per_table["HR.EMP"].recommended_batch_size == 1234
        assert "tight" in adv.per_table["HR.EMP"].rationale
        assert adv.notes == ["watch the FK fan-out on EMP.manager_id"]

    def test_refinement_clamped_to_bounds(self):
        s = schema(tables=[("HR.EMP", [col("id", "INTEGER")])])
        ai = FakeAI(
            {
                "refinements": [
                    {"qualified_name": "HR.EMP", "batch_size": 10_000_000, "reason": "huge"}
                ]
            }
        )
        adv = advise(s, ai_client=ai)
        # Clamped to MAX (50,000)
        assert adv.per_table["HR.EMP"].recommended_batch_size == 50_000

    def test_unknown_table_in_refinement_is_ignored(self):
        s = schema(tables=[("HR.EMP", [col("id", "INTEGER")])])
        ai = FakeAI(
            {
                "refinements": [
                    {"qualified_name": "HR.GHOST", "batch_size": 7777, "reason": "x"}
                ]
            }
        )
        adv = advise(s, ai_client=ai)
        # EMP unchanged from baseline
        assert "Claude refinement" not in adv.per_table["HR.EMP"].rationale
        # No GHOST entry created
        assert "HR.GHOST" not in adv.per_table

    def test_ai_failure_falls_back_to_baseline(self):
        s = schema(tables=[("HR.EMP", [col("id", "INTEGER")])])
        ai = FakeAI(RuntimeError("network down"))
        adv = advise(s, ai_client=ai)
        # Baseline still applied; no AI annotation
        assert adv.used_ai is False
        assert adv.per_table["HR.EMP"].recommended_batch_size == 50_000
        assert adv.notes == []

    def test_payload_carries_signal_columns(self):
        s = schema(
            tables=[("HR.DOC", [col("id", "INTEGER"), col("body", "CLOB")])],
            pks={"HR.DOC": ["id"]},
        )
        ai = FakeAI({"refinements": [], "notes": []})
        advise(s, row_counts={"HR.DOC": 5_000_000}, ai_client=ai)
        assert len(ai.calls) == 1
        payload = json.loads(ai.calls[0]["user"])
        item = payload["tables"][0]
        assert item["qualified_name"] == "HR.DOC"
        assert item["row_count"] == 5_000_000
        assert item["has_lob_column"] is True
        assert item["primary_key"] == ["id"]


# ─── MigrationAdvice helper ──────────────────────────────────────────────────


class TestMigrationAdvice:
    def test_batch_size_lookup_falls_back(self):
        adv = MigrationAdvice(
            per_table={
                "HR.EMP": TableAdvice(
                    qualified_name="HR.EMP",
                    estimated_row_width_bytes=16,
                    recommended_batch_size=10_000,
                    rationale="...",
                )
            }
        )
        assert adv.batch_size("HR.EMP") == 10_000
        assert adv.batch_size("HR.UNKNOWN") == 5000
        assert adv.batch_size("HR.UNKNOWN", default=999) == 999
