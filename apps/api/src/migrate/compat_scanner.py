"""Layer 10 — Oracle→PostgreSQL Application SQL Compatibility Scanner.

Scans Oracle database objects (views, stored procedures, functions,
triggers, packages) for SQL constructs that do not exist or behave
differently in PostgreSQL. Produces a finding list with severity ratings
and PG equivalents so operators can scope the application-layer work
before committing to cutover.

This module is pure logic — no DB access, no I/O. Feed it the raw source
text from Oracle system views (ALL_VIEWS, ALL_SOURCE) and it returns
structured findings. The service layer handles DB queries.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple


# ─── Finding model ────────────────────────────────────────────────────────────


@dataclass
class CompatFinding:
    construct: str          # e.g. "ROWNUM", "CONNECT_BY", "NVL"
    severity: str           # "blocking" | "advisory" | "info"
    pg_equivalent: str      # short migration note
    locations: List[str]    # object names where found, e.g. ["VIEW:V_ORDERS"]
    count: int = 0          # total occurrences across all locations


@dataclass
class CompatScanResult:
    oracle_objects_scanned: int
    blocking_count: int
    advisory_count: int
    info_count: int
    findings: List[CompatFinding]
    complexity_score: int    # 0–100; 100 = fully compatible, 0 = all blocking


# ─── Construct definitions ────────────────────────────────────────────────────

# Each entry: (construct_key, severity, pg_equivalent, regex_pattern)
# Patterns are applied case-insensitively against the full object source text.
_CONSTRUCTS: List[Tuple[str, str, str, str]] = [
    # ── Blocking — require code changes, no drop-in replacement ──────────────
    (
        "CONNECT_BY",
        "blocking",
        "Rewrite as recursive CTE: WITH RECURSIVE ... AS (...)",
        r"\bCONNECT\s+BY\b",
    ),
    (
        "START_WITH",
        "blocking",
        "Part of hierarchical query (CONNECT BY); rewrite as recursive CTE",
        r"\bSTART\s+WITH\b",
    ),
    (
        "OUTER_JOIN_PLUS",
        "blocking",
        "Replace Oracle (+) outer-join syntax with ANSI LEFT/RIGHT JOIN",
        r"\(\s*\+\s*\)",
    ),
    (
        "MINUS_SET_OP",
        "blocking",
        "Replace MINUS with EXCEPT (same semantics in PostgreSQL)",
        r"\bMINUS\b",
    ),
    (
        "ROWNUM",
        "blocking",
        "Replace with ROW_NUMBER() OVER (...) or LIMIT/OFFSET",
        r"\bROWNUM\b",
    ),
    (
        "PIVOT_UNPIVOT",
        "blocking",
        "No native PIVOT/UNPIVOT in PostgreSQL; use CASE WHEN or crosstab()",
        r"\b(?:PIVOT|UNPIVOT)\b",
    ),
    (
        "MODEL_CLAUSE",
        "blocking",
        "Oracle MODEL clause has no PG equivalent; rewrite with recursive CTE",
        r"\bMODEL\b\s*\(",
    ),
    (
        "XMLDB",
        "blocking",
        "Oracle XMLDB/XMLTYPE differs from PG xml type; verify query-by-query",
        r"\bXMLTYPE\b|\bXMLDB\b|\bXMLELEMENT\b|\bXMLAGG\b|\bXMLFORESTRECORD\b",
    ),
    (
        "DBMS_OUTPUT",
        "blocking",
        "No equivalent in PG; replace with RAISE NOTICE for procedures",
        r"\bDBMS_OUTPUT\s*\.",
    ),
    (
        "EXECUTE_IMMEDIATE",
        "blocking",
        "Replace with PG EXECUTE ... USING dynamic SQL",
        r"\bEXECUTE\s+IMMEDIATE\b",
    ),
    (
        "BULK_COLLECT",
        "blocking",
        "Replace with cursor loops or SELECT INTO arrays in PL/pgSQL",
        r"\bBULK\s+COLLECT\b",
    ),
    (
        "FORALL",
        "blocking",
        "Replace with FOREACH in PL/pgSQL or set-based UPDATE/INSERT",
        r"\bFORALL\b",
    ),
    (
        "PACKAGE",
        "blocking",
        "Oracle packages have no PG equivalent; split into schemas + functions",
        r"\bCREATE\s+(?:OR\s+REPLACE\s+)?PACKAGE\b",
    ),
    (
        "PRAGMA",
        "blocking",
        "Oracle PRAGMA directives have no PG equivalent; remove or refactor",
        r"\bPRAGMA\b",
    ),
    # ── Advisory — have PG equivalents but require manual substitution ─────
    (
        "NVL",
        "advisory",
        "Replace NVL(a, b) with COALESCE(a, b)",
        r"\bNVL\s*\(",
    ),
    (
        "NVL2",
        "advisory",
        "Replace NVL2(a, b, c) with CASE WHEN a IS NOT NULL THEN b ELSE c END",
        r"\bNVL2\s*\(",
    ),
    (
        "DECODE",
        "advisory",
        "Replace DECODE(x, v1, r1, ...) with CASE WHEN x=v1 THEN r1 ... END",
        r"\bDECODE\s*\(",
    ),
    (
        "SYSDATE",
        "advisory",
        "Replace SYSDATE with NOW() or CURRENT_TIMESTAMP",
        r"\bSYSDATE\b",
    ),
    (
        "SYSTIMESTAMP",
        "advisory",
        "Replace SYSTIMESTAMP with CURRENT_TIMESTAMP or CLOCK_TIMESTAMP()",
        r"\bSYSTIMESTAMP\b",
    ),
    (
        "DUAL_TABLE",
        "advisory",
        "Remove FROM DUAL — PostgreSQL does not need a dummy table",
        r"\bFROM\s+DUAL\b",
    ),
    (
        "SEQUENCE_NEXTVAL",
        "advisory",
        "Replace seq.NEXTVAL with NEXTVAL('seq') in PostgreSQL",
        r"\w+\s*\.\s*NEXTVAL\b",
    ),
    (
        "SEQUENCE_CURRVAL",
        "advisory",
        "Replace seq.CURRVAL with CURRVAL('seq') in PostgreSQL",
        r"\w+\s*\.\s*CURRVAL\b",
    ),
    (
        "TO_DATE_ORACLE",
        "advisory",
        "Replace TO_DATE(str, fmt) with TO_DATE(str, fmt) — verify Oracle format masks match PG",
        r"\bTO_DATE\s*\(",
    ),
    (
        "TO_NUMBER",
        "advisory",
        "Replace TO_NUMBER() with CAST(... AS NUMERIC) or TO_NUMBER() — verify format masks",
        r"\bTO_NUMBER\s*\(",
    ),
    (
        "TO_CHAR_NUMBER",
        "advisory",
        "TO_CHAR() works in PG but Oracle format masks may differ; verify each call",
        r"\bTO_CHAR\s*\(",
    ),
    (
        "INSTR",
        "advisory",
        "Replace INSTR(str, sub) with POSITION(sub IN str) or STRPOS(str, sub)",
        r"\bINSTR\s*\(",
    ),
    (
        "SUBSTR",
        "advisory",
        "Replace SUBSTR() with SUBSTRING() — same semantics, different name",
        r"\bSUBSTR\s*\(",
    ),
    (
        "TRUNC_DATE",
        "advisory",
        "Replace TRUNC(date) with DATE_TRUNC('day', date)",
        r"\bTRUNC\s*\(",
    ),
    (
        "ADD_MONTHS",
        "advisory",
        "Replace ADD_MONTHS(d, n) with d + INTERVAL 'n months'",
        r"\bADD_MONTHS\s*\(",
    ),
    (
        "MONTHS_BETWEEN",
        "advisory",
        "Replace MONTHS_BETWEEN(d1, d2) with EXTRACT(MONTH FROM AGE(d1, d2))",
        r"\bMONTHS_BETWEEN\s*\(",
    ),
    (
        "LAST_DAY",
        "advisory",
        "Replace LAST_DAY(d) with DATE_TRUNC('month', d) + INTERVAL '1 month - 1 day'",
        r"\bLAST_DAY\s*\(",
    ),
    (
        "GREATEST_LEAST",
        "advisory",
        "GREATEST/LEAST work in PG — no change needed but NULL handling differs",
        r"\b(?:GREATEST|LEAST)\s*\(",
    ),
    (
        "EXCEPTION_ORACLE",
        "advisory",
        "Remap Oracle named exceptions (DUP_VAL_ON_INDEX → UNIQUE_VIOLATION, etc.)",
        r"\b(?:DUP_VAL_ON_INDEX|NO_DATA_FOUND|TOO_MANY_ROWS|ZERO_DIVIDE|VALUE_ERROR)\b",
    ),
    (
        "RAISE_APPLICATION_ERROR",
        "advisory",
        "Replace RAISE_APPLICATION_ERROR(-20xxx, msg) with RAISE EXCEPTION '%', msg",
        r"\bRAISE_APPLICATION_ERROR\s*\(",
    ),
    # ── Info — compatible but worth reviewing ──────────────────────────────
    (
        "HINT_COMMENT",
        "info",
        "Oracle optimizer hints (/*+ ... */) are ignored in PG; remove them",
        r"/\*\+",
    ),
    (
        "VARCHAR2",
        "info",
        "VARCHAR2 → VARCHAR in PostgreSQL; semantically identical",
        r"\bVARCHAR2\b",
    ),
    (
        "NUMBER_TYPE",
        "info",
        "NUMBER → NUMERIC in PG; precision/scale rules differ for NUMBER with no args",
        r"\bNUMBER\b",
    ),
    (
        "ROWID",
        "info",
        "ROWID has no PG equivalent; application must use PK instead",
        r"\bROWID\b",
    ),
    (
        "NOCOPY",
        "info",
        "NOCOPY parameter hint is silently ignored in PL/pgSQL; remove",
        r"\bNOCOPY\b",
    ),
]

# Pre-compile for performance
_COMPILED: List[Tuple[str, str, str, re.Pattern]] = [
    (key, sev, pg_eq, re.compile(pattern, re.IGNORECASE))
    for key, sev, pg_eq, pattern in _CONSTRUCTS
]


# ─── Scanner ──────────────────────────────────────────────────────────────────


def scan_objects(
    objects: List[Dict[str, str]],
) -> CompatScanResult:
    """Scan a list of Oracle source objects for PG-incompatible constructs.

    Args:
        objects: list of dicts with keys:
            - ``type``: "VIEW" | "PROCEDURE" | "FUNCTION" | "TRIGGER" | "PACKAGE" | ...
            - ``name``: object name (e.g. "V_ORDERS")
            - ``text``: full source text

    Returns:
        CompatScanResult with all findings aggregated.
    """
    # Accumulate: construct_key → {finding_meta, set of object locations, total count}
    accumulator: Dict[str, _AccEntry] = {}

    for obj in objects:
        obj_label = f"{obj.get('type', 'OBJECT')}:{obj.get('name', '?')}"
        text = obj.get("text") or ""
        _scan_one(text, obj_label, accumulator)

    findings = _build_findings(accumulator)
    blocking = sum(1 for f in findings if f.severity == "blocking")
    advisory = sum(1 for f in findings if f.severity == "advisory")
    info = sum(1 for f in findings if f.severity == "info")

    score = _compute_score(blocking, advisory, info)

    return CompatScanResult(
        oracle_objects_scanned=len(objects),
        blocking_count=blocking,
        advisory_count=advisory,
        info_count=info,
        findings=findings,
        complexity_score=score,
    )


@dataclass
class _AccEntry:
    construct: str
    severity: str
    pg_equivalent: str
    locations: List[str] = field(default_factory=list)
    count: int = 0


def _scan_one(text: str, obj_label: str, acc: Dict[str, _AccEntry]) -> None:
    for key, severity, pg_eq, pattern in _COMPILED:
        matches = pattern.findall(text)
        if not matches:
            continue
        if key not in acc:
            acc[key] = _AccEntry(
                construct=key,
                severity=severity,
                pg_equivalent=pg_eq,
            )
        entry = acc[key]
        if obj_label not in entry.locations:
            entry.locations.append(obj_label)
        entry.count += len(matches)


def _build_findings(acc: Dict[str, _AccEntry]) -> List[CompatFinding]:
    # Order: blocking first, then advisory, then info; within each severity
    # sort by occurrence count descending (most common = most impactful).
    order = {"blocking": 0, "advisory": 1, "info": 2}
    entries = sorted(
        acc.values(),
        key=lambda e: (order.get(e.severity, 9), -e.count),
    )
    return [
        CompatFinding(
            construct=e.construct,
            severity=e.severity,
            pg_equivalent=e.pg_equivalent,
            locations=e.locations,
            count=e.count,
        )
        for e in entries
    ]


def _compute_score(blocking: int, advisory: int, info: int) -> int:
    """Complexity score: 100 = fully compatible, 0 = maximally incompatible.

    Each blocking finding deducts 20 points (capped at 5 distincts = 0).
    Each advisory finding deducts 5 points.
    Each info finding deducts 1 point.
    Floor at 0.
    """
    score = 100 - (blocking * 20) - (advisory * 5) - (info * 1)
    return max(0, score)
