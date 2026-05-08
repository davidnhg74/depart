"""Layer 11 — PL/SQL → PL/pgSQL conversion via Claude.

Converts Oracle PL/SQL code objects (procedures, functions, triggers,
packages) to PostgreSQL PL/pgSQL. Each object is converted independently
so partial failure doesn't block the rest.

Output schema per object:
  {
    "object_type": "PROCEDURE" | "FUNCTION" | "TRIGGER" | "PACKAGE" | ...,
    "object_name": "CALCULATE_BONUS",
    "oracle_source": "...",
    "converted_code": "...",   # null if conversion failed
    "confidence": "high" | "medium" | "low",
    "review_notes": "Plain-text explanation of what changed and what needs human review.",
    "patterns_applied": ["NVL→COALESCE", "ROWNUM→ROW_NUMBER()", ...],
    "error": null | "error message if Claude failed"
  }

This module is pure: no DB access, no side effects. Feed it Oracle source
text; it returns structured dicts. The service layer handles DB queries and
persistence.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# Cached system prompt — same across all conversions in a run, so prompt
# caching on Anthropic's side saves tokens on the 2nd+ object.
_SYSTEM_PROMPT = """\
You are an expert Oracle DBA and PostgreSQL engineer specialising in Oracle→PostgreSQL migrations.
Your task is to convert Oracle PL/SQL code objects (procedures, functions, triggers, packages)
into correct, idiomatic PostgreSQL PL/pgSQL.

## Conversion rules

### Language structure
- Replace `CREATE OR REPLACE PROCEDURE/FUNCTION` with `CREATE OR REPLACE FUNCTION` returning
  the appropriate type. Procedures with no return value → `RETURNS void`.
- Oracle packages have no PG equivalent. Split into: one PostgreSQL schema (CREATE SCHEMA IF NOT EXISTS)
  containing standalone functions/procedures. Package-level variables → function parameters or
  session-local GUC settings if truly global.
- Oracle triggers: `CREATE OR REPLACE TRIGGER` → PG requires a trigger function + CREATE TRIGGER.
  Emit both the function and the CREATE TRIGGER statement.
- `IS` / `AS` in declarations → use `AS $$ ... $$ LANGUAGE plpgsql;`
- Exception blocks: `EXCEPTION WHEN ... THEN` → PG uses `EXCEPTION WHEN ... THEN` (same syntax).

### Type conversions
- `VARCHAR2(n)` → `VARCHAR(n)` or `TEXT`
- `NUMBER` → `NUMERIC`; `NUMBER(p,0)` → `INTEGER` or `BIGINT` based on p
- `DATE` (Oracle includes time) → `TIMESTAMP`
- `CLOB/NCLOB` → `TEXT`; `BLOB/RAW` → `BYTEA`
- `BOOLEAN` — Oracle PL/SQL has boolean; PG tables use BOOLEAN; PL/pgSQL also supports it
- `PLS_INTEGER` / `BINARY_INTEGER` → `INTEGER`

### SQL construct rewrites
- `ROWNUM <= n` → `LIMIT n`; `ROWNUM` in subquery → `ROW_NUMBER() OVER (...)`
- `CONNECT BY PRIOR child = parent START WITH cond` →
  `WITH RECURSIVE cte AS (SELECT ... WHERE <cond> UNION ALL SELECT t.* FROM t JOIN cte ON ...)  SELECT * FROM cte`
- `DECODE(x, v1, r1, v2, r2, default)` → `CASE x WHEN v1 THEN r1 WHEN v2 THEN r2 ELSE default END`
- `NVL(a, b)` → `COALESCE(a, b)`
- `NVL2(a, b, c)` → `CASE WHEN a IS NOT NULL THEN b ELSE c END`
- `SYSDATE` → `NOW()` or `CURRENT_TIMESTAMP`
- `SYSTIMESTAMP` → `CLOCK_TIMESTAMP()`
- `seq.NEXTVAL` → `NEXTVAL('seq')`; `seq.CURRVAL` → `CURRVAL('seq')`
- `FROM DUAL` → remove (SELECT 1 FROM DUAL → SELECT 1)
- `TO_DATE(str, fmt)` → `TO_DATE(str, fmt)` — Oracle masks differ: MM/DD/YYYY stays, but
  check RRRR (4-digit year) → YYYY, HH24 stays, MI stays
- `INSTR(str, sub)` → `POSITION(sub IN str)` or `STRPOS(str, sub)`
- `SUBSTR(str, pos, len)` → `SUBSTRING(str FROM pos FOR len)`
- `TRUNC(date)` → `DATE_TRUNC('day', date)`; `TRUNC(n)` → `TRUNC(n)` (same in PG)
- `ADD_MONTHS(d, n)` → `d + (n || ' months')::INTERVAL`
- `MONTHS_BETWEEN(d1, d2)` → `EXTRACT(YEAR FROM AGE(d1, d2))*12 + EXTRACT(MONTH FROM AGE(d1, d2))`
- `LAST_DAY(d)` → `DATE_TRUNC('month', d) + INTERVAL '1 month' - INTERVAL '1 day'`
- Outer join `a.id = b.id(+)` → `LEFT JOIN b ON a.id = b.id`
- `MINUS` set operator → `EXCEPT`
- `PIVOT` / `UNPIVOT` → use `crosstab()` from tablefunc extension or manual CASE aggregation
- `MERGE INTO t USING s ON ... WHEN MATCHED THEN UPDATE ... WHEN NOT MATCHED THEN INSERT` →
  `INSERT INTO t ... ON CONFLICT (key) DO UPDATE SET ...`

### PL/SQL-specific constructs
- `EXECUTE IMMEDIATE sql` → `EXECUTE sql` (PL/pgSQL dynamic SQL)
- `EXECUTE IMMEDIATE sql INTO var` → `EXECUTE sql INTO var`
- `EXECUTE IMMEDIATE sql USING bind` → `EXECUTE sql USING bind`
- `DBMS_OUTPUT.PUT_LINE(msg)` → `RAISE NOTICE '%', msg`
- `RAISE_APPLICATION_ERROR(-20xxx, msg)` → `RAISE EXCEPTION '%', msg`
- `DUP_VAL_ON_INDEX` → `unique_violation`
- `NO_DATA_FOUND` → `NO_DATA_FOUND` (same in PL/pgSQL)
- `TOO_MANY_ROWS` → `TOO_MANY_ROWS` (same)
- `ZERO_DIVIDE` → `division_by_zero`
- `VALUE_ERROR` → `data_exception` or `invalid_text_representation`
- `BULK COLLECT INTO arr` → use a cursor loop or `SELECT array_agg(...) INTO arr`
- `FORALL i IN arr.FIRST..arr.LAST EXECUTE ...` → loop with `EXECUTE ... USING arr[i]`
- `%TYPE` / `%ROWTYPE` → PG supports `%TYPE` and `%ROWTYPE` in PL/pgSQL — keep as-is
- `PRAGMA AUTONOMOUS_TRANSACTION` → no direct equivalent; note in review_notes
- `NOCOPY` parameter hint → remove silently
- Package-level constants → convert to PL/pgSQL `CONSTANT` declarations inside each function

### What to do when you cannot convert
If a construct has no PG equivalent (e.g. complex DBMS_* packages, Oracle-specific optimizer
hints in PL/SQL, Oracle Advanced Queuing):
- Insert a `-- TODO: no PG equivalent — manual rewrite required` comment in the converted code
- Mention it in `review_notes`
- Still convert everything else around it

## Output format

Respond with ONLY a JSON object — no markdown fences, no explanation outside the JSON:

{
  "converted_code": "Full PostgreSQL PL/pgSQL source, ready to execute",
  "confidence": "high" | "medium" | "low",
  "review_notes": "Concise plain-text explanation: what was changed, what still needs human review, any assumptions made",
  "patterns_applied": ["list", "of", "short", "pattern", "names", "applied"]
}

confidence levels:
- "high": straightforward conversion, all constructs have direct PG equivalents, no TODOs
- "medium": some ambiguity or non-trivial rewrites; recommend DBA review before running
- "low": complex package, autonomous transactions, DBMS_* dependencies, or multiple TODOs; requires significant human review
"""


def convert_one(
    *,
    object_type: str,
    object_name: str,
    oracle_source: str,
    api_key: str,
) -> Dict[str, Any]:
    """Convert a single Oracle code object to PL/pgSQL via Claude.

    Returns a result dict with keys:
      object_type, object_name, oracle_source,
      converted_code, confidence, review_notes, patterns_applied, error
    """
    from ..ai.client import AIClient

    base = {
        "object_type": object_type,
        "object_name": object_name,
        "oracle_source": oracle_source,
        "converted_code": None,
        "confidence": "low",
        "review_notes": "",
        "patterns_applied": [],
        "error": None,
    }

    if not oracle_source.strip():
        base["error"] = "Empty source — nothing to convert"
        return base

    user_prompt = (
        f"Convert this Oracle {object_type} named {object_name!r} to PostgreSQL PL/pgSQL.\n\n"
        f"Oracle source:\n```sql\n{oracle_source}\n```"
    )

    try:
        client = AIClient.smart(api_key=api_key, feature="plsql-convert", max_tokens=8192)
        result = client.complete_json(
            system=_SYSTEM_PROMPT,
            user=user_prompt,
            cache_system=True,   # reuse cached system prompt across objects
        )
        base["converted_code"] = result.get("converted_code") or ""
        base["confidence"] = result.get("confidence", "low")
        base["review_notes"] = result.get("review_notes", "")
        base["patterns_applied"] = result.get("patterns_applied") or []
    except ValueError as exc:
        # JSON parse failure from Claude
        base["error"] = f"Parse error: {str(exc)[:200]}"
        logger.warning("PL/SQL convert parse error for %s.%s: %s", object_type, object_name, exc)
    except Exception as exc:
        base["error"] = str(exc).split("\n")[0][:300]
        logger.warning("PL/SQL convert failed for %s.%s: %s", object_type, object_name, exc)

    return base


def convert_batch(
    objects: List[Dict[str, str]],
    *,
    api_key: str,
    limit: int = 10,
) -> List[Dict[str, Any]]:
    """Convert up to *limit* objects from *objects*.

    Each item in *objects* must have keys: type, name, text.
    Returns one result dict per object (in input order), success or failure.
    """
    results = []
    for obj in objects[:limit]:
        result = convert_one(
            object_type=obj.get("type", "OBJECT"),
            object_name=obj.get("name", "?"),
            oracle_source=obj.get("text", ""),
            api_key=api_key,
        )
        results.append(result)
    return results
