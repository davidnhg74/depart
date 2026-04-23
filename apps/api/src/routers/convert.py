"""Public `/convert/{tag}` endpoint — canonical Oracle → Postgres
conversion examples, keyed on construct tag.

This is the "AI conversion preview" lane for the free tier. The
examples are hand-curated and static — fast, deterministic, no token
cost on every page load. Paid tiers get a companion `/convert` POST
that runs Claude against the caller's actual snippet, grounded in the
same examples as few-shot RAG context.

Keeping examples in code (not Markdown files or a DB table) so they
travel with deploys, can be type-checked, and stay close to the tag
enum they key off of. When we add the paid `/convert` POST, its RAG
retrieval will use these same strings as the canonical seed corpus.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Dict

from fastapi import APIRouter, Depends, HTTPException, Request, status
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from ..ai.client import AIClient
from ..auth.roles import require_role
from ..core.ir.nodes import ConstructTag
from ..db import get_db
from ..license.dependencies import require_feature
from ..license.verifier import LicenseStatus
from ..services.audit import log_event
from ..services.settings_service import get_effective_anthropic_key
from ..validators.plpgsql_validator import ConversionValidator


logger = logging.getLogger(__name__)


router = APIRouter(prefix="/api/v1/convert", tags=["convert"])


# ─── Response schema ─────────────────────────────────────────────────────────


class ConversionExample(BaseModel):
    """One side-by-side example. The frontend renders `oracle` and
    `postgres` as two code blocks and `reasoning` as narrative below.

    `validation_errors` and `validation_warnings` are populated only by
    the live AI path — the canonical hand-curated examples are
    pre-validated and ship empty lists. When `validation_errors` is
    non-empty the UI should render a "do not trust" banner; warnings
    are advisory."""

    tag: str
    title: str
    oracle: str
    postgres: str
    reasoning: str
    confidence: str  # "high" | "medium" | "needs-review"
    validation_errors: list[str] = []
    validation_warnings: list[str] = []


# ─── Example content ─────────────────────────────────────────────────────────


@dataclass(frozen=True)
class _Ex:
    title: str
    oracle: str
    postgres: str
    reasoning: str
    confidence: str = "high"


# Tier B — mechanical with review
_MERGE = _Ex(
    title="MERGE → INSERT ... ON CONFLICT DO UPDATE",
    oracle="""MERGE INTO employee_audit a
USING (SELECT :emp_id AS emp_id, :org_path AS org_path FROM dual) s
   ON (a.emp_id = s.emp_id)
WHEN MATCHED THEN
    UPDATE SET a.org_path = s.org_path, a.updated_at = SYSDATE
WHEN NOT MATCHED THEN
    INSERT (emp_id, org_path, updated_at)
    VALUES (s.emp_id, s.org_path, SYSDATE);""",
    postgres="""INSERT INTO employee_audit (emp_id, org_path, updated_at)
VALUES (:emp_id, :org_path, NOW())
ON CONFLICT (emp_id) DO UPDATE
SET org_path   = EXCLUDED.org_path,
    updated_at = NOW();""",
    reasoning=(
        "Postgres doesn't need a USING source at all when the MERGE is a "
        "single-row upsert — INSERT ... ON CONFLICT expresses the same "
        "intent more compactly. The target column must have a UNIQUE or "
        "PRIMARY KEY constraint on `emp_id` for ON CONFLICT to match. "
        "Replace SYSDATE with NOW() (or CURRENT_TIMESTAMP). If the Oracle "
        "MERGE uses WHEN NOT MATCHED BY SOURCE to delete rows, that has "
        "no direct PG equivalent and must be split into a separate DELETE."
    ),
)

_CONNECT_BY = _Ex(
    title="CONNECT BY → WITH RECURSIVE CTE",
    oracle="""SELECT employee_id, last_name, LEVEL AS lvl
  FROM employees
  START WITH employee_id = :root_id
  CONNECT BY PRIOR manager_id = employee_id
  ORDER SIBLINGS BY last_name;""",
    postgres="""WITH RECURSIVE org_tree AS (
    SELECT employee_id, manager_id, last_name, 1 AS lvl
      FROM employees
     WHERE employee_id = :root_id
    UNION ALL
    SELECT e.employee_id, e.manager_id, e.last_name, t.lvl + 1
      FROM employees e
      JOIN org_tree t ON e.employee_id = t.manager_id
)
SELECT employee_id, last_name, lvl
  FROM org_tree
 ORDER BY lvl, last_name;""",
    reasoning=(
        "CONNECT BY PRIOR x = y translates to a recursive CTE: the anchor "
        "term is the START WITH predicate, the recursive term joins the "
        "working set back to the base table on the PRIOR/current pair. "
        "LEVEL becomes an explicit counter column. ORDER SIBLINGS BY has "
        "no direct equivalent — ordering by (lvl, last_name) gives the "
        "same visual shape for most cases; if you need strict sibling "
        "grouping, carry a path-array and sort by that."
    ),
)

_AUTONOMOUS_TXN = _Ex(
    title="AUTONOMOUS_TRANSACTION → redesign with dblink or separate connection",
    oracle="""CREATE OR REPLACE PROCEDURE log_audit(p_msg VARCHAR2) IS
    PRAGMA AUTONOMOUS_TRANSACTION;
BEGIN
    INSERT INTO audit_log(msg, ts) VALUES (p_msg, SYSDATE);
    COMMIT;
END;""",
    postgres="""-- Option A: use dblink to open a second connection that
-- commits independently of the caller's transaction.
CREATE OR REPLACE PROCEDURE log_audit(p_msg TEXT) AS $$
DECLARE
    dsn TEXT := 'dbname=' || current_database();
BEGIN
    PERFORM dblink_exec(
        dsn,
        format('INSERT INTO audit_log(msg, ts) VALUES (%L, NOW())', p_msg)
    );
END;
$$ LANGUAGE plpgsql;

-- Option B (preferred for new designs): move audit writes to an async
-- worker via LISTEN/NOTIFY or a queue (pgmq). The caller's transaction
-- stays clean and audit delivery becomes at-least-once instead of
-- transactional.""",
    reasoning=(
        "PostgreSQL has no AUTONOMOUS_TRANSACTION equivalent. The two "
        "honest translations are: (A) dblink, which opens a second "
        "backend connection whose COMMIT is independent — cheap to "
        "translate but the extra connection has non-trivial cost under "
        "load; or (B) push the audit write to a queue and consume it "
        "asynchronously. Option B is usually the right long-term "
        "design. Flag this for architectural review — do not auto-apply."
    ),
    confidence="needs-review",
)

_DBMS_SCHEDULER = _Ex(
    title="DBMS_SCHEDULER.CREATE_JOB → pg_cron",
    oracle="""BEGIN
    DBMS_SCHEDULER.CREATE_JOB (
        job_name        => 'REBUILD_EMPLOYEE_AUDIT',
        job_type        => 'PLSQL_BLOCK',
        job_action      => 'BEGIN hr.sync_employee_audit(NULL); END;',
        repeat_interval => 'FREQ=DAILY;BYHOUR=2',
        enabled         => TRUE
    );
END;""",
    postgres="""-- Requires the pg_cron extension (available on most managed
-- Postgres services including RDS, Aurora, Supabase, Crunchy Bridge).
SELECT cron.schedule(
    'rebuild_employee_audit',
    '0 2 * * *',
    $$CALL hr.sync_employee_audit(NULL)$$
);""",
    reasoning=(
        "pg_cron stores schedules in a regular table and runs them on "
        "the Postgres instance itself. The Oracle calendar syntax "
        "(FREQ=DAILY;BYHOUR=2) becomes standard cron (0 2 * * *). If "
        "your managed Postgres doesn't offer pg_cron, fall back to an "
        "external scheduler (Airflow, cron on an app server, Kubernetes "
        "CronJob) invoking psql. Do not bake cron schedules into "
        "application code — they'll fan out of control across deploys."
    ),
    confidence="medium",
)

_OUTER_JOIN_PLUS = _Ex(
    title="Legacy (+) outer-join → ANSI LEFT JOIN",
    oracle="""SELECT e.last_name, d.department_name
  FROM employees e, departments d
 WHERE e.department_id = d.department_id(+);""",
    postgres="""SELECT e.last_name, d.department_name
  FROM employees e
  LEFT JOIN departments d ON e.department_id = d.department_id;""",
    reasoning=(
        "Oracle's (+) on the right side of a predicate means 'the row "
        "on the other side is optional' — i.e. a LEFT OUTER JOIN on that "
        "direction. Moving to ANSI JOIN syntax is mechanical but error-"
        "prone when predicates mix join conditions with filter conditions; "
        "split any WHERE clauses that reference only one side into ON "
        "clauses to preserve outer-join semantics."
    ),
)

_ROWNUM = _Ex(
    title="ROWNUM pagination → LIMIT / OFFSET or ROW_NUMBER()",
    oracle="""SELECT *
  FROM (
    SELECT e.*, ROWNUM rn
      FROM employees e
     ORDER BY hire_date DESC
  )
 WHERE rn BETWEEN 21 AND 40;""",
    postgres="""SELECT *
  FROM employees
 ORDER BY hire_date DESC
 LIMIT 20 OFFSET 20;""",
    reasoning=(
        "ROWNUM in Oracle is assigned before ORDER BY, which is why the "
        "classic pattern uses a subquery to force the order first. "
        "Postgres evaluates LIMIT/OFFSET after ORDER BY by construction, "
        "so the subquery disappears. If you need the row number as a "
        "column (not just pagination), use ROW_NUMBER() OVER (ORDER BY ...) "
        "instead."
    ),
)

_VPD_POLICY = _Ex(
    title="VPD policy → Row-Level Security (RLS)",
    oracle="""-- Oracle VPD function + policy
CREATE OR REPLACE FUNCTION tenant_filter(
    schema_name VARCHAR2, table_name VARCHAR2
) RETURN VARCHAR2 IS
BEGIN
    RETURN 'tenant_id = SYS_CONTEXT(''app_ctx'', ''tenant_id'')';
END;

BEGIN
    DBMS_RLS.ADD_POLICY(
        object_schema   => 'HR',
        object_name     => 'EMPLOYEES',
        policy_name     => 'tenant_iso',
        function_schema => 'HR',
        policy_function => 'TENANT_FILTER',
        statement_types => 'SELECT,UPDATE,DELETE'
    );
END;""",
    postgres="""-- Postgres RLS — enable, then define a policy expression.
ALTER TABLE hr.employees ENABLE ROW LEVEL SECURITY;

CREATE POLICY tenant_iso ON hr.employees
    USING (tenant_id = current_setting('app.tenant_id')::int);

-- Application sets the tenant context per session:
--   SET app.tenant_id = '42';
-- (use SET LOCAL inside a transaction for per-request scoping.)""",
    reasoning=(
        "RLS maps cleanly onto VPD — both filter rows based on a "
        "predicate evaluated per query. SYS_CONTEXT becomes "
        "current_setting. The main gotcha: table owners bypass RLS by "
        "default; use ALTER TABLE ... FORCE ROW LEVEL SECURITY if your "
        "application connects as the owner. Separate policies are "
        "usually cleaner than one policy with OR branches — you get "
        "readable CREATE POLICY names for each case."
    ),
    confidence="medium",
)

_DBLINK = _Ex(
    title="Database links → postgres_fdw",
    oracle="""SELECT e.last_name, d.department_name
  FROM employees@prod_link e
  JOIN departments d ON e.department_id = d.department_id;""",
    postgres="""-- One-time setup per remote database:
CREATE EXTENSION IF NOT EXISTS postgres_fdw;
CREATE SERVER prod_server
    FOREIGN DATA WRAPPER postgres_fdw
    OPTIONS (host 'prod.db.internal', dbname 'hafen', port '5432');
CREATE USER MAPPING FOR current_user
    SERVER prod_server OPTIONS (user 'reader', password :pw);

-- Import the remote table(s) into the local schema:
IMPORT FOREIGN SCHEMA public
    LIMIT TO (employees)
    FROM SERVER prod_server INTO ext_prod;

-- Query is then identical to local:
SELECT e.last_name, d.department_name
  FROM ext_prod.employees e
  JOIN departments d ON e.department_id = d.department_id;""",
    reasoning=(
        "postgres_fdw gives you dblink-style cross-database joins, but "
        "with proper planner integration — predicates get pushed down. "
        "For the narrow case of dblink-ing into another Oracle during "
        "cutover, use oracle_fdw instead. Don't leave FDWs pointing at "
        "live Oracle in steady state — once the migration completes, "
        "drop the server and mapping so you're not keeping an Oracle "
        "license alive for a single fact table."
    ),
    confidence="medium",
)

_BULK_COLLECT = _Ex(
    title="BULK COLLECT INTO → array aggregation",
    oracle="""DECLARE
    TYPE name_tab IS TABLE OF VARCHAR2(100);
    v_names name_tab;
BEGIN
    SELECT last_name BULK COLLECT INTO v_names
      FROM employees
     WHERE department_id = :dept_id;

    FOR i IN 1..v_names.COUNT LOOP
        do_something(v_names(i));
    END LOOP;
END;""",
    postgres="""DO $$
DECLARE
    v_names TEXT[];
    v_name  TEXT;
BEGIN
    SELECT ARRAY(
        SELECT last_name FROM employees WHERE department_id = :dept_id
    ) INTO v_names;

    FOREACH v_name IN ARRAY v_names LOOP
        PERFORM do_something(v_name);
    END LOOP;
END;
$$;""",
    reasoning=(
        "For read-then-iterate, PL/pgSQL FOREACH over an array matches "
        "the Oracle pattern. For most new code though, a set-based "
        "approach is better: PERFORM do_something(last_name) directly "
        "inside a FOR rec IN SELECT ... loop, or rewrite do_something "
        "to take a relation input and run the whole thing as a single "
        "UPDATE/INSERT. Array round-trips are slower than set-based SQL "
        "for anything over ~10K rows."
    ),
    confidence="medium",
)

_UTL_FILE = _Ex(
    title="UTL_FILE filesystem I/O → COPY or application layer",
    oracle="""DECLARE
    f UTL_FILE.FILE_TYPE;
BEGIN
    f := UTL_FILE.FOPEN('EXPORT_DIR', 'employees.csv', 'W');
    FOR rec IN (SELECT employee_id, last_name FROM employees) LOOP
        UTL_FILE.PUT_LINE(f, rec.employee_id || ',' || rec.last_name);
    END LOOP;
    UTL_FILE.FCLOSE(f);
END;""",
    postgres="""-- Option A: COPY from inside SQL (runs server-side, requires
-- pg_write_server_files role or COPY TO PROGRAM privileges).
COPY (SELECT employee_id, last_name FROM employees)
  TO '/var/lib/postgresql/export/employees.csv'
  WITH CSV;

-- Option B (recommended): do the export from the application,
-- \\copy client-side over psql, or use pg_dump. Database servers are
-- a bad place for filesystem I/O — they shouldn't see your export
-- directory at all in modern deploys.""",
    reasoning=(
        "UTL_FILE belongs to an era when the database server was the "
        "application. In Postgres, server-side COPY works, but most "
        "managed Postgres services (RDS, Cloud SQL, Supabase) lock it "
        "down — you'll need an application-layer export (psql \\copy, "
        "a script using COPY TO STDOUT, or pg_dump). Flag UTL_FILE "
        "sites for architectural review; they're usually a sign of "
        "logic that should live outside the database entirely."
    ),
    confidence="needs-review",
)


_NVL = _Ex(
    title="NVL / NVL2 → COALESCE / CASE",
    oracle="""SELECT employee_id,
       NVL(commission_pct, 0) AS commission_pct,
       NVL2(manager_id, 'managed', 'unmanaged') AS reports_to_status
  FROM employees;""",
    postgres="""SELECT employee_id,
       COALESCE(commission_pct, 0) AS commission_pct,
       CASE WHEN manager_id IS NOT NULL THEN 'managed'
            ELSE 'unmanaged'
       END AS reports_to_status
  FROM employees;""",
    reasoning=(
        "NVL is just two-argument COALESCE — direct swap, same NULL "
        "semantics. NVL2(expr, a, b) doesn't have a single-function "
        "equivalent, but `CASE WHEN expr IS NOT NULL THEN a ELSE b END` "
        "is the canonical translation. Watch for type coercion: NVL "
        "implicitly converts both args to the type of the first; "
        "COALESCE returns the first non-NULL with no implicit conversion, "
        "so explicitly cast if the original relied on Oracle's coercion."
    ),
)


_DECODE = _Ex(
    title="DECODE → CASE expression",
    oracle="""SELECT employee_id,
       DECODE(department_id,
              10, 'Admin',
              20, 'Engineering',
              30, 'Sales',
                  'Other'
       ) AS dept_label
  FROM employees;""",
    postgres="""SELECT employee_id,
       CASE department_id
            WHEN 10 THEN 'Admin'
            WHEN 20 THEN 'Engineering'
            WHEN 30 THEN 'Sales'
            ELSE 'Other'
       END AS dept_label
  FROM employees;""",
    reasoning=(
        "DECODE(expr, k1, v1, k2, v2, ..., default) is positional — "
        "every odd-indexed arg after `expr` is a key, every even-indexed "
        "is its value, and a final unpaired arg is the default. Translate "
        "directly to a `CASE expr WHEN k THEN v ... ELSE default END`. "
        "One subtle difference: DECODE treats two NULLs as equal, while "
        "ANSI CASE does NOT — `CASE NULL WHEN NULL THEN ...` never "
        "matches. If the original relied on NULL-equals-NULL semantics, "
        "use `CASE WHEN expr IS NULL OR expr = ... THEN ...` instead."
    ),
)


_INTERVAL = _Ex(
    title="INTERVAL types + arithmetic → PG INTERVAL (mostly drop-in)",
    oracle="""-- Oracle INTERVAL columns and arithmetic
CREATE TABLE leases (
    lease_id    NUMBER(10) PRIMARY KEY,
    start_date  DATE,
    duration    INTERVAL YEAR(2) TO MONTH,
    notice      INTERVAL DAY(3) TO SECOND
);

SELECT lease_id, start_date + duration AS end_date,
       SYSDATE - notice AS notice_starts
  FROM leases;

INSERT INTO leases VALUES (
    1, DATE '2026-01-01',
    INTERVAL '2-6' YEAR TO MONTH,
    INTERVAL '30 12:00:00.0' DAY TO SECOND
);""",
    postgres="""CREATE TABLE leases (
    lease_id    BIGINT PRIMARY KEY,
    start_date  TIMESTAMP,
    duration    INTERVAL,
    notice      INTERVAL
);

SELECT lease_id, start_date + duration AS end_date,
       NOW() - notice AS notice_starts
  FROM leases;

INSERT INTO leases VALUES (
    1, TIMESTAMP '2026-01-01 00:00:00',
    INTERVAL '2 years 6 months',
    INTERVAL '30 days 12 hours'
);""",
    reasoning=(
        "Postgres has a single INTERVAL type that stores months + days + "
        "microseconds; Oracle splits its INTERVAL into YEAR-TO-MONTH and "
        "DAY-TO-SECOND variants with explicit precision. Drop the "
        "subtype qualifier on the column declaration — PG INTERVAL "
        "covers both ranges. Literal syntax differs: Oracle uses ANSI "
        "interval literals (`INTERVAL '2-6' YEAR TO MONTH`); Postgres "
        "accepts a casual phrase form (`INTERVAL '2 years 6 months'`) "
        "that's easier to read and write. Arithmetic between DATE/"
        "TIMESTAMP and INTERVAL works identically. SYSDATE → NOW() "
        "(or CURRENT_TIMESTAMP) per the rest of this conversion guide."
    ),
)


_EXAMPLES: Dict[ConstructTag, _Ex] = {
    ConstructTag.MERGE: _MERGE,
    ConstructTag.CONNECT_BY: _CONNECT_BY,
    ConstructTag.HIERARCHICAL_PSEUDOCOLUMN: _CONNECT_BY,  # share the CTE example
    ConstructTag.AUTONOMOUS_TXN: _AUTONOMOUS_TXN,
    ConstructTag.DBMS_SCHEDULER: _DBMS_SCHEDULER,
    ConstructTag.OUTER_JOIN_PLUS: _OUTER_JOIN_PLUS,
    ConstructTag.ROWNUM: _ROWNUM,
    ConstructTag.VPD_POLICY: _VPD_POLICY,
    ConstructTag.DBLINK: _DBLINK,
    ConstructTag.BULK_COLLECT: _BULK_COLLECT,
    ConstructTag.UTL_FILE: _UTL_FILE,
    ConstructTag.NVL: _NVL,
    ConstructTag.DECODE: _DECODE,
    ConstructTag.INTERVAL: _INTERVAL,
}


# ─── Handler ─────────────────────────────────────────────────────────────────


@router.get("/{tag}", response_model=ConversionExample)
async def get_canonical_conversion(tag: str) -> ConversionExample:
    """Return a hand-curated Oracle→PG conversion for the given tag.

    Tag names come straight from the assessment response
    (`risks[].tag`), so the frontend doesn't need its own map."""
    try:
        parsed = ConstructTag(tag)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"unknown construct tag: {tag!r}",
        )

    ex = _EXAMPLES.get(parsed)
    if ex is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=(
                f"no canonical example available for {tag!r} yet. "
                "Paid tier runs AI conversion on your actual snippet."
            ),
        )

    return ConversionExample(
        tag=parsed.value,
        title=ex.title,
        oracle=ex.oracle,
        postgres=ex.postgres,
        reasoning=ex.reasoning,
        confidence=ex.confidence,
    )


# ─── Live BYOK conversion (POST) ─────────────────────────────────────────────
#
# Takes the operator's actual Oracle snippet and returns a Claude-generated
# Postgres conversion, grounded in the canonical example above as a few-shot
# anchor. The BYOK Anthropic key is pulled from the InstanceSettings singleton
# (operator-set via /settings) or falls back to the env var.
#
# No auth — this is the self-hosted product, the admin is whoever can reach
# localhost:8000. License-tier gating lands in task #22 once the verifier
# module is in place.


class LiveConvertRequest(BaseModel):
    """The caller's actual Oracle snippet plus optional surrounding
    context (e.g. the enclosing procedure signature) to improve the
    conversion. Keep the total under a few KB — anything larger should
    be chunked by the caller."""

    snippet: str = Field(..., min_length=1, max_length=20_000)
    context: str | None = Field(default=None, max_length=5_000)


_SYSTEM_PROMPT = """You are an expert Oracle DBA and PostgreSQL engineer helping a team migrate PL/SQL to PL/pgSQL.

For the given Oracle snippet and construct tag, produce a JSON object with exactly these fields:
  "oracle":     the Oracle snippet, reproduced verbatim (no edits)
  "postgres":   the equivalent Postgres / PL/pgSQL rewrite
  "reasoning":  2-5 sentences explaining the translation choices,
                calling out anything that needs human review
  "confidence": one of "high" | "medium" | "needs-review"

Rules:
  * Output ONLY the JSON object, no prose before or after.
  * Preserve the caller's identifiers and literals — do not invent names.
  * When a construct has no direct PG equivalent (autonomous transactions,
    some dbms_* packages), set confidence to "needs-review" and explain.
  * Prefer set-based SQL over row-by-row PL/pgSQL where the original permits it.
  * Use NOW() / CURRENT_TIMESTAMP instead of SYSDATE.
"""


@router.post("/{tag}", response_model=ConversionExample)
def convert_live(
    tag: str,
    body: LiveConvertRequest,
    request: Request,
    db: Session = Depends(get_db),
    # Authenticated admin or operator — viewers shouldn't burn tokens.
    caller=Depends(require_role("admin", "operator")),
    # Pro-tier gate. A missing / invalid license returns 402 with a
    # JSON body containing { error, feature, upgrade_url } that the UI
    # parses to route operators to /settings/instance.
    _license: LicenseStatus = Depends(require_feature("ai_conversion")),
) -> ConversionExample:
    """Run Claude against the caller's snippet for the given construct
    tag. Uses the BYOK key (UI setting > env var).

    Returns the same ConversionExample shape as the canonical GET so
    the frontend can render them interchangeably."""
    try:
        parsed = ConstructTag(tag)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"unknown construct tag: {tag!r}",
        )

    api_key = get_effective_anthropic_key(db)
    if not api_key:
        # 412 because the request is fine but the instance isn't configured
        # yet. Surface this via the "Anthropic key not configured" message
        # and let the UI route the user to /settings.
        raise HTTPException(
            status_code=status.HTTP_412_PRECONDITION_FAILED,
            detail=(
                "No Anthropic API key configured. Set one in /settings or "
                "via the ANTHROPIC_API_KEY env var to enable live AI conversion."
            ),
        )

    # Anchor with the canonical example when we have one — gives Claude a
    # known-good shape for this construct and reduces drift across calls.
    anchor = _EXAMPLES.get(parsed)
    anchor_block = (
        f"\n\nReference conversion for tag {parsed.value}:\n"
        f"Oracle:\n{anchor.oracle}\n\nPostgres:\n{anchor.postgres}\n\n"
        f"Reasoning: {anchor.reasoning}\n"
        if anchor is not None
        else ""
    )
    context_block = f"\n\nEnclosing context:\n{body.context}\n" if body.context else ""

    user_prompt = (
        f"Construct tag: {parsed.value}"
        f"{context_block}"
        f"\n\nCaller's Oracle snippet to convert:\n{body.snippet}"
        f"{anchor_block}"
    )

    try:
        client = AIClient(api_key=api_key, feature="live-convert")
        raw = client.complete_json(system=_SYSTEM_PROMPT, user=user_prompt)
    except Exception as exc:  # noqa: BLE001
        logger.exception("live conversion failed for tag=%s", parsed.value)
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"AI conversion failed: {exc}",
        )

    log_event(
        db,
        request=request,
        user=caller,
        action="convert.live",
        resource_type="construct",
        resource_id=parsed.value,
        details={"snippet_len": len(body.snippet)},
    )

    # `raw` is already a dict from complete_json; fall back gracefully
    # if Claude skipped a field.
    converted = str(raw.get("postgres") or "")
    confidence = _normalize_confidence(raw.get("confidence"))

    # Gate Claude's output through the syntax validator before handing
    # it back to the operator. Validation errors are unrecoverable
    # (unbalanced delimiters, missing LANGUAGE clause, hallucinated
    # Oracle remnants in the "converted" output) — when any are
    # present we force-downgrade confidence to needs-review so the UI
    # bannerizes the result and the operator doesn't paste it
    # unchanged into a production schema. Validation warnings (e.g.,
    # DATE without timezone hint) ride along but don't change confidence.
    validation = ConversionValidator().validate_conversion(
        original=body.snippet,
        converted=converted,
        construct_type=parsed.value,
    )
    if validation.errors:
        confidence = "needs-review"

    return ConversionExample(
        tag=parsed.value,
        title=(anchor.title if anchor else f"{parsed.value} → Postgres"),
        oracle=str(raw.get("oracle") or body.snippet),
        postgres=converted,
        reasoning=str(raw.get("reasoning") or ""),
        confidence=confidence,
        validation_errors=list(validation.errors),
        validation_warnings=list(validation.warnings),
    )


def _normalize_confidence(value) -> str:
    """Claude occasionally returns variants like 'medium-high' — coerce
    to the three-level scale the UI renders."""
    if not value:
        return "medium"
    v = str(value).lower().strip()
    if "review" in v or "low" in v:
        return "needs-review"
    if "high" in v:
        return "high"
    return "medium"
