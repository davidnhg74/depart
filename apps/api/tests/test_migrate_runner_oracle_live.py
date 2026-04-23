"""End-to-end Runner test: live Oracle → live Postgres.

This is the test that converts "all the unit tests pass" into "we
actually moved rows from a real Oracle to a real Postgres."

What it exercises in one pass:
  • Source-side introspection against Oracle (HR demo schema)
  • Type mapping (NUMBER, VARCHAR2, DATE → PG equivalents)
  • DDL generation + apply on the target
  • The planner ordering (no FKs in the small subset, but the wiring
    is the same as for the FK case)
  • Keyset reads from Oracle including the composite-PK code path
    (HR.JOB_HISTORY has a 2-column PK — the exact construct we
    just hardened in keyset.py)
  • Binary COPY into Postgres
  • Merkle-hash verification across the wire

Skipped automatically when:
  • Oracle is not reachable at ORACLE_TEST_URL (default points at
    the user's `oracle-free` container as system/oracle)
  • Postgres test DB is not reachable
"""

from __future__ import annotations

import os
import uuid

import psycopg
import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import DatabaseError, OperationalError
from sqlalchemy.orm import sessionmaker

from src.config import settings as env_settings
from src.migrate.ddl import apply_ddl, generate_schema_ddl, map_oracle_type
from src.migrate.introspect import introspect
from src.migrate.keyset import Dialect
from src.migrate.planner import plan_load_order
from src.migrate.runner import Runner


ORACLE_URL = os.environ.get(
    "ORACLE_TEST_URL",
    "oracle+oracledb://system:oracle@localhost:1521/?service_name=FREEPDB1",
)


# Tables we drive through the runner. Picked to exercise:
#   REGIONS — single-column PK, 5 rows, no internal FKs (smoke)
#   JOB_HISTORY — composite PK (employee_id, start_date), 10 rows
#                 (the exact case we just guarded against NULL halts)
#
# We deliberately leave EMPLOYEES out of this initial pass — its
# self-referential FK on manager_id needs the NULL-then-UPDATE pass
# the runner advertises but which is a separate code path worth
# isolating in its own test.
TARGET_TABLES = ["REGIONS", "JOB_HISTORY"]


# ─── Fixtures ────────────────────────────────────────────────────────────────


@pytest.fixture(scope="module")
def oracle_session():
    try:
        engine = create_engine(ORACLE_URL)
        Session = sessionmaker(bind=engine)
        s = Session()
        s.execute(text("SELECT 1 FROM dual"))
    except (DatabaseError, OperationalError, Exception) as e:  # noqa: BLE001
        pytest.skip(f"Oracle not reachable at {ORACLE_URL!r}: {e}")
    yield s
    s.close()
    engine.dispose()


@pytest.fixture
def pg_url():
    # Strip the SQLAlchemy driver prefix so psycopg can use it directly.
    return env_settings.database_url.replace("postgresql+psycopg://", "postgresql://")


@pytest.fixture
def pg_target_schema(pg_url):
    """Throwaway PG schema for the target side. Dropped after the test
    regardless of pass/fail so re-runs always start clean."""
    schema = f"runner_e2e_{uuid.uuid4().hex[:8]}"
    conn = psycopg.connect(pg_url)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(f'CREATE SCHEMA "{schema}"')
    conn.close()
    yield schema
    # Cleanup
    conn = psycopg.connect(pg_url)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')
    conn.close()


@pytest.fixture
def pg_session(pg_url):
    engine = create_engine(env_settings.database_url)
    Session = sessionmaker(bind=engine)
    s = Session()
    yield s
    s.close()
    engine.dispose()


@pytest.fixture
def pg_raw_conn(pg_url):
    conn = psycopg.connect(pg_url)
    conn.autocommit = True
    yield conn
    conn.close()


# ─── The actual test ─────────────────────────────────────────────────────────


def test_oracle_to_postgres_runner_roundtrip(
    oracle_session, pg_target_schema, pg_session, pg_raw_conn, pg_url
):
    """Full Oracle → Postgres migration of a small HR table set.

    Asserts:
      1. DDL was generated and applied (target tables exist).
      2. Runner.execute() completed without exception.
      3. Per-table row counts match between source and target.
      4. Per-table Merkle hashes match (verified=True for every table).
      5. The composite-PK table (JOB_HISTORY) actually round-tripped
         — this is the case our keyset NULL guard would have broken
         silently before today's fix.
    """
    # 1. Introspect Oracle, scope to the tables we want to move.
    schema = introspect(oracle_session, Dialect.ORACLE, "HR")
    schema.tables = [t for t in schema.tables if t.name in TARGET_TABLES]
    assert {t.name for t in schema.tables} == set(TARGET_TABLES), (
        f"expected to find {TARGET_TABLES} in HR; got "
        f"{[t.name for t in schema.tables]}"
    )

    # 2. Build TableSpecs rewriting the destination schema to our
    #    throwaway PG schema. Build_specs filters out tables without
    #    a PK; both our targets have one (JOB_HISTORY's is composite).
    specs = schema.build_specs(target_schema=pg_target_schema)
    assert len(specs) == len(TARGET_TABLES)

    # 3. Generate + apply CREATE TABLE statements on the PG target.
    cols_by_target = {
        s.target_table.qualified(): schema.column_metadata[
            s.source_table.qualified()
        ]
        for s in specs.values()
    }
    pks_by_target = {
        s.target_table.qualified(): s.pk_columns for s in specs.values()
    }
    target_refs = [s.target_table for s in specs.values()]
    ddl_stmts = generate_schema_ddl(
        target_refs, cols_by_target, pks_by_target, map_type=map_oracle_type
    )
    # apply_ddl wants a non-autocommit connection. Open a dedicated one.
    ddl_conn = psycopg.connect(pg_url)
    try:
        apply_ddl(ddl_conn, ddl_stmts)
        ddl_conn.commit()
    finally:
        ddl_conn.close()

    # Sanity check: tables actually exist on the target now.
    with pg_raw_conn.cursor() as cur:
        cur.execute(
            "SELECT tablename FROM pg_tables WHERE schemaname = %s ORDER BY tablename",
            (pg_target_schema,),
        )
        present = [r[0] for r in cur.fetchall()]
    # Quoted identifiers preserve Oracle's uppercase through to PG.
    assert sorted(present) == sorted(TARGET_TABLES)

    # 4. Build the load plan. With no FKs in our subset, both tables
    #    end up in independent single-table groups.
    plan = plan_load_order(target_refs, [])
    assert len(plan.flat_tables()) == len(TARGET_TABLES)

    # 5. Run it.
    runner = Runner(
        source_session=oracle_session,
        target_session=pg_session,
        target_pg_conn=pg_raw_conn,
        source_dialect=Dialect.ORACLE,
        batch_size=100,  # small enough to force multiple batches on
                        # bigger tables; harmless for these small ones
    )
    result = runner.execute(plan, specs)

    # 6. Per-table row counts match what we expected from the HR schema
    #    (5 regions, 10 job_history rows in the standard demo).
    row_counts = {qn: tr.rows_copied for qn, tr in result.tables.items()}
    assert row_counts[f"{pg_target_schema}.REGIONS"] == 5
    assert row_counts[f"{pg_target_schema}.JOB_HISTORY"] == 10

    # 7. Data really landed — count + spot-check on the PG side
    #    directly, outside the runner's bookkeeping.
    with pg_raw_conn.cursor() as cur:
        cur.execute(f'SELECT COUNT(*) FROM "{pg_target_schema}"."REGIONS"')
        assert cur.fetchone()[0] == 5
        cur.execute(f'SELECT COUNT(*) FROM "{pg_target_schema}"."JOB_HISTORY"')
        assert cur.fetchone()[0] == 10

        # Spot-check a value. region_id 10 = "Europe" in the standard
        # HR demo. Catches column ordering / encoding bugs that a row
        # count alone wouldn't.
        cur.execute(
            f'SELECT "REGION_NAME" FROM "{pg_target_schema}"."REGIONS" '
            f'WHERE "REGION_ID" = 10'
        )
        assert cur.fetchone()[0] == "Europe"

    # 8. Merkle verification — both per-table and as a whole.
    #    Cross-driver canonicalization (verify.py:_canonical) collapses
    #    int/Decimal, datetime variants, and bytes flavors into one
    #    bucket per logical type so oracledb/psycopg type differences
    #    don't fail an otherwise-correct migration. If this regresses,
    #    the discrepancy field will name the table that broke.
    failures = {
        qn: tr.discrepancy
        for qn, tr in result.tables.items()
        if not tr.verified
    }
    assert not failures, f"verification failures: {failures}"
    assert result.all_verified


# ─── Self-referential FK (EMPLOYEES.manager_id → EMPLOYEES.id) ───────────────


def test_self_referential_fk_round_trips(
    oracle_session, pg_target_schema, pg_session, pg_raw_conn, pg_url
):
    """EMPLOYEES has `manager_id → employee_id` — a self-FK that's the
    canonical stress test for data movement.

    Documents the runner's *actual* handling today:

    1. Default flow — `generate_create_table` does NOT emit any FK
       constraints, so the self-FK simply doesn't exist on the target
       at load time. COPY never conflicts with it. Verification hashes
       the full EMPLOYEES row (including manager_id) on both sides,
       so data correctness is preserved even though the constraint
       isn't enforced until an operator adds it later.

    2. What ISN'T tested here: an operator pre-creating the target
       with the self-FK installed. The runner has NO NULL-then-UPDATE
       pass — `collect_self_referential_fks()` exists in planner.py
       but no consumer calls it. With an installed self-FK and
       keyset-ordered loads, success depends on whether employee_ids
       happen to be hierarchically ordered (Steven King at 100, his
       reports at 101+). The standard HR data is, by luck.
    """
    schema = introspect(oracle_session, Dialect.ORACLE, "HR")
    schema.tables = [t for t in schema.tables if t.name == "EMPLOYEES"]
    specs = schema.build_specs(target_schema=pg_target_schema)
    assert len(specs) == 1

    cols_by_target = {
        s.target_table.qualified(): schema.column_metadata[s.source_table.qualified()]
        for s in specs.values()
    }
    pks_by_target = {
        s.target_table.qualified(): s.pk_columns for s in specs.values()
    }
    target_refs = [s.target_table for s in specs.values()]
    ddl_stmts = generate_schema_ddl(
        target_refs, cols_by_target, pks_by_target, map_type=map_oracle_type
    )

    # Confirm what we claim above: no FK clause in the generated DDL.
    for stmt in ddl_stmts:
        assert "FOREIGN KEY" not in stmt.upper(), (
            "generate_create_table unexpectedly emitted a FOREIGN KEY — "
            "this test documented the opposite behavior."
        )

    ddl_conn = psycopg.connect(pg_url)
    try:
        apply_ddl(ddl_conn, ddl_stmts)
        ddl_conn.commit()
    finally:
        ddl_conn.close()

    plan = plan_load_order(target_refs, [])
    runner = Runner(
        source_session=oracle_session,
        target_session=pg_session,
        target_pg_conn=pg_raw_conn,
        source_dialect=Dialect.ORACLE,
        batch_size=50,  # exercises multiple batches against 107 rows
    )
    result = runner.execute(plan, specs)

    # All 107 HR employees copied, merkle hashes agree.
    target_result = result.tables[f"{pg_target_schema}.EMPLOYEES"]
    assert target_result.rows_copied == 107
    assert target_result.verified, target_result.discrepancy

    with pg_raw_conn.cursor() as cur:
        cur.execute(f'SELECT COUNT(*) FROM "{pg_target_schema}"."EMPLOYEES"')
        assert cur.fetchone()[0] == 107

        # Steven King (employee_id=100) is the top of the tree —
        # manager_id is NULL. Everyone else has a manager. Spot-check
        # that both shapes landed correctly.
        cur.execute(
            f'SELECT "LAST_NAME", "MANAGER_ID" '
            f'FROM "{pg_target_schema}"."EMPLOYEES" '
            f'WHERE "EMPLOYEE_ID" = 100'
        )
        last_name, manager_id = cur.fetchone()
        assert last_name == "King"
        assert manager_id is None

        cur.execute(
            f'SELECT COUNT(*) FROM "{pg_target_schema}"."EMPLOYEES" '
            f'WHERE "MANAGER_ID" IS NOT NULL'
        )
        assert cur.fetchone()[0] == 106  # everyone except King


def test_self_fk_installed_on_target_uses_null_then_update_pass(
    oracle_session, pg_target_schema, pg_session, pg_raw_conn, pg_url
):
    """Live proof of the runner's NULL-then-UPDATE path.

    The target is pre-created with the self-FK installed, so a naive
    COPY would have to load every parent row before any child row
    referencing it. That happens to work for the HR demo data
    (Steven King at id=100, reports at 101+) but breaks the moment a
    real schema has any forward reference. The runner's two-pass
    handling (NULL the FK during COPY, UPDATE it from source after)
    works for both cases — locked in here against a real Oracle.
    """
    schema = introspect(oracle_session, Dialect.ORACLE, "HR")
    schema.tables = [t for t in schema.tables if t.name == "EMPLOYEES"]
    specs = schema.build_specs(target_schema=pg_target_schema)

    cols_by_target = {
        s.target_table.qualified(): schema.column_metadata[s.source_table.qualified()]
        for s in specs.values()
    }
    pks_by_target = {
        s.target_table.qualified(): s.pk_columns for s in specs.values()
    }
    target_refs = [s.target_table for s in specs.values()]
    ddl_stmts = generate_schema_ddl(
        target_refs, cols_by_target, pks_by_target, map_type=map_oracle_type
    )

    ddl_conn = psycopg.connect(pg_url)
    try:
        apply_ddl(ddl_conn, ddl_stmts)
        # Manually install the self-FK that Hafen's DDL emitter omits —
        # this is what an operator who pre-creates the target schema
        # would have. Without the runner's NULL-then-UPDATE pass, the
        # COPY would FK-fail for any row whose manager_id points at a
        # not-yet-loaded employee_id.
        with ddl_conn.cursor() as cur:
            cur.execute(
                f'ALTER TABLE "{pg_target_schema}"."EMPLOYEES" '
                f'ADD CONSTRAINT emp_mgr_fk FOREIGN KEY ("MANAGER_ID") '
                f'REFERENCES "{pg_target_schema}"."EMPLOYEES" ("EMPLOYEE_ID")'
            )
        ddl_conn.commit()
    finally:
        ddl_conn.close()

    plan = plan_load_order(target_refs, [])
    target_qn = f"{pg_target_schema}.EMPLOYEES"
    runner = Runner(
        source_session=oracle_session,
        target_session=pg_session,
        target_pg_conn=pg_raw_conn,
        source_dialect=Dialect.ORACLE,
        batch_size=50,
        null_then_update_columns={target_qn: ["MANAGER_ID"]},
    )
    result = runner.execute(plan, specs)
    target_result = result.tables[target_qn]
    assert target_result.rows_copied == 107
    assert target_result.verified, target_result.discrepancy

    # Manager edges intact post-update — Steven King NULL, everyone
    # else points at a real employee.
    with pg_raw_conn.cursor() as cur:
        cur.execute(
            f'SELECT "MANAGER_ID" FROM "{pg_target_schema}"."EMPLOYEES" '
            f'WHERE "EMPLOYEE_ID" = 100'
        )
        assert cur.fetchone()[0] is None
        cur.execute(
            f'SELECT COUNT(*) FROM "{pg_target_schema}"."EMPLOYEES" '
            f'WHERE "MANAGER_ID" IS NOT NULL'
        )
        assert cur.fetchone()[0] == 106


# ─── BLOB round-trip via the CO demo schema ─────────────────────────────────


def test_blob_round_trips_via_co_products(
    oracle_session, pg_target_schema, pg_session, pg_raw_conn, pg_url
):
    """Drive a full Runner.execute() round-trip on a BLOB-bearing
    table. Exercises:

      * Oracle BLOB → PG BYTEA type mapping (ddl.py:_map_oracle_type)
      * Row materialization for LOB-typed columns (runner._materialize_value)
      * Merkle verification across the bytes payload (verify.py
        canonical form X:<hex>)
      * Mixed populated/NULL LOB columns: CO.PRODUCTS has all 46 rows
        populated for PRODUCT_DETAILS but ALL NULL for PRODUCT_IMAGE.
        Both shapes hash correctly only if the canonicalization handles
        bytes and None in a single row consistently.

    BLOB and CLOB go through the same oracledb.LOB / .read() path;
    proving BLOB closes the LOB-handling concern for both.
    """
    schema = introspect(oracle_session, Dialect.ORACLE, "CO")
    schema.tables = [t for t in schema.tables if t.name == "PRODUCTS"]
    assert len(schema.tables) == 1, "CO.PRODUCTS missing from your container"

    specs = schema.build_specs(target_schema=pg_target_schema)
    cols_by_target = {
        s.target_table.qualified(): schema.column_metadata[s.source_table.qualified()]
        for s in specs.values()
    }
    pks_by_target = {
        s.target_table.qualified(): s.pk_columns for s in specs.values()
    }
    target_refs = [s.target_table for s in specs.values()]
    ddl_stmts = generate_schema_ddl(
        target_refs, cols_by_target, pks_by_target, map_type=map_oracle_type
    )

    # Sanity-check: BLOB columns mapped to BYTEA in the generated DDL.
    create_stmt = next(s for s in ddl_stmts if "PRODUCTS" in s)
    assert "BYTEA" in create_stmt, (
        f"expected BLOB→BYTEA in DDL, got:\n{create_stmt}"
    )

    ddl_conn = psycopg.connect(pg_url)
    try:
        apply_ddl(ddl_conn, ddl_stmts)
        ddl_conn.commit()
    finally:
        ddl_conn.close()

    plan = plan_load_order(target_refs, [])
    runner = Runner(
        source_session=oracle_session,
        target_session=pg_session,
        target_pg_conn=pg_raw_conn,
        source_dialect=Dialect.ORACLE,
        batch_size=20,  # forces ~3 batches against 46 rows
    )
    result = runner.execute(plan, specs)

    target_result = result.tables[f"{pg_target_schema}.PRODUCTS"]
    assert target_result.rows_copied == 46
    # Critical assertion: verification passes despite the BLOB column.
    # Before the materializer + canonicalization fixes, this would
    # diverge because (a) repr() of an oracledb.LOB embeds an address,
    # and (b) bytes vs Decimal type-name mismatches between drivers.
    assert target_result.verified, target_result.discrepancy

    with pg_raw_conn.cursor() as cur:
        # Row count round-trip.
        cur.execute(f'SELECT COUNT(*) FROM "{pg_target_schema}"."PRODUCTS"')
        assert cur.fetchone()[0] == 46

        # PRODUCT_IMAGE is all-NULL in CO.PRODUCTS — confirm NULL LOBs
        # land as NULL on PG, not as empty bytes.
        cur.execute(
            f'SELECT COUNT(*) FROM "{pg_target_schema}"."PRODUCTS" '
            f'WHERE "PRODUCT_IMAGE" IS NULL'
        )
        assert cur.fetchone()[0] == 46

        # PRODUCT_DETAILS is fully populated; spot-check that one row's
        # bytes are non-empty and look BLOB-shaped.
        cur.execute(
            f'SELECT "PRODUCT_DETAILS" FROM "{pg_target_schema}"."PRODUCTS" '
            f'ORDER BY "PRODUCT_ID" LIMIT 1'
        )
        sample = cur.fetchone()[0]
        assert isinstance(sample, (bytes, memoryview))
        # CO.PRODUCTS.PRODUCT_DETAILS values are non-trivial JSON-ish
        # blobs; "small but not zero" is the right shape to assert.
        assert len(bytes(sample)) > 10


# ─── CLOB round-trip via a synthesized test table ────────────────────────────


_CLOB_TABLE = "HAFEN_CLOB_TEST"


@pytest.fixture
def synth_clob_table(oracle_session):
    """Create a single throwaway CLOB-bearing table in the user's
    Oracle (`system.HAFEN_CLOB_TEST`), populate with mixed NULL +
    non-NULL CLOB rows, drop it after the test.

    No user-data tables are touched — the table lives in the SYSTEM
    schema, has a name that won't collide with anything Oracle ships,
    and is dropped on teardown even if the test fails.

    Idempotent: drops any leftover from a previously-failed run before
    creating the new one."""
    drop_sql = f"BEGIN EXECUTE IMMEDIATE 'DROP TABLE system.{_CLOB_TABLE} PURGE'; EXCEPTION WHEN OTHERS THEN NULL; END;"
    oracle_session.execute(text(drop_sql))
    oracle_session.commit()

    create_sql = f"""
    CREATE TABLE system.{_CLOB_TABLE} (
        id    NUMBER(6) NOT NULL PRIMARY KEY,
        title VARCHAR2(50),
        body  CLOB
    )
    """
    oracle_session.execute(text(create_sql))
    # Three rows: short CLOB, long CLOB (forces oracledb's lazy
    # streaming path on the larger value), one NULL CLOB.
    long_text = "x" * 5000  # well past the inline-vs-out-of-line cutoff
    oracle_session.execute(
        text(f"INSERT INTO system.{_CLOB_TABLE} VALUES (1, 'short', 'hello world')")
    )
    oracle_session.execute(
        text(
            f"INSERT INTO system.{_CLOB_TABLE} VALUES (2, 'long', :body)"
        ),
        {"body": long_text},
    )
    oracle_session.execute(
        text(f"INSERT INTO system.{_CLOB_TABLE} VALUES (3, 'null-clob', NULL)")
    )
    oracle_session.commit()

    yield long_text

    # Teardown — drop unconditionally, swallowing errors so a partial
    # setup doesn't poison subsequent runs.
    oracle_session.execute(text(drop_sql))
    oracle_session.commit()


def test_clob_round_trips_via_synthesized_table(
    oracle_session,
    pg_target_schema,
    pg_session,
    pg_raw_conn,
    pg_url,
    synth_clob_table,
):
    """Drive a full Runner.execute() round-trip on a real CLOB column.

    BLOB has been proven via CO.PRODUCTS, but CLOB exercises a
    different oracledb internal path (text vs binary LOB), and the
    audit specifically called out the CLOB hazard. This test closes
    that loop end-to-end.

    Asserts:
      * 3 rows copied (mixed NULL and non-NULL CLOB)
      * Merkle verification passes — proves the CLOB content
        round-tripped bit-identically AND the materializer produced
        stable hash inputs across source/target reads
      * Direct PG read confirms the long CLOB landed intact (not
        truncated, not the LOB locator's repr)
    """
    long_text = synth_clob_table

    schema = introspect(oracle_session, Dialect.ORACLE, "SYSTEM")
    schema.tables = [t for t in schema.tables if t.name == _CLOB_TABLE]
    assert len(schema.tables) == 1, "test fixture failed to create the CLOB table"

    specs = schema.build_specs(target_schema=pg_target_schema)
    cols_by_target = {
        s.target_table.qualified(): schema.column_metadata[s.source_table.qualified()]
        for s in specs.values()
    }
    pks_by_target = {
        s.target_table.qualified(): s.pk_columns for s in specs.values()
    }
    target_refs = [s.target_table for s in specs.values()]
    ddl_stmts = generate_schema_ddl(
        target_refs, cols_by_target, pks_by_target, map_type=map_oracle_type
    )

    # Sanity-check: CLOB → TEXT in the generated DDL.
    create_stmt = next(s for s in ddl_stmts if _CLOB_TABLE in s)
    assert "TEXT" in create_stmt.upper(), (
        f"expected CLOB→TEXT in DDL, got:\n{create_stmt}"
    )

    ddl_conn = psycopg.connect(pg_url)
    try:
        apply_ddl(ddl_conn, ddl_stmts)
        ddl_conn.commit()
    finally:
        ddl_conn.close()

    plan = plan_load_order(target_refs, [])
    runner = Runner(
        source_session=oracle_session,
        target_session=pg_session,
        target_pg_conn=pg_raw_conn,
        source_dialect=Dialect.ORACLE,
        batch_size=10,
    )
    result = runner.execute(plan, specs)

    target_result = result.tables[f"{pg_target_schema}.{_CLOB_TABLE}"]
    assert target_result.rows_copied == 3
    # The headline assertion: merkle verification passes despite a
    # CLOB column. Without the materializer, the source-side hash
    # would include `<oracledb.lob.LOB object at 0x...>`. Without
    # canonicalization, the target side reads back as `str` and the
    # hashes diverge regardless.
    assert target_result.verified, target_result.discrepancy

    with pg_raw_conn.cursor() as cur:
        # Long CLOB landed intact.
        cur.execute(
            f'SELECT "BODY" FROM "{pg_target_schema}"."{_CLOB_TABLE}" '
            f'WHERE "ID" = 2'
        )
        assert cur.fetchone()[0] == long_text

        # NULL CLOB stayed NULL (didn't materialize as empty string).
        cur.execute(
            f'SELECT "BODY" FROM "{pg_target_schema}"."{_CLOB_TABLE}" '
            f'WHERE "ID" = 3'
        )
        assert cur.fetchone()[0] is None

        # Short CLOB exact-match.
        cur.execute(
            f'SELECT "BODY" FROM "{pg_target_schema}"."{_CLOB_TABLE}" '
            f'WHERE "ID" = 1'
        )
        assert cur.fetchone()[0] == "hello world"
