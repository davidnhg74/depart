"""Live-Oracle introspector integration test.

Only runs when an Oracle Free container is reachable on localhost:1521
with the HR fixture schema from `docker/oracle-init/01-hr-schema.sql`
applied. Skipped otherwise so the default suite stays fast and doesn't
require a 6 GB image.

To run locally:

    docker compose --profile oracle up -d oracle
    # wait for healthy, then (if container is fresh) load fixture:
    docker cp docker/oracle-init/01-hr-schema.sql depart_oracle:/tmp/
    docker exec depart_oracle \
        bash -c 'sqlplus -s system/${ORACLE_PASSWORD}@FREEPDB1 @/tmp/01-hr-schema.sql'

    cd apps/api && pytest tests/test_migrate_introspect_oracle_live.py --no-cov

The test asserts the introspector returns the three HR tables with
correct columns/PKs/FKs, that the planner orders parents before
children, and that the type metadata captured from `all_tab_columns` is
what the DDL mapper expects (so a round-trip DDL would emit sensible
PG types).
"""

from __future__ import annotations

import os

import pytest
from sqlalchemy import create_engine
from sqlalchemy.exc import DatabaseError, OperationalError
from sqlalchemy.orm import sessionmaker

from src.migrate.ddl import ColumnMeta, generate_create_table, map_oracle_type
from src.migrate.introspect import introspect
from src.migrate.keyset import Dialect
from src.migrate.planner import plan_load_order


ORACLE_URL = os.environ.get(
    "ORACLE_TEST_URL",
    "oracle+oracledb://hr:hr_pw@localhost:1521/?service_name=FREEPDB1",
)


@pytest.fixture(scope="module")
def oracle_session():
    """Connect to the live Oracle container, or skip the module if the
    service isn't reachable. Scope is module so we pay the connection
    overhead once for all tests below."""
    try:
        engine = create_engine(ORACLE_URL)
        Session = sessionmaker(bind=engine)
        s = Session()
        # Smoke the connection — oracledb's error surface is noisy, so
        # we wrap the whole thing.
        s.execute.__self__  # attribute access only — don't query yet
        from sqlalchemy import text

        s.execute(text("SELECT 1 FROM dual"))
    except (DatabaseError, OperationalError, Exception) as e:  # noqa: BLE001
        pytest.skip(f"Oracle not reachable at {ORACLE_URL!r}: {e}")
    yield s
    s.close()
    engine.dispose()


# ─── Catalog matches the fixture ─────────────────────────────────────────────


def test_introspects_three_hr_tables(oracle_session):
    schema = introspect(oracle_session, Dialect.ORACLE, "HR")
    table_names = sorted(t.name for t in schema.tables)
    assert table_names == ["DEPARTMENTS", "EMPLOYEES", "JOBS"]


def test_primary_keys_recovered(oracle_session):
    schema = introspect(oracle_session, Dialect.ORACLE, "HR")
    assert schema.primary_keys["HR.DEPARTMENTS"] == ["DEPARTMENT_ID"]
    assert schema.primary_keys["HR.EMPLOYEES"] == ["EMPLOYEE_ID"]
    assert schema.primary_keys["HR.JOBS"] == ["JOB_ID"]


def test_foreign_keys_recovered(oracle_session):
    schema = introspect(oracle_session, Dialect.ORACLE, "HR")
    fk_names = sorted(fk.name for fk in schema.foreign_keys)
    assert fk_names == ["EMP_DEPT_FK", "EMP_JOB_FK", "EMP_MGR_FK"]


def test_column_metadata_captures_types(oracle_session):
    schema = introspect(oracle_session, Dialect.ORACLE, "HR")
    by_name = {c.name: c for c in schema.column_metadata["HR.EMPLOYEES"]}

    # PK — NUMBER(6) → BIGINT? Actually precision=6 ≤ 9 → INTEGER.
    emp_id = by_name["EMPLOYEE_ID"]
    assert emp_id.data_type == "NUMBER"
    assert emp_id.precision == 6
    assert emp_id.scale == 0
    assert not emp_id.nullable
    assert map_oracle_type(emp_id) == "INTEGER"

    # VARCHAR2 length travels through.
    first = by_name["FIRST_NAME"]
    assert first.data_type == "VARCHAR2"
    assert first.length == 20
    assert first.nullable
    assert map_oracle_type(first) == "VARCHAR(20)"

    # NUMBER(8,2) → NUMERIC(8, 2) for currency.
    salary = by_name["SALARY"]
    assert salary.data_type == "NUMBER"
    assert salary.precision == 8
    assert salary.scale == 2
    assert map_oracle_type(salary) == "NUMERIC(8, 2)"

    # DATE → TIMESTAMP (Oracle DATE carries time-of-day).
    hire = by_name["HIRE_DATE"]
    assert hire.data_type == "DATE"
    assert map_oracle_type(hire) == "TIMESTAMP"

    # CLOB → TEXT.
    bio = by_name["BIO"]
    assert bio.data_type == "CLOB"
    assert map_oracle_type(bio) == "TEXT"


# ─── Planner uses the FKs to get load order right ────────────────────────────


def test_planner_orders_parents_before_children(oracle_session):
    schema = introspect(oracle_session, Dialect.ORACLE, "HR")
    plan = plan_load_order(schema.tables, schema.foreign_keys)
    flat = [t.name for g in plan.groups for t in g.tables]

    # employees references departments and jobs → both must come first.
    assert flat.index("DEPARTMENTS") < flat.index("EMPLOYEES")
    assert flat.index("JOBS") < flat.index("EMPLOYEES")


# ─── build_specs + DDL round-trip ────────────────────────────────────────────


def test_build_specs_and_generate_ddl(oracle_session):
    """End-to-end rehearsal of the greenfield flow — introspect Oracle,
    hand-generate a PG `CREATE TABLE`, and verify the DDL string looks
    like production PG DDL (quoted identifiers, proper types, PK inline).
    We don't execute it here — that's covered by the PG-to-PG runner
    test — this proves the Oracle-side pipeline end-to-end."""
    schema = introspect(oracle_session, Dialect.ORACLE, "HR")
    specs = schema.build_specs(target_schema="public")
    assert len(specs) == 3  # DEPARTMENTS, JOBS, EMPLOYEES all have PKs.

    # Walk the employees spec end-to-end.
    employees = next(s for s in specs.values() if s.source_table.name == "EMPLOYEES")
    cols = schema.column_metadata[employees.source_table.qualified()]
    ddl = generate_create_table(
        employees.target_table,
        cols,
        employees.pk_columns,
        map_type=map_oracle_type,
    )
    assert 'CREATE TABLE IF NOT EXISTS "public"."EMPLOYEES"' in ddl
    assert '"EMPLOYEE_ID" INTEGER NOT NULL' in ddl
    assert '"FIRST_NAME" VARCHAR(20)' in ddl
    assert '"SALARY" NUMERIC(8, 2)' in ddl
    assert '"HIRE_DATE" TIMESTAMP NOT NULL' in ddl
    assert '"BIO" TEXT' in ddl
    assert 'PRIMARY KEY ("EMPLOYEE_ID")' in ddl
