"""End-to-end parser tests against the public `parse()` facade.

These run on whichever implementation is active (interim by default; ANTLR
when `_generated/` exists). Both must satisfy these assertions — they are
the contract.
"""
import pytest

from src.core.ir.nodes import (
    ConstructTag,
    ObjectKind,
    Package,
    Sequence,
    Subprogram,
    Table,
    Trigger,
    View,
    Index,
)
from src.source.oracle.parser import parse


def _kinds(module) -> list:
    """Object kinds present, ignoring the synthetic <module-constructs>."""
    return [o.kind for o in module.objects if o.name != "<module-constructs>"]


def _names(module) -> list:
    return [o.name for o in module.objects if o.name != "<module-constructs>"]


def _construct_tags(module) -> set:
    """Set of ConstructTag enums seen across the module."""
    tags = set()
    for o in module.objects:
        if isinstance(o, Subprogram):
            tags.update(r.tag for r in o.referenced_constructs)
    return tags


# ─── Object detection ────────────────────────────────────────────────────────


class TestObjectDetection:
    def test_create_table(self):
        m = parse("CREATE TABLE employees (id NUMBER PRIMARY KEY, name VARCHAR2(100));")
        assert ObjectKind.TABLE in _kinds(m)
        assert "EMPLOYEES" in (n.upper() for n in _names(m))

    def test_create_global_temp_table(self):
        m = parse("CREATE GLOBAL TEMPORARY TABLE session_t (id NUMBER) ON COMMIT DELETE ROWS;")
        tables = [o for o in m.objects if isinstance(o, Table)]
        assert tables and tables[0].is_global_temp is True

    def test_create_view(self):
        m = parse("CREATE OR REPLACE VIEW employee_summary AS SELECT id, name FROM employees;")
        assert ObjectKind.VIEW in _kinds(m)

    def test_create_materialized_view(self):
        m = parse("CREATE MATERIALIZED VIEW emp_mv AS SELECT * FROM employees;")
        kinds = _kinds(m)
        # Either kind is acceptable depending on parser sophistication; the
        # IR distinguishes them but the interim parser may report VIEW.
        assert ObjectKind.MATERIALIZED_VIEW in kinds or ObjectKind.VIEW in kinds

    def test_create_sequence(self):
        m = parse("CREATE SEQUENCE emp_seq START WITH 1000 INCREMENT BY 1 CACHE 20;")
        assert ObjectKind.SEQUENCE in _kinds(m)

    def test_create_unique_index(self):
        m = parse("CREATE UNIQUE INDEX ix_emp_email ON employees(email);")
        idxs = [o for o in m.objects if isinstance(o, Index)]
        assert idxs and idxs[0].unique is True

    def test_create_trigger(self):
        m = parse("""
        CREATE OR REPLACE TRIGGER emp_audit
        BEFORE INSERT ON employees
        FOR EACH ROW
        BEGIN
            :NEW.created_at := SYSDATE;
        END;
        """)
        assert ObjectKind.TRIGGER in _kinds(m)

    def test_create_procedure(self):
        m = parse("CREATE OR REPLACE PROCEDURE noop AS BEGIN NULL; END;")
        assert ObjectKind.PROCEDURE in _kinds(m)

    def test_create_function(self):
        m = parse("""
        CREATE OR REPLACE FUNCTION add_one(p IN NUMBER) RETURN NUMBER AS
        BEGIN
            RETURN p + 1;
        END;
        """)
        assert ObjectKind.FUNCTION in _kinds(m)

    def test_create_package_and_body(self):
        m = parse("""
        CREATE OR REPLACE PACKAGE my_pkg AS
            PROCEDURE p;
            FUNCTION f RETURN NUMBER;
        END my_pkg;

        CREATE OR REPLACE PACKAGE BODY my_pkg AS
            PROCEDURE p IS BEGIN NULL; END;
            FUNCTION f RETURN NUMBER IS BEGIN RETURN 1; END;
        END my_pkg;
        """)
        kinds = _kinds(m)
        assert ObjectKind.PACKAGE in kinds
        assert ObjectKind.PACKAGE_BODY in kinds


# ─── Construct tagging ──────────────────────────────────────────────────────


class TestConstructTagging:
    def test_connect_by(self):
        m = parse("""
        CREATE OR REPLACE PROCEDURE org_chart AS
        BEGIN
            FOR r IN (
                SELECT employee_id, manager_id FROM employees
                START WITH manager_id IS NULL
                CONNECT BY PRIOR employee_id = manager_id
            ) LOOP NULL; END LOOP;
        END;
        """)
        assert ConstructTag.CONNECT_BY in _construct_tags(m)

    def test_merge(self):
        m = parse("""
        CREATE OR REPLACE PROCEDURE upsert AS
        BEGIN
            MERGE INTO target t USING source s ON (t.id = s.id)
            WHEN MATCHED THEN UPDATE SET t.x = s.x
            WHEN NOT MATCHED THEN INSERT (id, x) VALUES (s.id, s.x);
        END;
        """)
        assert ConstructTag.MERGE in _construct_tags(m)

    def test_autonomous_transaction(self):
        m = parse("""
        CREATE OR REPLACE PROCEDURE audit_log(msg VARCHAR2) AS
            PRAGMA AUTONOMOUS_TRANSACTION;
        BEGIN
            INSERT INTO audit (msg) VALUES (msg);
            COMMIT;
        END;
        """)
        assert ConstructTag.AUTONOMOUS_TXN in _construct_tags(m)

    def test_execute_immediate(self):
        m = parse("""
        CREATE OR REPLACE PROCEDURE dyn AS
        BEGIN
            EXECUTE IMMEDIATE 'TRUNCATE TABLE staging';
        END;
        """)
        assert ConstructTag.EXECUTE_IMMEDIATE in _construct_tags(m)

    def test_bulk_collect(self):
        m = parse("""
        CREATE OR REPLACE PROCEDURE bulk_load AS
            TYPE t_arr IS TABLE OF employees%ROWTYPE;
            v_emps t_arr;
        BEGIN
            SELECT * BULK COLLECT INTO v_emps FROM employees;
        END;
        """)
        # BULK COLLECT and %ROWTYPE both expected.
        tags = _construct_tags(m)
        assert ConstructTag.BULK_COLLECT in tags
        assert ConstructTag.PERCENT_TYPE in tags

    def test_dbms_packages(self):
        m = parse("""
        CREATE OR REPLACE PROCEDURE notify AS
        BEGIN
            DBMS_OUTPUT.PUT_LINE('hello');
            DBMS_SCHEDULER.CREATE_JOB(job_name => 'x');
        END;
        """)
        tags = _construct_tags(m)
        assert ConstructTag.DBMS_OUTPUT in tags
        assert ConstructTag.DBMS_SCHEDULER in tags

    def test_database_link(self):
        m = parse("""
        CREATE OR REPLACE PROCEDURE pull AS
        BEGIN
            INSERT INTO local_t SELECT * FROM remote_t@prod_link;
        END;
        """)
        assert ConstructTag.DBLINK in _construct_tags(m)

    def test_percent_type_and_rowtype(self):
        m = parse("""
        CREATE OR REPLACE PROCEDURE tdemo AS
            v_id  employees.id%TYPE;
            v_row employees%ROWTYPE;
        BEGIN
            NULL;
        END;
        """)
        assert ConstructTag.PERCENT_TYPE in _construct_tags(m)


# ─── False-positive guards ──────────────────────────────────────────────────


class TestFalsePositiveGuards:
    """The regex parser scored these. The new parser must not."""

    def test_keyword_in_string_literal(self):
        m = parse("""
        CREATE OR REPLACE PROCEDURE p AS
        BEGIN
            INSERT INTO log (msg) VALUES ('CONNECT BY example')
            ; INSERT INTO log (msg) VALUES ('MERGE INTO not really');
        END;
        """)
        tags = _construct_tags(m)
        assert ConstructTag.CONNECT_BY not in tags
        assert ConstructTag.MERGE not in tags

    def test_keyword_in_line_comment(self):
        m = parse("""
        CREATE OR REPLACE PROCEDURE p AS
        BEGIN
            -- MERGE INTO target t USING ...
            -- CONNECT BY PRIOR ...
            NULL;
        END;
        """)
        tags = _construct_tags(m)
        assert ConstructTag.MERGE not in tags
        assert ConstructTag.CONNECT_BY not in tags

    def test_keyword_in_block_comment(self):
        m = parse("""
        CREATE OR REPLACE PROCEDURE p AS
        BEGIN
            /*
             * MERGE INTO is documented at:
             *   https://example.com/merge
             */
            NULL;
        END;
        """)
        assert ConstructTag.MERGE not in _construct_tags(m)


# ─── Multi-object files ─────────────────────────────────────────────────────


class TestMultiObject:
    def test_two_tables_and_a_view(self):
        m = parse("""
        CREATE TABLE a (id NUMBER);
        CREATE TABLE b (id NUMBER);
        CREATE OR REPLACE VIEW v AS SELECT * FROM a;
        """)
        kinds = _kinds(m)
        assert kinds.count(ObjectKind.TABLE) == 2
        assert kinds.count(ObjectKind.VIEW) == 1

    def test_empty_input(self):
        m = parse("")
        assert _kinds(m) == []

    def test_only_comments(self):
        m = parse("-- nothing here\n/* still nothing */")
        assert _kinds(m) == []


# ─── Span correctness ───────────────────────────────────────────────────────


class TestSpans:
    def test_object_span_covers_definition(self):
        m = parse("CREATE TABLE foo (id NUMBER);")
        obj = next(o for o in m.objects if isinstance(o, Table))
        assert obj.span.start_line == 1
        assert obj.span.start_col == 1

    def test_object_span_records_multiline_extent(self):
        src = (
            "CREATE OR REPLACE PROCEDURE p AS\n"
            "BEGIN\n"
            "    NULL;\n"
            "END;\n"
        )
        m = parse(src)
        obj = next(o for o in m.objects if isinstance(o, Subprogram))
        assert obj.span.start_line == 1
        assert obj.span.end_line >= 3
