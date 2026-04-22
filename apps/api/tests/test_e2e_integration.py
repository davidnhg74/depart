"""
End-to-end integration tests for Depart platform.
Tests full flow: complexity analysis → conversion → migration report.
"""
import pytest
from src.analyzers.complexity_scorer import ComplexityScorer
from src.converters.plsql_converter import PlSqlConverter
from src.converters.schema_converter import SchemaConverter
from src.test_gen.pgtap_generator import PgTAPGenerator
from src.models import MigrationReport


class TestE2EComplexityToConversion:
    """Test full flow from complexity analysis to conversion."""

    def test_simple_procedure_e2e(self):
        """Test: analyze procedure → convert → generate tests."""
        oracle_code = """
        CREATE OR REPLACE PROCEDURE process_emp(p_emp_id NUMBER) AS
        BEGIN
          INSERT INTO emp_log VALUES (p_emp_id, SYSDATE);
          COMMIT;
        END;
        """

        # Step 1: Analyze complexity
        scorer = ComplexityScorer()
        complexity_report = scorer.analyze(oracle_code, rate_per_day=1000)

        assert complexity_report.total_lines > 0
        assert complexity_report.score > 0
        assert "PROCEDURE" in complexity_report.construct_counts

        # Step 2: Convert
        converter = PlSqlConverter(use_llm=False)
        result = converter.convert_procedure(oracle_code)

        assert result.success
        assert "CREATE OR REPLACE PROCEDURE" in result.converted
        assert "$$ LANGUAGE plpgsql" in result.converted
        assert "COMMIT" not in result.converted or "-- COMMIT" in result.converted

        # Step 3: Generate pgTAP tests
        generator = PgTAPGenerator()
        test_code = generator.generate_for_procedure(
            "process_emp",
            oracle_code,
            result.converted
        )

        assert "pgTAP" in test_code
        assert "process_emp" in test_code
        assert "BEGIN" in test_code

        test_cases = generator.get_test_cases()
        assert len(test_cases) > 0
        for tc in test_cases:
            assert tc.test_sql
            assert tc.name

    def test_function_with_return_e2e(self):
        """Test: analyze function → convert → validate → generate tests."""
        oracle_code = """
        CREATE OR REPLACE FUNCTION calc_bonus(p_salary NUMBER) RETURN NUMBER AS
        BEGIN
          RETURN p_salary * 0.1;
        END;
        """

        # Step 1: Analyze
        scorer = ComplexityScorer()
        report = scorer.analyze(oracle_code)

        assert "FUNCTION" in report.construct_counts
        assert report.score > 0

        # Step 2: Convert
        converter = PlSqlConverter(use_llm=False)
        result = converter.convert_function(oracle_code)

        assert result.success
        assert "RETURNS" in result.converted
        assert "LANGUAGE plpgsql" in result.converted
        assert result.converted != oracle_code

        # Step 3: Generate tests
        generator = PgTAPGenerator()
        test_code = generator.generate_for_function(
            "calc_bonus",
            oracle_code,
            result.converted,
            "NUMERIC"
        )

        assert "calc_bonus" in test_code
        assert "NUMERIC" in test_code
        assert "return type" in test_code.lower()

    def test_schema_ddl_e2e(self):
        """Test: convert DDL → validate structure."""
        oracle_ddl = """
        CREATE TABLE employees (
            employee_id NUMBER(6) PRIMARY KEY,
            first_name VARCHAR2(50) NOT NULL,
            salary NUMBER(10,2),
            hire_date DATE
        );
        """

        # Convert schema
        converter = SchemaConverter()
        result = converter.convert_table(oracle_ddl)

        assert result.construct_type == "TABLE"
        assert "NUMERIC" in result.converted
        assert "VARCHAR" in result.converted
        assert "TIMESTAMP" in result.converted
        assert "PRIMARY KEY" in result.converted

    def test_migration_report_model(self):
        """Test: MigrationReport model with realistic data."""
        report = MigrationReport(
            migration_id="test-123",
            total_objects=5,
            converted_count=4,
            tests_generated=4,
            conversion_percentage=80.0,
            risk_breakdown={"high": 1, "medium": 0, "low": 4},
            blockers=[{"name": "complex_proc", "reason": "Uses DBMS_SCHEDULER"}],
            generated_at="2026-04-21T12:00:00Z"
        )

        assert report.migration_id == "test-123"
        assert report.conversion_percentage == 80.0
        assert len(report.blockers) == 1
        assert report.risk_breakdown["high"] == 1


class TestE2ECombinedScenario:
    """Test complete migration scenario with multiple objects."""

    def test_package_conversion_scenario(self):
        """Test converting a simple Oracle package."""
        # Simulate a package with procedure and function
        proc_code = """
        CREATE OR REPLACE PROCEDURE log_action(p_action VARCHAR2) AS
        BEGIN
          INSERT INTO audit_log (action, log_date) VALUES (p_action, SYSDATE);
        END;
        """

        func_code = """
        CREATE OR REPLACE FUNCTION get_user_count RETURN NUMBER AS
          v_count NUMBER;
        BEGIN
          SELECT COUNT(*) INTO v_count FROM users;
          RETURN v_count;
        END;
        """

        converter = PlSqlConverter(use_llm=False)

        # Convert both
        proc_result = converter.convert_procedure(proc_code)
        func_result = converter.convert_function(func_code)

        assert proc_result.success
        assert func_result.success

        # Generate pgTAP for both
        proc_gen = PgTAPGenerator()
        func_gen = PgTAPGenerator()

        proc_test = proc_gen.generate_for_procedure("log_action", proc_code, proc_result.converted)
        func_test = func_gen.generate_for_function("get_user_count", func_code, func_result.converted, "NUMERIC")

        assert "log_action" in proc_test
        assert "get_user_count" in func_test

        # Verify test case tracking
        assert len(proc_gen.get_test_cases()) > 0
        assert len(func_gen.get_test_cases()) > 0
