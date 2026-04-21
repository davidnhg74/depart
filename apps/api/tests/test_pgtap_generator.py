"""
Tests for pgTAP test harness generator.
"""
import pytest
from src.test_gen.pgtap_generator import PgTAPGenerator, ComparisonTestGenerator


class TestPgTAPGenerator:
    """Test pgTAP test generation."""

    @pytest.fixture
    def generator(self):
        return PgTAPGenerator()

    def test_generate_basic_procedure_test(self, generator):
        oracle_proc = """
        CREATE OR REPLACE PROCEDURE greet(p_name VARCHAR2) AS
        BEGIN
          INSERT INTO greetings (name, greeting_time) VALUES (p_name, SYSDATE);
          COMMIT;
        END greet;
        """
        converted_plpgsql = """
        CREATE OR REPLACE PROCEDURE greet(p_name VARCHAR) AS $$
        BEGIN
          INSERT INTO greetings (name, greeting_time) VALUES (p_name, CURRENT_TIMESTAMP);
        END;
        $$ LANGUAGE plpgsql;
        """

        test_code = generator.generate_for_procedure("greet", oracle_proc, converted_plpgsql)

        assert "greet" in test_code
        assert "pgTAP" in test_code
        assert "BEGIN" in test_code
        assert "finish()" in test_code
        assert "ROLLBACK" in test_code

    def test_extract_parameters(self, generator):
        oracle_code = "CREATE PROCEDURE test_proc(p_id NUMBER, p_name VARCHAR2) AS"
        params = generator._extract_parameters(oracle_code)

        assert len(params) == 2
        assert "p_id" in params
        assert "p_name" in params

    def test_extract_queries(self, generator):
        oracle_code = """
        CREATE PROCEDURE process AS
        BEGIN
          SELECT COUNT(*) INTO v_count FROM employees;
          INSERT INTO log VALUES (v_count);
          UPDATE employees SET active = 1;
        END;
        """
        queries = generator._extract_queries(oracle_code)

        assert len(queries) > 0
        assert any("SELECT" in q for q in queries)
        assert any("INSERT" in q for q in queries)
        assert any("UPDATE" in q for q in queries)

    def test_generate_function_test(self, generator):
        oracle_func = """
        CREATE OR REPLACE FUNCTION double_it(p_val NUMBER) RETURN NUMBER AS
        BEGIN
          RETURN p_val * 2;
        END;
        """
        converted_plpgsql = """
        CREATE OR REPLACE FUNCTION double_it(p_val NUMERIC)
        RETURNS NUMERIC AS $$
        BEGIN
          RETURN p_val * 2;
        END;
        $$ LANGUAGE plpgsql;
        """

        test_code = generator.generate_for_function("double_it", oracle_func, converted_plpgsql, "NUMERIC")

        assert "double_it" in test_code
        assert "NUMERIC" in test_code
        assert "return type" in test_code.lower()

    def test_null_handling_test(self, generator):
        oracle_proc = "CREATE PROCEDURE test(p_id NUMBER) AS BEGIN NULL; END;"
        converted = "CREATE OR REPLACE PROCEDURE test(p_id INT) AS $$ BEGIN NULL; END; $$ LANGUAGE plpgsql;"

        test_code = generator.generate_for_procedure("test", oracle_proc, converted)

        assert "null" in test_code.lower()

    def test_math_function_edge_cases(self, generator):
        oracle_func = """
        CREATE FUNCTION calculate_tax(p_amount NUMBER) RETURN NUMBER AS
        BEGIN
          RETURN p_amount * 0.1;
        END;
        """
        converted = """
        CREATE OR REPLACE FUNCTION calculate_tax(p_amount NUMERIC)
        RETURNS NUMERIC AS $$
        BEGIN
          RETURN p_amount * 0.1;
        END;
        $$ LANGUAGE plpgsql;
        """

        test_code = generator.generate_for_function("calculate_tax", oracle_func, converted, "NUMERIC")

        assert "zero" in test_code.lower() or "0" in test_code
        assert "negative" in test_code.lower() or "-" in test_code

    def test_is_math_function(self, generator):
        math_code = "CREATE FUNCTION calc(x NUMBER) RETURN NUMBER AS BEGIN RETURN x * 2; END;"
        assert generator._is_math_function(math_code) is True

        non_math_code = "CREATE FUNCTION greet() RETURN VARCHAR2 AS BEGIN RETURN 'hi'; END;"
        assert generator._is_math_function(non_math_code) is False


class TestComparisonTestGenerator:
    """Test dual-database comparison test generation."""

    def test_generate_dual_test(self):
        test_code = ComparisonTestGenerator.generate_dual_test(
            "calculate_bonus",
            [{"salary": 50000}, {"salary": 100000}, {"salary": 0}],
        )

        assert "calculate_bonus" in test_code
        assert "dblink" in test_code.lower()
        assert "Oracle" in test_code
        assert "PostgreSQL" in test_code
        assert len([line for line in test_code.split("\n") if "Test" in line]) >= 3

    def test_dual_test_structure(self):
        test_code = ComparisonTestGenerator.generate_dual_test("test_func", [{"x": 1}])

        assert "BEGIN" in test_code
        assert "finish()" in test_code
        assert "ROLLBACK" in test_code
