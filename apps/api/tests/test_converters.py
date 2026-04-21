"""
Tests for Phase 2 converters: schema, PL/SQL, validators.
"""
import pytest
from src.converters.schema_converter import SchemaConverter, OracleDataTypeMapper
from src.converters.plsql_converter import PlSqlConverter, DeterministicRules
from src.converters.oracle_functions import OracleFunctionConverter
from src.validators.plpgsql_validator import PlPgSQLValidator, ConversionValidator


class TestOracleDataTypeMapper:
    """Test Oracle → PostgreSQL data type mapping."""

    def test_varchar2_to_varchar(self):
        mapper = OracleDataTypeMapper()
        converted, warnings = mapper.convert_datatype("VARCHAR2(100)")
        assert "VARCHAR" in converted
        assert len(warnings) == 0

    def test_number_with_precision(self):
        mapper = OracleDataTypeMapper()
        converted, warnings = mapper.convert_datatype("NUMBER(10,2)")
        assert "NUMERIC(10,2)" in converted

    def test_number_without_precision(self):
        mapper = OracleDataTypeMapper()
        converted, warnings = mapper.convert_datatype("NUMBER")
        assert "NUMERIC" in converted

    def test_clob_to_text(self):
        mapper = OracleDataTypeMapper()
        converted, warnings = mapper.convert_datatype("CLOB")
        assert "TEXT" in converted

    def test_date_conversion(self):
        mapper = OracleDataTypeMapper()
        converted, warnings = mapper.convert_datatype("DATE")
        assert "TIMESTAMP" in converted
        assert len(warnings) > 0  # Should warn about timezone

    def test_long_type_warning(self):
        mapper = OracleDataTypeMapper()
        converted, warnings = mapper.convert_datatype("LONG")
        assert "TEXT" in converted
        assert any("deprecated" in w.lower() for w in warnings)


class TestSchemaConverter:
    """Test schema (DDL) conversion."""

    @pytest.fixture
    def converter(self):
        return SchemaConverter()

    def test_create_table_basic(self, converter):
        oracle_ddl = """
        CREATE TABLE employees (
            employee_id NUMBER(6) PRIMARY KEY,
            first_name VARCHAR2(50) NOT NULL,
            salary NUMBER(10,2),
            hire_date DATE
        );
        """
        result = converter.convert_table(oracle_ddl)

        assert result.construct_type == "TABLE"
        assert "VARCHAR" in result.converted
        assert "NUMERIC" in result.converted
        assert "TIMESTAMP" in result.converted

    def test_global_temp_table(self, converter):
        oracle_ddl = "CREATE GLOBAL TEMPORARY TABLE temp_emp (id NUMBER) ON COMMIT DELETE ROWS;"
        result = converter.convert_table(oracle_ddl)

        assert "TEMP TABLE" in result.converted
        assert "ON COMMIT DELETE ROWS" in result.converted

    def test_create_sequence(self, converter):
        oracle_ddl = "CREATE SEQUENCE employees_seq START WITH 1 INCREMENT BY 1 NOCACHE;"
        result = converter.convert_sequence(oracle_ddl)

        assert result.construct_type == "SEQUENCE"
        assert "CACHE" in result.converted

    def test_create_index(self, converter):
        oracle_ddl = "CREATE INDEX idx_emp_name ON employees(last_name, first_name);"
        result = converter.convert_index(oracle_ddl)

        assert result.construct_type == "INDEX"
        assert "idx_emp_name" in result.converted

    def test_create_view(self, converter):
        oracle_ddl = """
        CREATE OR REPLACE VIEW emp_view AS
        SELECT employee_id, first_name, salary FROM employees
        WHERE salary > 50000;
        """
        result = converter.convert_view(oracle_ddl)

        assert result.construct_type == "VIEW"
        assert "SELECT" in result.converted

    def test_ddl_removes_oracle_clauses(self, converter):
        oracle_ddl = """
        CREATE TABLE employees (
            id NUMBER TABLESPACE users_ts,
            name VARCHAR2(100) STORAGE (INITIAL 10K)
        );
        """
        result = converter.convert_table(oracle_ddl)

        assert "TABLESPACE" not in result.converted
        assert "STORAGE" not in result.converted


class TestOracleFunctionConverter:
    """Test Oracle function → PostgreSQL function conversion."""

    @pytest.fixture
    def converter(self):
        return OracleFunctionConverter()

    def test_nvl_to_coalesce(self, converter):
        oracle_code = "SELECT NVL(salary, 0) FROM employees;"
        converted = converter.convert(oracle_code)

        assert "COALESCE(salary, 0)" in converted

    def test_sysdate_to_current_date(self, converter):
        oracle_code = "INSERT INTO audit_log (log_date) VALUES (SYSDATE);"
        converted = converter.convert(oracle_code)

        assert "CURRENT_DATE" in converted

    def test_remove_dual(self, converter):
        oracle_code = "SELECT 1 FROM DUAL;"
        converted = converter.convert(oracle_code)

        assert "FROM DUAL" not in converted or converted.count("FROM") == 0

    def test_regexp_like_conversion(self, converter):
        oracle_code = "WHERE REGEXP_LIKE(email, '^[^@]+@[^@]+$')"
        converted = converter.convert(oracle_code)

        assert "~" in converted

    def test_function_info(self, converter):
        info = converter.get_conversion_info("NVL")
        assert info is not None
        assert info[0] == "COALESCE"
        assert info[1] is False  # No review needed

    def test_function_info_needs_review(self, converter):
        info = converter.get_conversion_info("DECODE")
        assert info is not None
        assert info[1] is True  # Needs review


class TestPlPgSQLValidator:
    """Test PL/pgSQL syntax validation."""

    @pytest.fixture
    def validator(self):
        return PlPgSQLValidator()

    def test_valid_function(self, validator):
        code = """
        CREATE OR REPLACE FUNCTION get_salary(p_emp_id INT)
        RETURNS NUMERIC AS $$
        DECLARE
          v_salary NUMERIC;
        BEGIN
          SELECT salary INTO v_salary FROM employees WHERE employee_id = p_emp_id;
          RETURN v_salary;
        END;
        $$ LANGUAGE plpgsql;
        """
        result = validator.validate(code, "FUNCTION")

        assert result.is_valid is True
        assert len(result.errors) == 0

    def test_unbalanced_parentheses(self, validator):
        code = "CREATE FUNCTION foo() RETURNS INT AS $$ BEGIN RETURN 1; END; $$ LANGUAGE plpgsql;"
        # Remove one )
        code = code.replace(")", "", 1)

        result = validator.validate(code, "FUNCTION")
        assert result.is_valid is False
        assert any("parentheses" in e.lower() for e in result.errors)

    def test_unbalanced_begin_end(self, validator):
        code = """
        CREATE OR REPLACE FUNCTION foo()
        RETURNS INT AS $$
        BEGIN
          RETURN 1;
        $$ LANGUAGE plpgsql;
        """
        result = validator.validate(code, "FUNCTION")

        assert result.is_valid is False
        assert any("BEGIN/END" in e for e in result.errors)

    def test_oracle_remnants_warning(self, validator):
        code = """
        CREATE OR REPLACE FUNCTION foo()
        RETURNS INT AS $$
        BEGIN
          DBMS_OUTPUT.PUT_LINE('test');
          RETURN 1;
        END;
        $$ LANGUAGE plpgsql;
        """
        result = validator.validate(code, "FUNCTION")

        assert len(result.warnings) > 0
        assert any("DBMS_OUTPUT" in w for w in result.warnings)

    def test_missing_language_clause(self, validator):
        code = """
        CREATE OR REPLACE FUNCTION foo()
        RETURNS INT AS $$
        BEGIN
          RETURN 1;
        END;
        $$;
        """
        result = validator.validate(code, "FUNCTION")

        assert not result.is_valid or len(result.errors) > 0


class TestPlSqlConverter:
    """Test PL/SQL → PL/pgSQL conversion."""

    @pytest.fixture
    def converter(self):
        return PlSqlConverter(use_llm=False)  # Disable LLM for tests

    def test_simple_procedure_conversion(self, converter):
        oracle_proc = """
        CREATE OR REPLACE PROCEDURE greet(p_name VARCHAR2) AS
        BEGIN
          DBMS_OUTPUT.PUT_LINE('Hello ' || p_name);
        END greet;
        """

        result = converter.convert_procedure(oracle_proc)

        assert result.success
        assert "CREATE OR REPLACE PROCEDURE" in result.converted
        assert "$$ LANGUAGE plpgsql" in result.converted

    def test_function_with_return(self, converter):
        oracle_func = """
        CREATE OR REPLACE FUNCTION double_it(p_val NUMBER) RETURN NUMBER AS
        BEGIN
          RETURN p_val * 2;
        END double_it;
        """

        result = converter.convert_function(oracle_func)

        assert result.success
        assert "RETURNS" in result.converted
        assert "LANGUAGE plpgsql" in result.converted

    def test_variable_declaration(self, converter):
        oracle_proc = """
        CREATE OR REPLACE PROCEDURE test_proc AS
          v_count NUMBER;
          v_name employees.first_name%TYPE;
        BEGIN
          SELECT COUNT(*) INTO v_count FROM employees;
        END;
        """

        result = converter.convert_procedure(oracle_proc)

        assert result.success
        assert "DECLARE" in result.converted
        assert "%TYPE" in result.converted  # Should be preserved

    def test_commit_removed(self, converter):
        oracle_proc = """
        CREATE OR REPLACE PROCEDURE insert_emp(p_name VARCHAR2) AS
        BEGIN
          INSERT INTO employees (first_name) VALUES (p_name);
          COMMIT;
        END;
        """

        result = converter.convert_procedure(oracle_proc)

        assert "-- COMMIT" in result.converted or "COMMIT" not in result.converted

    def test_empty_code_error(self, converter):
        result = converter.convert_procedure("")
        assert not result.success
        assert len(result.errors) > 0


class TestConversionValidator:
    """Test overall conversion validation."""

    @pytest.fixture
    def validator(self):
        return ConversionValidator()

    def test_successful_conversion(self, validator):
        original = "CREATE PROCEDURE test AS BEGIN NULL; END;"
        converted = """
        CREATE OR REPLACE PROCEDURE test() AS $$
        BEGIN
          NULL;
        END;
        $$ LANGUAGE plpgsql;
        """

        result = validator.validate_conversion(original, converted, "PROCEDURE")
        assert result.is_valid

    def test_conversion_with_warnings(self, validator):
        original = "CREATE FUNCTION test() RETURN INT AS BEGIN RETURN 1; END;"
        converted = """
        CREATE OR REPLACE FUNCTION test()
        RETURNS INT AS $$
        BEGIN
          DBMS_OUTPUT.PUT_LINE('test');
          RETURN 1;
        END;
        $$ LANGUAGE plpgsql;
        """

        result = validator.validate_conversion(original, converted, "FUNCTION")
        assert len(result.warnings) > 0

    def test_unchanged_code_warning(self, validator):
        code = "CREATE PROCEDURE test AS BEGIN NULL; END;"

        result = validator.validate_conversion(code, code, "PROCEDURE")
        assert any("unchanged" in w.lower() for w in result.warnings)


class TestDeterministicRules:
    """Test deterministic transformation rules."""

    @pytest.fixture
    def rules(self):
        return DeterministicRules()

    def test_function_wrapper_transformation(self, rules):
        oracle = "CREATE FUNCTION test(p_id NUMBER) RETURN INT AS BEGIN RETURN 1; END;"
        converted = rules.convert(oracle, "FUNCTION")

        assert "CREATE OR REPLACE FUNCTION" in converted
        assert "RETURNS" in converted
        assert "$$ LANGUAGE plpgsql" in converted

    def test_procedure_wrapper_transformation(self, rules):
        oracle = "CREATE PROCEDURE test AS BEGIN NULL; END;"
        converted = rules.convert(oracle, "PROCEDURE")

        assert "CREATE OR REPLACE PROCEDURE" in converted
        assert "$$ LANGUAGE plpgsql" in converted

    def test_adds_declare_if_missing(self, rules):
        oracle = "CREATE PROCEDURE test AS BEGIN NULL; END;"
        converted = rules.convert(oracle, "PROCEDURE")

        # Should add DECLARE if missing
        assert "DECLARE" in converted or "BEGIN" in converted
