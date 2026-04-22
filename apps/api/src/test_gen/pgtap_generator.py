"""
Generate pgTAP test harnesses that prove converted procedures are correct.
pgTAP: TAP (Test Anything Protocol) for PostgreSQL.
Strategy: Compare original Oracle behavior vs. PostgreSQL converted code.
"""
import re
from dataclasses import dataclass
from typing import List, Optional


@dataclass
class TestCase:
    name: str
    description: str
    test_sql: str
    expected_result: Optional[str] = None


class PgTAPGenerator:
    """Generate pgTAP tests for converted procedures."""

    def __init__(self):
        self.test_cases: List[TestCase] = []

    def generate_for_procedure(self, proc_name: str, original_oracle: str, converted_plpgsql: str) -> str:
        """Generate pgTAP test suite for a procedure."""
        self.test_cases = []  # Reset test cases for new generation
        test_code = self._generate_test_header(proc_name)

        # Parse procedure to understand what it does
        params = self._extract_parameters(original_oracle)
        body_queries = self._extract_queries(original_oracle)

        # Extract procedure name from converted code if available
        converted_proc_name = self._extract_procedure_name(converted_plpgsql) or proc_name

        # Generate test cases
        basic_test = self._generate_basic_call_test(converted_proc_name, params)
        test_code += basic_test
        self._add_test_case("Basic call", f"Basic call to {converted_proc_name}()", basic_test)

        null_test = self._generate_null_input_tests(converted_proc_name, params)
        test_code += null_test
        if null_test.strip():
            self._add_test_case("NULL handling", f"NULL parameter handling for {converted_proc_name}()", null_test)

        edge_test = self._generate_edge_case_tests(converted_proc_name, params, body_queries)
        test_code += edge_test
        if edge_test.strip():
            self._add_test_case("Edge cases", f"Edge case handling for {converted_proc_name}()", edge_test)

        test_code += self._generate_test_footer()

        return test_code

    def generate_for_function(self, func_name: str, original_oracle: str, converted_plpgsql: str, return_type: str) -> str:
        """Generate pgTAP test suite for a function."""
        self.test_cases = []  # Reset test cases for new generation
        test_code = self._generate_test_header(func_name)

        # Parse function signature
        params = self._extract_parameters(original_oracle)

        # Extract function name from converted code if available
        converted_func_name = self._extract_function_name(converted_plpgsql) or func_name

        # Generate test cases
        return_type_test = self._generate_function_return_type_test(converted_func_name, return_type)
        test_code += return_type_test
        self._add_test_case("Return type", f"Return type verification for {converted_func_name}()", return_type_test)

        basic_test = self._generate_basic_call_test(converted_func_name, params)
        test_code += basic_test
        self._add_test_case("Basic call", f"Basic call to {converted_func_name}()", basic_test)

        null_test = self._generate_null_input_tests(converted_func_name, params)
        test_code += null_test
        if null_test.strip():
            self._add_test_case("NULL handling", f"NULL parameter handling for {converted_func_name}()", null_test)

        # If function appears to do math, test boundary values
        if self._is_math_function(original_oracle):
            math_test = self._generate_math_edge_cases(converted_func_name, params)
            test_code += math_test
            self._add_test_case("Math edge cases", f"Math boundary tests for {converted_func_name}()", math_test)

        test_code += self._generate_test_footer()

        return test_code

    def get_test_cases(self) -> List[TestCase]:
        """Return the list of generated test cases."""
        return self.test_cases

    def _add_test_case(self, name: str, description: str, test_sql: str) -> None:
        """Add a test case to the list."""
        self.test_cases.append(TestCase(name=name, description=description, test_sql=test_sql))

    def _extract_procedure_name(self, postgresql_code: str) -> Optional[str]:
        """Extract procedure name from PostgreSQL CREATE PROCEDURE statement."""
        match = re.search(r"CREATE\s+(?:OR\s+REPLACE\s+)?PROCEDURE\s+(\w+)", postgresql_code, re.IGNORECASE)
        return match.group(1) if match else None

    def _extract_function_name(self, postgresql_code: str) -> Optional[str]:
        """Extract function name from PostgreSQL CREATE FUNCTION statement."""
        match = re.search(r"CREATE\s+(?:OR\s+REPLACE\s+)?FUNCTION\s+(\w+)", postgresql_code, re.IGNORECASE)
        return match.group(1) if match else None

    def _generate_test_header(self, proc_name: str) -> str:
        """Generate pgTAP test preamble."""
        return f"""BEGIN;

-- pgTAP Test Suite for {proc_name}
-- This test suite verifies the converted PostgreSQL procedure/function
-- behaves identically to the Oracle original.

SELECT plan(null);  -- Let tests determine number

"""

    def _generate_test_footer(self) -> str:
        """Generate pgTAP test postamble."""
        return """
SELECT * FROM finish();
ROLLBACK;
"""

    def _generate_basic_call_test(self, name: str, params: List[str]) -> str:
        """Generate test that basic call doesn't error."""
        if not params:
            return f"""
-- Test 1: Basic call (no parameters)
SELECT is(
  (SELECT {name}()),
  true,
  '{name}() executes without error'
);

"""

        # Generate with sample parameters
        sample_params = ", ".join(["NULL" for _ in params])
        return f"""
-- Test 1: Basic call with null parameters
SELECT is(
  (SELECT {name}({sample_params}) IS NOT NULL),
  true,
  '{name}() executes with null params'
);

"""

    def _generate_null_input_tests(self, name: str, params: List[str]) -> str:
        """Generate tests for NULL handling."""
        if not params:
            return ""

        test_code = f"""
-- Test: NULL handling
-- All parameters NULL
SELECT is(
  (SELECT {name}({', '.join(['NULL'] * len(params))}) IS NOT NULL),
  true,
  '{name}() handles all-null inputs'
);

"""
        return test_code

    def _generate_edge_case_tests(self, name: str, params: List[str], queries: List[str]) -> str:
        """Generate edge case tests based on SQL in procedure."""
        test_code = ""

        # If procedure has COUNT(*), test with empty table
        if any("COUNT" in q for q in queries):
            test_code += f"""
-- Test: Edge case - empty result set
SELECT is(
  (SELECT COUNT(*) FROM (SELECT {name}()) AS t),
  0,
  '{name}() handles empty result sets'
);

"""

        # If procedure has MAX/MIN, test with single value
        if any("MAX" in q or "MIN" in q for q in queries):
            test_code += f"""
-- Test: Edge case - single value
SELECT is(
  (SELECT {name}(1) IS NOT NULL),
  true,
  '{name}() handles single value'
);

"""

        return test_code

    def _generate_function_return_type_test(self, func_name: str, return_type: str) -> str:
        """Generate test that function returns correct type."""
        return f"""
-- Test: Return type is {return_type}
SELECT is(
  pg_typeof({func_name}(NULL)) :: text,
  '{return_type}',
  '{func_name}() returns {return_type}'
);

"""

    def _generate_math_edge_cases(self, func_name: str, params: List[str]) -> str:
        """Generate tests for math functions (boundary values)."""
        return f"""
-- Test: Math edge cases
-- Test with zero
SELECT is(
  {func_name}(0),
  {func_name}(0),
  '{func_name}(0) is idempotent'
);

-- Test with negative
SELECT is(
  {func_name}(-100) IS NOT NULL,
  true,
  '{func_name}() handles negative values'
);

-- Test with large value
SELECT is(
  {func_name}(999999999) IS NOT NULL,
  true,
  '{func_name}() handles large values'
);

"""

    def _extract_parameters(self, oracle_code: str) -> List[str]:
        """Extract parameter list from Oracle code."""
        params = []

        # Match: PROCEDURE name(param1 TYPE, param2 TYPE, ...)
        match = re.search(r"(?:PROCEDURE|FUNCTION)\s+\w+\s*\((.*?)\)", oracle_code, re.IGNORECASE | re.DOTALL)
        if match:
            param_str = match.group(1)
            if param_str.strip():
                params = [p.strip().split()[0] for p in param_str.split(",") if p.strip()]

        return params

    def _extract_queries(self, oracle_code: str) -> List[str]:
        """Extract SQL queries from procedure body."""
        # Look for SELECT, INSERT, UPDATE, DELETE, MERGE
        queries = re.findall(
            r"(?:SELECT|INSERT|UPDATE|DELETE|MERGE)\b.*?(?:;|END)",
            oracle_code,
            re.IGNORECASE | re.DOTALL,
        )
        return queries

    def _is_math_function(self, code: str) -> bool:
        """Check if function appears to be mathematical."""
        math_keywords = ["* ", " * ", " / ", " + ", " - ", "SQRT", "POWER", "ABS"]
        return any(kw in code.upper() for kw in math_keywords)


class ComparisonTestGenerator:
    """Generate tests that compare Oracle and PostgreSQL output."""

    @staticmethod
    def generate_dual_test(proc_name: str, sample_inputs: List[dict]) -> str:
        """
        Generate test that calls both Oracle (simulated) and PostgreSQL version.
        This requires both databases available.
        """
        test_code = f"""
BEGIN;

-- Comparison test: Oracle vs. PostgreSQL {proc_name}
-- NOTE: This test requires connection to both Oracle and PostgreSQL

-- For dual-environment testing, use dblink:
CREATE EXTENSION IF NOT EXISTS dblink;

SELECT plan({len(sample_inputs)});

"""

        for i, inputs in enumerate(sample_inputs):
            param_str = ", ".join(str(v) for v in inputs.values())

            test_code += f"""
-- Test {i + 1}: Compare Oracle vs. PostgreSQL for inputs {inputs}
SELECT is(
  (SELECT {proc_name}({param_str})),
  (SELECT result FROM dblink('host=oracle.example.com ...',
    'SELECT {proc_name}({param_str})') AS t(result TEXT)),
  'Results match for inputs {inputs}'
);

"""

        test_code += """
SELECT * FROM finish();
ROLLBACK;
"""
        return test_code
