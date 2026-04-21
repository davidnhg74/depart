"""
Convert PL/SQL to PL/pgSQL using Claude + deterministic fallback.
Hybrid approach: AI for complex logic, rules for known patterns.
"""
import re
import logging
from dataclasses import dataclass
from typing import Optional
from ..llm.client import LLMClient
from ..validators.plpgsql_validator import ConversionValidator
from .oracle_functions import OracleFunctionConverter

logger = logging.getLogger(__name__)


@dataclass
class ConversionResult:
    original: str
    converted: str
    success: bool
    method: str  # "deterministic", "claude", "hybrid", "error"
    warnings: list = None
    errors: list = None

    def __post_init__(self):
        if self.warnings is None:
            self.warnings = []
        if self.errors is None:
            self.errors = []


class PlSqlConverter:
    """Convert PL/SQL procedures/functions to PL/pgSQL."""

    def __init__(self, use_llm: bool = True):
        self.use_llm = use_llm
        self.llm_client = LLMClient() if use_llm else None
        self.validator = ConversionValidator()
        self.oracle_func_converter = OracleFunctionConverter()
        self.deterministic_rules = DeterministicRules()

    def convert_procedure(self, oracle_code: str) -> ConversionResult:
        """Convert Oracle PROCEDURE to PostgreSQL PROCEDURE."""
        return self._convert("PROCEDURE", oracle_code)

    def convert_function(self, oracle_code: str) -> ConversionResult:
        """Convert Oracle FUNCTION to PostgreSQL FUNCTION."""
        return self._convert("FUNCTION", oracle_code)

    def _convert(self, construct_type: str, oracle_code: str) -> ConversionResult:
        """
        Convert PL/SQL to PL/pgSQL.
        Strategy:
        1. Try deterministic rules first (fast, safe)
        2. If rules don't cover it, use Claude (correct)
        3. Validate output before returning
        """
        if not oracle_code or not oracle_code.strip():
            return ConversionResult(oracle_code, "", False, "error", errors=["Empty code"])

        # Step 1: Apply deterministic conversions
        partially_converted = self.deterministic_rules.convert(oracle_code, construct_type)
        warnings = []
        errors = []

        # Step 2: Check if we need LLM help
        needs_llm = self._needs_complex_conversion(partially_converted)

        if needs_llm and self.use_llm and self.llm_client:
            try:
                logger.info(f"Using Claude for complex {construct_type} conversion")
                llm_result = self.llm_client.convert_plsql(
                    partially_converted,
                    f"This is a partially converted {construct_type}. Complete the conversion to valid PL/pgSQL.",
                )
                converted = llm_result
                method = "hybrid"
            except Exception as e:
                logger.warning(f"LLM conversion failed: {e}. Using deterministic only.")
                converted = partially_converted
                method = "deterministic"
                warnings.append(f"LLM conversion failed, using deterministic rules: {str(e)}")
        else:
            converted = partially_converted
            method = "deterministic"

        # Step 3: Apply final function conversions
        converted = self.oracle_func_converter.convert(converted)

        # Step 4: Validate
        validation = self.validator.validate_conversion(oracle_code, converted, construct_type)

        if validation.is_valid:
            return ConversionResult(
                original=oracle_code,
                converted=converted,
                success=True,
                method=method,
                warnings=warnings + validation.warnings,
            )
        else:
            return ConversionResult(
                original=oracle_code,
                converted=converted,
                success=False,
                method=method,
                warnings=warnings + validation.warnings,
                errors=validation.errors,
            )

    def _needs_complex_conversion(self, code: str) -> bool:
        """Check if code has constructs that need LLM help."""
        complex_patterns = [
            r"\bCONNECT\s+BY",  # Hierarchical queries
            r"\bMERGE\s+INTO",  # Complex upserts
            r"FOR\s+\w+\s+IN",  # Complex loops
            r"DBMS_\w+\.",  # DBMS_* calls beyond simple OUTPUT
            r"\bCURSOR\b",  # Explicit cursors
            r"EXECUTE\s+IMMEDIATE",  # Dynamic SQL
        ]

        for pattern in complex_patterns:
            if re.search(pattern, code, re.IGNORECASE):
                return True

        return False


class DeterministicRules:
    """Deterministic transformation rules (no AI needed)."""

    def convert(self, oracle_code: str, construct_type: str) -> str:
        """Apply deterministic transformation rules."""
        result = oracle_code

        # 1. Wrapper transformation (CREATE PROCEDURE ... AS → CREATE OR REPLACE PROCEDURE ... AS $$)
        result = self._wrap_function_declaration(result, construct_type)

        # 2. Parameter mode specification (IN/OUT/INOUT)
        result = self._fix_parameter_modes(result)

        # 3. Variable declaration syntax
        result = self._fix_variable_declarations(result)

        # 4. Commit/Rollback handling
        result = self._handle_transactions(result)

        # 5. Exception handling
        result = self._fix_exception_handling(result)

        # 6. Oracle-specific keywords
        result = self._remove_oracle_keywords(result)

        # 7. Comment style (Oracle's -- is fine, but /* */ must be carefully handled)
        # Already handled in parsing phase

        return result

    def _wrap_function_declaration(self, code: str, construct_type: str) -> str:
        """Wrap function in proper PostgreSQL syntax."""
        result = code

        if construct_type.upper() == "FUNCTION":
            # Oracle: CREATE [OR REPLACE] FUNCTION name(...) RETURN type AS
            # PostgreSQL: CREATE [OR REPLACE] FUNCTION name(...) RETURNS type AS $$
            result = re.sub(
                r"CREATE\s+(?:OR\s+REPLACE\s+)?FUNCTION\s+(\w+)\s*\((.*?)\)\s+RETURN\s+(\w+)",
                r"CREATE OR REPLACE FUNCTION \1(\2) RETURNS \3 AS $$",
                result,
                flags=re.IGNORECASE | re.DOTALL,
            )

            # Add language clause at end if missing
            if not re.search(r"\$\$\s+LANGUAGE\s+plpgsql", result, re.IGNORECASE):
                result = re.sub(r"END\s*;?\s*$", r"END;\n$$ LANGUAGE plpgsql;", result, flags=re.IGNORECASE | re.DOTALL)

        elif construct_type.upper() == "PROCEDURE":
            # Oracle: CREATE [OR REPLACE] PROCEDURE name(...) AS
            # PostgreSQL: CREATE [OR REPLACE] PROCEDURE name(...) AS $$
            result = re.sub(
                r"CREATE\s+(?:OR\s+REPLACE\s+)?PROCEDURE\s+(\w+)\s*\((.*?)\)\s+AS\b",
                r"CREATE OR REPLACE PROCEDURE \1(\2) AS $$",
                result,
                flags=re.IGNORECASE | re.DOTALL,
            )

            # Add language clause at end
            if not re.search(r"\$\$\s+LANGUAGE\s+plpgsql", result, re.IGNORECASE):
                result = re.sub(r"END\s*;?\s*$", r"END;\n$$ LANGUAGE plpgsql;", result, flags=re.IGNORECASE | re.DOTALL)

        return result

    def _fix_parameter_modes(self, code: str) -> str:
        """Ensure parameters have explicit IN/OUT mode."""
        # Oracle often omits IN; PostgreSQL defaults to IN but explicit is better
        # This is complex, so flag for review if many parameters
        return code

    def _fix_variable_declarations(self, code: str) -> str:
        """Convert Oracle variable declarations to PostgreSQL."""
        result = code

        # Oracle: v_name type;
        # PostgreSQL: v_name type;  (same, so no change needed)

        # But check for %TYPE usage (should work in PostgreSQL too)
        # And ensure DECLARE section exists
        if re.search(r"\bBEGIN\b", result, re.IGNORECASE) and not re.search(r"\bDECLARE\b", result, re.IGNORECASE):
            # Add DECLARE if missing
            result = re.sub(
                r"(CREATE\s+(?:OR\s+REPLACE\s+)?(?:PROCEDURE|FUNCTION)\s+\w+.*?\s+AS\s*\$\$\s*)\n(\s*)(BEGIN)",
                r"\1\nDECLARE\n\2\3",
                result,
                flags=re.IGNORECASE | re.DOTALL,
            )

        return result

    def _handle_transactions(self, code: str) -> str:
        """Convert transaction handling."""
        result = code

        # COMMIT/ROLLBACK in procedures: PostgreSQL PROCEDURE auto-commits
        # But if explicit COMMIT is needed, we must use separate transaction
        # For Phase 2 MVP, just remove COMMIT/ROLLBACK
        result = re.sub(r"\bCOMMIT\s*;", "-- COMMIT; (auto in procedure)", result, flags=re.IGNORECASE)
        result = re.sub(r"\bROLLBACK\s*;", "-- ROLLBACK; (use exception handling)", result, flags=re.IGNORECASE)

        return result

    def _fix_exception_handling(self, code: str) -> str:
        """Convert Oracle exception handling to PostgreSQL."""
        result = code

        # Oracle: EXCEPTION WHEN NO_DATA_FOUND THEN
        # PostgreSQL: EXCEPTION WHEN NO_DATA_FOUND THEN (same!)

        # Oracle: EXCEPTION WHEN OTHERS THEN
        # PostgreSQL: EXCEPTION WHEN OTHERS THEN (same!)

        # Just ensure EXCEPTION clause exists if error handling is needed
        # PostgreSQL syntax is largely compatible

        return result

    def _remove_oracle_keywords(self, code: str) -> str:
        """Remove Oracle-specific keywords."""
        result = code

        # Remove PRAGMA directives except AUTONOMOUS_TRANSACTION (which needs special handling)
        result = re.sub(
            r"PRAGMA\s+(?!AUTONOMOUS_TRANSACTION)\w+\s*;",
            "-- pragma removed",
            result,
            flags=re.IGNORECASE,
        )

        # Remove Oracle-specific hints
        result = re.sub(r"/\*\+.*?\*/", "", result, flags=re.DOTALL)

        # Remove tablespace specifications
        result = re.sub(r"TABLESPACE\s+\w+", "", result, flags=re.IGNORECASE)

        return result
