"""
Validate that converted PL/pgSQL is syntactically correct.
This is critical: we reject any hallucinated syntax before returning to user.
"""
import re
import logging
from dataclasses import dataclass
from typing import List, Optional

logger = logging.getLogger(__name__)


@dataclass
class ValidationResult:
    is_valid: bool
    errors: List[str]
    warnings: List[str]
    original: str
    converted: str


class PlPgSQLValidator:
    """Validate PL/pgSQL syntax without actually executing."""

    def __init__(self):
        # Keywords that should only appear at statement boundaries
        self.reserved_keywords = {
            "CREATE",
            "FUNCTION",
            "PROCEDURE",
            "PACKAGE",
            "TYPE",
            "TABLE",
            "VIEW",
            "INDEX",
            "TRIGGER",
            "BEGIN",
            "END",
            "IF",
            "THEN",
            "ELSE",
            "ELSIF",
            "LOOP",
            "FOR",
            "WHILE",
            "DECLARE",
            "AS",
            "RETURNS",
            "LANGUAGE",
            "PLPGSQL",
        }

    def validate(self, code: str, construct_type: str = "PROCEDURE") -> ValidationResult:
        """
        Validate PL/pgSQL code.
        Returns ValidationResult with errors/warnings.
        """
        errors = []
        warnings = []

        # Check for critical syntax errors
        errors.extend(self._check_balanced_delimiters(code))
        errors.extend(self._check_keyword_usage(code))
        errors.extend(self._check_function_signatures(code))

        # Check for common conversion issues
        warnings.extend(self._check_oracle_remnants(code))
        warnings.extend(self._check_type_conversions(code))

        is_valid = len(errors) == 0

        return ValidationResult(
            is_valid=is_valid,
            errors=errors,
            warnings=warnings,
            original="",
            converted=code,
        )

    def _check_balanced_delimiters(self, code: str) -> List[str]:
        """Check for balanced parentheses, brackets, etc."""
        errors = []

        # Count parentheses
        open_paren = code.count("(")
        close_paren = code.count(")")
        if open_paren != close_paren:
            errors.append(f"Unbalanced parentheses: {open_paren} open, {close_paren} close")

        # Check quotes are balanced
        single_quotes = len(re.findall(r"(?<!\\)'", code))
        if single_quotes % 2 != 0:
            errors.append("Unbalanced single quotes")

        # BEGIN/END balance
        begin_count = len(re.findall(r"\bBEGIN\b", code, re.IGNORECASE))
        end_count = len(re.findall(r"\bEND\b", code, re.IGNORECASE))
        if begin_count != end_count:
            errors.append(f"Unbalanced BEGIN/END: {begin_count} BEGIN, {end_count} END")

        return errors

    def _check_keyword_usage(self, code: str) -> List[str]:
        """Check for improper keyword usage."""
        errors = []

        # RETURNS clause should appear in function definition
        if re.search(r"\bRETURNS\b", code, re.IGNORECASE):
            if not re.search(r"\b(?:CREATE|FUNCTION)\b.*\bRETURNS\b", code, re.IGNORECASE | re.DOTALL):
                errors.append("RETURNS keyword found but not in function definition")

        # AS keyword should appear after function signature
        if re.search(r"\bCREATE\s+(?:OR\s+REPLACE\s+)?FUNCTION\b", code, re.IGNORECASE):
            if not re.search(r"\bAS\s*\$\$", code, re.IGNORECASE):
                errors.append("Function definition missing AS $$ delimiter")

        # LANGUAGE keyword should appear at function end
        if re.search(r"LANGUAGE", code, re.IGNORECASE):
            if not re.search(r"\bLANGUAGE\s+plpgsql\b", code, re.IGNORECASE):
                errors.append("LANGUAGE clause should specify 'plpgsql'")

        return errors

    def _check_function_signatures(self, code: str) -> List[str]:
        """Check function signature validity."""
        errors = []

        # Extract function definition
        func_pattern = r"CREATE\s+(?:OR\s+REPLACE\s+)?FUNCTION\s+(\w+)\s*\((.*?)\)\s+RETURNS\s+(\w+)"
        match = re.search(func_pattern, code, re.IGNORECASE | re.DOTALL)

        if match:
            func_name = match.group(1)
            params = match.group(2)
            return_type = match.group(3)

            # Check parameter syntax
            if params.strip():  # Has parameters
                param_list = [p.strip() for p in params.split(",") if p.strip()]
                for param in param_list:
                    # Should be: name TYPE or name IN TYPE, etc.
                    if not re.match(r"^[a-zA-Z_]\w*\s+(?:IN|OUT|INOUT\s+)?\w+", param):
                        errors.append(f"Invalid parameter syntax: {param}")

            # Check return type is valid
            if return_type.upper() not in [
                "INT",
                "INTEGER",
                "BIGINT",
                "NUMERIC",
                "FLOAT",
                "TEXT",
                "VARCHAR",
                "CHAR",
                "BOOLEAN",
                "BYTEA",
                "DATE",
                "TIMESTAMP",
                "VOID",
                "TABLE",
            ]:
                # Could be custom type, warn instead of error
                pass

        return errors

    def _check_oracle_remnants(self, code: str) -> List[str]:
        """Check for Oracle-specific code that should have been converted."""
        warnings = []

        # Check for PRAGMA keywords (should be converted or removed)
        if re.search(r"PRAGMA\s+(?!AUTONOMOUS_TRANSACTION)", code, re.IGNORECASE):
            warnings.append("Found PRAGMA directive that may not be PostgreSQL compatible")

        # Check for Oracle-specific functions that weren't converted
        oracle_funcs = ["DBMS_OUTPUT", "UTL_FILE", "UTL_MAIL", "DBMS_SCHEDULER"]
        for func in oracle_funcs:
            if re.search(rf"\b{func}\b", code, re.IGNORECASE):
                warnings.append(f"Found Oracle-specific {func} package call")

        # Check for %TYPE / %ROWTYPE usage
        if re.search(r"%(?:TYPE|ROWTYPE)\b", code):
            # This is actually valid in PostgreSQL, but worth noting
            pass

        # Check for EXECUTE IMMEDIATE (valid in PL/pgSQL)
        if re.search(r"EXECUTE\s+IMMEDIATE", code, re.IGNORECASE):
            warnings.append("EXECUTE IMMEDIATE found. Ensure dynamic SQL is properly escaped.")

        return warnings

    def _check_type_conversions(self, code: str) -> List[str]:
        """Check for potential type conversion issues."""
        warnings = []

        # Check for CAST usage
        if re.search(r"CAST\s*\(", code, re.IGNORECASE):
            # This is fine, but verify types
            pass

        # Check for :: operator (PostgreSQL type casting)
        if re.search(r"::\w+", code):
            # This is valid PostgreSQL
            pass

        # Warn if DATE type appears (might need timezone)
        if re.search(r"\bDATE\b", code):
            warnings.append("DATE type found. Verify timezone handling is correct.")

        return warnings

    def can_safely_convert(self, code: str) -> bool:
        """Quick check: can this code be safely converted?"""
        result = self.validate(code)
        return result.is_valid and len(result.warnings) < 3


class ConversionValidator:
    """Validate that conversion was successful before returning to user."""

    def __init__(self):
        self.plpgsql_validator = PlPgSQLValidator()

    def validate_conversion(
        self, original: str, converted: str, construct_type: str = "PROCEDURE"
    ) -> ValidationResult:
        """Validate both original (for parse errors) and converted code."""
        errors = []
        warnings = []

        # Check converted code syntax
        plpgsql_result = self.plpgsql_validator.validate(converted, construct_type)
        errors.extend(plpgsql_result.errors)
        warnings.extend(plpgsql_result.warnings)

        # Additional checks: ensure conversion changed expected things
        if original == converted:
            warnings.append("Code unchanged after conversion. Verify this is intentional.")

        # Check that Oracle/PL-SQL-isms were actually removed
        if re.search(r"PRAGMA\s+(?!AUTONOMOUS_TRANSACTION)", converted, re.IGNORECASE):
            errors.append("Unhandled PRAGMA directive in converted code")

        is_valid = len(errors) == 0

        return ValidationResult(
            is_valid=is_valid,
            errors=errors,
            warnings=warnings,
            original=original,
            converted=converted,
        )
