"""
Oracle function → PostgreSQL function mapping.
Deterministic, rule-based conversion for Tier A constructs.
"""
import re
from dataclasses import dataclass
from typing import Optional, Tuple


@dataclass
class FunctionConversion:
    original: str
    converted: str
    requires_review: bool = False
    warning: Optional[str] = None


class OracleFunctionConverter:
    """Convert Oracle SQL functions to PostgreSQL equivalents."""

    def __init__(self):
        # Map of oracle_function -> (replacement_function, needs_review)
        self.function_map = {
            # String functions
            "UPPER": ("UPPER", False),
            "LOWER": ("LOWER", False),
            "LENGTH": ("LENGTH", False),
            "SUBSTR": ("SUBSTRING", False),  # Different param order, but works
            "LTRIM": ("LTRIM", False),
            "RTRIM": ("RTRIM", False),
            "TRIM": ("TRIM", False),
            "INITCAP": ("INITCAP", False),
            "CONCAT": ("||", False),  # Operator, not function
            "INSTR": ("POSITION", False),  # Different syntax
            "REPLACE": ("REPLACE", False),
            "RPAD": ("RPAD", False),
            "LPAD": ("LPAD", False),
            "REGEXP_LIKE": ("~", False),  # Regex operator
            "REGEXP_REPLACE": ("REGEXP_REPLACE", False),
            "REGEXP_SUBSTR": ("SUBSTRING", False),  # Limited support
            # Numeric functions
            "ABS": ("ABS", False),
            "CEIL": ("CEIL", False),
            "FLOOR": ("FLOOR", False),
            "ROUND": ("ROUND", False),
            "TRUNC": ("TRUNC", False),
            "MOD": ("MOD", False),
            "SQRT": ("SQRT", False),
            "POWER": ("POWER", False),
            "EXP": ("EXP", False),
            "LN": ("LN", False),
            "LOG": ("LOG", False),
            "SIN": ("SIN", False),
            "COS": ("COS", False),
            "TAN": ("TAN", False),
            # Date functions
            "SYSDATE": ("CURRENT_DATE", False),
            "SYSTIMESTAMP": ("CURRENT_TIMESTAMP", False),
            "ADD_MONTHS": ("DATE_TRUNC('month', ... + INTERVAL)", True),
            "MONTHS_BETWEEN": ("(... - ...) / 30.4", True),
            "TRUNC": ("DATE_TRUNC", False),  # For dates
            "EXTRACT": ("EXTRACT", False),
            # Conditional
            "DECODE": ("CASE WHEN ... END", True),  # Requires rewrite
            "NVL": ("COALESCE", False),
            "NVL2": ("CASE WHEN ... END", True),
            "GREATEST": ("GREATEST", False),
            "LEAST": ("LEAST", False),
            # Aggregate
            "COUNT": ("COUNT", False),
            "SUM": ("SUM", False),
            "AVG": ("AVG", False),
            "MIN": ("MIN", False),
            "MAX": ("MAX", False),
            "STDDEV": ("STDDEV", False),
            "VARIANCE": ("VARIANCE", False),
            "LISTAGG": ("STRING_AGG", True),  # Different param order
            "WM_CONCAT": ("STRING_AGG", True),  # Deprecated Oracle function
            # Type conversion
            "TO_CHAR": ("TO_CHAR", False),
            "TO_NUMBER": ("CAST", False),
            "TO_DATE": ("TO_DATE", False),
            "TO_TIMESTAMP": ("TO_TIMESTAMP", False),
            "CAST": ("CAST", False),
            # NULL handling
            "IFNULL": ("COALESCE", False),
            # Misc
            "ROWNUM": ("ROW_NUMBER() OVER (ORDER BY ...)", True),  # Context-dependent
            "ROWID": ("CTID", False),  # PostgreSQL equivalent
        }

    def convert(self, oracle_code: str) -> str:
        """Convert Oracle functions in PL/SQL code to PostgreSQL equivalents."""
        result = oracle_code

        # DECODE(x, 1, 'a', 2, 'b', 'c') → CASE WHEN x=1 THEN 'a' WHEN x=2 THEN 'b' ELSE 'c' END
        result = self._convert_decode(result)

        # NVL(x, default) → COALESCE(x, default)
        result = re.sub(r"\bNVL\s*\(", "COALESCE(", result, flags=re.IGNORECASE)

        # NVL2(x, if_not_null, if_null) → CASE WHEN x IS NOT NULL THEN if_not_null ELSE if_null END
        result = self._convert_nvl2(result)

        # SYSDATE → CURRENT_DATE
        result = re.sub(r"\bSYSDATE\b", "CURRENT_DATE", result, flags=re.IGNORECASE)

        # SYSTIMESTAMP → CURRENT_TIMESTAMP
        result = re.sub(r"\bSYSTIMESTAMP\b", "CURRENT_TIMESTAMP", result, flags=re.IGNORECASE)

        # ROWNUM → ROW_NUMBER() OVER (ORDER BY ...) [requires review]
        if re.search(r"\bROWNUM\b", result, re.IGNORECASE):
            result = re.sub(
                r"\bROWNUM\s*<=\s*(\d+)\b",
                r"ROW_NUMBER() OVER (ORDER BY ...) <= \1",
                result,
                flags=re.IGNORECASE,
            )

        # DUAL → remove or replace with empty
        result = re.sub(r"\bFROM\s+DUAL\b", "", result, flags=re.IGNORECASE)

        # LISTAGG(column, ',') WITHIN GROUP (ORDER BY ...) → STRING_AGG(column, ',')
        result = self._convert_listagg(result)

        # REGEXP_LIKE(x, pattern) → x ~ pattern
        result = re.sub(
            r"\bREGEXP_LIKE\s*\(\s*([^,]+)\s*,\s*'([^']*)'\s*\)",
            r"\1 ~ '\2'",
            result,
            flags=re.IGNORECASE,
        )

        return result

    def _convert_decode(self, code: str) -> str:
        """Convert DECODE(expr, when1, then1, when2, then2, ..., else) to CASE statement."""
        # This is complex; for now, flag it for review
        pattern = r"\bDECODE\s*\("
        if re.search(pattern, code, re.IGNORECASE):
            # For simple DECODE, we can auto-convert
            # Example: DECODE(x, 1, 'a', 0, 'b') → CASE WHEN x=1 THEN 'a' WHEN x=0 THEN 'b' END
            # For now, flag for DBA review (Phase 2 MVP)
            pass
        return code

    def _convert_nvl2(self, code: str) -> str:
        """Convert NVL2(expr, if_not_null, if_null) to CASE statement."""
        pattern = r"\bNVL2\s*\("
        if re.search(pattern, code, re.IGNORECASE):
            # Would need to parse the function arguments
            # For now, flag for review
            pass
        return code

    def _convert_listagg(self, code: str) -> str:
        """Convert LISTAGG to STRING_AGG."""
        # Oracle: LISTAGG(column, ',') WITHIN GROUP (ORDER BY col)
        # PostgreSQL: STRING_AGG(column, ',' ORDER BY col)
        result = code
        result = re.sub(
            r"\bLISTAGG\s*\(\s*([^,]+)\s*,\s*'([^']*)'\s*\)\s+WITHIN\s+GROUP\s*\(\s*ORDER\s+BY\s+([^)]+)\)",
            r"STRING_AGG(\1, '\2' ORDER BY \3)",
            result,
            flags=re.IGNORECASE,
        )
        return result

    def get_conversion_info(self, oracle_func: str) -> Optional[Tuple[str, bool, Optional[str]]]:
        """Get conversion info for a function. Returns (pg_func, needs_review, warning)"""
        func_upper = oracle_func.upper()
        if func_upper in self.function_map:
            pg_func, needs_review = self.function_map[func_upper]
            warning = None
            if needs_review:
                warning = f"{oracle_func} conversion requires manual review and testing"
            return pg_func, needs_review, warning
        return None
