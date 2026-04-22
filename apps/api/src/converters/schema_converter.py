"""
Convert Oracle DDL to PostgreSQL DDL.
100% deterministic — no LLM calls needed.
"""
import re
from dataclasses import dataclass
from typing import List, Dict, Tuple


@dataclass
class ConvertedDDL:
    original: str
    converted: str
    construct_type: str  # TABLE, SEQUENCE, INDEX, VIEW, CONSTRAINT
    construct_name: str
    warnings: List[str] = None

    def __post_init__(self):
        if self.warnings is None:
            self.warnings = []


class OracleDataTypeMapper:
    """Map Oracle data types to PostgreSQL equivalents."""

    DATATYPE_MAP = {
        # Exact matches
        r"\bINT(?:EGER)?\b": "INTEGER",
        r"\bBIGINT\b": "BIGINT",
        r"\bSMALLINT\b": "SMALLINT",
        r"\bFLOAT\b": "DOUBLE PRECISION",
        r"\bREAL\b": "REAL",
        r"\bBOOLEAN\b": "BOOLEAN",
        r"\bVARCHAR\b": "VARCHAR",
        r"\bCHAR\b": "CHAR",
        r"\bTEXT\b": "TEXT",
        r"\bBYTEA\b": "BYTEA",
        # Oracle-specific
        r"\bNUMBER(?:\s*\(\s*(\d+)\s*,\s*(\d+)\s*\))?\b": "NUMERIC\\1,\\2",
        r"\bVARCHAR2\b": "VARCHAR",
        r"\bNCHAR\b": "CHAR",
        r"\bNVARCHAR2\b": "VARCHAR",
        r"\bCLOB\b": "TEXT",
        r"\bBLOB\b": "BYTEA",
        r"\bRAW\b": "BYTEA",
        r"\bLONG\b": "TEXT",
        r"\bLONG\s+RAW\b": "BYTEA",
        r"\bDATE\b": "TIMESTAMP WITHOUT TIME ZONE",
        r"\bTIMESTAMP(?:\s+WITH\s+TIME\s+ZONE)?\b": "TIMESTAMP WITH TIME ZONE",
    }

    @staticmethod
    def convert_datatype(oracle_type: str) -> Tuple[str, List[str]]:
        """Convert Oracle datatype to PostgreSQL. Returns (new_type, warnings)"""
        warnings = []
        result = oracle_type.strip()

        # Handle NUMBER(p,s) → NUMERIC(p,s)
        if re.search(r"\bNUMBER\b", result, re.IGNORECASE):
            match = re.search(r"NUMBER\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)", result, re.IGNORECASE)
            if match:
                result = re.sub(
                    r"NUMBER\s*\(\s*\d+\s*,\s*\d+\s*\)",
                    f"NUMERIC({match.group(1)},{match.group(2)})",
                    result,
                    flags=re.IGNORECASE,
                )
            else:
                # NUMBER without precision
                result = re.sub(r"\bNUMBER\b", "NUMERIC", result, flags=re.IGNORECASE)

        # VARCHAR2 → VARCHAR
        result = re.sub(r"\bVARCHAR2\b", "VARCHAR", result, flags=re.IGNORECASE)

        # DATE → TIMESTAMP (warn about timezone)
        if re.search(r"\bDATE\b", result, re.IGNORECASE):
            warnings.append("Oracle DATE has no timezone; PostgreSQL TIMESTAMP WITH TIME ZONE assumed. Verify in tests.")
            result = re.sub(r"\bDATE\b", "TIMESTAMP WITHOUT TIME ZONE", result, flags=re.IGNORECASE)

        # LONG → TEXT
        if re.search(r"\bLONG\b", result, re.IGNORECASE):
            warnings.append("LONG is deprecated Oracle type. TEXT has no length limit in PostgreSQL.")
            result = re.sub(r"\bLONG\b", "TEXT", result, flags=re.IGNORECASE)

        # CLOB → TEXT
        result = re.sub(r"\bCLOB\b", "TEXT", result, flags=re.IGNORECASE)

        # BLOB → BYTEA
        result = re.sub(r"\bBLOB\b", "BYTEA", result, flags=re.IGNORECASE)

        return result, warnings


class SchemaConverter:
    def __init__(self):
        self.datatype_mapper = OracleDataTypeMapper()

    def convert_table(self, oracle_ddl: str) -> ConvertedDDL:
        """Convert CREATE TABLE from Oracle to PostgreSQL."""
        warnings = []

        # Extract table name
        table_match = re.search(r"CREATE\s+(?:GLOBAL\s+TEMPORARY\s+)?TABLE\s+(\w+)", oracle_ddl, re.IGNORECASE)
        if not table_match:
            return ConvertedDDL(oracle_ddl, "", "TABLE", "", ["Could not parse table definition"])

        table_name = table_match.group(1)
        converted = oracle_ddl

        # Remove Oracle-specific clauses
        converted = re.sub(
            r"(?:TABLESPACE|STORAGE|PCTFREE|INITRANS|LOGGING|NOCOMPRESS)\s+[^,;]*",
            "",
            converted,
            flags=re.IGNORECASE,
        )

        # Convert GLOBAL TEMPORARY TABLE
        if re.search(r"GLOBAL\s+TEMPORARY", converted, re.IGNORECASE):
            converted = re.sub(r"CREATE\s+GLOBAL\s+TEMPORARY\s+TABLE", "CREATE TEMP TABLE", converted, flags=re.IGNORECASE)
            converted += "\nON COMMIT DELETE ROWS;"

        # Convert data types in column definitions
        converted = self._convert_column_datatypes(converted)

        # Convert constraints
        converted = self._convert_constraints(converted)

        # Clean up extra commas
        converted = re.sub(r",\s*,", ",", converted)
        converted = re.sub(r",\s*\)", ")", converted)

        # Ensure semicolon
        if not converted.rstrip().endswith(";"):
            converted = converted.rstrip() + ";"

        return ConvertedDDL(oracle_ddl, converted, "TABLE", table_name, warnings)

    def convert_sequence(self, oracle_ddl: str) -> ConvertedDDL:
        """Convert CREATE SEQUENCE from Oracle to PostgreSQL.
        Oracle and PostgreSQL syntax is nearly identical."""
        warnings = []

        seq_match = re.search(r"CREATE\s+SEQUENCE\s+(\w+)", oracle_ddl, re.IGNORECASE)
        if not seq_match:
            return ConvertedDDL(oracle_ddl, "", "SEQUENCE", "", ["Could not parse sequence definition"])

        seq_name = seq_match.group(1)
        converted = oracle_ddl

        # Oracle-specific: NOCACHE → PostgreSQL uses CACHE by default (change if explicit NOCACHE)
        if re.search(r"NOCACHE", converted, re.IGNORECASE):
            converted = re.sub(r"\s+NOCACHE\b", " CACHE 1", converted, flags=re.IGNORECASE)
            warnings.append("Oracle NOCACHE → PostgreSQL CACHE 1 (both avoid caching)")

        # Remove irrelevant Oracle clauses (CYCLE/NOCYCLE are same)
        # Most Oracle sequence options work in PostgreSQL

        if not converted.rstrip().endswith(";"):
            converted = converted.rstrip() + ";"

        return ConvertedDDL(oracle_ddl, converted, "SEQUENCE", seq_name, warnings)

    def convert_index(self, oracle_ddl: str) -> ConvertedDDL:
        """Convert CREATE INDEX from Oracle to PostgreSQL.
        Most syntax is identical; remove Oracle-specific hints."""
        warnings = []

        idx_match = re.search(r"CREATE\s+(?:UNIQUE\s+)?INDEX\s+(\w+)", oracle_ddl, re.IGNORECASE)
        if not idx_match:
            return ConvertedDDL(oracle_ddl, "", "INDEX", "", ["Could not parse index definition"])

        idx_name = idx_match.group(1)
        converted = oracle_ddl

        # Remove Oracle-specific index clauses
        converted = re.sub(
            r"(?:TABLESPACE|STORAGE|PCTFREE|INITRANS|LOGGING|PARALLEL)\s+[^,;]*",
            "",
            converted,
            flags=re.IGNORECASE,
        )

        # Remove hints in index (Oracle specific)
        if re.search(r"/\*\+", converted):
            converted = re.sub(r"/\*\+.*?\*/", "", converted, flags=re.DOTALL)
            warnings.append("Oracle index hints removed (not supported in PostgreSQL)")

        # Clean up
        converted = re.sub(r"\s+", " ", converted)
        converted = converted.rstrip()
        if not converted.endswith(";"):
            converted += ";"

        return ConvertedDDL(oracle_ddl, converted, "INDEX", idx_name, warnings)

    def convert_view(self, oracle_ddl: str) -> ConvertedDDL:
        """Convert CREATE VIEW from Oracle to PostgreSQL.
        Main difference: remove hints, convert embedded queries."""
        warnings = []

        view_match = re.search(r"CREATE\s+(?:OR\s+REPLACE\s+)?VIEW\s+(\w+)", oracle_ddl, re.IGNORECASE)
        if not view_match:
            return ConvertedDDL(oracle_ddl, "", "VIEW", "", ["Could not parse view definition"])

        view_name = view_match.group(1)
        converted = oracle_ddl

        # Remove hints
        if re.search(r"/\*\+", converted):
            converted = re.sub(r"/\*\+.*?\*/", "", converted, flags=re.DOTALL)
            warnings.append("Oracle hints removed from view query")

        # Convert embedded Oracle functions
        converted = self._convert_oracle_functions(converted)

        # Ensure semicolon
        if not converted.rstrip().endswith(";"):
            converted = converted.rstrip() + ";"

        return ConvertedDDL(oracle_ddl, converted, "VIEW", view_name, warnings)

    def _convert_column_datatypes(self, ddl: str) -> str:
        """Convert all data types in a DDL statement."""
        result = ddl

        # Find all column definitions and convert their data types
        # This is simplistic but works for most cases
        for oracle_type, pg_type in [
            ("VARCHAR2", "VARCHAR"),
            ("NVARCHAR2", "VARCHAR"),
            ("NCHAR", "CHAR"),
            ("CLOB", "TEXT"),
            ("BLOB", "BYTEA"),
            ("LONG RAW", "BYTEA"),
            ("RAW", "BYTEA"),
            ("DATE", "TIMESTAMP WITHOUT TIME ZONE"),
        ]:
            result = re.sub(rf"\b{oracle_type}\b", pg_type, result, flags=re.IGNORECASE)

        # Handle NUMBER(p,s) → NUMERIC(p,s)
        result = re.sub(
            r"\bNUMBER\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)",
            r"NUMERIC(\1,\2)",
            result,
            flags=re.IGNORECASE,
        )
        # Handle NUMBER without precision
        result = re.sub(r"\bNUMBER\b", "NUMERIC", result, flags=re.IGNORECASE)

        return result

    def _convert_constraints(self, ddl: str) -> str:
        """Convert constraint definitions."""
        result = ddl

        # CONSTRAINT pk_name PRIMARY KEY → PRIMARY KEY (PostgreSQL infers name)
        # Keep explicit names for clarity
        result = re.sub(
            r"CONSTRAINT\s+(\w+)\s+PRIMARY\s+KEY",
            r"CONSTRAINT \1 PRIMARY KEY",
            result,
            flags=re.IGNORECASE,
        )

        return result

    def _convert_oracle_functions(self, ddl: str) -> str:
        """Convert Oracle functions embedded in DDL (e.g., views, check constraints)."""
        result = ddl

        # Simple replacements (can expand)
        replacements = [
            (r"\bSYSDATE\b", "CURRENT_TIMESTAMP"),
            (r"\bDUAL\b", ""),  # Remove DUAL table reference
            (r"\bNVL\(", "COALESCE("),
        ]

        for oracle_func, pg_func in replacements:
            result = re.sub(oracle_func, pg_func, result, flags=re.IGNORECASE)

        return result

    def convert(self, oracle_ddl: str) -> ConvertedDDL:
        """Route to appropriate converter based on DDL type."""
        if re.search(r"CREATE\s+(?:OR\s+REPLACE\s+)?(?:GLOBAL\s+TEMPORARY\s+)?TABLE\b", oracle_ddl, re.IGNORECASE):
            return self.convert_table(oracle_ddl)
        elif re.search(r"CREATE\s+(?:OR\s+REPLACE\s+)?VIEW\b", oracle_ddl, re.IGNORECASE):
            return self.convert_view(oracle_ddl)
        elif re.search(r"CREATE\s+SEQUENCE\b", oracle_ddl, re.IGNORECASE):
            return self.convert_sequence(oracle_ddl)
        elif re.search(r"CREATE\s+(?:UNIQUE\s+)?INDEX\b", oracle_ddl, re.IGNORECASE):
            return self.convert_index(oracle_ddl)
        else:
            return ConvertedDDL(oracle_ddl, oracle_ddl, "UNKNOWN", "", ["Could not identify DDL type"])
