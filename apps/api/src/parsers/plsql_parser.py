import re
from dataclasses import dataclass, field
from enum import Enum
from typing import List, Dict, Set


class ConstructType(str, Enum):
    PROCEDURE = "PROCEDURE"
    FUNCTION = "FUNCTION"
    PACKAGE = "PACKAGE"
    TRIGGER = "TRIGGER"
    VIEW = "VIEW"
    SEQUENCE = "SEQUENCE"
    TABLE = "TABLE"
    INDEX = "INDEX"
    CONSTRAINT = "CONSTRAINT"
    TYPE = "TYPE"
    PRAGMA = "PRAGMA"
    CONNECT_BY = "CONNECT_BY"
    MERGE = "MERGE"
    DBMS_CALL = "DBMS_CALL"
    AUTONOMOUS_TXN = "AUTONOMOUS_TXN"
    ROWTYPE = "ROWTYPE"
    TYPE_ATTR = "TYPE_ATTR"
    GLOBAL_TEMP_TABLE = "GLOBAL_TEMP_TABLE"
    VPD = "VPD"
    EXECUTE_IMMEDIATE = "EXECUTE_IMMEDIATE"
    OBJECT_TYPE = "OBJECT_TYPE"
    NESTED_TABLE = "NESTED_TABLE"
    DATABASE_LINK = "DATABASE_LINK"
    EXTERNAL_PROCEDURE = "EXTERNAL_PROCEDURE"
    DBMS_SCHEDULER = "DBMS_SCHEDULER"
    DBMS_AQ = "DBMS_AQ"
    DBMS_CRYPTO = "DBMS_CRYPTO"
    SPATIAL = "SPATIAL"
    ORACLE_TEXT = "ORACLE_TEXT"


@dataclass
class Construct:
    type: ConstructType
    name: str
    start_line: int
    end_line: int
    line_count: int
    content: str = ""


@dataclass
class ParserResult:
    total_lines: int
    constructs: List[Construct] = field(default_factory=list)
    tier_a_lines: int = 0
    tier_b_lines: int = 0
    tier_c_lines: int = 0
    parse_errors: List[str] = field(default_factory=list)


class PlSqlParser:
    def __init__(self):
        self.constructs: List[Construct] = []
        self.tier_a_constructs = {
            ConstructType.PROCEDURE,
            ConstructType.FUNCTION,
            ConstructType.PACKAGE,
            ConstructType.TRIGGER,
            ConstructType.VIEW,
            ConstructType.SEQUENCE,
            ConstructType.TABLE,
            ConstructType.INDEX,
        }
        self.tier_b_constructs = {
            ConstructType.CONNECT_BY,
            ConstructType.MERGE,
            ConstructType.ROWTYPE,
            ConstructType.TYPE_ATTR,
            ConstructType.GLOBAL_TEMP_TABLE,
            ConstructType.VPD,
            ConstructType.EXECUTE_IMMEDIATE,
            ConstructType.OBJECT_TYPE,
            ConstructType.NESTED_TABLE,
        }
        self.tier_c_constructs = {
            ConstructType.AUTONOMOUS_TXN,
            ConstructType.DBMS_SCHEDULER,
            ConstructType.DBMS_AQ,
            ConstructType.DBMS_CRYPTO,
            ConstructType.SPATIAL,
            ConstructType.ORACLE_TEXT,
            ConstructType.DATABASE_LINK,
            ConstructType.EXTERNAL_PROCEDURE,
        }

    def parse(self, content: str) -> ParserResult:
        """Parse Oracle PL/SQL content and return structured analysis."""
        lines = content.split("\n") if content.strip() else []
        total_lines = len(lines)

        result = ParserResult(total_lines=total_lines)

        # Remove comments for analysis
        cleaned = self._remove_comments(content)

        # Find all constructs
        self._find_constructs(cleaned, lines)

        # Classify lines by tier
        tier_map = self._classify_lines(cleaned)
        result.tier_a_lines = tier_map.get("tier_a", 0)
        result.tier_b_lines = tier_map.get("tier_b", 0)
        result.tier_c_lines = tier_map.get("tier_c", 0)
        result.constructs = self.constructs

        return result

    def _remove_comments(self, content: str) -> str:
        """Remove single-line and multi-line comments."""
        # Remove /* */ comments
        content = re.sub(r"/\*.*?\*/", "", content, flags=re.DOTALL)
        # Remove -- comments
        content = re.sub(r"--[^\n]*", "", content)
        return content

    def _find_constructs(self, cleaned: str, original_lines: List[str]):
        """Find constructs in PL/SQL code."""
        # Procedure/Function/Package
        self._find_procedures_functions(cleaned, original_lines)
        self._find_triggers(cleaned, original_lines)
        self._find_tables(cleaned, original_lines)
        self._find_views(cleaned, original_lines)
        self._find_sequences(cleaned, original_lines)
        self._find_packages(cleaned, original_lines)
        self._find_special_constructs(cleaned, original_lines)

    def _find_procedures_functions(self, content: str, lines: List[str]):
        """Find PROCEDURE and FUNCTION definitions."""
        proc_pattern = r"(?:CREATE\s+(?:OR\s+REPLACE\s+)?)?(?:PROCEDURE|FUNCTION)\s+(\w+)"
        for match in re.finditer(proc_pattern, content, re.IGNORECASE):
            construct_type = ConstructType.FUNCTION if "FUNCTION" in match.group(0).upper() else ConstructType.PROCEDURE
            self.constructs.append(
                Construct(
                    type=construct_type,
                    name=match.group(1),
                    start_line=self._get_line_number(content, match.start()),
                    end_line=self._get_line_number(content, match.end()),
                    line_count=1,
                )
            )

    def _find_triggers(self, content: str, lines: List[str]):
        """Find TRIGGER definitions."""
        trigger_pattern = r"(?:CREATE\s+(?:OR\s+REPLACE\s+)?)?TRIGGER\s+(\w+)"
        for match in re.finditer(trigger_pattern, content, re.IGNORECASE):
            self.constructs.append(
                Construct(
                    type=ConstructType.TRIGGER,
                    name=match.group(1),
                    start_line=self._get_line_number(content, match.start()),
                    end_line=self._get_line_number(content, match.end()),
                    line_count=1,
                )
            )

    def _find_tables(self, content: str, lines: List[str]):
        """Find TABLE definitions."""
        table_pattern = r"(?:CREATE\s+)?(?:GLOBAL\s+TEMPORARY\s+)?TABLE\s+(\w+)"
        for match in re.finditer(table_pattern, content, re.IGNORECASE):
            is_global_temp = "GLOBAL" in match.group(0).upper()
            construct_type = ConstructType.GLOBAL_TEMP_TABLE if is_global_temp else ConstructType.TABLE
            self.constructs.append(
                Construct(
                    type=construct_type,
                    name=match.group(1),
                    start_line=self._get_line_number(content, match.start()),
                    end_line=self._get_line_number(content, match.end()),
                    line_count=1,
                )
            )

    def _find_views(self, content: str, lines: List[str]):
        """Find VIEW definitions."""
        view_pattern = r"(?:CREATE\s+(?:OR\s+REPLACE\s+)?)?VIEW\s+(\w+)"
        for match in re.finditer(view_pattern, content, re.IGNORECASE):
            self.constructs.append(
                Construct(
                    type=ConstructType.VIEW,
                    name=match.group(1),
                    start_line=self._get_line_number(content, match.start()),
                    end_line=self._get_line_number(content, match.end()),
                    line_count=1,
                )
            )

    def _find_sequences(self, content: str, lines: List[str]):
        """Find SEQUENCE definitions."""
        seq_pattern = r"(?:CREATE\s+)?SEQUENCE\s+(\w+)"
        for match in re.finditer(seq_pattern, content, re.IGNORECASE):
            self.constructs.append(
                Construct(
                    type=ConstructType.SEQUENCE,
                    name=match.group(1),
                    start_line=self._get_line_number(content, match.start()),
                    end_line=self._get_line_number(content, match.end()),
                    line_count=1,
                )
            )

    def _find_packages(self, content: str, lines: List[str]):
        """Find PACKAGE definitions."""
        pkg_pattern = r"(?:CREATE\s+(?:OR\s+REPLACE\s+)?)?PACKAGE\s+(?:BODY\s+)?(\w+)"
        for match in re.finditer(pkg_pattern, content, re.IGNORECASE):
            self.constructs.append(
                Construct(
                    type=ConstructType.PACKAGE,
                    name=match.group(1),
                    start_line=self._get_line_number(content, match.start()),
                    end_line=self._get_line_number(content, match.end()),
                    line_count=1,
                )
            )

    def _find_special_constructs(self, content: str, lines: List[str]):
        """Find special constructs: CONNECT BY, MERGE, DBMS_*, etc."""
        # CONNECT BY
        if re.search(r"CONNECT\s+BY", content, re.IGNORECASE):
            self.constructs.append(
                Construct(
                    type=ConstructType.CONNECT_BY,
                    name="CONNECT_BY",
                    start_line=0,
                    end_line=len(lines),
                    line_count=1,
                )
            )

        # MERGE
        if re.search(r"MERGE\s+INTO", content, re.IGNORECASE):
            self.constructs.append(
                Construct(
                    type=ConstructType.MERGE,
                    name="MERGE",
                    start_line=0,
                    end_line=len(lines),
                    line_count=1,
                )
            )

        # DBMS_* packages
        dbms_pattern = r"DBMS_(\w+)"
        for match in re.finditer(dbms_pattern, content, re.IGNORECASE):
            dbms_name = match.group(1).upper()
            if dbms_name == "SCHEDULER":
                construct_type = ConstructType.DBMS_SCHEDULER
            elif dbms_name == "AQ":
                construct_type = ConstructType.DBMS_AQ
            elif dbms_name == "CRYPTO":
                construct_type = ConstructType.DBMS_CRYPTO
            else:
                construct_type = ConstructType.DBMS_CALL

            # Check if already added
            if not any(c.type == construct_type for c in self.constructs):
                self.constructs.append(
                    Construct(
                        type=construct_type,
                        name=f"DBMS_{dbms_name}",
                        start_line=self._get_line_number(content, match.start()),
                        end_line=self._get_line_number(content, match.end()),
                        line_count=1,
                    )
                )

        # Autonomous transaction
        if re.search(r"PRAGMA\s+AUTONOMOUS_TRANSACTION", content, re.IGNORECASE):
            self.constructs.append(
                Construct(
                    type=ConstructType.AUTONOMOUS_TXN,
                    name="AUTONOMOUS_TRANSACTION",
                    start_line=0,
                    end_line=len(lines),
                    line_count=1,
                )
            )

        # %TYPE / %ROWTYPE
        if re.search(r"%(?:TYPE|ROWTYPE)", content, re.IGNORECASE):
            self.constructs.append(
                Construct(
                    type=ConstructType.ROWTYPE,
                    name="TYPE_ATTRIBUTE",
                    start_line=0,
                    end_line=len(lines),
                    line_count=1,
                )
            )

        # EXECUTE IMMEDIATE
        if re.search(r"EXECUTE\s+IMMEDIATE", content, re.IGNORECASE):
            self.constructs.append(
                Construct(
                    type=ConstructType.EXECUTE_IMMEDIATE,
                    name="EXECUTE_IMMEDIATE",
                    start_line=0,
                    end_line=len(lines),
                    line_count=1,
                )
            )

        # Spatial / Oracle Text
        if re.search(r"SDO_", content):
            self.constructs.append(
                Construct(
                    type=ConstructType.SPATIAL,
                    name="SPATIAL",
                    start_line=0,
                    end_line=len(lines),
                    line_count=1,
                )
            )

        if re.search(r"CONTAINS|CTXCAT", content):
            self.constructs.append(
                Construct(
                    type=ConstructType.ORACLE_TEXT,
                    name="ORACLE_TEXT",
                    start_line=0,
                    end_line=len(lines),
                    line_count=1,
                )
            )

        # Database links
        if re.search(r"@\w+", content):
            self.constructs.append(
                Construct(
                    type=ConstructType.DATABASE_LINK,
                    name="DATABASE_LINK",
                    start_line=0,
                    end_line=len(lines),
                    line_count=1,
                )
            )

    def _classify_lines(self, content: str) -> Dict[str, int]:
        """Classify lines by tier."""
        tier_a_count = 0
        tier_b_count = 0
        tier_c_count = 0

        # Simplified: count based on constructs found
        for construct in self.constructs:
            if construct.type in self.tier_a_constructs:
                tier_a_count += construct.line_count
            elif construct.type in self.tier_b_constructs:
                tier_b_count += construct.line_count
            elif construct.type in self.tier_c_constructs:
                tier_c_count += construct.line_count

        return {"tier_a": tier_a_count, "tier_b": tier_b_count, "tier_c": tier_c_count}

    def _get_line_number(self, content: str, position: int) -> int:
        """Get line number from character position."""
        return content[:position].count("\n") + 1
