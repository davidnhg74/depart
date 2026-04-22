"""
Unit tests for semantic error detection.
Tests DDL extraction, static analysis, and Claude integration.
"""

import pytest
from unittest.mock import MagicMock, patch
from apps.api.src.analyzers.semantic_analyzer import (
    StaticDDLExtractor,
    SemanticAnalyzer,
    SemanticIssue,
    IssueSeverity,
    IssueType,
)


# ============================================================================
# Fixtures
# ============================================================================

ORACLE_DDL_WITH_RISKS = """
CREATE TABLE orders (
    order_id    NUMBER(10) PRIMARY KEY,
    amount      NUMBER(12,2),
    order_date  DATE,
    is_active   NUMBER(1),
    description VARCHAR2(500 BYTE),
    created_at  TIMESTAMP,
    CONSTRAINT pk_orders PRIMARY KEY (order_id)
);
"""

PG_DDL_NARROWED = """
CREATE TABLE orders (
    order_id    INTEGER PRIMARY KEY,
    amount      NUMERIC(10,2),
    order_date  DATE,
    is_active   SMALLINT,
    description VARCHAR(500),
    created_at  TIMESTAMP WITHOUT TIME ZONE,
    CONSTRAINT pk_orders PRIMARY KEY (order_id)
);
"""

ORACLE_DDL_SIMPLE = """
CREATE TABLE employees (
    employee_id NUMBER(10),
    first_name  VARCHAR2(100),
    salary      NUMBER(10,2)
);
"""

PG_DDL_SIMPLE = """
CREATE TABLE employees (
    employee_id INTEGER,
    first_name  VARCHAR(100),
    salary      NUMERIC(10,2)
);
"""


# ============================================================================
# StaticDDLExtractor Tests
# ============================================================================

class TestStaticDDLExtractor:
    """Test DDL parsing and type extraction."""

    def test_extract_simple_columns(self):
        """Extract basic column definitions."""
        extractor = StaticDDLExtractor()
        mappings = extractor.extract_type_mappings(
            ORACLE_DDL_SIMPLE,
            PG_DDL_SIMPLE,
        )

        assert len(mappings) == 3
        assert mappings[0]["column"] == "employee_id"
        assert mappings[0]["oracle_type"] == "NUMBER(10)"
        assert mappings[0]["pg_type"] == "INTEGER"

    def test_extract_with_precision(self):
        """Extract columns with precision/scale."""
        extractor = StaticDDLExtractor()
        mappings = extractor.extract_type_mappings(
            ORACLE_DDL_SIMPLE,
            PG_DDL_SIMPLE,
        )

        salary_map = next(m for m in mappings if m["column"] == "salary")
        assert salary_map["oracle_type"] == "NUMBER(10,2)"
        assert salary_map["pg_type"] == "NUMERIC(10,2)"

    def test_extract_date_columns(self):
        """Extract DATE columns (behavioral change risk)."""
        extractor = StaticDDLExtractor()
        mappings = extractor.extract_type_mappings(
            ORACLE_DDL_WITH_RISKS,
            PG_DDL_NARROWED,
        )

        date_map = next(m for m in mappings if m["column"] == "order_date")
        assert date_map["oracle_type"] == "DATE"
        assert date_map["pg_type"] == "DATE"

    def test_extract_varchar_with_byte_qualifier(self):
        """Extract VARCHAR2 with BYTE qualifier (encoding risk)."""
        extractor = StaticDDLExtractor()
        mappings = extractor.extract_type_mappings(
            ORACLE_DDL_WITH_RISKS,
            PG_DDL_NARROWED,
        )

        desc_map = next(m for m in mappings if m["column"] == "description")
        assert "BYTE" in desc_map["oracle_type"]
        assert desc_map["pg_type"] == "VARCHAR(500)"

    def test_extract_precision_change(self):
        """Detect precision narrowing (NUMBER 12,2 → NUMERIC 10,2)."""
        extractor = StaticDDLExtractor()
        mappings = extractor.extract_type_mappings(
            ORACLE_DDL_WITH_RISKS,
            PG_DDL_NARROWED,
        )

        amount_map = next(m for m in mappings if m["column"] == "amount")
        assert amount_map["oracle_type"] == "NUMBER(12,2)"
        assert amount_map["pg_type"] == "NUMERIC(10,2)"

    def test_case_insensitive_matching(self):
        """Match columns case-insensitively."""
        oracle = "CREATE TABLE test (ID NUMBER(10), Name VARCHAR2(100));"
        pg = "CREATE TABLE test (id INTEGER, name VARCHAR(100));"

        extractor = StaticDDLExtractor()
        mappings = extractor.extract_type_mappings(oracle, pg)

        assert len(mappings) == 2
        assert any(m["column"].upper() == "ID" for m in mappings)

    def test_handle_missing_table_gracefully(self):
        """Gracefully handle table mismatch."""
        oracle = "CREATE TABLE orders (id NUMBER(10));"
        pg = "CREATE TABLE items (id INTEGER);"  # Different table name

        extractor = StaticDDLExtractor()
        mappings = extractor.extract_type_mappings(oracle, pg)

        assert len(mappings) == 0

    def test_parse_multiple_tables(self):
        """Extract columns from multiple tables."""
        oracle = """
        CREATE TABLE orders (order_id NUMBER(10));
        CREATE TABLE items (item_id NUMBER(10));
        """
        pg = """
        CREATE TABLE orders (order_id INTEGER);
        CREATE TABLE items (item_id INTEGER);
        """

        extractor = StaticDDLExtractor()
        mappings = extractor.extract_type_mappings(oracle, pg)

        assert len(mappings) == 2
        assert any(m["table"] == "orders" for m in mappings)
        assert any(m["table"] == "items" for m in mappings)


# ============================================================================
# SemanticAnalyzer Tests
# ============================================================================

class TestSemanticAnalyzer:
    """Test semantic analysis logic."""

    def test_analyze_static_returns_result(self):
        """Static analysis completes without errors."""
        mock_llm = MagicMock()
        mock_llm.detect_semantic_issues.return_value = []

        analyzer = SemanticAnalyzer(mock_llm)
        result = analyzer.analyze_static(ORACLE_DDL_SIMPLE, PG_DDL_SIMPLE)

        assert result.mode == "static"
        assert result.analyzed_objects > 0
        assert result.issues == []

    def test_analyze_static_with_precision_issue(self):
        """Detect precision loss issue."""
        mock_llm = MagicMock()
        mock_llm.detect_semantic_issues.return_value = [
            {
                "severity": "CRITICAL",
                "issue_type": "PRECISION_LOSS",
                "affected_object": "ORDERS.AMOUNT",
                "oracle_type": "NUMBER(12,2)",
                "pg_type": "NUMERIC(10,2)",
                "description": "Precision reduced from 12 to 10 digits. "
                               "Values > 99,999.99 will be truncated or raise exceptions.",
                "recommendation": "Use NUMERIC(12,2) in PostgreSQL schema "
                                 "to match Oracle precision.",
            }
        ]

        analyzer = SemanticAnalyzer(mock_llm)
        result = analyzer.analyze_static(
            ORACLE_DDL_WITH_RISKS,
            PG_DDL_NARROWED,
        )

        assert len(result.issues) == 1
        assert result.issues[0].severity == IssueSeverity.CRITICAL
        assert result.issues[0].issue_type == IssueType.PRECISION_LOSS

    def test_analyze_static_with_date_issue(self):
        """Detect DATE behavior change issue."""
        mock_llm = MagicMock()
        mock_llm.detect_semantic_issues.return_value = [
            {
                "severity": "ERROR",
                "issue_type": "DATE_BEHAVIOR",
                "affected_object": "ORDERS.ORDER_DATE",
                "oracle_type": "DATE",
                "pg_type": "DATE",
                "description": "Oracle DATE stores time (HH:MI:SS); "
                               "PostgreSQL DATE does not. Time component is lost.",
                "recommendation": "Use TIMESTAMP in PostgreSQL if time precision is needed.",
            }
        ]

        analyzer = SemanticAnalyzer(mock_llm)
        result = analyzer.analyze_static(
            ORACLE_DDL_WITH_RISKS,
            PG_DDL_NARROWED,
        )

        assert len(result.issues) == 1
        assert result.issues[0].issue_type == IssueType.DATE_BEHAVIOR

    def test_severity_distribution(self):
        """Aggregate severity distribution from multiple issues."""
        mock_llm = MagicMock()
        mock_llm.detect_semantic_issues.return_value = [
            {
                "severity": "CRITICAL",
                "issue_type": "PRECISION_LOSS",
                "affected_object": "T1.C1",
                "oracle_type": "NUMBER(10)",
                "pg_type": "NUMERIC(5)",
                "description": "Test",
                "recommendation": "Test",
            },
            {
                "severity": "WARNING",
                "issue_type": "DATE_BEHAVIOR",
                "affected_object": "T1.C2",
                "oracle_type": "DATE",
                "pg_type": "DATE",
                "description": "Test",
                "recommendation": "Test",
            },
        ]

        analyzer = SemanticAnalyzer(mock_llm)
        result = analyzer.analyze_static(
            ORACLE_DDL_SIMPLE,
            PG_DDL_SIMPLE,
        )

        assert len(result.issues) == 2
        critical = [i for i in result.issues if i.severity == IssueSeverity.CRITICAL]
        warnings = [i for i in result.issues if i.severity == IssueSeverity.WARNING]
        assert len(critical) == 1
        assert len(warnings) == 1

    def test_handle_empty_ddl_gracefully(self):
        """Handle empty DDL without crashing."""
        mock_llm = MagicMock()
        analyzer = SemanticAnalyzer(mock_llm)

        result = analyzer.analyze_static("", "")

        assert result.mode == "static"
        assert result.issues == []
        assert result.analyzed_objects == 0

    def test_handle_llm_error_gracefully(self):
        """Handle LLM errors gracefully."""
        mock_llm = MagicMock()
        mock_llm.detect_semantic_issues.side_effect = Exception("API error")

        analyzer = SemanticAnalyzer(mock_llm)
        result = analyzer.analyze_static(ORACLE_DDL_SIMPLE, PG_DDL_SIMPLE)

        assert result.error is not None
        assert result.issues == []

    def test_join_metadata_inner_join(self):
        """Join Oracle and PostgreSQL metadata by table/column."""
        oracle_meta = [
            {"table_name": "users", "column_name": "id", "data_type": "NUMBER(10)"},
            {"table_name": "users", "column_name": "name", "data_type": "VARCHAR2(100)"},
        ]

        pg_meta = [
            {"table_name": "users", "column_name": "id", "data_type": "INTEGER"},
            {"table_name": "users", "column_name": "name", "data_type": "VARCHAR(100)"},
        ]

        result = SemanticAnalyzer._join_metadata(oracle_meta, pg_meta)

        assert len(result) == 2
        assert result[0]["table"] == "users"
        assert result[0]["oracle_type"] == "NUMBER(10)"
        assert result[0]["pg_type"] == "INTEGER"

    def test_join_metadata_case_insensitive(self):
        """Join metadata with case-insensitive matching."""
        oracle_meta = [
            {"table_name": "USERS", "column_name": "ID", "data_type": "NUMBER(10)"},
        ]

        pg_meta = [
            {"table_name": "users", "column_name": "id", "data_type": "INTEGER"},
        ]

        result = SemanticAnalyzer._join_metadata(oracle_meta, pg_meta)

        assert len(result) == 1
        assert result[0]["table"] == "USERS"

    def test_join_metadata_missing_column_excluded(self):
        """Exclude columns that don't exist in both schemas."""
        oracle_meta = [
            {"table_name": "users", "column_name": "id", "data_type": "NUMBER(10)"},
            {"table_name": "users", "column_name": "extra_col", "data_type": "VARCHAR2(50)"},
        ]

        pg_meta = [
            {"table_name": "users", "column_name": "id", "data_type": "INTEGER"},
        ]

        result = SemanticAnalyzer._join_metadata(oracle_meta, pg_meta)

        assert len(result) == 1  # Only 'id' matched
        assert result[0]["column"] == "id"


# ============================================================================
# Integration Tests (marked to skip if no LLM configured)
# ============================================================================

@pytest.mark.integration
class TestSemanticAnalyzerIntegration:
    """Integration tests requiring Claude API."""

    def test_real_semantic_analysis_with_claude(self):
        """Full semantic analysis with actual Claude API (if available)."""
        try:
            from apps.api.src.llm.client import LLMClient
            from apps.api.src.config import settings

            if not settings.anthropic_api_key:
                pytest.skip("No Anthropic API key configured")

            llm = LLMClient()
            analyzer = SemanticAnalyzer(llm)

            result = analyzer.analyze_static(
                ORACLE_DDL_WITH_RISKS,
                PG_DDL_NARROWED,
            )

            # At minimum, should detect precision or date issues
            assert result.mode == "static"
            assert result.analyzed_objects >= 3  # At least 3 columns matched
            # May or may not have issues depending on Claude's analysis
            assert isinstance(result.issues, list)

        except ImportError:
            pytest.skip("LLMClient not available")
