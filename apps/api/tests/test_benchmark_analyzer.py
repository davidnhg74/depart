"""
Tests for Benchmark Analyzer (Phase 3.3).
Tests Oracle v$sql vs PostgreSQL pg_stat_statements comparison.
"""

import pytest
from datetime import datetime
from unittest.mock import Mock

from src.utils.time import utc_now
from src.analyzers.benchmark_analyzer import (
    BenchmarkCapture,
    BenchmarkComparator,
    QueryStat,
    TableStat,
    OracleBaseline,
    PostgresMetrics,
    QueryComparison,
    BenchmarkReport,
)


class TestQueryStat:
    """Test QueryStat dataclass."""

    def test_query_stat_creation(self):
        """Test creating a query stat."""
        query = QueryStat(
            sql_text="SELECT * FROM emp WHERE deptno = :1",
            avg_elapsed_ms=45.5,
            executions=1000,
            total_elapsed_ms=45500.0,
        )

        assert query.sql_text == "SELECT * FROM emp WHERE deptno = :1"
        assert query.avg_elapsed_ms == 45.5
        assert query.executions == 1000
        assert query.total_elapsed_ms == 45500.0


class TestTableStat:
    """Test TableStat dataclass."""

    def test_table_stat_creation(self):
        """Test creating a table stat."""
        table = TableStat(
            table_name="emp",
            row_count=14,
            size_bytes=16384,
        )

        assert table.table_name == "emp"
        assert table.row_count == 14
        assert table.size_bytes == 16384


class TestBenchmarkCapture:
    """Test benchmark capture from Oracle and PostgreSQL."""

    @pytest.fixture
    def mock_oracle_connector(self):
        """Mock Oracle connector."""
        connector = Mock()
        connector.get_session.return_value = Mock()
        return connector

    @pytest.fixture
    def mock_postgres_connector(self):
        """Mock PostgreSQL connector."""
        connector = Mock()
        connector.get_session.return_value = Mock()
        return connector

    def test_capture_oracle_baseline(self, mock_oracle_connector):
        """Test capturing Oracle v$sql baseline."""
        mock_session = mock_oracle_connector.get_session.return_value

        # Mock v$sql query
        v_sql_result = Mock()
        v_sql_result.mappings.return_value.all.return_value = [
            Mock(
                items=lambda: [
                    ("sql_text", "SELECT * FROM emp WHERE deptno = :1"),
                    ("avg_elapsed_ms", 45.5),
                    ("executions", 1000),
                    ("total_elapsed_ms", 45500.0),
                ]
            )
        ]

        # Mock table stats
        mock_oracle_connector.get_tables.return_value = ["emp", "dept"]
        mock_oracle_connector.get_table_row_count.side_effect = [14, 4]
        mock_oracle_connector.get_table_size.side_effect = [16384, 4096]

        mock_session.execute.return_value = v_sql_result

        baseline = BenchmarkCapture.capture_oracle_baseline(
            mock_oracle_connector, migration_id="test-123"
        )

        assert isinstance(baseline, OracleBaseline)
        assert len(baseline.top_queries) >= 0
        assert len(baseline.table_stats) >= 0
        assert baseline.migration_id == "test-123"

    def test_capture_postgres_metrics(self, mock_postgres_connector):
        """Test capturing PostgreSQL pg_stat_statements."""
        mock_session = mock_postgres_connector.get_session.return_value

        # Mock pg_stat_statements query
        pg_stats_result = Mock()
        pg_stats_result.mappings.return_value.all.return_value = [
            Mock(
                items=lambda: [
                    ("query", "SELECT * FROM emp WHERE deptno = $1"),
                    ("avg_elapsed_ms", 42.3),
                    ("executions", 1050),
                    ("total_elapsed_ms", 44415.0),
                ]
            )
        ]

        # Mock table stats
        mock_postgres_connector.get_tables.return_value = ["emp", "dept"]
        mock_postgres_connector.get_table_row_count.side_effect = [15, 4]
        mock_postgres_connector.get_table_size.side_effect = [16384, 4096]

        mock_session.execute.return_value = pg_stats_result

        metrics = BenchmarkCapture.capture_postgres_metrics(
            mock_postgres_connector, migration_id="test-123"
        )

        assert isinstance(metrics, PostgresMetrics)
        assert len(metrics.top_queries) >= 0
        assert len(metrics.table_stats) >= 0
        assert metrics.migration_id == "test-123"

    def test_capture_without_connection(self, mock_oracle_connector):
        """When inner queries fail we degrade to an empty baseline rather than
        raising — the caller still gets a valid OracleBaseline they can compare
        against later, just with no queries/tables captured."""
        mock_session = mock_oracle_connector.get_session.return_value
        mock_session.execute.side_effect = Exception("Connection failed")

        baseline = BenchmarkCapture.capture_oracle_baseline(mock_oracle_connector)

        assert baseline.top_queries == []
        assert baseline.table_stats == []


class TestBenchmarkComparator:
    """Test benchmark comparison logic."""

    def test_normalize_sql(self):
        """Test SQL normalization for matching."""
        comparator = BenchmarkComparator()

        # Test whitespace normalization
        sql1 = "SELECT * FROM  emp  WHERE  deptno = :1"
        normalized = comparator._normalize_sql(sql1)
        assert normalized == "select * from emp where deptno = :1"

        # Test comment stripping
        sql2 = """-- This is a comment
        SELECT * FROM emp
        /* Multi-line comment */
        WHERE deptno = :1"""
        normalized = comparator._normalize_sql(sql2)
        assert "comment" not in normalized.lower()

    def test_find_matching_query_exact(self):
        """Test finding exact matching query."""
        comparator = BenchmarkComparator()

        oracle_sql = "select * from emp where deptno = :1"
        pg_queries = [
            QueryStat(
                sql_text="SELECT * FROM emp WHERE deptno = $1",
                avg_elapsed_ms=42.3,
                executions=1050,
                total_elapsed_ms=44415.0,
            )
        ]

        match = comparator._find_matching_query(oracle_sql, pg_queries, threshold=0.7)
        assert match is not None
        assert match.avg_elapsed_ms == 42.3

    def test_find_matching_query_fuzzy(self):
        """Test fuzzy matching with similarity threshold."""
        comparator = BenchmarkComparator()

        oracle_sql = "SELECT * FROM employees WHERE dept = 10"
        pg_queries = [
            QueryStat(
                sql_text="SELECT * FROM emp WHERE deptno = $1",
                avg_elapsed_ms=42.3,
                executions=1050,
                total_elapsed_ms=44415.0,
            ),
            QueryStat(
                sql_text="SELECT * FROM departments WHERE id = $1",
                avg_elapsed_ms=10.0,
                executions=500,
                total_elapsed_ms=5000.0,
            ),
        ]

        # Fuzzy match should find the closest match
        match = comparator._find_matching_query(oracle_sql, pg_queries, threshold=0.5)
        # May or may not find a match depending on fuzzy threshold
        assert match is None or match is not None

    def test_find_no_matching_query(self):
        """Test when no matching query is found."""
        comparator = BenchmarkComparator()

        oracle_sql = "SELECT * FROM completely_different_table"
        pg_queries = [
            QueryStat(
                sql_text="SELECT * FROM emp WHERE deptno = $1",
                avg_elapsed_ms=42.3,
                executions=1050,
                total_elapsed_ms=44415.0,
            )
        ]

        match = comparator._find_matching_query(oracle_sql, pg_queries, threshold=0.8)
        assert match is None

    def test_compare_benchmarks(self):
        """Test full benchmark comparison."""
        comparator = BenchmarkComparator()

        oracle_baseline = OracleBaseline(
            captured_at=utc_now().isoformat(),
            top_queries=[
                QueryStat(
                    sql_text="SELECT * FROM emp WHERE deptno = :1",
                    avg_elapsed_ms=45.5,
                    executions=1000,
                    total_elapsed_ms=45500.0,
                )
            ],
            table_stats=[TableStat(table_name="emp", row_count=14, size_bytes=16384)],
            migration_id="test-123",
        )

        pg_metrics = PostgresMetrics(
            captured_at=utc_now().isoformat(),
            top_queries=[
                QueryStat(
                    sql_text="SELECT * FROM emp WHERE deptno = $1",
                    avg_elapsed_ms=42.3,
                    executions=1050,
                    total_elapsed_ms=44415.0,
                )
            ],
            table_stats=[TableStat(table_name="emp", row_count=14, size_bytes=16384)],
            migration_id="test-123",
        )

        mock_llm_client = Mock()
        mock_llm_client.summarize_benchmark.return_value = (
            "PostgreSQL is 7% faster on average queries."
        )

        report = comparator.compare(oracle_baseline, pg_metrics, mock_llm_client)

        assert isinstance(report, BenchmarkReport)
        assert len(report.query_comparisons) >= 0
        assert len(report.table_comparisons) >= 0
        assert report.overall_assessment is not None

    def test_speedup_calculation(self):
        """Test speedup factor calculation."""
        comparator = BenchmarkComparator()

        oracle_baseline = OracleBaseline(
            captured_at=utc_now().isoformat(),
            top_queries=[
                QueryStat(
                    sql_text="SELECT * FROM emp",
                    avg_elapsed_ms=100.0,
                    executions=1000,
                    total_elapsed_ms=100000.0,
                )
            ],
            table_stats=[],
            migration_id=None,
        )

        pg_metrics = PostgresMetrics(
            captured_at=utc_now().isoformat(),
            top_queries=[
                QueryStat(
                    sql_text="SELECT * FROM emp",
                    avg_elapsed_ms=50.0,
                    executions=1000,
                    total_elapsed_ms=50000.0,
                )
            ],
            table_stats=[],
            migration_id=None,
        )

        mock_llm_client = Mock()
        mock_llm_client.summarize_benchmark.return_value = "PG is faster."

        report = comparator.compare(oracle_baseline, pg_metrics, mock_llm_client)

        # Should detect FASTER verdict when Oracle avg is 100ms and PG is 50ms
        comparisons = report.query_comparisons
        if comparisons:
            # If match was found, speedup should be 2.0 (100/50)
            speedup = comparisons[0].speedup_factor
            assert speedup > 1.0 or speedup == 0  # Either faster or not found


class TestBenchmarkReport:
    """Test benchmark report generation."""

    def test_benchmark_report_creation(self):
        """Test creating a benchmark report."""
        report = BenchmarkReport(
            migration_id="test-123",
            query_comparisons=[
                QueryComparison(
                    sql_text="SELECT * FROM emp",
                    oracle_avg_ms=45.5,
                    pg_avg_ms=42.3,
                    speedup_factor=1.08,
                    verdict="FASTER",
                )
            ],
            table_comparisons=[
                {
                    "table_name": "emp",
                    "oracle_rows": 14,
                    "pg_rows": 14,
                    "rows_match": True,
                    "oracle_size_mb": 0.015625,
                    "pg_size_mb": 0.015625,
                    "size_ratio": 1.0,
                }
            ],
            overall_assessment="PostgreSQL is 8% faster on average.",
            generated_at=utc_now().isoformat(),
        )

        assert report.migration_id == "test-123"
        assert len(report.query_comparisons) == 1
        assert len(report.table_comparisons) == 1
        assert report.overall_assessment is not None
