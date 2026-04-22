"""
Benchmark analyzer for Oracle → PostgreSQL performance comparison.
Captures v$sql (Oracle) and pg_stat_statements (PostgreSQL) metrics.
"""

from dataclasses import dataclass, field
from typing import List, Dict, Optional, Any
from datetime import datetime
import logging
import re

logger = logging.getLogger(__name__)


@dataclass
class QueryStat:
    """Statistics for a single query."""

    sql_text: str
    avg_elapsed_ms: float
    executions: int
    total_elapsed_ms: float


@dataclass
class TableStat:
    """Statistics for a single table."""

    table_name: str
    row_count: int
    size_bytes: int


@dataclass
class OracleBaseline:
    """Oracle performance baseline capture."""

    captured_at: str
    top_queries: List[QueryStat]
    table_stats: List[TableStat]
    migration_id: Optional[str] = None


@dataclass
class PostgresMetrics:
    """PostgreSQL performance metrics."""

    captured_at: str
    top_queries: List[QueryStat]
    table_stats: List[TableStat]
    migration_id: Optional[str] = None


@dataclass
class QueryComparison:
    """Comparison of a single query between Oracle and PostgreSQL."""

    sql_text: str
    oracle_avg_ms: float
    pg_avg_ms: float
    speedup_factor: float  # >1 means PG is faster
    verdict: str  # FASTER, SLOWER, EQUIVALENT, NOT_AVAILABLE


@dataclass
class BenchmarkReport:
    """Complete benchmark comparison report."""

    migration_id: Optional[str]
    query_comparisons: List[QueryComparison]
    table_comparisons: List[Dict[str, Any]] = field(default_factory=list)
    overall_assessment: str = ""
    generated_at: str = ""


class BenchmarkCapture:
    """Capture performance metrics from Oracle or PostgreSQL."""

    @staticmethod
    def capture_oracle_baseline(
        oracle_connector, migration_id: Optional[str] = None
    ) -> OracleBaseline:
        """
        Capture Oracle performance baseline from v$sql and table stats.

        Args:
            oracle_connector: OracleConnector with active session
            migration_id: Optional migration ID for tracking

        Returns:
            OracleBaseline with top queries and table stats
        """
        session = oracle_connector.get_session()
        top_queries: List[QueryStat] = []
        table_stats: List[TableStat] = []

        try:
            # Capture top 20 slowest queries by average elapsed time
            v_sql = """
                SELECT sql_text,
                       ROUND(elapsed_time/executions/1000, 2) AS avg_elapsed_ms,
                       executions,
                       ROUND(elapsed_time/1000, 2) AS total_elapsed_ms
                FROM v$sql
                WHERE executions > 0 AND sql_text IS NOT NULL
                ORDER BY elapsed_time/executions DESC
                FETCH FIRST 20 ROWS ONLY
            """
            try:
                from sqlalchemy import text

                rows = session.execute(text(v_sql)).mappings().all()
                for r in rows:
                    # Normalize SQL: strip whitespace, lowercase
                    sql_normalized = " ".join(r["sql_text"].split()).lower()
                    top_queries.append(
                        QueryStat(
                            sql_text=sql_normalized,
                            avg_elapsed_ms=float(r["avg_elapsed_ms"] or 0),
                            executions=int(r["executions"] or 0),
                            total_elapsed_ms=float(r["total_elapsed_ms"] or 0),
                        )
                    )
                logger.info(f"Captured {len(top_queries)} top Oracle queries")
            except Exception as e:
                logger.warning(f"Could not capture v$sql data: {e}")

            # Capture table stats (reuse existing methods if available)
            try:
                all_tables = oracle_connector.get_tables()
                for table_name in all_tables:
                    try:
                        row_count = oracle_connector.get_table_row_count(table_name)
                        size_bytes = oracle_connector.get_table_size(table_name)
                        if row_count is not None and size_bytes is not None:
                            table_stats.append(
                                TableStat(
                                    table_name=table_name,
                                    row_count=int(row_count),
                                    size_bytes=int(size_bytes),
                                )
                            )
                    except Exception as e:
                        logger.debug(f"Could not get stats for table {table_name}: {e}")
                logger.info(f"Captured stats for {len(table_stats)} Oracle tables")
            except Exception as e:
                logger.warning(f"Could not capture table stats: {e}")

        except Exception as e:
            logger.error(f"Error capturing Oracle baseline: {e}")
            raise
        finally:
            session.close()

        return OracleBaseline(
            captured_at=datetime.utcnow().isoformat(),
            top_queries=top_queries,
            table_stats=table_stats,
            migration_id=migration_id,
        )

    @staticmethod
    def capture_postgres_metrics(
        postgres_connector, migration_id: Optional[str] = None
    ) -> PostgresMetrics:
        """
        Capture PostgreSQL performance metrics from pg_stat_statements and tables.

        Args:
            postgres_connector: PostgresConnector with active session
            migration_id: Optional migration ID for tracking

        Returns:
            PostgresMetrics with top queries and table stats
        """
        session = postgres_connector.get_session()
        top_queries: List[QueryStat] = []
        table_stats: List[TableStat] = []

        try:
            # Capture top 20 slowest queries by mean execution time
            pg_sql = """
                SELECT query,
                       ROUND(mean_exec_time::numeric, 2) AS avg_elapsed_ms,
                       calls AS executions,
                       ROUND(total_exec_time::numeric, 2) AS total_elapsed_ms
                FROM pg_stat_statements
                WHERE calls > 0
                ORDER BY mean_exec_time DESC
                LIMIT 20
            """
            try:
                from sqlalchemy import text

                rows = session.execute(text(pg_sql)).mappings().all()
                for r in rows:
                    # Normalize SQL: strip whitespace, lowercase
                    sql_normalized = " ".join(r["query"].split()).lower()
                    top_queries.append(
                        QueryStat(
                            sql_text=sql_normalized,
                            avg_elapsed_ms=float(r["avg_elapsed_ms"] or 0),
                            executions=int(r["executions"] or 0),
                            total_elapsed_ms=float(r["total_elapsed_ms"] or 0),
                        )
                    )
                logger.info(f"Captured {len(top_queries)} top PostgreSQL queries")
            except Exception as e:
                logger.warning(
                    f"Could not capture pg_stat_statements data (may need to enable extension): {e}"
                )

            # Capture table stats
            try:
                all_tables = postgres_connector.get_tables()
                for table_name in all_tables:
                    try:
                        row_count = postgres_connector.get_table_row_count(table_name)
                        size_bytes = postgres_connector.get_table_size(table_name)
                        if row_count is not None and size_bytes is not None:
                            table_stats.append(
                                TableStat(
                                    table_name=table_name,
                                    row_count=int(row_count),
                                    size_bytes=int(size_bytes),
                                )
                            )
                    except Exception as e:
                        logger.debug(f"Could not get stats for table {table_name}: {e}")
                logger.info(f"Captured stats for {len(table_stats)} PostgreSQL tables")
            except Exception as e:
                logger.warning(f"Could not capture table stats: {e}")

        except Exception as e:
            logger.error(f"Error capturing PostgreSQL metrics: {e}")
            raise
        finally:
            session.close()

        return PostgresMetrics(
            captured_at=datetime.utcnow().isoformat(),
            top_queries=top_queries,
            table_stats=table_stats,
            migration_id=migration_id,
        )


class BenchmarkComparator:
    """Compare Oracle baseline vs PostgreSQL metrics."""

    @staticmethod
    def _normalize_sql(sql: str) -> str:
        """Normalize SQL for fuzzy matching."""
        # Strip comments, normalize whitespace, lowercase
        sql = re.sub(r"--.*$", "", sql, flags=re.MULTILINE)
        sql = re.sub(r"/\*.*?\*/", "", sql, flags=re.DOTALL)
        sql = " ".join(sql.split()).lower()
        return sql

    @staticmethod
    def _find_matching_query(
        oracle_sql: str, pg_queries: List[QueryStat], threshold: float = 0.7
    ) -> Optional[QueryStat]:
        """Find best-matching PostgreSQL query using fuzzy string matching."""
        oracle_normalized = BenchmarkComparator._normalize_sql(oracle_sql)

        best_match = None
        best_ratio = 0

        for pg_query in pg_queries:
            pg_normalized = BenchmarkComparator._normalize_sql(pg_query.sql_text)

            # Simple substring match (good enough for most cases)
            if oracle_normalized in pg_normalized or pg_normalized in oracle_normalized:
                return pg_query

            # Fuzzy match using SequenceMatcher
            from difflib import SequenceMatcher

            ratio = SequenceMatcher(None, oracle_normalized, pg_normalized).ratio()
            if ratio > best_ratio:
                best_ratio = ratio
                best_match = pg_query if ratio >= threshold else None

        return best_match

    @staticmethod
    def compare(oracle: OracleBaseline, pg: PostgresMetrics, llm_client=None) -> BenchmarkReport:
        """
        Compare Oracle and PostgreSQL performance metrics.

        Args:
            oracle: OracleBaseline capture
            pg: PostgresMetrics capture
            llm_client: Optional LLMClient for generating summary (requires)

        Returns:
            BenchmarkReport with query comparisons and overall assessment
        """
        query_comparisons: List[QueryComparison] = []
        table_comparisons: List[Dict[str, Any]] = []

        # Compare queries
        for oracle_query in oracle.top_queries:
            matching_pg_query = BenchmarkComparator._find_matching_query(
                oracle_query.sql_text, pg.top_queries, threshold=0.7
            )

            if matching_pg_query:
                speedup_factor = (
                    oracle_query.avg_elapsed_ms / matching_pg_query.avg_elapsed_ms
                    if matching_pg_query.avg_elapsed_ms > 0
                    else 1.0
                )

                if speedup_factor > 1.1:
                    verdict = "FASTER"
                elif speedup_factor < 0.9:
                    verdict = "SLOWER"
                else:
                    verdict = "EQUIVALENT"
            else:
                speedup_factor = 0
                verdict = "NOT_AVAILABLE"

            query_comparisons.append(
                QueryComparison(
                    sql_text=oracle_query.sql_text[:100],  # Truncate for readability
                    oracle_avg_ms=oracle_query.avg_elapsed_ms,
                    pg_avg_ms=matching_pg_query.avg_elapsed_ms if matching_pg_query else 0,
                    speedup_factor=speedup_factor,
                    verdict=verdict,
                )
            )

        # Compare tables
        oracle_tables_by_name = {t.table_name.upper(): t for t in oracle.table_stats}
        pg_tables_by_name = {t.table_name.upper(): t for t in pg.table_stats}

        for table_name_upper, oracle_table in oracle_tables_by_name.items():
            pg_table = pg_tables_by_name.get(table_name_upper)
            if pg_table:
                table_comparisons.append(
                    {
                        "table_name": oracle_table.table_name,
                        "oracle_rows": oracle_table.row_count,
                        "pg_rows": pg_table.row_count,
                        "rows_match": oracle_table.row_count == pg_table.row_count,
                        "oracle_size_mb": oracle_table.size_bytes / (1024 * 1024),
                        "pg_size_mb": pg_table.size_bytes / (1024 * 1024),
                        "size_ratio": (
                            pg_table.size_bytes / oracle_table.size_bytes
                            if oracle_table.size_bytes > 0
                            else 0
                        ),
                    }
                )

        # Generate overall assessment via Claude if available
        overall_assessment = ""
        if llm_client:
            try:
                import json

                report_dict = {
                    "query_comparisons": [
                        {
                            "sql": c.sql_text,
                            "oracle_ms": c.oracle_avg_ms,
                            "pg_ms": c.pg_avg_ms,
                            "verdict": c.verdict,
                        }
                        for c in query_comparisons
                    ],
                    "table_comparisons": table_comparisons,
                    "faster_count": sum(1 for c in query_comparisons if c.verdict == "FASTER"),
                    "slower_count": sum(1 for c in query_comparisons if c.verdict == "SLOWER"),
                    "not_available_count": sum(
                        1 for c in query_comparisons if c.verdict == "NOT_AVAILABLE"
                    ),
                }
                overall_assessment = llm_client.summarize_benchmark(json.dumps(report_dict))
            except Exception as e:
                logger.warning(f"Could not generate benchmark summary via Claude: {e}")
                overall_assessment = f"Compared {len(query_comparisons)} queries and {len(table_comparisons)} tables."

        return BenchmarkReport(
            migration_id=oracle.migration_id or pg.migration_id,
            query_comparisons=query_comparisons,
            table_comparisons=table_comparisons,
            overall_assessment=overall_assessment,
            generated_at=datetime.utcnow().isoformat(),
        )
