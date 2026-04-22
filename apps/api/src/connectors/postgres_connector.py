"""
PostgreSQL Database Connector.
Handles connection pooling, session management, and query execution.
"""

from sqlalchemy import create_engine, pool, text
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.exc import OperationalError
from typing import Optional, List, Dict, Any
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class PostgresConnector:
    """Manages PostgreSQL database connections with pooling."""

    def __init__(
        self,
        host: str,
        port: int,
        database: str,
        username: str,
        password: str,
        pool_size: int = 5,
        max_overflow: int = 10,
        timeout_seconds: int = 30,
    ):
        """
        Initialize PostgreSQL connector.

        Args:
            host: PostgreSQL server hostname/IP
            port: PostgreSQL port (default 5432)
            database: Database name
            username: Database username
            password: Database password
            pool_size: Number of connections in pool
            max_overflow: Max connections beyond pool_size
            timeout_seconds: Connection timeout
        """
        self.host = host
        self.port = port
        self.database = database
        self.username = username
        self.pool_size = pool_size
        self.max_overflow = max_overflow
        self.timeout_seconds = timeout_seconds

        self.engine = None
        self.SessionLocal = None
        self.connected = False
        self.last_check = None

    def build_connection_string(self) -> str:
        """Build SQLAlchemy connection string."""
        return (
            f"postgresql+psycopg://{self.username}:{self.password}@"
            f"{self.host}:{self.port}/{self.database}"
        )

    def connect(self) -> bool:
        """
        Test connection and create engine.
        Returns True if successful, False otherwise.
        """
        try:
            connection_string = self.build_connection_string()

            self.engine = create_engine(
                connection_string,
                poolclass=pool.QueuePool,
                pool_size=self.pool_size,
                max_overflow=self.max_overflow,
                pool_pre_ping=True,  # Test connections before use
                echo=False,
                connect_args={
                    "connect_timeout": self.timeout_seconds,
                    "application_name": "depart_migration",
                },
            )

            # Test connection
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT 1"))
                assert result.scalar() == 1

            self.SessionLocal = sessionmaker(bind=self.engine, expire_on_commit=False)
            self.connected = True
            self.last_check = datetime.utcnow()

            logger.info(
                f"✓ PostgreSQL connection successful: {self.host}:{self.port}/{self.database}"
            )
            return True

        except OperationalError as e:
            logger.error(f"✗ PostgreSQL connection failed: {e}")
            self.connected = False
            return False
        except Exception as e:
            logger.error(f"✗ Unexpected error: {e}")
            self.connected = False
            return False

    def get_session(self) -> Session:
        """
        Get a new database session.
        Raises RuntimeError if not connected.
        """
        if not self.connected or not self.SessionLocal:
            raise RuntimeError("Not connected to PostgreSQL database")
        return self.SessionLocal()

    def get_version(self) -> Optional[str]:
        """Get PostgreSQL version."""
        try:
            session = self.get_session()
            result = session.execute(text("SELECT version()")).scalar()
            session.close()
            return result
        except Exception as e:
            logger.warning(f"Failed to get PostgreSQL version: {e}")
            return None

    def get_tables(self) -> List[str]:
        """Get list of all tables in current schema."""
        try:
            session = self.get_session()
            tables = (
                session.execute(
                    text(
                        """
                    SELECT tablename FROM pg_tables
                    WHERE schemaname = 'public'
                    ORDER BY tablename
                """
                    )
                )
                .scalars()
                .all()
            )
            session.close()
            return tables
        except Exception as e:
            logger.error(f"Failed to get tables: {e}")
            return []

    def get_table_row_count(self, table_name: str) -> int:
        """Get row count for a table."""
        try:
            session = self.get_session()
            count = session.execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar()
            session.close()
            return count or 0
        except Exception as e:
            logger.warning(f"Failed to count rows in {table_name}: {e}")
            return 0

    def get_table_size(self, table_name: str) -> int:
        """
        Get table size in bytes.
        Returns 0 if unable to determine.
        """
        try:
            session = self.get_session()
            size = session.execute(
                text(f"SELECT pg_total_relation_size('{table_name}'::regclass)")
            ).scalar()
            session.close()
            return size or 0
        except Exception:
            # Fallback: estimate 1KB per row
            return self.get_table_row_count(table_name) * 1024

    def test_select(self) -> bool:
        """Test basic SELECT capability."""
        try:
            session = self.get_session()
            session.execute(text("SELECT 1"))
            session.close()
            return True
        except Exception as e:
            logger.warning(f"SELECT test failed: {e}")
            return False

    def test_create(self) -> bool:
        """Test CREATE TABLE capability."""
        try:
            session = self.get_session()
            table_name = "depart_test_migration_table"

            # Create test table
            session.execute(
                text(
                    f"""
                    CREATE TABLE {table_name} (
                        id INTEGER PRIMARY KEY,
                        test_col VARCHAR(100)
                    )
                """
                )
            )

            # Drop test table
            session.execute(text(f"DROP TABLE {table_name}"))
            session.commit()
            session.close()
            return True
        except Exception as e:
            logger.warning(f"CREATE test failed: {e}")
            try:
                session.rollback()
                session.close()
            except:
                pass
            return False

    def test_insert(self) -> bool:
        """Test INSERT capability."""
        try:
            session = self.get_session()
            table_name = "depart_test_migration_table"

            # Create table
            session.execute(
                text(
                    f"""
                    CREATE TABLE {table_name} (
                        id INTEGER PRIMARY KEY,
                        test_col VARCHAR(100)
                    )
                """
                )
            )

            # Insert row
            session.execute(text(f"INSERT INTO {table_name} VALUES (1, 'test')"))

            # Drop table
            session.execute(text(f"DROP TABLE {table_name}"))
            session.commit()
            session.close()
            return True
        except Exception as e:
            logger.warning(f"INSERT test failed: {e}")
            try:
                session.rollback()
                session.close()
            except:
                pass
            return False

    def health_check(self) -> Dict[str, Any]:
        """
        Perform full health check.
        Returns dict with status and details.
        """
        return {
            "connected": self.connected,
            "version": self.get_version(),
            "can_select": self.test_select(),
            "can_create": self.test_create(),
            "can_insert": self.test_insert(),
            "last_check": self.last_check.isoformat() if self.last_check else None,
        }

    def get_column_metadata(
        self,
        schema_name: str = "public",
        table_names: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Query PostgreSQL information_schema for column metadata.
        Returns list of dicts with table_name, column_name, data_type, character_maximum_length,
        numeric_precision, numeric_scale, is_nullable, udt_name.

        Args:
            schema_name: Schema name (default "public").
            table_names: Optional list of table names to filter. If None, returns all.

        Returns:
            List of column metadata dicts.
        """
        session = self.get_session()
        try:
            table_filter = ""
            if table_names:
                names = ", ".join(f"'{t.lower()}'" for t in table_names)
                table_filter = f"AND table_name IN ({names})"

            sql = f"""
                SELECT
                    table_name,
                    column_name,
                    data_type,
                    character_maximum_length,
                    numeric_precision,
                    numeric_scale,
                    is_nullable,
                    udt_name
                FROM information_schema.columns
                WHERE table_schema = '{schema_name}'
                {table_filter}
                ORDER BY table_name, ordinal_position
            """

            rows = session.execute(text(sql)).mappings().all()
            return [dict(r) for r in rows]

        except Exception as e:
            logger.error(f"Error fetching column metadata: {e}")
            return []
        finally:
            session.close()

    def close(self):
        """Close connection pool."""
        if self.engine:
            self.engine.dispose()
            self.connected = False
            logger.info("PostgreSQL connection pool closed")

    def __del__(self):
        """Cleanup on deletion."""
        self.close()
