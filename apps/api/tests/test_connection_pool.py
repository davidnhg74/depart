"""
Tests for Connection Pool (Phase 3.3).
Tests connection pooling, health monitoring, and statistics.
"""

import pytest
from datetime import datetime, timedelta
from unittest.mock import Mock

from src.utils.time import utc_now
from src.connectors.connection_pool import (
    ConnectionPool,
    ConnectionStats,
    CachedConnectionStats,
    get_connection_pool,
    get_stats_cache,
)


class TestConnectionStats:
    """Test ConnectionStats dataclass."""

    def test_stats_creation(self):
        """Test creating connection stats."""
        stats = ConnectionStats(
            connection_id="oracle-1",
            database_type="oracle",
            host="oracle.company.com",
            port=1521,
            created_at=utc_now(),
            last_used=utc_now(),
            use_count=42,
            active_queries=3,
            health_status="healthy",
            last_health_check=utc_now(),
            response_time_ms=45.5,
        )

        assert stats.connection_id == "oracle-1"
        assert stats.database_type == "oracle"
        assert stats.use_count == 42
        assert stats.health_status == "healthy"


class TestConnectionPool:
    """Test connection pooling."""

    @pytest.fixture
    def pool(self):
        """Create a connection pool."""
        return ConnectionPool(min_size=2, max_size=5, max_idle_seconds=300)

    @pytest.fixture
    def mock_connector(self):
        """Mock database connector."""
        connector = Mock()
        connector.connected = True
        connector.health_check.return_value = {"status": "ok"}
        connector.database_type = "oracle"
        connector.host = "oracle.test.com"
        connector.port = 1521
        return connector

    @pytest.fixture
    def mock_manager(self, mock_connector):
        """Mock connection manager."""
        manager = Mock()
        manager.get_connector.return_value = mock_connector
        return manager

    def test_get_connection_from_empty_pool(self, pool, mock_manager, mock_connector):
        """Test getting connection from empty pool (creates new)."""
        conn = pool.get_connection("oracle-1", mock_manager)

        assert conn is not None
        assert mock_manager.get_connector.called

    def test_connection_reuse_from_pool(self, pool, mock_manager, mock_connector):
        """Test connection is reused from pool."""
        # Get first connection
        conn1 = pool.get_connection("oracle-1", mock_manager)

        # Return it to pool
        pool.return_connection("oracle-1", conn1)

        # Get again - should be same connection
        conn2 = pool.get_connection("oracle-1", mock_manager)

        assert conn1 is conn2

    def test_multiple_connections_per_pool(self, pool, mock_manager, mock_connector):
        """Test multiple connections in pool."""
        mock_connector_2 = Mock()
        mock_connector_2.connected = True
        mock_connector_2.health_check.return_value = {"status": "ok"}

        mock_manager.get_connector.side_effect = [mock_connector, mock_connector_2]

        conn1 = pool.get_connection("oracle-1", mock_manager)
        # Keep conn1 active

        # Get second connection
        conn2 = pool.get_connection("oracle-1", mock_manager)

        assert conn1 is not conn2

    def test_health_check_healthy(self, pool, mock_connector):
        """Test health check for healthy connection."""
        status = pool.health_check("oracle-1", mock_connector)

        assert status == "healthy"

    def test_health_check_degraded(self, pool, mock_connector):
        """Test health check for degraded connection."""
        mock_connector.health_check.return_value = {"status": "slow"}

        status = pool.health_check("oracle-1", mock_connector)

        assert status == "degraded"

    def test_health_check_unhealthy(self, pool, mock_connector):
        """Test health check for unhealthy connection."""
        mock_connector.health_check.side_effect = Exception("Connection lost")

        status = pool.health_check("oracle-1", mock_connector)

        assert status == "unhealthy"

    def test_stats_tracking(self, pool, mock_manager, mock_connector):
        """Test connection statistics tracking."""
        conn = pool.get_connection("oracle-1", mock_manager)

        stats = pool.get_stats("oracle-1")

        assert stats is not None
        assert stats.connection_id == "oracle-1"
        assert stats.use_count == 1
        assert stats.database_type == "oracle"

    def test_stats_increments_on_reuse(self, pool, mock_manager, mock_connector):
        """Test use count increments on reuse."""
        conn = pool.get_connection("oracle-1", mock_manager)
        stats1 = pool.get_stats("oracle-1")
        count1 = stats1.use_count

        pool.return_connection("oracle-1", conn)
        conn = pool.get_connection("oracle-1", mock_manager)
        stats2 = pool.get_stats("oracle-1")
        count2 = stats2.use_count

        assert count2 > count1

    def test_clear_idle_connections(self, pool, mock_manager, mock_connector):
        """Test clearing idle connections."""
        # Add old connection
        pool.pools["oracle-1"] = Mock()
        stats = ConnectionStats(
            connection_id="oracle-1",
            database_type="oracle",
            host="test",
            port=1521,
            created_at=utc_now(),
            last_used=utc_now() - timedelta(seconds=400),  # 400 seconds ago
            use_count=1,
            active_queries=0,
            health_status="healthy",
            last_health_check=utc_now(),
            response_time_ms=10.0,
        )
        pool.stats["oracle-1"] = stats
        # `empty()` is consulted in `while not pool.empty()`, so it must drain:
        # False once (enter the loop), then True (exit). Returning False forever
        # spins the loop indefinitely.
        pool.pools["oracle-1"].empty.side_effect = [False, True]
        pool.pools["oracle-1"].get_nowait.return_value = mock_connector

        pool.clear_idle_connections()

        # Idle connection should be closed
        mock_connector.close.assert_called()

    def test_close_all(self, pool, mock_connector):
        """Test closing all connections."""
        pool.pools["oracle-1"] = Mock()
        # Drain after one connector — same pattern as test_clear_idle_connections.
        pool.pools["oracle-1"].empty.side_effect = [False, True]
        pool.pools["oracle-1"].get_nowait.return_value = mock_connector

        pool.close_all()

        mock_connector.close.assert_called()
        assert len(pool.pools) == 0


class TestCachedConnectionStats:
    """Test statistics caching."""

    @pytest.fixture
    def cache(self):
        """Create stats cache."""
        return CachedConnectionStats(ttl_seconds=60)

    def test_set_and_get(self, cache):
        """Test caching stats."""
        stats = {"use_count": 10, "health": "healthy"}

        cache.set("oracle-1", stats)
        retrieved = cache.get("oracle-1")

        assert retrieved == stats

    def test_expired_cache_returns_none(self, cache):
        """Test expired cache is removed."""
        stats = {"use_count": 10}

        cache.set("oracle-1", stats)
        # Manually set old timestamp
        cache.cache_time["oracle-1"] = utc_now() - timedelta(seconds=70)

        retrieved = cache.get("oracle-1")

        assert retrieved is None

    def test_invalidate_specific_cache(self, cache):
        """Test invalidating specific connection cache."""
        cache.set("oracle-1", {"data": "test"})
        cache.set("postgres-1", {"data": "test"})

        cache.invalidate("oracle-1")

        assert cache.get("oracle-1") is None
        assert cache.get("postgres-1") is not None

    def test_clear_all_cache(self, cache):
        """Test clearing all cached stats."""
        cache.set("oracle-1", {"data": "test"})
        cache.set("postgres-1", {"data": "test"})

        cache.clear()

        assert cache.get("oracle-1") is None
        assert cache.get("postgres-1") is None


class TestGlobalInstances:
    """Test global pool and cache instances."""

    def test_get_connection_pool_singleton(self):
        """Test connection pool is singleton."""
        pool1 = get_connection_pool()
        pool2 = get_connection_pool()

        assert pool1 is pool2

    def test_get_stats_cache_singleton(self):
        """Test stats cache is singleton."""
        cache1 = get_stats_cache()
        cache2 = get_stats_cache()

        assert cache1 is cache2
