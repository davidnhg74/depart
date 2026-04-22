"""
Enhanced connection pooling and health monitoring for Phase 3.3.
Adds connection pooling, health checks, and performance caching.
"""

from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta
from dataclasses import dataclass
import logging
import threading
from queue import Queue, Empty

from .connection_manager import ConnectionManager

logger = logging.getLogger(__name__)


@dataclass
class ConnectionStats:
    """Statistics for a pooled connection."""

    connection_id: str
    database_type: str
    host: str
    port: int
    created_at: datetime
    last_used: datetime
    use_count: int
    active_queries: int
    health_status: str  # "healthy", "degraded", "unhealthy"
    last_health_check: datetime
    response_time_ms: float  # Average response time


class ConnectionPool:
    """
    Connection pool with health monitoring and statistics.
    Reuses connections for better performance.
    """

    def __init__(self, min_size: int = 2, max_size: int = 10, max_idle_seconds: int = 300):
        """
        Initialize connection pool.

        Args:
            min_size: Minimum connections to maintain
            max_size: Maximum connections allowed
            max_idle_seconds: Close idle connections after this time
        """
        self.min_size = min_size
        self.max_size = max_size
        self.max_idle_seconds = max_idle_seconds

        self.pools: Dict[str, Queue] = {}  # connection_id -> Queue of available connections
        self.active_connections: Dict[str, List[Any]] = (
            {}
        )  # connection_id -> list of active connectors
        self.stats: Dict[str, ConnectionStats] = {}  # connection_id -> stats
        self.locks: Dict[str, threading.RLock] = {}  # per-connection locking

    def get_connection(self, connection_id: str, manager: ConnectionManager):
        """
        Get a connection from pool.
        Creates new connection if needed.

        Args:
            connection_id: Connection identifier
            manager: Connection manager for creating new connections

        Returns:
            Connector instance or None
        """
        if connection_id not in self.locks:
            self.locks[connection_id] = threading.RLock()

        with self.locks[connection_id]:
            # Try to get from pool
            if connection_id not in self.pools:
                self.pools[connection_id] = Queue(maxsize=self.max_size)
                self.active_connections[connection_id] = []

            try:
                connector = self.pools[connection_id].get_nowait()

                # Check if still healthy
                if not connector.connected:
                    connector.connect()

                # Update stats
                self._update_stats_on_use(connection_id, connector)
                self.active_connections[connection_id].append(connector)

                return connector

            except Empty:
                # Pool is empty, create new if under limit
                if len(self.active_connections[connection_id]) < self.max_size:
                    connector = manager.get_connector(connection_id)
                    if connector:
                        if not connector.connected:
                            connector.connect()
                        self.active_connections[connection_id].append(connector)
                        self._update_stats_on_use(connection_id, connector)
                        return connector

                return None

    def return_connection(self, connection_id: str, connector: Any):
        """
        Return connection to pool for reuse.

        Args:
            connection_id: Connection identifier
            connector: Connector instance
        """
        if connection_id not in self.locks:
            self.locks[connection_id] = threading.RLock()

        with self.locks[connection_id]:
            if connection_id in self.active_connections:
                if connector in self.active_connections[connection_id]:
                    self.active_connections[connection_id].remove(connector)

            # Return to pool if healthy
            if connection_id in self.pools and connector.connected:
                try:
                    self.pools[connection_id].put_nowait(connector)
                except:
                    connector.close()

    def health_check(self, connection_id: str, connector: Any) -> str:
        """
        Check connection health.

        Returns:
            "healthy", "degraded", or "unhealthy"
        """
        try:
            health = connector.health_check()

            if health.get("status") == "ok":
                return "healthy"
            elif health.get("status") == "slow":
                return "degraded"
            else:
                return "unhealthy"

        except Exception as e:
            logger.warning(f"Health check failed for {connection_id}: {e}")
            return "unhealthy"

    def _update_stats_on_use(self, connection_id: str, connector: Any):
        """Update statistics when connection is used."""
        if connection_id not in self.stats:
            config = getattr(connector, "config", None) or {}
            self.stats[connection_id] = ConnectionStats(
                connection_id=connection_id,
                database_type=getattr(connector, "database_type", "unknown"),
                host=getattr(connector, "host", "unknown"),
                port=getattr(connector, "port", 0),
                created_at=datetime.utcnow(),
                last_used=datetime.utcnow(),
                use_count=1,
                active_queries=0,
                health_status="healthy",
                last_health_check=datetime.utcnow(),
                response_time_ms=0.0,
            )
        else:
            stats = self.stats[connection_id]
            stats.last_used = datetime.utcnow()
            stats.use_count += 1

    def get_stats(self, connection_id: str) -> Optional[ConnectionStats]:
        """Get connection statistics."""
        return self.stats.get(connection_id)

    def clear_idle_connections(self):
        """Remove idle connections to free resources."""
        now = datetime.utcnow()
        cutoff = timedelta(seconds=self.max_idle_seconds)

        for conn_id in list(self.pools.keys()):
            pool = self.pools[conn_id]

            # Try to remove idle connections from pool
            items_to_remove = []
            while not pool.empty():
                try:
                    connector = pool.get_nowait()
                    stats = self.stats.get(conn_id)

                    if stats and (now - stats.last_used) > cutoff:
                        # Connection is idle, close it
                        connector.close()
                        items_to_remove.append(None)
                    else:
                        # Keep it in pool
                        items_to_remove.append(connector)
                except Empty:
                    break

            # Put back non-idle connections
            for connector in items_to_remove:
                if connector:
                    try:
                        pool.put_nowait(connector)
                    except:
                        pass

    def close_all(self):
        """Close all connections in all pools."""
        for pool in self.pools.values():
            while not pool.empty():
                try:
                    connector = pool.get_nowait()
                    connector.close()
                except Empty:
                    break

        self.pools.clear()
        self.active_connections.clear()
        self.stats.clear()


class CachedConnectionStats:
    """
    Cache for connection statistics and capabilities.
    Useful for benchmark capture to avoid repeated queries.
    """

    def __init__(self, ttl_seconds: int = 3600):
        """
        Initialize cache.

        Args:
            ttl_seconds: Time to live for cached stats
        """
        self.ttl_seconds = ttl_seconds
        self.cache: Dict[str, Dict[str, Any]] = {}
        self.cache_time: Dict[str, datetime] = {}

    def get(self, connection_id: str) -> Optional[Dict[str, Any]]:
        """Get cached stats if still valid."""
        if connection_id not in self.cache:
            return None

        cached_at = self.cache_time.get(connection_id)
        if cached_at and (datetime.utcnow() - cached_at).total_seconds() > self.ttl_seconds:
            # Expired, remove
            del self.cache[connection_id]
            del self.cache_time[connection_id]
            return None

        return self.cache[connection_id]

    def set(self, connection_id: str, stats: Dict[str, Any]):
        """Cache stats."""
        self.cache[connection_id] = stats
        self.cache_time[connection_id] = datetime.utcnow()

    def invalidate(self, connection_id: str):
        """Invalidate cached stats for a connection."""
        self.cache.pop(connection_id, None)
        self.cache_time.pop(connection_id, None)

    def clear(self):
        """Clear all cached stats."""
        self.cache.clear()
        self.cache_time.clear()


# Global pool instance
_connection_pool: Optional[ConnectionPool] = None
_stats_cache: Optional[CachedConnectionStats] = None


def get_connection_pool() -> ConnectionPool:
    """Get or create global connection pool."""
    global _connection_pool

    if _connection_pool is None:
        _connection_pool = ConnectionPool(min_size=2, max_size=10)

    return _connection_pool


def get_stats_cache() -> CachedConnectionStats:
    """Get or create global stats cache."""
    global _stats_cache

    if _stats_cache is None:
        _stats_cache = CachedConnectionStats(ttl_seconds=3600)

    return _stats_cache
