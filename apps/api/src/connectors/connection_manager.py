"""
Connection management with secure credential storage.
Handles encryption, credential persistence, and connection lifecycle.
"""

from pydantic import BaseModel
from typing import Optional, Dict, Any
from cryptography.fernet import Fernet
import json
import logging
import os

from .oracle_connector import OracleConnector
from .postgres_connector import PostgresConnector

logger = logging.getLogger(__name__)


class ConnectionConfig(BaseModel):
    """Connection configuration (user-provided)."""

    database_type: str  # "oracle" or "postgres"
    host: str
    port: int
    username: str
    password: str
    service_name: Optional[str] = None  # Oracle only
    database: Optional[str] = None  # PostgreSQL only

    class Config:
        json_schema_extra = {
            "example": {
                "database_type": "oracle",
                "host": "oracle.company.com",
                "port": 1521,
                "username": "migration_user",
                "password": "password123",
                "service_name": "ORCL",
            }
        }


class SecureCredentialManager:
    """Encrypt/decrypt connection strings at rest."""

    def __init__(self, encryption_key: Optional[str] = None):
        """
        Initialize credential manager.

        Args:
            encryption_key: Fernet key for encryption (from environment or Vault)
                          If None, generates new key (for development only)
        """
        if encryption_key is None:
            # Development only: generate new key
            encryption_key = Fernet.generate_key()
            logger.warning(
                "No encryption key provided. Generated new key (dev mode only). "
                "For production, set ENCRYPTION_KEY environment variable."
            )

        if isinstance(encryption_key, str):
            encryption_key = encryption_key.encode()

        self.cipher = Fernet(encryption_key)

    def encrypt_connection_string(self, connection_string: str) -> str:
        """
        Encrypt connection string for storage.

        Args:
            connection_string: Plaintext connection string

        Returns:
            Encrypted string (safe to store in DB)
        """
        encrypted = self.cipher.encrypt(connection_string.encode())
        return encrypted.decode()

    def decrypt_connection_string(self, encrypted: str) -> str:
        """
        Decrypt connection string for use.

        Args:
            encrypted: Encrypted connection string from database

        Returns:
            Plaintext connection string
        """
        decrypted = self.cipher.decrypt(encrypted.encode())
        return decrypted.decode()


class ConnectionManager:
    """Manages database connections and lifecycle."""

    def __init__(self, encryption_key: Optional[str] = None):
        """
        Initialize connection manager.

        Args:
            encryption_key: For encrypting stored credentials
        """
        self.credential_manager = SecureCredentialManager(encryption_key)
        self.active_connections: Dict[str, Any] = {}

    def create_connection(self, connection_id: str, config: ConnectionConfig) -> bool:
        """
        Create and test a new connection.

        Args:
            connection_id: Unique identifier for this connection
            config: Connection configuration

        Returns:
            True if connection successful, False otherwise
        """
        try:
            if config.database_type == "oracle":
                connector = OracleConnector(
                    host=config.host,
                    port=config.port,
                    service_name=config.service_name,
                    username=config.username,
                    password=config.password,
                )
            elif config.database_type == "postgres":
                connector = PostgresConnector(
                    host=config.host,
                    port=config.port,
                    database=config.database,
                    username=config.username,
                    password=config.password,
                )
            else:
                logger.error(f"Unknown database type: {config.database_type}")
                return False

            # Test connection
            if not connector.connect():
                return False

            # Store connection
            self.active_connections[connection_id] = {
                "connector": connector,
                "config": config,
                "type": config.database_type,
            }

            logger.info(f"Created connection {connection_id} ({config.database_type})")
            return True

        except Exception as e:
            logger.error(f"Failed to create connection: {e}")
            return False

    def get_connector(self, connection_id: str) -> Optional[Any]:
        """Get connector for a connection ID."""
        conn = self.active_connections.get(connection_id)
        return conn["connector"] if conn else None

    def test_connection(self, config: ConnectionConfig) -> Dict[str, Any]:
        """
        Test connection without storing it.

        Returns:
            Dict with status, version, and capabilities
        """
        try:
            if config.database_type == "oracle":
                connector = OracleConnector(
                    host=config.host,
                    port=config.port,
                    service_name=config.service_name,
                    username=config.username,
                    password=config.password,
                )
            elif config.database_type == "postgres":
                connector = PostgresConnector(
                    host=config.host,
                    port=config.port,
                    database=config.database,
                    username=config.username,
                    password=config.password,
                )
            else:
                return {"status": "failed", "error": f"Unknown database type: {config.database_type}"}

            # Test connection
            if not connector.connect():
                return {"status": "failed", "error": "Connection test failed"}

            # Get health check
            health = connector.health_check()
            connector.close()

            return {
                "status": "connected",
                "database_type": config.database_type,
                "host": config.host,
                "health": health,
            }

        except Exception as e:
            logger.error(f"Connection test error: {e}")
            return {"status": "failed", "error": str(e)}

    def close_connection(self, connection_id: str):
        """Close a connection and remove it."""
        conn = self.active_connections.get(connection_id)
        if conn:
            conn["connector"].close()
            del self.active_connections[connection_id]
            logger.info(f"Closed connection {connection_id}")

    def close_all(self):
        """Close all active connections."""
        for connection_id in list(self.active_connections.keys()):
            self.close_connection(connection_id)

    def list_connections(self) -> Dict[str, Dict[str, Any]]:
        """List all active connections (without credentials)."""
        result = {}
        for conn_id, conn_data in self.active_connections.items():
            result[conn_id] = {
                "type": conn_data["type"],
                "host": conn_data["config"].host,
                "port": conn_data["config"].port,
                "connected": conn_data["connector"].connected,
            }
        return result

    def __del__(self):
        """Cleanup on deletion."""
        self.close_all()


# Global connection manager instance
_connection_manager: Optional[ConnectionManager] = None


def get_connection_manager() -> ConnectionManager:
    """Get or create global connection manager."""
    global _connection_manager

    if _connection_manager is None:
        encryption_key = os.getenv("ENCRYPTION_KEY")
        _connection_manager = ConnectionManager(encryption_key)

    return _connection_manager
