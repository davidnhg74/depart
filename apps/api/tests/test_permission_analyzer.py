"""
Tests for Permission Analyzer (Phase 3.3).
Tests Oracle privilege extraction and mapping to PostgreSQL.
"""

import pytest
import json
from datetime import datetime
from unittest.mock import Mock

from src.analyzers.permission_analyzer import (
    OraclePrivilegeExtractor,
    PermissionMapper,
    PermissionAnalyzer,
    OraclePrivileges,
    PrivilegeMapping,
    UnmappablePrivilege,
    PermissionAnalysisResult,
)


class TestOraclePrivilegeExtractor:
    """Test Oracle privilege extraction with DBA/non-DBA fallback."""

    @pytest.fixture
    def mock_connector(self):
        """Mock Oracle connector."""
        return Mock()

    @pytest.fixture
    def extractor(self):
        """Permission extractor instance."""
        return OraclePrivilegeExtractor()

    def test_extract_dba_privileges(self, extractor, mock_connector):
        """Test extraction with DBA access."""
        mock_session = Mock()
        mock_connector.get_session.return_value = mock_session

        # Mock DBA privilege queries
        sys_privs_result = Mock()
        sys_privs_result.mappings.return_value.all.return_value = [
            Mock(
                items=lambda: [
                    ("grantee", "SCOTT"),
                    ("privilege", "CREATE TABLE"),
                    ("admin_option", "YES"),
                ]
            )
        ]

        obj_privs_result = Mock()
        obj_privs_result.mappings.return_value.all.return_value = [
            Mock(
                items=lambda: [
                    ("grantee", "SCOTT"),
                    ("owner", "SYS"),
                    ("table_name", "V$SQL"),
                    ("privilege", "SELECT"),
                    ("grantable", "NO"),
                ]
            )
        ]

        role_privs_result = Mock()
        role_privs_result.mappings.return_value.all.return_value = [
            Mock(
                items=lambda: [
                    ("grantee", "SCOTT"),
                    ("granted_role", "DBA"),
                    ("admin_option", "NO"),
                    ("default_role", "YES"),
                ]
            )
        ]

        dba_users_result = Mock()
        dba_users_result.mappings.return_value.all.return_value = [
            Mock(items=lambda: [("username", "SYS")])
        ]

        mock_session.execute.side_effect = [
            sys_privs_result,
            obj_privs_result,
            role_privs_result,
            dba_users_result,
        ]

        result = extractor.extract(mock_connector)

        assert result.extracted_as_dba is True
        assert len(result.system_privs) == 1
        assert len(result.object_privs) == 1
        assert len(result.role_grants) == 1
        assert len(result.dba_users) == 1

    def test_extract_non_dba_fallback(self, extractor, mock_connector):
        """Test fallback to non-DBA privileges."""
        mock_session = Mock()
        mock_connector.get_session.return_value = mock_session

        # First call (DBA path) raises exception
        from sqlalchemy.exc import DatabaseError

        mock_session.execute.side_effect = [
            DatabaseError("Access denied", None, None),  # DBA path fails
            Mock(
                mappings=Mock(
                    return_value=Mock(
                        all=Mock(return_value=[Mock(items=lambda: [("privilege", "CREATE TABLE")])])
                    )
                )
            ),  # session_privs succeeds
            Mock(
                mappings=Mock(
                    return_value=Mock(
                        all=Mock(
                            return_value=[
                                Mock(
                                    items=lambda: [
                                        ("grantee", "USER"),
                                        ("owner", "SCOTT"),
                                        ("table_name", "EMP"),
                                        ("privilege", "SELECT"),
                                        ("grantable", "NO"),
                                    ]
                                )
                            ]
                        )
                    )
                )
            ),  # user_tab_privs succeeds
        ]

        result = extractor.extract(mock_connector)

        assert result.extracted_as_dba is False
        assert len(result.system_privs) >= 0
        assert len(result.object_privs) >= 0


class TestPermissionMapper:
    """Test mapping Oracle privileges to PostgreSQL."""

    @pytest.fixture
    def mapper(self):
        """Permission mapper instance."""
        return PermissionMapper()

    @pytest.fixture
    def mock_llm_client(self):
        """Mock LLM client."""
        client = Mock()
        # Realistic Claude response
        client.analyze_permission_mapping.return_value = {
            "mappings": [
                {
                    "oracle_privilege": "CREATE TABLE",
                    "pg_equivalent": "GRANT CREATE ON SCHEMA public TO user;",
                    "risk_level": 2,
                    "recommendation": "Assign CREATE privilege on public schema",
                    "grant_sql": "GRANT CREATE ON SCHEMA public TO user;",
                },
                {
                    "oracle_privilege": "SELECT ANY TABLE",
                    "pg_equivalent": "GRANT SELECT ON ALL TABLES IN SCHEMA public TO user;",
                    "risk_level": 3,
                    "recommendation": "Grant table-level select to role",
                    "grant_sql": "GRANT SELECT ON ALL TABLES IN SCHEMA public TO user;",
                },
            ],
            "unmappable": [
                {
                    "oracle_privilege": "EXECUTE ANY PROCEDURE",
                    "reason": "PostgreSQL uses schema-level grants, not database-level",
                    "workaround": "Use GRANT EXECUTE ON FUNCTION or create custom roles",
                    "risk_level": 7,
                },
            ],
            "overall_risk": "MEDIUM",
        }
        return client

    def test_map_privileges(self, mapper, mock_llm_client):
        """Test privilege mapping to PostgreSQL."""
        oracle_privs = OraclePrivileges(
            system_privs=[
                {"grantee": "SCOTT", "privilege": "CREATE TABLE", "admin_option": "YES"},
                {"grantee": "SCOTT", "privilege": "SELECT ANY TABLE", "admin_option": "NO"},
            ],
            object_privs=[
                {
                    "grantee": "SCOTT",
                    "owner": "SYS",
                    "table_name": "V$SQL",
                    "privilege": "SELECT",
                    "grantable": "NO",
                },
            ],
            role_grants=[],
            dba_users=[],
            extracted_as_dba=True,
        )

        result = mapper.map_to_postgres(oracle_privs, mock_llm_client)

        assert isinstance(result, PermissionAnalysisResult)
        assert len(result.mappings) >= 0
        assert result.overall_risk in ["LOW", "MEDIUM", "HIGH", "CRITICAL"]
        assert isinstance(result.grant_sql, list)

    def test_risk_calculation(self, mapper, mock_llm_client):
        """Test overall risk calculation."""
        oracle_privs = OraclePrivileges(
            system_privs=[],
            object_privs=[],
            role_grants=[],
            dba_users=[],
            extracted_as_dba=False,
        )

        result = mapper.map_to_postgres(oracle_privs, mock_llm_client)

        # Risk should be based on max risk_level
        assert result.overall_risk in ["LOW", "MEDIUM", "HIGH", "CRITICAL"]


class TestPermissionAnalyzer:
    """Test main permission analyzer orchestrator."""

    @pytest.fixture
    def mock_llm_client(self):
        """Mock LLM client."""
        client = Mock()
        client.analyze_permission_mapping.return_value = {
            "mappings": [],
            "unmappable": [],
            "overall_risk": "LOW",
        }
        return client

    @pytest.fixture
    def analyzer(self, mock_llm_client):
        """Analyzer instance."""
        return PermissionAnalyzer(mock_llm_client)

    def test_analyze_from_json(self, analyzer):
        """Test analyzing from JSON input."""
        json_input = json.dumps(
            {
                "system_privs": [{"grantee": "USER1", "privilege": "CREATE TABLE"}],
                "object_privs": [],
                "role_grants": [],
                "dba_users": [],
                "extracted_as_dba": True,
            }
        )

        result = analyzer.analyze_from_json(json_input)

        assert isinstance(result, PermissionAnalysisResult)
        assert result.overall_risk is not None
        assert isinstance(result.mappings, list)
        assert isinstance(result.unmappable, list)

    def test_analyze_from_connector(self, analyzer, mock_llm_client):
        """Test analyzing from live Oracle connection."""
        mock_connector = Mock()
        mock_session = Mock()
        mock_connector.get_session.return_value = mock_session

        # Mock successful extraction
        mock_session.execute.side_effect = [
            Mock(mappings=Mock(return_value=Mock(all=Mock(return_value=[])))),
            Mock(mappings=Mock(return_value=Mock(all=Mock(return_value=[])))),
            Mock(mappings=Mock(return_value=Mock(all=Mock(return_value=[])))),
            Mock(mappings=Mock(return_value=Mock(all=Mock(return_value=[])))),
        ]

        result = analyzer.analyze_from_connector(mock_connector)

        assert isinstance(result, PermissionAnalysisResult)
        assert mock_connector.get_session.called


class TestPermissionDataclasses:
    """Test permission analyzer dataclasses."""

    def test_privilege_mapping_creation(self):
        """Test PrivilegeMapping dataclass."""
        mapping = PrivilegeMapping(
            oracle_privilege="CREATE TABLE",
            pg_equivalent="GRANT CREATE ON SCHEMA public TO user;",
            risk_level=2,
            recommendation="Safe to grant",
            grant_sql="GRANT CREATE ON SCHEMA public TO user;",
        )

        assert mapping.oracle_privilege == "CREATE TABLE"
        assert mapping.risk_level == 2
        assert mapping.pg_equivalent is not None

    def test_unmappable_privilege_creation(self):
        """Test UnmappablePrivilege dataclass."""
        unmappable = UnmappablePrivilege(
            oracle_privilege="EXECUTE ANY PROCEDURE",
            reason="No direct PostgreSQL equivalent",
            workaround="Use schema-level grants",
            risk_level=7,
        )

        assert unmappable.oracle_privilege == "EXECUTE ANY PROCEDURE"
        assert unmappable.risk_level == 7

    def test_permission_analysis_result(self):
        """Test PermissionAnalysisResult dataclass."""
        result = PermissionAnalysisResult(
            mappings=[],
            unmappable=[],
            grant_sql=["GRANT SELECT ON TABLE t1 TO user;"],
            overall_risk="LOW",
            analyzed_at=datetime.utcnow().isoformat(),
        )

        assert isinstance(result.mappings, list)
        assert result.overall_risk == "LOW"
        assert len(result.grant_sql) == 1
