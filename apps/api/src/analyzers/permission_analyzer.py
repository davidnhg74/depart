"""
Permission/Grant analyzer for Oracle → PostgreSQL migrations.
Extracts Oracle privileges and generates PostgreSQL GRANT statements.
"""

from dataclasses import dataclass, field
from typing import List, Dict, Optional, Any
from datetime import datetime
import logging
from sqlalchemy import text

logger = logging.getLogger(__name__)


@dataclass
class PrivilegeMapping:
    """Mapping of a single Oracle privilege to PostgreSQL equivalent."""
    oracle_privilege: str
    pg_equivalent: Optional[str]
    risk_level: int              # 1-10
    recommendation: str
    grant_sql: Optional[str] = None


@dataclass
class UnmappablePrivilege:
    """Oracle privilege that doesn't map to PostgreSQL."""
    oracle_privilege: str
    reason: str
    workaround: str
    risk_level: int              # 1-10


@dataclass
class OraclePrivileges:
    """Raw Oracle privilege data from data dictionary."""
    system_privs: List[Dict[str, Any]] = field(default_factory=list)
    object_privs: List[Dict[str, Any]] = field(default_factory=list)
    role_grants: List[Dict[str, Any]] = field(default_factory=list)
    dba_users: List[str] = field(default_factory=list)
    extracted_as_dba: bool = False


@dataclass
class PermissionAnalysisResult:
    """Complete permission analysis result."""
    mappings: List[PrivilegeMapping]
    unmappable: List[UnmappablePrivilege]
    grant_sql: List[str]
    overall_risk: str           # LOW, MEDIUM, HIGH, CRITICAL
    analyzed_at: str


class OraclePrivilegeExtractor:
    """Extract Oracle privileges from data dictionary."""

    def extract(self, oracle_connector) -> OraclePrivileges:
        """
        Extract all privileges from Oracle data dictionary.
        Tries DBA path first, falls back to current-user privileges.

        Args:
            oracle_connector: OracleConnector instance with active session

        Returns:
            OraclePrivileges dataclass with all extracted data
        """
        session = oracle_connector.get_session()
        result = OraclePrivileges()

        try:
            # Try DBA path (requires DBA or SELECT on DBA_SYS_PRIVS)
            try:
                # System privileges
                sys_privs_sql = """
                    SELECT grantee, privilege, admin_option
                    FROM dba_sys_privs
                    ORDER BY grantee, privilege
                """
                rows = session.execute(text(sys_privs_sql)).mappings().all()
                result.system_privs = [dict(r) for r in rows]
                logger.info(f"Extracted {len(result.system_privs)} system privileges (DBA path)")

                # Object privileges
                obj_privs_sql = """
                    SELECT grantee, owner, table_name, privilege, grantable
                    FROM dba_tab_privs
                    ORDER BY grantee, owner, table_name
                """
                rows = session.execute(text(obj_privs_sql)).mappings().all()
                result.object_privs = [dict(r) for r in rows]
                logger.info(f"Extracted {len(result.object_privs)} object privileges (DBA path)")

                # Role grants
                role_sql = """
                    SELECT grantee, granted_role, admin_option, default_role
                    FROM dba_role_privs
                    ORDER BY grantee, granted_role
                """
                rows = session.execute(text(role_sql)).mappings().all()
                result.role_grants = [dict(r) for r in rows]
                logger.info(f"Extracted {len(result.role_grants)} role grants (DBA path)")

                # DBA users
                dba_users_sql = """
                    SELECT DISTINCT username
                    FROM dba_users
                    WHERE username IN (
                        SELECT grantee FROM dba_role_privs WHERE granted_role='DBA'
                    )
                    ORDER BY username
                """
                rows = session.execute(text(dba_users_sql)).mappings().all()
                result.dba_users = [r["username"] for r in rows]
                logger.info(f"Extracted {len(result.dba_users)} DBA users")

                result.extracted_as_dba = True

            except Exception as dba_error:
                # Fallback to current-user privileges (non-DBA)
                logger.warning(f"DBA path failed ({dba_error}), falling back to current-user privileges")

                # Session privileges (what current user has)
                session_privs_sql = """
                    SELECT privilege FROM session_privs
                    ORDER BY privilege
                """
                try:
                    rows = session.execute(text(session_privs_sql)).mappings().all()
                    result.system_privs = [{"privilege": r["privilege"], "grantee": "CURRENT_USER"} for r in rows]
                    logger.info(f"Extracted {len(result.system_privs)} session privileges")
                except Exception as e:
                    logger.warning(f"Could not extract session privileges: {e}")

                # User table privileges
                user_obj_sql = """
                    SELECT grantee, owner, table_name, privilege, grantable
                    FROM user_tab_privs
                    ORDER BY owner, table_name
                """
                try:
                    rows = session.execute(text(user_obj_sql)).mappings().all()
                    result.object_privs = [dict(r) for r in rows]
                    logger.info(f"Extracted {len(result.object_privs)} object privileges (non-DBA)")
                except Exception as e:
                    logger.warning(f"Could not extract object privileges: {e}")

                result.extracted_as_dba = False

        except Exception as e:
            logger.error(f"Error extracting privileges: {e}")
            raise
        finally:
            session.close()

        return result


class PermissionMapper:
    """Map Oracle privileges to PostgreSQL equivalents."""

    def map_to_postgres(
        self,
        oracle_privs: OraclePrivileges,
        llm_client,
    ) -> PermissionAnalysisResult:
        """
        Map Oracle privileges to PostgreSQL using Claude AI.

        Args:
            oracle_privs: Extracted Oracle privileges
            llm_client: LLMClient for Claude analysis

        Returns:
            PermissionAnalysisResult with mappings and generated GRANT SQL
        """
        import json

        # Prepare data for Claude
        privs_dict = {
            "system_privs": oracle_privs.system_privs,
            "object_privs": oracle_privs.object_privs,
            "role_grants": oracle_privs.role_grants,
            "dba_users": oracle_privs.dba_users,
            "extracted_as_dba": oracle_privs.extracted_as_dba,
        }

        try:
            # Call Claude for analysis
            claude_result = llm_client.analyze_permission_mapping(json.dumps(privs_dict))

            # Parse Claude response into dataclasses
            mappings = []
            for m in claude_result.get("mappings", []):
                mappings.append(PrivilegeMapping(
                    oracle_privilege=m["oracle_privilege"],
                    pg_equivalent=m.get("pg_equivalent"),
                    risk_level=m.get("risk_level", 5),
                    recommendation=m.get("recommendation", ""),
                    grant_sql=m.get("grant_sql"),
                ))

            unmappable = []
            for u in claude_result.get("unmappable", []):
                unmappable.append(UnmappablePrivilege(
                    oracle_privilege=u["oracle_privilege"],
                    reason=u.get("reason", ""),
                    workaround=u.get("workaround", ""),
                    risk_level=u.get("risk_level", 5),
                ))

            # Compile GRANT SQL
            grant_sql = [m.grant_sql for m in mappings if m.grant_sql]

            # Determine overall risk
            all_risks = [m.risk_level for m in mappings] + [u.risk_level for u in unmappable]
            max_risk = max(all_risks) if all_risks else 0
            if max_risk >= 8:
                overall_risk = "CRITICAL"
            elif max_risk >= 6:
                overall_risk = "HIGH"
            elif max_risk >= 4:
                overall_risk = "MEDIUM"
            else:
                overall_risk = "LOW"

            return PermissionAnalysisResult(
                mappings=mappings,
                unmappable=unmappable,
                grant_sql=grant_sql,
                overall_risk=overall_risk,
                analyzed_at=datetime.utcnow().isoformat(),
            )

        except Exception as e:
            logger.error(f"Error mapping permissions: {e}")
            raise


class PermissionAnalyzer:
    """Main orchestrator for permission analysis."""

    def __init__(self, llm_client):
        self.extractor = OraclePrivilegeExtractor()
        self.mapper = PermissionMapper()
        self.llm = llm_client

    def analyze_from_connector(
        self,
        oracle_connector,
    ) -> PermissionAnalysisResult:
        """
        Full pipeline: extract Oracle privileges, map to PostgreSQL.

        Args:
            oracle_connector: OracleConnector with active connection

        Returns:
            PermissionAnalysisResult
        """
        oracle_privs = self.extractor.extract(oracle_connector)
        return self.mapper.map_to_postgres(oracle_privs, self.llm)

    def analyze_from_json(
        self,
        privileges_json: str,
    ) -> PermissionAnalysisResult:
        """
        Analyze from raw JSON privilege data (for testing/manual input).

        Args:
            privileges_json: JSON string with oracle privileges

        Returns:
            PermissionAnalysisResult
        """
        import json

        oracle_privs_dict = json.loads(privileges_json)
        oracle_privs = OraclePrivileges(
            system_privs=oracle_privs_dict.get("system_privs", []),
            object_privs=oracle_privs_dict.get("object_privs", []),
            role_grants=oracle_privs_dict.get("role_grants", []),
            dba_users=oracle_privs_dict.get("dba_users", []),
            extracted_as_dba=oracle_privs_dict.get("extracted_as_dba", False),
        )
        return self.mapper.map_to_postgres(oracle_privs, self.llm)
