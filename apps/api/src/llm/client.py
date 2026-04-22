from anthropic import Anthropic
from ..config import settings
import json
import logging

logger = logging.getLogger(__name__)


class LLMClient:
    def __init__(self):
        self.client = Anthropic(api_key=settings.anthropic_api_key)
        self.model = "claude-sonnet-4-20250514"

    def convert_plsql(self, plsql_code: str, context: str = "") -> str:
        """
        Convert PL/SQL to PL/pgSQL using Claude.
        Phase 2 implementation.
        """
        prompt = f"""You are an expert Oracle DBA and PostgreSQL engineer.
Convert the following Oracle PL/SQL code to PostgreSQL PL/pgSQL.
Output ONLY the converted code, no explanations.

Oracle PL/SQL:
{plsql_code}

Context: {context}
"""
        message = self.client.messages.create(
            model=self.model,
            max_tokens=4096,
            messages=[
                {"role": "user", "content": prompt}
            ]
        )
        return message.content[0].text

    def analyze_complexity(self, plsql_code: str) -> dict:
        """
        Analyze PL/SQL complexity using Claude.
        Phase 2 optimization - currently handled deterministically.
        """
        # Stub for Phase 2
        return {}

    def detect_semantic_issues(
        self,
        type_mappings: list,
        context: str = "",
    ) -> list:
        """
        Detect semantic/logical errors in Oracle → PostgreSQL type mappings.
        Uses Claude to reason about precision loss, date behavior changes, NULL semantics, etc.

        Args:
            type_mappings: List of {table, column, oracle_type, pg_type} dicts
            context: Optional additional context for analysis

        Returns:
            List of issue dicts with severity, issue_type, affected_object, etc.
        """
        prompt = f"""You are an expert Oracle-to-PostgreSQL migration engineer.

Analyze these type mappings for semantic risks: precision loss, date behavior
changes, implicit casts, NULL semantic differences, and encoding mismatches.

TYPE MAPPINGS:
{json.dumps(type_mappings, indent=2)}

{f"Additional context: {context}" if context else ""}

Known semantic rules to check:
1. NUMBER(p,s)→NUMERIC(p,s): If p decreased, values exceeding new precision will raise exceptions or be truncated.
2. Oracle DATE stores time (HH:MI:SS); PG DATE does not — use TIMESTAMP.
3. Oracle NUMBER used as boolean (0/1) has no implicit cast to PG BOOLEAN.
4. VARCHAR2(N BYTE) vs VARCHAR2(N CHAR): multibyte chars may truncate.
5. Oracle '' IS NULL; PG '' IS NOT NULL — affects NOT NULL constraints and application logic.
6. TIMESTAMP WITHOUT TIME ZONE vs TIMESTAMP WITH TIME ZONE — AT TIME ZONE behavior differs.
7. Oracle LONG → PostgreSQL TEXT: loses constraints, may cause index issues.
8. Oracle RAW → PostgreSQL BYTEA: encoding semantics differ.

IMPORTANT: Output ONLY valid JSON, no markdown, no explanation.

{{
  "issues": [
    {{
      "severity": "CRITICAL|ERROR|WARNING|INFO",
      "issue_type": "PRECISION_LOSS|DATE_BEHAVIOR|TYPE_COERCION|ENCODING_MISMATCH|NULL_SEMANTICS|IMPLICIT_CAST|RANGE_CHANGE",
      "affected_object": "TABLE.COLUMN",
      "oracle_type": "...",
      "pg_type": "...",
      "description": "...",
      "recommendation": "..."
    }}
  ]
}}"""

        try:
            message = self.client.messages.create(
                model=self.model,
                max_tokens=4096,
                messages=[{"role": "user", "content": prompt}],
            )
            text = message.content[0].text.strip()

            # Strip markdown code blocks if present
            for prefix in ("```json", "```"):
                if text.startswith(prefix):
                    text = text[len(prefix):]
            if text.endswith("```"):
                text = text[:-3]
            text = text.strip()

            result = json.loads(text)
            return result.get("issues", [])

        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse Claude response as JSON: {e}")
            return []
        except Exception as e:
            logger.error(f"Error detecting semantic issues: {e}")
            return []

    def analyze_permission_mapping(self, oracle_privs_json: str) -> dict:
        """
        Map Oracle privileges to PostgreSQL equivalents using Claude.

        Args:
            oracle_privs_json: JSON string with system_privs, object_privs, role_grants, dba_users

        Returns:
            Dict with mappings, unmappable, and overall_risk fields
        """
        prompt = f"""You are an expert Oracle and PostgreSQL permission/grants engineer.

Analyze these Oracle privileges and map each to PostgreSQL equivalents. Consider:
1. System privileges → GRANT statements on roles, databases, schemas
2. Object privileges → GRANT on tables, sequences, functions
3. Role grants → CREATE ROLE + role membership via GRANT role_name TO user
4. DBA users → PostgreSQL superuser or special roles
5. Unmappable privileges → no direct PostgreSQL equivalent; suggest workarounds

ORACLE PRIVILEGES DATA:
{oracle_privs_json}

IMPORTANT: Output ONLY valid JSON, no markdown, no explanation.

{{
  "mappings": [
    {{
      "oracle_privilege": "...",
      "pg_equivalent": "GRANT ... ON ... TO ...",
      "risk_level": 1-10,
      "recommendation": "...",
      "grant_sql": "GRANT ... ON ... TO ... ;"
    }}
  ],
  "unmappable": [
    {{
      "oracle_privilege": "...",
      "reason": "...",
      "workaround": "...",
      "risk_level": 1-10
    }}
  ],
  "overall_risk": "LOW|MEDIUM|HIGH|CRITICAL"
}}"""

        try:
            message = self.client.messages.create(
                model=self.model,
                max_tokens=4096,
                messages=[{"role": "user", "content": prompt}],
            )
            text = message.content[0].text.strip()

            # Strip markdown code blocks if present
            for prefix in ("```json", "```"):
                if text.startswith(prefix):
                    text = text[len(prefix):]
            if text.endswith("```"):
                text = text[:-3]
            text = text.strip()

            result = json.loads(text)
            return result

        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse permission mapping response as JSON: {e}")
            return {"mappings": [], "unmappable": [], "overall_risk": "HIGH"}
        except Exception as e:
            logger.error(f"Error analyzing permission mapping: {e}")
            return {"mappings": [], "unmappable": [], "overall_risk": "HIGH"}

    def summarize_benchmark(self, report_json: str) -> str:
        """
        Generate a summary of benchmark comparison results using Claude.

        Args:
            report_json: JSON string with query_comparisons, table_comparisons, counts

        Returns:
            Plain text 2-3 sentence summary of benchmark results
        """
        prompt = f"""You are an expert database performance engineer comparing Oracle and PostgreSQL.

Summarize the following benchmark comparison in 2-3 sentences. Focus on:
1. Overall performance trend (PostgreSQL faster, slower, or equivalent)
2. Most significant differences (if any)
3. Recommendation for migration readiness

BENCHMARK DATA:
{report_json}

Respond with ONLY the summary text, no JSON, no markdown."""

        try:
            message = self.client.messages.create(
                model=self.model,
                max_tokens=512,
                messages=[{"role": "user", "content": prompt}],
            )
            return message.content[0].text.strip()

        except Exception as e:
            logger.error(f"Error summarizing benchmark: {e}")
            return "Benchmark comparison completed. Review detailed metrics above for analysis."
