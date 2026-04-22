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
