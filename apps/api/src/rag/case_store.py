"""
Conversion case storage and retrieval.
Stores successful conversions in PostgreSQL with pgvector for similarity search.
"""

from sqlalchemy.orm import Session
from datetime import datetime
from typing import List
from .embeddings import EmbeddingGenerator
from ..utils.time import utc_now


class ConversionCase:
    """Model for storing conversion cases (for ORM definition)."""

    def __init__(
        self,
        construct_type: str,  # PROCEDURE, FUNCTION, TABLE, etc.
        oracle_code: str,
        postgres_code: str,
        embedding: List[float],
        success_count: int = 1,
        fail_count: int = 0,
    ):
        self.construct_type = construct_type
        self.oracle_code = oracle_code
        self.postgres_code = postgres_code
        self.embedding = embedding
        self.success_count = success_count
        self.fail_count = fail_count
        self.created_at = utc_now()
        self.updated_at = utc_now()

    @property
    def success_rate(self) -> float:
        """Calculate success rate for this conversion pattern."""
        total = self.success_count + self.fail_count
        if total == 0:
            return 0.0
        return self.success_count / total

    @property
    def pattern_signature(self) -> str:
        """Extract pattern signature for grouping similar conversions."""
        # Remove variable names and data types to find common pattern
        lines = self.oracle_code.split("\n")
        signature = " ".join(
            line.strip() for line in lines if line.strip() and not line.strip().startswith("--")
        )
        return signature[:200]  # First 200 chars as signature


class ConversionCaseStore:
    """Store and retrieve conversion cases from database."""

    def __init__(self, db: Session):
        self.db = db
        self.embedder = EmbeddingGenerator()

    def store_case(
        self,
        construct_type: str,
        oracle_code: str,
        postgres_code: str,
        success: bool = True,
    ) -> str:
        """
        Store a conversion case with embedding.

        Args:
            construct_type: Type of construct (PROCEDURE, FUNCTION, etc.)
            oracle_code: Original Oracle code
            postgres_code: Converted PostgreSQL code
            success: Whether this conversion was successful

        Returns:
            Case ID
        """
        embedding = self.embedder.generate(oracle_code)

        # Import here to avoid circular dependency
        from ..models import ConversionCaseRecord

        case = ConversionCaseRecord(
            construct_type=construct_type,
            oracle_code=oracle_code,
            postgres_code=postgres_code,
            embedding=embedding,
            success_count=1 if success else 0,
            fail_count=0 if success else 1,
        )

        self.db.add(case)
        self.db.commit()
        self.db.refresh(case)
        return str(case.id)

    def find_similar_cases(
        self,
        oracle_code: str,
        construct_type: str,
        top_k: int = 3,
        similarity_threshold: float = 0.6,
    ) -> List[tuple]:
        """
        Find similar conversion cases using embedding similarity.

        Args:
            oracle_code: Source code to find matches for
            construct_type: Filter by construct type
            top_k: Number of similar cases to return
            similarity_threshold: Minimum similarity score (0-1)

        Returns:
            List of (case, similarity_score) tuples
        """
        # Generate embedding for input code
        query_embedding = self.embedder.generate(oracle_code)

        # Query similar cases using pgvector cosine distance
        # Note: This requires pgvector extension in PostgreSQL
        # For now, return empty list until DB schema is migrated
        return []

    def update_case_feedback(self, case_id: str, success: bool):
        """
        Update feedback for a conversion case (used to track pattern effectiveness).

        Args:
            case_id: ID of the case
            success: Whether the conversion was successful in testing
        """
        from ..models import ConversionCaseRecord

        case = (
            self.db.query(ConversionCaseRecord).filter(ConversionCaseRecord.id == case_id).first()
        )

        if case:
            if success:
                case.success_count += 1
            else:
                case.fail_count += 1
            case.updated_at = utc_now()
            self.db.commit()

    def get_pattern_stats(self, construct_type: str) -> dict:
        """Get statistics on conversion patterns for a construct type."""
        from ..models import ConversionCaseRecord

        cases = (
            self.db.query(ConversionCaseRecord)
            .filter(ConversionCaseRecord.construct_type == construct_type)
            .all()
        )

        if not cases:
            return {
                "total_cases": 0,
                "average_success_rate": 0.0,
                "top_patterns": [],
            }

        success_rates = [
            (c.success_count / (c.success_count + c.fail_count))
            for c in cases
            if (c.success_count + c.fail_count) > 0
        ]

        return {
            "total_cases": len(cases),
            "average_success_rate": (
                sum(success_rates) / len(success_rates) if success_rates else 0.0
            ),
            "top_patterns": sorted(
                [(c.pattern_signature, c.success_rate) for c in cases],
                key=lambda x: x[1],
                reverse=True,
            )[:10],
        }
