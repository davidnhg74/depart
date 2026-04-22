"""
Embedding generation for PL/SQL code snippets.
Uses sentence-transformers with a code-aware model for semantic similarity.
"""

from sentence_transformers import SentenceTransformer
from typing import List


class EmbeddingGenerator:
    """Generate vector embeddings for PL/SQL code."""

    def __init__(self):
        # Use code-aware model that understands programming constructs
        self.model = SentenceTransformer("all-MiniLM-L6-v2")

    def generate(self, code: str) -> List[float]:
        """
        Generate embedding for a PL/SQL code snippet.

        Args:
            code: PL/SQL source code

        Returns:
            Vector embedding (384-dimensional for MiniLM model)
        """
        if not code or not code.strip():
            return [0.0] * 384

        # Normalize code: remove extra whitespace, truncate to reasonable length
        normalized = self._normalize_code(code)

        # Generate embedding
        embedding = self.model.encode(normalized, convert_to_numpy=True)
        return embedding.tolist()

    def generate_batch(self, codes: List[str]) -> List[List[float]]:
        """Generate embeddings for multiple code snippets."""
        normalized = [self._normalize_code(c) for c in codes]
        embeddings = self.model.encode(normalized, convert_to_numpy=True)
        return embeddings.tolist()

    @staticmethod
    def _normalize_code(code: str) -> str:
        """Normalize code for embedding."""
        # Remove extra whitespace but preserve structure
        lines = [line.strip() for line in code.split("\n") if line.strip()]
        normalized = " ".join(lines)

        # Truncate to first 1000 chars to avoid huge embeddings
        return normalized[:1000]

    @staticmethod
    def embedding_dimension() -> int:
        """Return embedding vector dimension."""
        return 384
