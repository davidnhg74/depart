"""
Similarity search engine using vector embeddings.
Finds similar conversion cases to provide context to Claude.
"""

import numpy as np
from typing import List, Tuple
from .embeddings import EmbeddingGenerator


class SimilaritySearchEngine:
    """Search for similar code patterns using vector similarity."""

    def __init__(self):
        self.embedder = EmbeddingGenerator()

    def cosine_similarity(self, vec1: List[float], vec2: List[float]) -> float:
        """Calculate cosine similarity between two vectors."""
        a = np.array(vec1)
        b = np.array(vec2)

        dot_product = np.dot(a, b)
        norm_a = np.linalg.norm(a)
        norm_b = np.linalg.norm(b)

        if norm_a == 0 or norm_b == 0:
            return 0.0

        return float(dot_product / (norm_a * norm_b))

    def find_similar(
        self,
        query_embedding: List[float],
        case_embeddings: List[Tuple[str, List[float]]],
        top_k: int = 3,
        min_similarity: float = 0.6,
    ) -> List[Tuple[str, float]]:
        """
        Find top-K most similar cases.

        Args:
            query_embedding: Vector embedding of query code
            case_embeddings: List of (case_id, embedding) tuples
            top_k: Number of results to return
            min_similarity: Minimum similarity threshold

        Returns:
            List of (case_id, similarity_score) sorted by similarity
        """
        similarities = []

        for case_id, embedding in case_embeddings:
            similarity = self.cosine_similarity(query_embedding, embedding)
            if similarity >= min_similarity:
                similarities.append((case_id, similarity))

        # Sort by similarity descending
        similarities.sort(key=lambda x: x[1], reverse=True)
        return similarities[:top_k]

    def rank_conversions(
        self,
        oracle_code: str,
        conversions: List[Tuple[str, str, float]],  # (code, id, baseline_score)
        similarity_weight: float = 0.3,
    ) -> List[Tuple[str, float]]:
        """
        Rank conversions by combining similarity score with baseline confidence.

        Args:
            oracle_code: Query code
            conversions: List of (postgres_code, case_id, baseline_confidence)
            similarity_weight: Weight for similarity in final score (0-1)

        Returns:
            List of (case_id, final_score) sorted by score
        """
        query_embedding = self.embedder.generate(oracle_code)
        ranked = []

        for postgres_code, case_id, baseline_score in conversions:
            conversion_embedding = self.embedder.generate(postgres_code)
            similarity = self.cosine_similarity(query_embedding, conversion_embedding)

            # Combine baseline confidence with similarity
            final_score = (
                baseline_score * (1 - similarity_weight) + similarity * similarity_weight
            )

            ranked.append((case_id, final_score))

        ranked.sort(key=lambda x: x[1], reverse=True)
        return ranked
