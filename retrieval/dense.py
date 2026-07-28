import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import numpy as np
from sentence_transformers import SentenceTransformer
from qdrant_client import QdrantClient
from qdrant_client.models import Filter, FieldCondition, MatchValue

# ─────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────

COLLECTION_NAME = "qdrant-rbi"
MODEL_NAME      = "sentence-transformers/all-mpnet-base-v2"
QDRANT_HOST     = "localhost"
QDRANT_PORT     = 6333

# How many results to retrieve
# We retrieve more than we need — reranker in Step 12 will trim
TOP_K = 20

# ─────────────────────────────────────────────
# DENSE RETRIEVER CLASS
# ─────────────────────────────────────────────

class DenseRetriever:
    """
    Retrieves chunks using vector similarity search.

    How it works:
    1. Embed the query using the same model used for chunks
    2. Search Qdrant for the most similar vectors
    3. Return chunks with their similarity scores
    """

    def __init__(self):
        print("🤖 Loading embedding model for retrieval...")
        self.model  = SentenceTransformer(MODEL_NAME)
        self.client = QdrantClient(host=QDRANT_HOST, port=QDRANT_PORT)
        print("✅ Dense retriever ready")

    def embed_query(self, query):
        """
        Embed a query string into a vector.

        Why normalize: our chunk embeddings were normalized in Step 6.
        The query embedding must also be normalized so cosine similarity
        scores are comparable (range 0-1 instead of arbitrary values).
        """
        embedding = self.model.encode(
            query,
            normalize_embeddings=True,
            convert_to_numpy=True,
        )
        return embedding

    def retrieve(self, query, top_k=TOP_K, filters=None):
        """
        Retrieve top_k most similar chunks for a query.

        Args:
            query:   user's question string
            top_k:   number of results to return
            filters: optional dict for metadata filtering
                     e.g. {"year": "2023"} or {"department": "Department of Regulation"}

        Returns:
            list of dicts with chunk data + similarity score
        """
        # Embed the query
        query_vector = self.embed_query(query)

        # Build optional filter
        qdrant_filter = None
        if filters:
            conditions = []
            for key, value in filters.items():
                conditions.append(
                    FieldCondition(
                        key=key,
                        match=MatchValue(value=value)
                    )
                )
            qdrant_filter = Filter(must=conditions)

        # Search Qdrant
        results = self.client.query_points(
            collection_name=COLLECTION_NAME,
            query=query_vector.tolist(),
            query_filter=qdrant_filter,
            limit=top_k,
            with_payload=True,
        ).points

        # Format results
        retrieved = []
        for result in results:
            retrieved.append({
                # Score — higher is more similar (0 to 1 for cosine)
                "score":           result.score,
                "retrieval_method": "dense",

                # Chunk content
                "child_text":      result.payload["child_text"],
                "parent_text":     result.payload["parent_text"],

                # Citation metadata
                "chunk_id":        result.payload["chunk_id"],
                "circular_number": result.payload["circular_number"],
                "title":           result.payload["title"],
                "date":            result.payload["date"],
                "year":            result.payload["year"],
                "department":      result.payload["department"],
                "detail_url":      result.payload["detail_url"],
            })

        return retrieved


# ─────────────────────────────────────────────
# TEST
# ─────────────────────────────────────────────

def test_dense_retrieval():
    retriever = DenseRetriever()

    test_queries = [
        "What are KYC requirements for small businesses?",
        "RBI guidelines on digital payments and UPI",
        "Non performing assets classification norms for banks",
    ]

    for query in test_queries:
        print(f"\n{'='*60}")
        print(f"Query: {query}")
        print(f"{'='*60}")

        results = retriever.retrieve(query, top_k=3)

        for i, r in enumerate(results, 1):
            print(f"\n  Result {i}:")
            print(f"  Score:    {r['score']:.4f}")
            print(f"  Circular: {r['circular_number']}")
            print(f"  Date:     {r['date']}")
            print(f"  Text:     {r['child_text'][:150]}...")

    # Test with year filter
    print(f"\n{'='*60}")
    print(f"Query with year filter (2023 only):")
    print(f"{'='*60}")
    results = retriever.retrieve(
        "bank regulations for digital lending",
        top_k=3,
        filters={"year": "2023"}
    )
    for i, r in enumerate(results, 1):
        print(f"\n  Result {i} (year={r['year']}):")
        print(f"  Score:    {r['score']:.4f}")
        print(f"  Circular: {r['circular_number']}")
        print(f"  Text:     {r['child_text'][:150]}...")


if __name__ == "__main__":
    test_dense_retrieval()