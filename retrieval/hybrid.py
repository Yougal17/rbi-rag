import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from retrieval.dense import DenseRetriever
from retrieval.sparse import SparseRetriever

# ─────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────

# RRF constant — 60 is the standard value used in the original paper
# Higher k = less reward for top ranks, more uniform merging
# Lower k = more reward for being ranked #1
RRF_K = 60

# How many results each retriever fetches
# We fetch more than we need — RRF + reranker will trim
RETRIEVER_TOP_K = 20

# Final number of results after RRF merging
HYBRID_TOP_K = 15

# ─────────────────────────────────────────────
# RECIPROCAL RANK FUSION
# ─────────────────────────────────────────────

def reciprocal_rank_fusion(ranked_lists, k=RRF_K):
    """
    Merge multiple ranked lists into one using RRF.

    Args:
        ranked_lists: list of lists, each containing dicts with "chunk_id"
                      First list = dense results
                      Second list = BM25 results
        k:            RRF constant (default 60)

    Returns:
        merged list sorted by RRF score (highest first)
        each item includes its original data + rrf_score
    """
    # Store RRF scores and chunk data
    rrf_scores = {}   # chunk_id → cumulative RRF score
    chunk_data  = {}  # chunk_id → chunk dict (for retrieving full data)

    for ranked_list in ranked_lists:
        for rank, chunk in enumerate(ranked_list, start=1):
            chunk_id = chunk["chunk_id"]

            # RRF formula
            rrf_score = 1.0 / (k + rank)

            # Accumulate score across retrieval methods
            if chunk_id not in rrf_scores:
                rrf_scores[chunk_id] = 0.0
                chunk_data[chunk_id] = chunk

            rrf_scores[chunk_id] += rrf_score

    # Sort by RRF score descending
    sorted_ids = sorted(
        rrf_scores.keys(),
        key=lambda cid: rrf_scores[cid],
        reverse=True
    )

    # Build final merged list
    merged = []
    for chunk_id in sorted_ids:
        result = chunk_data[chunk_id].copy()
        result["rrf_score"]        = rrf_scores[chunk_id]
        result["retrieval_method"] = "hybrid"
        merged.append(result)

    return merged


# ─────────────────────────────────────────────
# HYBRID RETRIEVER CLASS
# ─────────────────────────────────────────────

class HybridRetriever:
    """
    Combines dense + sparse retrieval using Reciprocal Rank Fusion.

    Pipeline:
    1. Dense retriever → top 20 results (semantic similarity)
    2. BM25 retriever  → top 20 results (keyword matching)
    3. RRF merges both → top 15 results (best of both worlds)
    """

    def __init__(self):
        print("🔀 Initializing Hybrid Retriever...")
        self.dense  = DenseRetriever()
        self.sparse = SparseRetriever()
        print("✅ Hybrid retriever ready\n")

    def retrieve(self, query, top_k=HYBRID_TOP_K, filters=None):
        """
        Retrieve chunks using hybrid dense + BM25 search.

        Args:
            query:   user's question string
            top_k:   final number of results after RRF merging
            filters: optional dict for metadata filtering
                     applied to dense retrieval (Qdrant-side)
                     and post-hoc to BM25 results

        Returns:
            list of dicts sorted by RRF score
        """
        # ── Run both retrievers ──────────────────────
        print(f"  🔍 Dense retrieval...")
        dense_results = self.dense.retrieve(
            query,
            top_k=RETRIEVER_TOP_K,
            filters=filters
        )
        print(f"     → {len(dense_results)} results")

        print(f"  🔍 BM25 retrieval...")
        sparse_results = self.sparse.retrieve(
            query,
            top_k=RETRIEVER_TOP_K,
            filters=filters
        )
        print(f"     → {len(sparse_results)} results")

        # ── Apply RRF ────────────────────────────────
        merged = reciprocal_rank_fusion(
            [dense_results, sparse_results],
            k=RRF_K
        )

        # ── Trim to top_k ────────────────────────────
        final = merged[:top_k]

        print(f"  ✅ Hybrid merged → {len(final)} results")
        return final

    def retrieve_with_breakdown(self, query, top_k=HYBRID_TOP_K, filters=None):
        """
        Same as retrieve() but also returns individual retriever results.
        Useful for debugging and understanding what each retriever found.
        """
        dense_results  = self.dense.retrieve(
            query, top_k=RETRIEVER_TOP_K, filters=filters
        )
        sparse_results = self.sparse.retrieve(
            query, top_k=RETRIEVER_TOP_K, filters=filters
        )
        merged = reciprocal_rank_fusion(
            [dense_results, sparse_results],
            k=RRF_K
        )[:top_k]

        return {
            "hybrid":  merged,
            "dense":   dense_results[:5],
            "sparse":  sparse_results[:5],
        }


# ─────────────────────────────────────────────
# TEST
# ─────────────────────────────────────────────

def test_hybrid_retrieval():

    retriever = HybridRetriever()

    # ── Test 1: Semantic query ───────────────────
    print(f"\n{'='*60}")
    print("TEST 1 — Semantic query (dense should lead)")
    print(f"{'='*60}")
    query = "What happens when banks fail to maintain capital adequacy?"

    results = retriever.retrieve(query, top_k=5)
    for i, r in enumerate(results, 1):
        print(f"\n  Result {i}:")
        print(f"  RRF Score: {r['rrf_score']:.5f}")
        print(f"  Circular:  {r['circular_number']}")
        print(f"  Date:      {r['date']}")
        print(f"  Text:      {r['child_text'][:150]}...")

    # ── Test 2: Exact term query ─────────────────
    print(f"\n{'='*60}")
    print("TEST 2 — Exact term query (BM25 should lead)")
    print(f"{'='*60}")
    query = "UAPA 1967 section 51A sanctions list"

    results = retriever.retrieve(query, top_k=5)
    for i, r in enumerate(results, 1):
        print(f"\n  Result {i}:")
        print(f"  RRF Score: {r['rrf_score']:.5f}")
        print(f"  Circular:  {r['circular_number']}")
        print(f"  Text:      {r['child_text'][:150]}...")

    # ── Test 3: Breakdown comparison ────────────
    print(f"\n{'='*60}")
    print("TEST 3 — Breakdown: Dense vs BM25 vs Hybrid")
    print(f"{'='*60}")
    query = "digital lending guidelines default loss guarantee"

    breakdown = retriever.retrieve_with_breakdown(query, top_k=5)

    print(f"\n  Dense top 3:")
    for r in breakdown["dense"][:3]:
        print(f"    [{r['score']:.4f}] {r['circular_number']} — {r['child_text'][:80]}...")

    print(f"\n  BM25 top 3:")
    for r in breakdown["sparse"][:3]:
        print(f"    [{r['score']:.2f}] {r['circular_number']} — {r['child_text'][:80]}...")

    print(f"\n  Hybrid top 3 (RRF merged):")
    for r in breakdown["hybrid"][:3]:
        print(f"    [{r['rrf_score']:.5f}] {r['circular_number']} — {r['child_text'][:80]}...")

    # ── Test 4: With year filter ─────────────────
    print(f"\n{'='*60}")
    print("TEST 4 — With year filter (2023 only)")
    print(f"{'='*60}")
    query = "KYC norms for banks"
    results = retriever.retrieve(query, top_k=3, filters={"year": "2023"})
    for i, r in enumerate(results, 1):
        print(f"\n  Result {i} (year={r['year']}):")
        print(f"  RRF Score: {r['rrf_score']:.5f}")
        print(f"  Circular:  {r['circular_number']}")
        print(f"  Text:      {r['child_text'][:150]}...")


if __name__ == "__main__":
    test_hybrid_retrieval()