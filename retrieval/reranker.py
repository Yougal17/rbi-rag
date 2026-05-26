import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sentence_transformers import CrossEncoder

# ─────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────

MODEL_NAME = "cross-encoder/ms-marco-MiniLM-L-6-v2"

# How many results to return after re-ranking
# We started with 15 from hybrid — re-ranker trims to 7
RERANK_TOP_K = 7

# Minimum score threshold
# Cross-encoder scores are logits — not strictly 0 to 1
# Scores below -5 are essentially "not relevant at all"
MIN_SCORE_THRESHOLD = -5.0

# ─────────────────────────────────────────────
# RERANKER CLASS
# ─────────────────────────────────────────────

class CrossEncoderReranker:
    """
    Re-ranks retrieval results using a cross-encoder model.

    Why cross-encoder over bi-encoder for re-ranking:
    - Bi-encoder: encodes query and chunk separately → fast but imprecise
    - Cross-encoder: encodes query+chunk together → slower but much more precise
    - Cross-encoder sees full interaction between query and chunk words
    - Used as second stage because it's too slow to run on all 4,368 chunks

    The trade-off:
    - Running cross-encoder on all 4,368 chunks: ~5-10 minutes per query
    - Running cross-encoder on top 15 chunks: ~1-2 seconds per query ✅
    """

    def __init__(self):
        print("🎯 Loading cross-encoder re-ranker...")
        # First run downloads ~66MB model
        self.model = CrossEncoder(MODEL_NAME)
        print(f"  ✅ Re-ranker ready: {MODEL_NAME}")

    def rerank(self, query, candidates, top_k=RERANK_TOP_K):
        """
        Re-rank candidate chunks using cross-encoder.

        Args:
            query:      user's question string
            candidates: list of chunk dicts from hybrid retrieval
            top_k:      number of results to return after re-ranking

        Returns:
            list of top_k chunk dicts sorted by cross-encoder score
        """
        if not candidates:
            return []

        # Build query-chunk pairs for cross-encoder
        # Cross-encoder takes (query, passage) pairs
        # We use child_text — short and precise
        pairs = [
            (query, candidate["child_text"])
            for candidate in candidates
        ]

        # Score all pairs
        # Output: array of relevance scores (logits)
        # Higher score = more relevant
        scores = self.model.predict(pairs)

        # Attach scores to candidates
        scored = []
        for candidate, score in zip(candidates, scores):
            if float(score) >= MIN_SCORE_THRESHOLD:
                result = candidate.copy()
                result["rerank_score"] = float(score)
                scored.append(result)

        # Sort by re-rank score descending
        scored.sort(key=lambda x: x["rerank_score"], reverse=True)

        # Return top_k
        return scored[:top_k]


# ─────────────────────────────────────────────
# FULL PIPELINE — Hybrid + Rerank
# ─────────────────────────────────────────────

class RetrievalPipeline:
    """
    Complete retrieval pipeline:
    Hybrid search (Dense + BM25 + RRF) → Cross-encoder re-ranking

    This is the class we'll use in the FastAPI backend (Step 18).
    All other retrieval classes feed into this one.
    """

    def __init__(self):
        from retrieval.hybrid import HybridRetriever
        self.hybrid   = HybridRetriever()
        self.reranker = CrossEncoderReranker()
        print("\n✅ Full retrieval pipeline ready")
        print("   Dense → BM25 → RRF → Cross-Encoder")

    def retrieve(self, query, top_k=5, filters=None):
        """
        Full pipeline: hybrid retrieval → re-ranking.

        Args:
            query:   user's question string
            top_k:   final number of results after re-ranking
            filters: optional metadata filters e.g. {"year": "2023"}

        Returns:
            list of top_k most relevant chunks with all scores
        """
        print(f"\n📥 Query: {query}")
        if filters:
            print(f"   Filters: {filters}")

        # Stage 1: Hybrid retrieval
        print(f"\n[Stage 1] Hybrid retrieval...")
        candidates = self.hybrid.retrieve(
            query,
            top_k=15,
            filters=filters
        )
        print(f"  → {len(candidates)} candidates")

        # Stage 2: Re-ranking
        print(f"\n[Stage 2] Cross-encoder re-ranking...")
        results = self.reranker.rerank(query, candidates, top_k=top_k)
        print(f"  → {len(results)} final results")

        return results


# ─────────────────────────────────────────────
# TEST
# ─────────────────────────────────────────────

def test_reranker():

    pipeline = RetrievalPipeline()

    test_queries = [
        "What are the KYC requirements for opening a bank account?",
        "RBI guidelines on non performing assets provisioning",
        "Digital lending regulations and borrower protection",
    ]

    for query in test_queries:
        print(f"\n{'='*60}")
        print(f"QUERY: {query}")
        print(f"{'='*60}")

        results = pipeline.retrieve(query, top_k=5)

        print(f"\n  Final Results after Re-ranking:")
        for i, r in enumerate(results, 1):
            print(f"\n  [{i}] Circular: {r['circular_number']}")
            print(f"      Date:     {r['date']}")
            print(f"      RRF:      {r.get('rrf_score', 0):.5f}")
            print(f"      Rerank:   {r['rerank_score']:.4f}")
            print(f"      Text:     {r['child_text'][:200]}...")

    # ── Show re-ranking effect ───────────────────
    # Compare hybrid order vs reranked order for same query
    print(f"\n{'='*60}")
    print("RE-RANKING EFFECT — Before vs After")
    print(f"{'='*60}")

    from retrieval.hybrid import HybridRetriever
    hybrid    = HybridRetriever()
    reranker  = CrossEncoderReranker()

    query     = "penalty for banks violating RBI directions"
    candidates = hybrid.retrieve(query, top_k=10)

    print(f"\nQuery: {query}")
    print(f"\nBefore re-ranking (RRF order):")
    for i, r in enumerate(candidates[:5], 1):
        print(f"  [{i}] {r['circular_number']} — {r['child_text'][:80]}...")

    reranked = reranker.rerank(query, candidates, top_k=5)
    print(f"\nAfter re-ranking (cross-encoder order):")
    for i, r in enumerate(reranked, 1):
        print(f"  [{i}] {r['circular_number']} "
              f"[score={r['rerank_score']:.3f}] "
              f"— {r['child_text'][:80]}...")


if __name__ == "__main__":
    test_reranker()