import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json
import re
import math
from rank_bm25 import BM25Okapi

# ─────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────

CHUNKS_FILE = "data/processed/chunks.json"
TOP_K       = 20

# ─────────────────────────────────────────────
# TOKENIZER
# ─────────────────────────────────────────────

# Words to ignore — so common they carry no meaning
# We keep domain-specific terms like "rbi", "bank", "circular"
# because in our corpus they DO carry meaning
STOPWORDS = {
    "a", "an", "the", "and", "or", "but", "in", "on", "at",
    "to", "for", "of", "with", "by", "from", "is", "are",
    "was", "were", "be", "been", "being", "have", "has", "had",
    "do", "does", "did", "will", "would", "could", "should",
    "may", "might", "shall", "that", "this", "these", "those",
    "it", "its", "we", "our", "they", "their", "as", "if",
    "not", "no", "so", "than", "then", "also", "into", "through",
}


def tokenize(text):
    """
    Convert text into a list of meaningful tokens.

    Steps:
    1. Lowercase everything
    2. Split on non-alphanumeric characters
    3. Remove stopwords
    4. Remove tokens shorter than 2 characters
    5. Keep numbers — "2023", "50000" are meaningful in RBI circulars

    Why this tokenizer over a library like NLTK:
    - No extra installation needed
    - We control exactly what gets kept
    - RBI-specific terms like "RBI/2023-24/27" need special handling
    """
    # Lowercase
    text = text.lower()

    # Split on whitespace and punctuation
    # But preserve circular number patterns like "2023-24"
    tokens = re.findall(r'\b[a-z0-9][a-z0-9\-]*[a-z0-9]\b|\b[a-z0-9]\b', text)

    # Filter stopwords and very short tokens
    tokens = [
        t for t in tokens
        if t not in STOPWORDS and len(t) >= 2
    ]

    return tokens


# ─────────────────────────────────────────────
# SPARSE RETRIEVER CLASS
# ─────────────────────────────────────────────

class SparseRetriever:
    """
    Retrieves chunks using BM25 keyword search.

    How it works:
    1. At init: tokenize all chunks and build BM25 index
    2. At query: tokenize query, score all chunks, return top K

    Why BM25Okapi specifically:
    BM25 has several variants. Okapi BM25 is the most widely used
    and best performing variant for document retrieval tasks.
    It's what Elasticsearch uses internally.
    """

    def __init__(self):
        print("📚 Building BM25 index...")

        # Load all chunks
        with open(CHUNKS_FILE, "r", encoding="utf-8") as f:
            self.chunks = json.load(f)

        # Tokenize all child texts
        # This is what BM25 searches over
        self.tokenized_corpus = [
            tokenize(chunk["child_text"])
            for chunk in self.chunks
        ]

        # Build BM25 index
        # BM25Okapi parameters:
        # k1=1.5 — term frequency saturation (default 1.5)
        #           higher = more weight to repeated terms
        # b=0.75  — length normalization (default 0.75)
        #           higher = more penalty for long documents
        self.bm25 = BM25Okapi(
            self.tokenized_corpus,
            k1=1.5,
            b=0.75
        )

        print(f"  ✅ BM25 index built over {len(self.chunks)} chunks")
        print(f"  📊 Vocabulary size: {len(self.bm25.idf)} unique terms")

    def retrieve(self, query, top_k=TOP_K, filters=None):
        """
        Retrieve top_k chunks for a query using BM25.

        Args:
            query:   user's question string
            top_k:   number of results to return
            filters: optional dict — filters results AFTER scoring
                     e.g. {"year": "2023"}
                     Note: BM25 doesn't support pre-filtering like Qdrant
                     so we filter results after scoring

        Returns:
            list of dicts with chunk data + BM25 score
        """
        # Tokenize query
        query_tokens = tokenize(query)

        if not query_tokens:
            print("  ⚠️  Query produced no tokens after filtering")
            return []

        # Get BM25 scores for all chunks
        scores = self.bm25.get_scores(query_tokens)

        # Get indices sorted by score (highest first)
        # We get more than top_k to account for post-filtering
        fetch_k = top_k * 3 if filters else top_k
        top_indices = sorted(
            range(len(scores)),
            key=lambda i: scores[i],
            reverse=True
        )[:fetch_k]

        # Build results
        results = []
        for idx in top_indices:
            chunk = self.chunks[idx]
            score = float(scores[idx])

            # Skip chunks with zero score — no keyword overlap at all
            if score == 0:
                continue

            # Apply filters if provided
            if filters:
                match = all(
                    chunk.get(key) == value
                    for key, value in filters.items()
                )
                if not match:
                    continue

            results.append({
                "score":            score,
                "retrieval_method": "sparse",

                # Chunk content
                "child_text":      chunk["child_text"],
                "parent_text":     chunk["parent_text"],

                # Citation metadata
                "chunk_id":        chunk["chunk_id"],
                "circular_number": chunk["circular_number"],
                "title":           chunk.get("title", ""),
                "date":            chunk.get("date", ""),
                "year":            chunk.get("year", ""),
                "department":      chunk.get("department", ""),
                "detail_url":      chunk.get("detail_url", ""),
            })

            if len(results) >= top_k:
                break

        return results

    def get_query_tokens(self, query):
        """Expose tokenized query — useful for hybrid search in Step 11."""
        return tokenize(query)


# ─────────────────────────────────────────────
# TEST
# ─────────────────────────────────────────────

def test_sparse_retrieval():
    retriever = SparseRetriever()

    test_queries = [
        "What are KYC requirements for small businesses?",
        "RBI guidelines on digital payments and UPI",
        "Non performing assets classification norms for banks",
        "UAPA circular implementation section 51A",
    ]

    for query in test_queries:
        print(f"\n{'='*60}")
        print(f"Query: {query}")
        print(f"{'='*60}")

        results = retriever.retrieve(query, top_k=3)

        if not results:
            print("  No results found")
            continue

        for i, r in enumerate(results, 1):
            print(f"\n  Result {i}:")
            print(f"  BM25 Score: {r['score']:.4f}")
            print(f"  Circular:   {r['circular_number']}")
            print(f"  Date:       {r['date']}")
            print(f"  Text:       {r['child_text'][:150]}...")

    # Test exact term matching — BM25's strength
    print(f"\n{'='*60}")
    print(f"Exact term test: 'UAPA 1967 section 51A'")
    print(f"(Dense retrieval would struggle here — BM25 should find it)")
    print(f"{'='*60}")
    results = retriever.retrieve("UAPA 1967 section 51A", top_k=3)
    for i, r in enumerate(results, 1):
        print(f"\n  Result {i}:")
        print(f"  BM25 Score: {r['score']:.4f}")
        print(f"  Circular:   {r['circular_number']}")
        print(f"  Text:       {r['child_text'][:200]}...")


if __name__ == "__main__":
    test_sparse_retrieval()