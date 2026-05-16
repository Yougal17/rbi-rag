import os
import json
import time
import numpy as np
from pathlib import Path
from sentence_transformers import SentenceTransformer

# ─────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────

CHUNKS_FILE     = "data/processed/chunks.json"
EMBEDDINGS_FILE = "data/processed/embeddings.npz"
METADATA_FILE   = "data/processed/embedding_metadata.json"

# Model choice — see Step 6 notes for why this one
MODEL_NAME = "sentence-transformers/all-mpnet-base-v2"

# How many chunks to embed at once
# Larger = faster but uses more RAM
# 64 is safe for most laptops
BATCH_SIZE = 64

# Which field to embed — we embed child_text for precise retrieval
EMBED_FIELD = "child_text"

# ─────────────────────────────────────────────
# MAIN EMBEDDER
# ─────────────────────────────────────────────

def run_embedder():
    print("=" * 60)
    print("RBI Circular Embedder")
    print(f"Model:      {MODEL_NAME}")
    print(f"Batch size: {BATCH_SIZE}")
    print(f"Field:      {EMBED_FIELD}")
    print("=" * 60)

    # ── Load chunks ──────────────────────────────
    print(f"\n📂 Loading chunks from {CHUNKS_FILE}...")
    with open(CHUNKS_FILE, "r", encoding="utf-8") as f:
        chunks = json.load(f)
    print(f"  ✅ Loaded {len(chunks)} chunks")

    # ── Load model ───────────────────────────────
    # First run downloads ~420MB model to ~/.cache/huggingface/
    # Subsequent runs load from cache instantly
    print(f"\n🤖 Loading embedding model...")
    print(f"  (First run downloads ~420MB — subsequent runs are instant)")
    model = SentenceTransformer(MODEL_NAME)
    print(f"  ✅ Model loaded")
    print(f"  Embedding dimensions: {model.get_sentence_embedding_dimension()}")

    # ── Extract texts to embed ───────────────────
    texts = [chunk[EMBED_FIELD] for chunk in chunks]
    chunk_ids = [chunk["chunk_id"] for chunk in chunks]

    print(f"\n⚡ Generating embeddings for {len(texts)} chunks...")
    print(f"  Batch size: {BATCH_SIZE}")
    print(f"  Estimated time: {len(texts) // BATCH_SIZE * 2} - {len(texts) // BATCH_SIZE * 4} seconds")

    start_time = time.time()

    # ── Generate embeddings in batches ───────────
    # show_progress_bar=True prints a progress bar
    # normalize_embeddings=True → vectors have length 1
    # This makes cosine similarity = dot product (faster search)
    embeddings = model.encode(
        texts,
        batch_size=BATCH_SIZE,
        show_progress_bar=True,
        normalize_embeddings=True,
        convert_to_numpy=True,
    )

    elapsed = time.time() - start_time
    print(f"\n  ✅ Done in {elapsed:.1f} seconds")
    print(f"  Embeddings shape: {embeddings.shape}")
    # Shape should be (4368, 768) — 4368 chunks, 768 dimensions each

    # ── Save embeddings ──────────────────────────
    # We use .npz format — compressed numpy array
    # Much smaller and faster to load than JSON
    # A 4368x768 float32 array in JSON would be ~100MB
    # In .npz it's ~12MB
    print(f"\n💾 Saving embeddings to {EMBEDDINGS_FILE}...")
    np.savez_compressed(
        EMBEDDINGS_FILE,
        embeddings=embeddings,
        chunk_ids=np.array(chunk_ids)
    )
    print(f"  ✅ Saved")

    # ── Save embedding metadata ──────────────────
    # Lightweight JSON that maps chunk_id → index position
    # Used in Step 8 to match embeddings back to chunks
    print(f"\n💾 Saving embedding metadata...")
    embedding_meta = {
        "model": MODEL_NAME,
        "dimensions": int(model.get_sentence_embedding_dimension()),
        "total_chunks": len(chunks),
        "embed_field": EMBED_FIELD,
        "chunk_id_to_index": {
            chunk_id: idx
            for idx, chunk_id in enumerate(chunk_ids)
        }
    }
    with open(METADATA_FILE, "w", encoding="utf-8") as f:
        json.dump(embedding_meta, f, indent=2)
    print(f"  ✅ Saved")

    # ── Quality check ────────────────────────────
    print(f"\n📋 Quick quality check...")

    # Test semantic similarity between two chunks
    # If embeddings are working, similar content should score high
    test_pairs = [
        (0, 1),   # adjacent chunks — likely similar
        (0, len(chunks)//2),   # far apart — likely different
    ]

    for idx_a, idx_b in test_pairs:
        vec_a = embeddings[idx_a]
        vec_b = embeddings[idx_b]
        # Cosine similarity — since normalized, this is just dot product
        similarity = float(np.dot(vec_a, vec_b))
        print(f"  Chunk {idx_a} vs {idx_b}: similarity = {similarity:.4f}")
        print(f"    A: {texts[idx_a][:60]}...")
        print(f"    B: {texts[idx_b][:60]}...")
        print()

    # ── Final summary ────────────────────────────
    print("=" * 60)
    print("EMBEDDING COMPLETE")
    print(f"  🧩 Chunks embedded:  {len(chunks)}")
    print(f"  📐 Dimensions:       {embeddings.shape[1]}")
    print(f"  ⏱️  Time taken:       {elapsed:.1f}s")
    print(f"  💾 Embeddings file:  {EMBEDDINGS_FILE}")
    print(f"  📊 File size:        {Path(EMBEDDINGS_FILE).stat().st_size / 1024 / 1024:.1f} MB")
    print("=" * 60)


if __name__ == "__main__":
    run_embedder()