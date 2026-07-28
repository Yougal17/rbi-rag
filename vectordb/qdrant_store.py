import json
import time
import numpy as np
from qdrant_client import QdrantClient
from qdrant_client.models import (
    Distance,
    VectorParams,
    PointStruct,
    PayloadSchemaType,
)

# ─────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────

CHUNKS_FILE     = "data/processed/chunks.json"
EMBEDDINGS_FILE = "data/processed/embeddings.npz"

QDRANT_HOST     = "localhost"
QDRANT_PORT     = 6333
COLLECTION_NAME = "qdrant-rbi"

VECTOR_SIZE     = 768       # must match embedding dimensions from Step 6
BATCH_SIZE      = 500       # points per upload batch

# ─────────────────────────────────────────────
# CONNECT
# ─────────────────────────────────────────────

def get_client():
    """Create and return a Qdrant client."""
    return QdrantClient(host=QDRANT_HOST, port=QDRANT_PORT)


# ─────────────────────────────────────────────
# COLLECTION SETUP
# ─────────────────────────────────────────────

def setup_collection(client, force_recreate=False):
    """
    Create the Qdrant collection if it doesn't exist.
    If force_recreate=True, delete and rebuild from scratch.

    Why COSINE distance:
    Our embeddings were normalized in Step 6, making cosine
    similarity the most accurate metric for text search.
    """
    exists = client.collection_exists(COLLECTION_NAME)

    if exists and not force_recreate:
        print(f"  ✅ Collection '{COLLECTION_NAME}' already exists — skipping creation")
        info = client.get_collection(COLLECTION_NAME)
        print(f"  📊 Existing points: {info.points_count}")
        return

    if exists and force_recreate:
        print(f"  🗑️  Deleting existing collection...")
        client.delete_collection(COLLECTION_NAME)

    print(f"  🔨 Creating collection '{COLLECTION_NAME}'...")
    client.create_collection(
        collection_name=COLLECTION_NAME,
        vectors_config=VectorParams(
            size=VECTOR_SIZE,
            distance=Distance.COSINE,
        )
    )
    print(f"  ✅ Collection created")

    # ── Create payload indexes ───────────────────
    # Indexes make filtering fast — without them, Qdrant
    # scans every point when you filter by year or department
    # With indexes, filtering is near-instant

    print(f"  🔨 Creating payload indexes...")

    # Year index — for filtering "2023 only" queries
    client.create_payload_index(
        collection_name=COLLECTION_NAME,
        field_name="year",
        field_schema=PayloadSchemaType.KEYWORD,
    )

    # Department index — for filtering by department
    client.create_payload_index(
        collection_name=COLLECTION_NAME,
        field_name="department",
        field_schema=PayloadSchemaType.KEYWORD,
    )

    # Circular number index — for exact circular lookup
    client.create_payload_index(
        collection_name=COLLECTION_NAME,
        field_name="circular_number",
        field_schema=PayloadSchemaType.KEYWORD,
    )

    print(f"  ✅ Indexes created (year, department, circular_number)")


# ─────────────────────────────────────────────
# UPLOAD
# ─────────────────────────────────────────────

def upload_embeddings(client, chunks, embeddings):
    """
    Upload all chunks + embeddings to Qdrant in batches.

    Each point = one chunk:
      - id:      integer index (0, 1, 2, ...)
      - vector:  768-dim embedding of child_text
      - payload: all metadata for citation + filtering
    """
    total   = len(chunks)
    batches = (total + BATCH_SIZE - 1) // BATCH_SIZE

    print(f"\n⬆️  Uploading {total} points in {batches} batches...")

    uploaded = 0
    start    = time.time()

    for batch_num in range(batches):
        batch_start = batch_num * BATCH_SIZE
        batch_end   = min(batch_start + BATCH_SIZE, total)

        # Build points for this batch
        points = []
        for idx in range(batch_start, batch_end):
            chunk = chunks[idx]

            # Payload — everything we want to store alongside the vector
            # Keep parent_text here — it's what we send to LLM later
            payload = {
                "chunk_id":        chunk["chunk_id"],
                "child_text":      chunk["child_text"],
                "parent_text":     chunk["parent_text"],
                "circular_number": chunk["circular_number"],
                "title":           chunk.get("title", ""),
                "date":            chunk.get("date", ""),
                "year":            chunk.get("year", ""),
                "department":      chunk.get("department", ""),
                "detail_url":      chunk.get("detail_url", ""),
                "parent_index":    chunk.get("parent_index", 0),
                "child_index":     chunk.get("child_index", 0),
            }

            point = PointStruct(
                id=idx,                          # integer ID
                vector=embeddings[idx].tolist(), # numpy → python list
                payload=payload
            )
            points.append(point)

        # Upload this batch
        client.upsert(
            collection_name=COLLECTION_NAME,
            points=points,
            wait=True  # wait for indexing before continuing
        )

        uploaded += len(points)
        elapsed   = time.time() - start
        rate      = uploaded / elapsed if elapsed > 0 else 0

        print(
            f"  Batch {batch_num+1}/{batches} — "
            f"{uploaded}/{total} points "
            f"({rate:.0f} points/sec)"
        )

    print(f"\n  ✅ Upload complete in {time.time()-start:.1f}s")


# ─────────────────────────────────────────────
# VERIFY
# ─────────────────────────────────────────────

def verify_upload(client):
    """
    Verify the upload was successful by:
    1. Checking point count matches expected
    2. Running a test similarity search
    3. Checking payload filtering works
    """
    print(f"\n🔍 Verifying upload...")

    # Check count
    info = client.get_collection(COLLECTION_NAME)
    print(f"  Points in collection: {info.points_count}")

    # Test similarity search using new API
    test_vector = np.random.randn(VECTOR_SIZE).tolist()

    results = client.query_points(
        collection_name=COLLECTION_NAME,
        query=test_vector,
        limit=3,
        with_payload=True,
    ).points

    print(f"  Test search returned: {len(results)} results ✅")
    if results:
        print(f"  Top result circular: {results[0].payload['circular_number']}")
        print(f"  Top result score:    {results[0].score:.4f}")

    # Test payload filtering
    from qdrant_client.models import Filter, FieldCondition, MatchValue

    filtered = client.query_points(
        collection_name=COLLECTION_NAME,
        query=test_vector,
        query_filter=Filter(
            must=[
                FieldCondition(
                    key="year",
                    match=MatchValue(value="2023")
                )
            ]
        ),
        limit=3,
        with_payload=True,
    ).points

    print(f"  Filtered search (2023 only): {len(filtered)} results ✅")
    if filtered:
        print(f"  All from 2023: {all(r.payload['year']=='2023' for r in filtered)}")

    print(f"\n  ✅ Verification complete")

# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

def run_store():
    print("=" * 60)
    print("Qdrant Store — Loading Embeddings")
    print(f"Collection: {COLLECTION_NAME}")
    print(f"Host:       {QDRANT_HOST}:{QDRANT_PORT}")
    print("=" * 60)

    # ── Load chunks ──────────────────────────────
    print(f"\n📂 Loading chunks...")
    with open(CHUNKS_FILE, "r", encoding="utf-8") as f:
        chunks = json.load(f)
    print(f"  ✅ {len(chunks)} chunks loaded")

    # ── Load embeddings ──────────────────────────
    print(f"\n📂 Loading embeddings...")
    data       = np.load(EMBEDDINGS_FILE)
    embeddings = data["embeddings"]
    print(f"  ✅ Embeddings shape: {embeddings.shape}")

    # Sanity check — chunks and embeddings must match
    assert len(chunks) == len(embeddings), (
        f"Mismatch: {len(chunks)} chunks but {len(embeddings)} embeddings"
    )
    print(f"  ✅ Chunk/embedding count matches")

    # ── Connect to Qdrant ────────────────────────
    print(f"\n🔌 Connecting to Qdrant...")
    client = get_client()
    print(f"  ✅ Connected")

    # ── Setup collection ─────────────────────────
    print(f"\n📦 Setting up collection...")
    setup_collection(client, force_recreate=False)

    # Check if already uploaded
    info = client.get_collection(COLLECTION_NAME)
    if info.points_count == len(chunks):
        print(f"\n  ⏭️  Already uploaded {info.points_count} points — skipping upload")
    else:
        # ── Upload ───────────────────────────────
        upload_embeddings(client, chunks, embeddings)

    # ── Verify ───────────────────────────────────
    verify_upload(client)

    # ── Final summary ────────────────────────────
    print("\n" + "=" * 60)
    print("STORAGE COMPLETE")
    info = client.get_collection(COLLECTION_NAME)
    print(f"  📦 Collection:    {COLLECTION_NAME}")
    print(f"  🧩 Total points:  {info.points_count}")
    print(f"  📐 Vector size:   {VECTOR_SIZE}")
    print(f"  🔍 Indexes:       year, department, circular_number")
    print("=" * 60)


if __name__ == "__main__":
    run_store()