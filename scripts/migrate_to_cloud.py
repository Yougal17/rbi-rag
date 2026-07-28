# Migrates local Qdrant data to Qdrant Cloud
import json
import numpy as np
from qdrant_client import QdrantClient
from qdrant_client.models import Distance, VectorParams, PointStruct

# ── Local client (source) ────────────────────
local_client = QdrantClient(host="localhost", port=6333)

# ── Cloud client (destination) ───────────────
# Replace with your actual values from Qdrant Cloud dashboard
CLOUD_URL     = "https://bd1f261b-457f-4921-8f85-ef16219ac101.eu-central-1-0.aws.cloud.qdrant.io"
CLOUD_API_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhY2Nlc3MiOiJtIiwic3ViamVjdCI6ImFwaS1rZXk6OGY2ZjQyM2ItMjA4Ny00NzlhLThlYzItMzVjMTMwZTNjY2EyIn0.Cc6PRvlMpJmHXhpwKHO3krXy4O64XUqxnzXiNtNzpDs"

cloud_client = QdrantClient(
    url=CLOUD_URL,
    api_key=CLOUD_API_KEY,
    port=6333,
    https=True,
    timeout=60,
)

COLLECTION_NAME = "qdrant-rbi"
VECTOR_SIZE     = 768
BATCH_SIZE      = 50

print("Loading local data...")
with open("data/processed/chunks.json", encoding="utf-8") as f:
    chunks = json.load(f)

data       = np.load("data/processed/embeddings.npz")
embeddings = data["embeddings"]

print(f"Chunks: {len(chunks)}, Embeddings: {embeddings.shape}")

# Create collection on cloud
if cloud_client.collection_exists(COLLECTION_NAME):
    cloud_client.delete_collection(COLLECTION_NAME)

cloud_client.create_collection(
    collection_name=COLLECTION_NAME,
    vectors_config=VectorParams(
        size=VECTOR_SIZE,
        distance=Distance.COSINE,
    )
)
print("✅ Cloud collection created")

# Upload in batches
total   = len(chunks)
batches = (total + BATCH_SIZE - 1) // BATCH_SIZE

for batch_num in range(batches):
    # Check how many points already exist
    existing_count = cloud_client.get_collection(COLLECTION_NAME).points_count
    print(f"Already uploaded: {existing_count} points")
    
    start_from = existing_count  # resume from here
    start = batch_num * BATCH_SIZE
    end   = min(start + BATCH_SIZE, total)

    # Skip already uploaded batches
    if end <= start_from:
        continue

    points = []
    for idx in range(start, end):
        chunk = chunks[idx]
        points.append(PointStruct(
            id=idx,
            vector=embeddings[idx].tolist(),
            payload={
                "chunk_id":        chunk["chunk_id"],
                "child_text":      chunk["child_text"],
                "parent_text":     chunk["parent_text"],
                "circular_number": chunk["circular_number"],
                "title":           chunk.get("title", ""),
                "date":            chunk.get("date", ""),
                "year":            chunk.get("year", ""),
                "department":      chunk.get("department", ""),
                "detail_url":      chunk.get("detail_url", ""),
            }
        ))

    # Retry up to 3 times on timeout
    for attempt in range(3):
        try:
            cloud_client.upsert(
                collection_name=COLLECTION_NAME,
                points=points,
                wait=True,
            )
            print(f"Batch {batch_num+1}/{batches} — {end}/{total} points uploaded")
            break
        except Exception as e:
            if attempt < 2:
                print(f"  Timeout on batch {batch_num+1}, retrying ({attempt+1}/3)...")
                import time
                time.sleep(5)
            else:
                print(f"  ❌ Failed batch {batch_num+1} after 3 attempts: {e}")
# Verify
info = cloud_client.get_collection(COLLECTION_NAME)
print(f"\n✅ Migration complete: {info.points_count} points in cloud")