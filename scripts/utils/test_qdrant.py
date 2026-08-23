from qdrant_client import QdrantClient
from qdrant_client.models import Distance, VectorParams

# Connect to local Qdrant
client = QdrantClient(host="localhost", port=6333)

# Check connection
info = client.get_collections()
print(f"✅ Connected to Qdrant")
print(f"   Existing collections: {info}")

# Create a test collection
client.recreate_collection(
    collection_name="test_collection",
    vectors_config=VectorParams(
        size=768,       # must match our embedding dimensions
        distance=Distance.COSINE  # similarity metric
    )
)
print(f"✅ Created test collection")

# Delete the test collection
client.delete_collection("test_collection")
print(f"✅ Deleted test collection")
print(f"\n🎉 Qdrant is ready for Step 8")