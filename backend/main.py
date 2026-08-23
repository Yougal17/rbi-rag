# backend/main.py

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import time
import json
from typing import Optional
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from dotenv import load_dotenv
from qdrant_client import QdrantClient

from generation.generator import RBIGenerator

load_dotenv()

# ─────────────────────────────────────────────
# QDRANT CLIENT — works both locally and on cloud
# ─────────────────────────────────────────────

def get_qdrant_client():
    """
    Returns a Qdrant client and collection name.
    Reads from environment variables when deployed to cloud.
    Falls back to local Docker instance for local development.
    """
    qdrant_url     = os.getenv("QDRANT_URL")
    qdrant_api_key = os.getenv("QDRANT_API_KEY")
    collection     = os.getenv("COLLECTION_NAME", "qdrant-rbi")

    if qdrant_url:
        # Cloud deployment — use Qdrant Cloud
        client = QdrantClient(
            url=qdrant_url,
            api_key=qdrant_api_key,
            timeout=30,
        )
        print(f"  🌐 Connected to Qdrant Cloud: {qdrant_url[:40]}...")
    else:
        # Local development — use Docker
        client = QdrantClient(
            host="localhost",
            port=6333,
        )
        print(f"  🏠 Connected to local Qdrant (Docker)")

    return client, collection


# ─────────────────────────────────────────────
# REQUEST / RESPONSE MODELS
# ─────────────────────────────────────────────

class QueryRequest(BaseModel):
    question: str = Field(
        ...,
        min_length=10,
        max_length=500,
        description="Question about RBI circulars",
        example="What are the KYC requirements for small businesses?"
    )
    year_filter: Optional[str] = Field(
        None,
        description="Filter results by year e.g. '2023'",
        example="2023"
    )
    top_k: Optional[int] = Field(
        5,
        ge=1,
        le=10,
        description="Number of chunks to retrieve"
    )


class SourceModel(BaseModel):
    circular_number: str
    title:           str
    date:            str
    department:      str
    url:             str


class QueryResponse(BaseModel):
    answer:        str
    sources:       list[SourceModel]
    total_sources: int
    timing:        dict
    query:         str


class HealthResponse(BaseModel):
    status:    str
    model:     str
    chunks:    int
    circulars: int
    uptime:    float


class StatsResponse(BaseModel):
    total_chunks:    int
    total_circulars: int
    years_covered:   list[str]
    model:           str
    retrieval:       str


# ─────────────────────────────────────────────
# APP STATE
# ─────────────────────────────────────────────

# Global instances — loaded once at startup
generator    = None
start_time   = None
system_stats = {}


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Startup and shutdown logic.
    Loads all models once when server starts.
    This is the correct FastAPI pattern for expensive initialization.
    """
    global generator, start_time, system_stats

    print("\n🚀 Starting RBI RAG API...")
    print("Loading models — this takes 30-60 seconds on first run...\n")

    start_time = time.time()

    # Initialize complete RAG pipeline
    generator = RBIGenerator()

    # Load system stats from metadata
    try:
        metadata_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "data", "processed", "metadata.json"
        )
        chunks_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "data", "processed", "chunks.json"
        )

        with open(metadata_path, encoding="utf-8") as f:
            metadata = json.load(f)
        with open(chunks_path, encoding="utf-8") as f:
            chunks = json.load(f)

        years = sorted(set(
            r.get("year", "")
            for r in metadata
            if r.get("year") and r.get("year") != "unknown"
        ))

        system_stats = {
            "total_chunks":    len(chunks),
            "total_circulars": len(metadata),
            "years_covered":   years,
            "model":           os.getenv("GEMINI_MODEL", "gemini-2.5-flash"),
            "retrieval":       "Dense + BM25 + RRF + Cross-Encoder",
        }

    except Exception as e:
        print(f"⚠️  Could not load stats: {e}")
        system_stats = {
            "total_chunks":    4368,
            "total_circulars": 317,
            "years_covered":   ["2022", "2023", "2024"],
            "model":           os.getenv("GEMINI_MODEL", "gemini-2.5-flash"),
            "retrieval":       "Dense + BM25 + RRF + Cross-Encoder",
        }

    elapsed = time.time() - start_time
    print(f"\n✅ RBI RAG API ready in {elapsed:.1f}s")
    print(f"   Circulars: {system_stats['total_circulars']}")
    print(f"   Chunks:    {system_stats['total_chunks']}")
    print(f"   Years:     {system_stats['years_covered']}")
    print(f"\n📖 API docs: http://localhost:8000/docs\n")

    yield  # Server runs here

    print("\n👋 Shutting down RBI RAG API...")


# ─────────────────────────────────────────────
# FASTAPI APP
# ─────────────────────────────────────────────

app = FastAPI(
    title="RBI Circular Intelligence API",
    description=(
        "Query RBI circulars using natural language. "
        "Returns cited answers with circular numbers, dates, and source URLs. "
        "Built with Dense + BM25 + RRF + Cross-Encoder retrieval pipeline."
    ),
    version="1.0.0",
    lifespan=lifespan,
)

# CORS — allows React frontend to call this API from any origin
# In production, replace "*" with your actual frontend URL for security
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ─────────────────────────────────────────────
# ENDPOINTS
# ─────────────────────────────────────────────

@app.get("/health", response_model=HealthResponse)
async def health_check():
    """
    Health check endpoint.
    Used by deployment platforms to verify the service is running.
    Returns system status, model info, and uptime.
    """
    if generator is None:
        raise HTTPException(
            status_code=503,
            detail="Generator not initialized — server is still starting up"
        )

    return HealthResponse(
        status="healthy",
        model=system_stats.get("model", "unknown"),
        chunks=system_stats.get("total_chunks", 0),
        circulars=system_stats.get("total_circulars", 0),
        uptime=round(time.time() - start_time, 1),
    )


@app.get("/stats", response_model=StatsResponse)
async def get_stats():
    """
    System statistics endpoint.
    Returns information about loaded data and models.
    Useful for the frontend to display system info.
    """
    return StatsResponse(**system_stats)


@app.post("/query", response_model=QueryResponse)
async def query(request: QueryRequest):
    """
    Main query endpoint.

    Takes a natural language question about RBI circulars
    and returns a cited answer with source references.

    The pipeline runs:
    1. Dense retrieval (semantic similarity via Qdrant)
    2. BM25 sparse retrieval (keyword matching)
    3. Reciprocal Rank Fusion (merge both result sets)
    4. Cross-encoder reranking (precise relevance scoring)
    5. Gemini generation (cited answer)

    Optionally filter results by year (2022, 2023, or 2024).
    """
    if generator is None:
        raise HTTPException(
            status_code=503,
            detail="Generator not initialized — server is still starting up"
        )

    # Build metadata filters
    filters = None
    if request.year_filter:
        if request.year_filter not in ["2022", "2023", "2024"]:
            raise HTTPException(
                status_code=400,
                detail="year_filter must be '2022', '2023', or '2024'"
            )
        filters = {"year": request.year_filter}

    # Run complete RAG pipeline
    try:
        result = generator.answer(
            query=request.question,
            filters=filters,
            top_k=request.top_k,
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Pipeline failed: {str(e)}"
        )

    # Handle empty or error answers
    answer_text = result.get("answer") or (
        "The available RBI circulars do not contain sufficient information "
        "to answer this question. This may be because the relevant circular "
        "predates our dataset (pre-2022), is covered in Master Directions "
        "not in our database, or falls outside RBI's direct regulatory scope. "
        "For authoritative information, visit: rbi.org.in"
    )

    # Format source citations
    sources = [
        SourceModel(
            circular_number=s.get("circular_number", ""),
            title=s.get("title", ""),
            date=s.get("date", ""),
            department=s.get("department", ""),
            url=s.get("detail_url", ""),
        )
        for s in result.get("sources", [])
    ]

    return QueryResponse(
        answer=answer_text,
        sources=sources,
        total_sources=len(sources),
        timing=result.get("timing", {}),
        query=request.question,
    )


# ─────────────────────────────────────────────
# RUN
# ─────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn

    port = int(os.getenv("PORT", 8000))

    uvicorn.run(
        "backend.main:app",
        host="0.0.0.0",
        port=port,
        reload=False,
        # reload=True causes models to reload on every code change
        # always keep False in production
    )