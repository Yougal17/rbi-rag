import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import time
from dotenv import load_dotenv
from google import genai
from google.genai import types

from generation.prompt import (
    SYSTEM_PROMPT,
    build_prompt,
    build_sources_block,
)
from retrieval.reranker import RetrievalPipeline

# ─────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────

load_dotenv()

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
GEMINI_MODEL   = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")

# Generation settings
TEMPERATURE      = 0.1    # low = factual, consistent
MAX_OUTPUT_TOKENS = 1500  # enough for detailed cited answers

# How many chunks to retrieve and pass to LLM
RETRIEVAL_TOP_K  = 5

# ─────────────────────────────────────────────
# RBI RAG GENERATOR
# ─────────────────────────────────────────────

class RBIGenerator:
    """
    Complete RAG pipeline:
    Question → Retrieve → Prompt → Generate → Cited Answer

    This is the central class of the entire project.
    The FastAPI backend (Step 18) will use this directly.
    """

    def __init__(self):
        print("🚀 Initializing RBI RAG Generator...")

        # Initialize Gemini client
        if not GEMINI_API_KEY:
            raise ValueError("GEMINI_API_KEY not found in .env file")

        self.client = genai.Client(api_key=GEMINI_API_KEY)
        self.model  = GEMINI_MODEL
        print(f"  ✅ Gemini client ready ({self.model})")

        # Initialize retrieval pipeline
        # This loads: embedding model + BM25 index + cross-encoder
        self.retrieval = RetrievalPipeline()
        print(f"  ✅ Retrieval pipeline ready")

        print(f"\n✅ RBI RAG Generator fully initialized\n")

    def answer(self, query, filters=None, top_k=RETRIEVAL_TOP_K):
        """
        Answer a question about RBI circulars.

        Args:
            query:   user's question string
            filters: optional metadata filter e.g. {"year": "2023"}
            top_k:   number of chunks to retrieve

        Returns:
            dict with:
                answer:   generated text answer with citations
                sources:  list of source circular metadata
                chunks:   raw retrieved chunks (for debugging)
                timing:   time breakdown for each stage
        """
        timing = {}
        print(f"\n{'='*60}")
        print(f"QUERY: {query}")
        if filters:
            print(f"FILTERS: {filters}")
        print(f"{'='*60}")

        # ── Stage 1: Retrieve ────────────────────────
        t0 = time.time()
        print(f"\n[1/3] Retrieving relevant chunks...")

        chunks = self.retrieval.retrieve(
            query,
            top_k=top_k,
            filters=filters
        )

        timing["retrieval"] = round(time.time() - t0, 2)
        print(f"  → Retrieved {len(chunks)} chunks in {timing['retrieval']}s")

        if not chunks:
            return {
                "answer": (
                    "The available RBI circulars (2022-2024) do not contain "
                    "sufficient information to answer this question.\n\n"
                    "This may be because:\n"
                    "- The relevant circular predates our dataset (pre-2022)\n"
                    "- This topic is covered in RBI Master Directions not in our database\n"
                    "- This falls outside RBI's direct regulatory scope\n\n"
                    "For authoritative information, visit: [https://rbi.org.in](https://rbi.org.in)"
                ),
                "sources": [],
                "chunks":  [],
                "timing":  timing,
            }

        # ── Stage 2: Build Prompt ────────────────────
        t1 = time.time()
        print(f"\n[2/3] Building prompt...")

        prompt  = build_prompt(query, chunks)
        sources = build_sources_block(chunks)

        timing["prompt_build"] = round(time.time() - t1, 2)
        print(f"  → Prompt length: {len(prompt):,} characters")
        print(f"  → Sources: {len(sources)} unique circulars")

        # ── Stage 3: Generate ────────────────────────
        t2 = time.time()
        print(f"\n[3/3] Generating answer with {self.model}...")

        try:
            # Retry up to 3 times on rate limit errors
            for attempt in range(3):
                try:
                    response = self.client.models.generate_content(
                        model=self.model,
                        contents=[
                            types.Content(
                                role="user",
                                parts=[types.Part(text=SYSTEM_PROMPT)]
                            ),
                            types.Content(
                                role="model",
                                parts=[types.Part(
                                    text="Understood. I will answer questions strictly based on the provided RBI circular excerpts, citing every claim with circular number and date."
                                )]
                            ),
                            types.Content(
                                role="user",
                                parts=[types.Part(text=prompt)]
                            ),
                        ],
                        config=types.GenerateContentConfig(
                            temperature=TEMPERATURE,
                            max_output_tokens=MAX_OUTPUT_TOKENS,
                        )
                    )
                    answer_text = response.text
                    break  # success — exit retry loop

                except Exception as e:
                    if "429" in str(e) and attempt < 2:
                        wait_time = 60 * (attempt + 1)
                        print(f"  ⏳ Rate limited — waiting {wait_time}s before retry {attempt+2}/3...")
                        time.sleep(wait_time)
                    else:
                        raise e

        except Exception as e:
            print(f"  ❌ Generation failed: {e}")
            answer_text = f"Generation error: {str(e)}"

        timing["generation"] = round(time.time() - t2, 2)
        timing["total"]      = round(time.time() - t0, 2)

        print(f"  → Generated in {timing['generation']}s")
        print(f"  → Total time: {timing['total']}s")

        return {
            "answer":  answer_text,
            "sources": sources,
            "chunks":  chunks,
            "timing":  timing,
        }

    def format_response(self, result):
        """
        Format the result dict into a clean printable string.
        Used for testing — the API will return the dict directly.
        """
        lines = []
        lines.append("\n" + "="*60)
        lines.append("ANSWER")
        lines.append("="*60)
        lines.append(result["answer"])

        lines.append("\n" + "="*60)
        lines.append("SOURCES")
        lines.append("="*60)
        for i, source in enumerate(result["sources"], 1):
            lines.append(f"\n[{i}] {source['circular_number']}")
            lines.append(f"     Title:      {source['title'][:70]}")
            lines.append(f"     Date:       {source['date']}")
            lines.append(f"     Department: {source['department'][:50]}")
            lines.append(f"     URL:        {source['detail_url']}")

        lines.append("\n" + "="*60)
        lines.append("TIMING")
        lines.append("="*60)
        for stage, seconds in result["timing"].items():
            lines.append(f"  {stage:20s}: {seconds}s")

        return "\n".join(lines)


# ─────────────────────────────────────────────
# TEST
# ─────────────────────────────────────────────

def test_generator():
    """
    Test the complete RAG pipeline end to end.
    These are real questions that compliance professionals ask.
    """
    generator = RBIGenerator()

    test_queries = [
        {
            "query": "What are the KYC requirements for opening a bank account?",
            "filters": None,
        },
        {
            "query": "What are RBI's guidelines on digital lending and borrower protection?",
            "filters": {"year": "2023"},
        },
        {
            "query": "What penalties can RBI impose on banks for violations?",
            "filters": None,
        },
    ]

    for test in test_queries:
        result = generator.answer(
            query=test["query"],
            filters=test["filters"],
        )
        print(generator.format_response(result))
        print("\n")

        # Small delay between queries — respect API rate limits
        time.sleep(3)


if __name__ == "__main__":
    test_generator()