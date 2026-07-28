# ─────────────────────────────────────────────
# SYSTEM PROMPT
# ─────────────────────────────────────────────
# This is sent once at the start of every conversation.
# It defines the LLM's role, constraints, and behavior.

SYSTEM_PROMPT = """You are an expert assistant specializing in Reserve Bank of India (RBI) circulars and regulatory guidelines.

Your role is to answer questions about RBI regulations, policies, and directives accurately and precisely.

STRICT RULES YOU MUST FOLLOW:
1. Answer ONLY based on the RBI circular excerpts provided in the context below.
2. NEVER use your training data or general knowledge to answer regulatory questions.
3. Every factual claim MUST be attributed to a specific circular with its number and date.
4. If the provided context does not contain enough information to answer the question, say exactly: "The available RBI circulars do not contain sufficient information to answer this question."
5. Do NOT speculate, infer, or extrapolate beyond what the circulars explicitly state.
6. Preserve the precision of regulatory language — do not paraphrase legal text loosely.
7. If multiple circulars address the same topic, synthesize them coherently and cite each one.

CITATION FORMAT:
- In the answer body: refer to circulars as "RBI circular [NUMBER] dated [DATE]"
- At the end: always include a numbered Sources list

TONE: Professional, precise, and direct. This is regulatory information that professionals rely on."""


# ─────────────────────────────────────────────
# CONTEXT BLOCK BUILDER
# ─────────────────────────────────────────────

def build_context_block(retrieved_chunks):
    """
    Format retrieved chunks into a structured context block
    for the LLM to read.

    Why we use parent_text not child_text:
    - child_text was used for retrieval (precise, short)
    - parent_text has full section context (better for generation)
    - The LLM needs enough context to give complete answers

    Why we deduplicate by circular_number:
    - Multiple chunks from the same circular shouldn't repeat
      the same header/metadata block
    - We group chunks by circular and present them together
    """
    if not retrieved_chunks:
        return "No relevant circulars found."

    # Group chunks by circular number
    circulars = {}
    for chunk in retrieved_chunks:
        circ_num = chunk["circular_number"]
        if circ_num not in circulars:
            circulars[circ_num] = {
                "circular_number": circ_num,
                "title":           chunk.get("title", ""),
                "date":            chunk.get("date", ""),
                "department":      chunk.get("department", ""),
                "detail_url":      chunk.get("detail_url", ""),
                "texts":           []
            }
        # Add parent_text if not already present
        parent = chunk.get("parent_text", chunk.get("child_text", ""))
        if parent not in circulars[circ_num]["texts"]:
            circulars[circ_num]["texts"].append(parent)

    # Build formatted context string
    context_parts = []
    for i, (circ_num, data) in enumerate(circulars.items(), 1):
        block = f"""--- CIRCULAR [{i}] ---
Circular Number: {data['circular_number']}
Title: {data['title']}
Date: {data['date']}
Department: {data['department']}
URL: {data['detail_url']}

Content:
{chr(10).join(data['texts'])}
--- END CIRCULAR [{i}] ---"""
        context_parts.append(block)

    return "\n\n".join(context_parts)


# ─────────────────────────────────────────────
# SOURCES BLOCK BUILDER
# ─────────────────────────────────────────────

def build_sources_block(retrieved_chunks):
    """
    Build a deduplicated sources list from retrieved chunks.
    This is appended to every answer for easy reference.
    """
    seen = set()
    sources = []

    for chunk in retrieved_chunks:
        circ_num = chunk["circular_number"]
        if circ_num in seen:
            continue
        seen.add(circ_num)

        sources.append({
            "circular_number": circ_num,
            "title":           chunk.get("title", ""),
            "date":            chunk.get("date", ""),
            "department":      chunk.get("department", ""),
            "detail_url":      chunk.get("detail_url", ""),
        })

    return sources


# ─────────────────────────────────────────────
# FULL PROMPT BUILDER
# ─────────────────────────────────────────────

def build_prompt(query, retrieved_chunks):
    """
    Build the complete prompt to send to Gemini.

    Structure:
    1. Context block — the retrieved circular excerpts
    2. Question — the user's query
    3. Instructions — how to format the answer

    Why we put context BEFORE the question:
    Research shows LLMs perform better when context comes first.
    The model "loads" the context, then applies it to the question.

    Returns:
        prompt string ready to send to Gemini
    """
    context = build_context_block(retrieved_chunks)

    prompt = f"""Below are relevant excerpts from RBI circulars retrieved for your question.
Read them carefully before answering.

{context}

─────────────────────────────────────────────
QUESTION: {query}
─────────────────────────────────────────────

INSTRUCTIONS FOR YOUR ANSWER:
1. Answer the question using ONLY the circular excerpts above.
2. Cite every claim with the circular number and date.
3. Use this citation format in your answer: "As per RBI circular [NUMBER] dated [DATE]..."
4. End your answer with a "Sources:" section listing all circulars you referenced.
5. If the context doesn't answer the question, say so clearly — do not guess.
6. Be precise with numbers, percentages, dates, and regulatory terms.

YOUR ANSWER:"""

    return prompt


# ─────────────────────────────────────────────
# TEST
# ─────────────────────────────────────────────

def test_prompt_builder():
    """Test the prompt builder with mock chunks."""

    # Mock retrieved chunks
    mock_chunks = [
        {
            "circular_number": "RBI/2023-24/27",
            "title": "Formalisation of Informal Micro Enterprises on Udyam Assist Platform",
            "date": "May 09, 2023",
            "department": "Financial Inclusion & Development Dept",
            "detail_url": "https://www.rbi.org.in/Scripts/BS_CircularIndexDisplay.aspx?Id=12500",
            "child_text": "Banks must ensure KYC verification for all micro enterprises registered on the Udyam Assist Platform within 30 days.",
            "parent_text": "Circular: RBI/2023-24/27\nDate: May 09, 2023\n---\nBanks must ensure KYC verification for all micro enterprises registered on the Udyam Assist Platform within 30 days of registration. All scheduled commercial banks are required to update their records accordingly.",
        },
        {
            "circular_number": "RBI/2022-23/66",
            "title": "KYC Directions Amendment",
            "date": "June 8, 2022",
            "department": "Department of Regulation",
            "detail_url": "https://www.rbi.org.in/Scripts/BS_CircularIndexDisplay.aspx?Id=12350",
            "child_text": "Doorstep banking services must comply with KYC Master Direction 2016.",
            "parent_text": "Circular: RBI/2022-23/66\nDate: June 8, 2022\n---\nAll banks offering doorstep banking services must comply with the Master Direction - Know Your Customer (KYC) Direction, 2016. Customer identification procedures must be followed strictly.",
        },
    ]

    query = "What are the KYC requirements for micro enterprises?"

    # Build and print the prompt
    prompt = build_prompt(query, mock_chunks)
    sources = build_sources_block(mock_chunks)

    print("=" * 60)
    print("GENERATED PROMPT")
    print("=" * 60)
    print(prompt)

    print("\n" + "=" * 60)
    print("SOURCES BLOCK")
    print("=" * 60)
    for i, s in enumerate(sources, 1):
        print(f"[{i}] {s['circular_number']} — {s['title']}")
        print(f"     Date: {s['date']}")
        print(f"     URL:  {s['detail_url']}")

    print("\n✅ Prompt builder working correctly")
    print(f"   Prompt length: {len(prompt)} characters")
    print(f"   Sources count: {len(sources)}")


if __name__ == "__main__":
    test_prompt_builder()