import os
import re
import json
from pathlib import Path

# ─────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────

TEXT_DIR      = "data/processed/texts"
CHUNKS_FILE   = "data/processed/chunks.json"
METADATA_FILE = "data/processed/metadata.json"

# Parent chunk config (sent to LLM for generation)
PARENT_CHUNK_SIZE    = 600   # target words per parent chunk
PARENT_CHUNK_OVERLAP = 50    # words of overlap between parent chunks

# Child chunk config (used for retrieval/embedding)
CHILD_CHUNK_SIZE    = 120   # target words per child chunk
CHILD_CHUNK_OVERLAP = 20    # words of overlap between child chunks

# Minimum chunk size — discard chunks smaller than this
MIN_CHUNK_WORDS = 20

# ─────────────────────────────────────────────
# SECTION DETECTION
# ─────────────────────────────────────────────

# Patterns that indicate a new section in RBI circulars
SECTION_PATTERNS = [
    r'^\d+\.\s+[A-Z]',           # "1. Please refer..."
    r'^\d+\.\d+\s+[A-Z]',        # "2.1 Ensure that..."
    r'^\d+\.\d+\.\d+\s+[A-Z]',   # "2.1.1 Banks shall..."
    r'^[A-Z][A-Z\s]{3,}:',       # "DEFINITIONS:" "SCOPE:"
    r'^Annex(ure)?[\s\-]',        # "Annexure I"
    r'^Schedule[\s\-]',           # "Schedule A"
    r'^Part\s+[IVX\d]+',         # "Part I" "Part 2"
]

SECTION_REGEX = re.compile(
    '|'.join(SECTION_PATTERNS),
    re.MULTILINE
)


def is_section_boundary(line):
    """Return True if this line starts a new section."""
    return bool(SECTION_REGEX.match(line.strip()))


# ─────────────────────────────────────────────
# HEADER EXTRACTION
# ─────────────────────────────────────────────

def extract_header(text, metadata):
    """
    Extract the circular header block — circular number, date,
    department, subject line. This gets prepended to every chunk
    so the LLM always knows which circular it's reading.

    Returns: (header_string, body_text)
    """
    lines = text.split('\n')

    header_lines = []
    body_start   = 0

    # The header is typically the first 10-15 lines
    # It contains: circular number, date, department, addressee, subject
    for i, line in enumerate(lines[:20]):
        line = line.strip()
        if not line:
            continue

        # Subject line marks end of header
        if line.lower().startswith('subject:') or line.lower().startswith('sub:'):
            header_lines.append(line)
            body_start = i + 1
            break

        header_lines.append(line)

    # If no subject line found, use first 8 non-empty lines as header
    if body_start == 0:
        non_empty = [(i, l) for i, l in enumerate(lines[:15]) if l.strip()]
        if non_empty:
            body_start = non_empty[min(7, len(non_empty)-1)][0] + 1
            header_lines = [l for _, l in non_empty[:8]]

    # Build clean header string using metadata we already have
    header = (
        f"Circular: {metadata.get('circular_number', 'Unknown')}\n"
        f"Date: {metadata.get('date', 'Unknown')}\n"
        f"Department: {metadata.get('department', 'Unknown')}\n"
        f"Title: {metadata.get('title', 'Unknown')}\n"
        f"---\n"
    )

    body = '\n'.join(lines[body_start:]).strip()

    return header, body


# ─────────────────────────────────────────────
# TEXT SPLITTING
# ─────────────────────────────────────────────

def split_into_sentences(text):
    """
    Split text into sentences.
    Handles abbreviations common in RBI circulars:
    e.g., Rs., No., Govt., para., viz., etc.
    """
    # Protect common abbreviations from being split
    abbreviations = [
        'Rs', 'No', 'Govt', 'viz', 'para', 'approx',
        'viz', 'Dr', 'Mr', 'Mrs', 'Sr', 'Jr', 'Ltd',
        'Co', 'Corp', 'Dept', 'RBI', 'SEBI', 'NABARD'
    ]
    protected = text
    for abbr in abbreviations:
        protected = protected.replace(f'{abbr}.', f'{abbr}DOTPROTECTED')

    # Split on sentence boundaries
    sentences = re.split(r'(?<=[.!?])\s+(?=[A-Z])', protected)

    # Restore protected dots
    sentences = [s.replace('DOTPROTECTED', '.') for s in sentences]

    return [s.strip() for s in sentences if s.strip()]


def words(text):
    """Count words in text."""
    return len(text.split())


def split_by_words(text, chunk_size, overlap):
    """
    Split text into chunks of approximately chunk_size words,
    with overlap words carried over between chunks.

    Why overlap? Without it, a sentence split across two chunks
    loses context. The overlapping words ensure continuity.
    """
    word_list = text.split()
    chunks    = []
    start     = 0

    while start < len(word_list):
        end        = min(start + chunk_size, len(word_list))
        chunk_text = ' '.join(word_list[start:end])
        chunks.append(chunk_text)

        if end == len(word_list):
            break

        # Next chunk starts (chunk_size - overlap) words later
        start += chunk_size - overlap

    return chunks


# ─────────────────────────────────────────────
# SECTION-AWARE SPLITTING
# ─────────────────────────────────────────────

def split_into_sections(text):
    """
    Split circular body into logical sections using RBI's
    numbered section structure.

    Returns list of section strings.
    """
    lines    = text.split('\n')
    sections = []
    current  = []

    for line in lines:
        if is_section_boundary(line) and current:
            # Save current section, start new one
            section_text = '\n'.join(current).strip()
            if words(section_text) >= MIN_CHUNK_WORDS:
                sections.append(section_text)
            current = [line]
        else:
            current.append(line)

    # Don't forget the last section
    if current:
        section_text = '\n'.join(current).strip()
        if words(section_text) >= MIN_CHUNK_WORDS:
            sections.append(section_text)

    # If no sections detected, treat whole body as one section
    if not sections:
        sections = [text]

    return sections


# ─────────────────────────────────────────────
# HIERARCHICAL CHUNKING — CORE FUNCTION
# ─────────────────────────────────────────────

def create_hierarchical_chunks(text, metadata):
    """
    Create parent and child chunks for one circular.

    Process:
    1. Extract header (circular number, date, department, subject)
    2. Split body into logical sections
    3. Group sections into parent chunks (~600 words)
    4. Split each parent chunk into child chunks (~120 words)
    5. Tag every child with: its parent text + circular metadata

    Returns list of chunk dicts.
    """
    circular_number = metadata.get('circular_number', 'Unknown')
    header, body    = extract_header(text, metadata)

    if not body.strip():
        body = text  # fallback: use full text if header extraction failed

    # Split body into sections
    sections = split_into_sections(body)

    # Group sections into parent chunks
    parent_chunks = []
    current_parent_words = []
    current_parent_sects = []

    for section in sections:
        section_word_count = words(section)

        # If adding this section would exceed parent size, save current parent
        if (current_parent_words and
            sum(words(s) for s in current_parent_sects) + section_word_count > PARENT_CHUNK_SIZE):

            parent_text = '\n\n'.join(current_parent_sects)
            parent_chunks.append(parent_text)
            # Overlap: carry last section into next parent
            current_parent_sects = current_parent_sects[-1:] + [section]
        else:
            current_parent_sects.append(section)

        current_parent_words = current_parent_sects

    # Don't forget last parent
    if current_parent_sects:
        parent_text = '\n\n'.join(current_parent_sects)
        parent_chunks.append(parent_text)

    # Now create child chunks from each parent
    all_chunks = []
    chunk_index = 0

    for parent_index, parent_text in enumerate(parent_chunks):

        # Full parent context = header + parent text
        # This is what gets sent to the LLM
        full_parent = header + parent_text

        # Split parent into child chunks
        child_texts = split_by_words(
            parent_text,
            CHILD_CHUNK_SIZE,
            CHILD_CHUNK_OVERLAP
        )

        for child_index, child_text in enumerate(child_texts):

            if words(child_text) < MIN_CHUNK_WORDS:
                continue  # skip tiny fragments

            # Every chunk gets full metadata for citation
            chunk = {
                # Unique ID for this chunk
                "chunk_id": f"{circular_number}__p{parent_index}__c{child_index}",

                # Child text — used to generate embedding
                # Short and precise → better retrieval
                "child_text": child_text,

                # Parent text — sent to LLM for answer generation
                # Full context → better answers
                "parent_text": full_parent,

                # Metadata — used for citations and filtering
                "circular_number": circular_number,
                "title":           metadata.get('title', ''),
                "date":            metadata.get('date', ''),
                "year":            metadata.get('year', ''),
                "department":      metadata.get('department', ''),
                "detail_url":      metadata.get('detail_url', ''),

                # Position info — useful for debugging
                "parent_index": parent_index,
                "child_index":  child_index,
                "total_parents": len(parent_chunks),
            }

            all_chunks.append(chunk)
            chunk_index += 1

    return all_chunks


# ─────────────────────────────────────────────
# MAIN CHUNKING PIPELINE
# ─────────────────────────────────────────────

def run_chunker():
    print("=" * 60)
    print("RBI Circular Chunker — Hierarchical Strategy")
    print(f"Parent chunk size: ~{PARENT_CHUNK_SIZE} words")
    print(f"Child chunk size:  ~{CHILD_CHUNK_SIZE} words")
    print("=" * 60)

    # Load metadata — keyed by pdf_filename for lookup
    with open(METADATA_FILE, "r", encoding="utf-8") as f:
        records = json.load(f)

    # Build lookup: text_filename → metadata record
    text_to_meta = {}
    for r in records:
        if r.get('text_filename'):
            text_to_meta[r['text_filename']] = r
        elif r.get('pdf_filename'):
            # Derive text filename from pdf filename
            text_fname = r['pdf_filename'].replace('.pdf', '.txt')
            text_to_meta[text_fname] = r

    # Get all text files
    text_files = sorted(Path(TEXT_DIR).glob("*.txt"))
    print(f"\n📂 Found {len(text_files)} text files to chunk\n")

    all_chunks     = []
    total_circs    = 0
    total_skipped  = 0

    for i, text_path in enumerate(text_files):
        filename = text_path.name
        metadata = text_to_meta.get(filename, {})

        if not metadata:
            print(f"[{i+1}] ⚠️  No metadata for {filename} — skipping")
            total_skipped += 1
            continue

        # Read text file
        text = text_path.read_text(encoding="utf-8")

        if len(text.strip()) < 100:
            print(f"[{i+1}] ⚠️  Too short, skipping: {filename}")
            total_skipped += 1
            continue

        # Create hierarchical chunks
        chunks = create_hierarchical_chunks(text, metadata)

        print(
            f"[{i+1}/{len(text_files)}] "
            f"{metadata.get('circular_number', filename)[:40]} "
            f"→ {len(chunks)} chunks"
        )

        all_chunks.extend(chunks)
        total_circs += 1

    # Save all chunks to single JSON file
    with open(CHUNKS_FILE, "w", encoding="utf-8") as f:
        json.dump(all_chunks, f, indent=2, ensure_ascii=False)

    # Summary
    print("\n" + "=" * 60)
    print("CHUNKING COMPLETE")
    print(f"  📄 Circulars processed: {total_circs}")
    print(f"  ⚠️  Skipped:            {total_skipped}")
    print(f"  🧩 Total chunks:        {len(all_chunks)}")
    print(f"  📊 Avg chunks/circular: {len(all_chunks)//max(total_circs,1)}")
    print(f"  💾 Saved to:            {CHUNKS_FILE}")
    print("=" * 60)

    # Quick quality check
    if all_chunks:
        sample = all_chunks[len(all_chunks)//2]  # middle chunk
        print(f"\n📋 Sample chunk:")
        print(f"  ID:          {sample['chunk_id']}")
        print(f"  Circular:    {sample['circular_number']}")
        print(f"  Child text:  {sample['child_text'][:150]}...")
        print(f"  Parent size: {words(sample['parent_text'])} words")
        print(f"  Child size:  {words(sample['child_text'])} words")


if __name__ == "__main__":
    run_chunker()