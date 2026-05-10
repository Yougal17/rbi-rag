import os
import re
import json
import fitz  # pymupdf
from pathlib import Path

# ─────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────

RAW_DIR       = "data/raw"
TEXT_DIR      = "data/processed/texts"
METADATA_FILE = "data/processed/metadata.json"
PARSE_LOG     = "data/processed/parse_log.json"

# If a page has fewer than this many characters, consider it scanned/empty
MIN_CHARS_PER_PAGE = 50

# ─────────────────────────────────────────────
# TEXT CLEANING
# ─────────────────────────────────────────────

def clean_text(raw_text):
    """
    Clean raw extracted PDF text into readable, consistent format.
    
    Why each step:
    - unicode normalization: PDFs often encode quotes and dashes oddly
    - ligature fix: PDFs sometimes store 'fi' as a single character
    - hyphen fix: words broken across lines need rejoining
    - whitespace: multiple spaces/blank lines add noise to embeddings
    - page artifacts: remove common RBI header/footer patterns
    """

    text = raw_text

    # ── Step 1: Normalize unicode ────────────────
    # Replace common unicode artifacts with ASCII equivalents
    replacements = {
        '\u2019': "'",   # right single quote → apostrophe
        '\u2018': "'",   # left single quote → apostrophe
        '\u201c': '"',   # left double quote → quote
        '\u201d': '"',   # right double quote → quote
        '\u2013': '-',   # en dash → hyphen
        '\u2014': '-',   # em dash → hyphen
        '\u2022': '*',   # bullet → asterisk
        '\u00a0': ' ',   # non-breaking space → space
        '\ufb01': 'fi',  # fi ligature
        '\ufb02': 'fl',  # fl ligature
        '\u00b7': '*',   # middle dot → asterisk
    }
    for unicode_char, replacement in replacements.items():
        text = text.replace(unicode_char, replacement)

    # ── Step 2: Fix broken hyphenations ─────────
    # PDFs break long words across lines: "regula-\ntion" → "regulation"
    text = re.sub(r'(\w)-\n(\w)', r'\1\2', text)

    # ── Step 3: Remove RBI letterhead patterns ───
    # These appear on every page and add noise
    patterns_to_remove = [
        r'Reserve Bank of India\s*',
        r'www\.rbi\.org\.in\s*',
        r'Kehkashan,\s*Bandra.*?\n',      # Mumbai address
        r'Garment House,\s*Dr\. Annie.*?\n',
        r'Page\s+\d+\s+of\s+\d+',        # "Page 1 of 4"
        r'^\s*\d+\s*$',                   # lone page numbers
        r'_{5,}',                          # long underlines
        r'-{5,}',                          # long dashes used as dividers
    ]
    for pattern in patterns_to_remove:
        text = re.sub(pattern, '', text, flags=re.MULTILINE | re.IGNORECASE)

    # ── Step 4: Normalize whitespace ────────────
    # Collapse multiple spaces to one
    text = re.sub(r'[ \t]+', ' ', text)

    # Collapse more than 2 consecutive newlines to 2
    text = re.sub(r'\n{3,}', '\n\n', text)

    # Strip leading/trailing whitespace from each line
    lines = [line.strip() for line in text.split('\n')]
    text = '\n'.join(lines)

    # ── Step 5: Final strip ──────────────────────
    text = text.strip()
    
    # ── Step 6: Remove garbled non-ASCII characters ──
    # RBI PDFs contain Hindi letterheads that don't extract cleanly
    # We keep only printable ASCII + common punctuation
    # The full English content is always preserved
    cleaned_lines = []
    for line in text.split('\n'):
        # Keep line if it has meaningful English content
        ascii_chars = sum(1 for c in line if ord(c) < 128)
        total_chars = len(line)
        if total_chars == 0:
            cleaned_lines.append(line)
            continue
        # If line is mostly ASCII (>60%), keep it — strip non-ASCII chars
        if ascii_chars / total_chars > 0.6:
            clean_line = ''.join(c if ord(c) < 128 else ' ' for c in line)
            clean_line = re.sub(r' +', ' ', clean_line).strip()
            cleaned_lines.append(clean_line)
        # Otherwise skip the line entirely (pure Hindi letterhead)
    text = '\n'.join(cleaned_lines)

    return text


# ─────────────────────────────────────────────
# SCANNED PDF DETECTION
# ─────────────────────────────────────────────

def is_scanned_pdf(doc):
    """
    Detect if a PDF is scanned (image-based) rather than text-based.
    
    Strategy: check the first 3 pages. If average characters per page
    is below MIN_CHARS_PER_PAGE, it's likely scanned.
    
    Why we don't OCR: OCR requires Tesseract installation, adds
    significant processing time, and RBI's 2022-2024 circulars are
    almost entirely text-based PDFs.
    """
    pages_to_check = min(3, len(doc))
    total_chars = 0

    for i in range(pages_to_check):
        page = doc[i]
        text = page.get_text()
        total_chars += len(text.strip())

    avg_chars = total_chars / pages_to_check if pages_to_check > 0 else 0
    return avg_chars < MIN_CHARS_PER_PAGE


# ─────────────────────────────────────────────
# SINGLE PDF PARSER
# ─────────────────────────────────────────────

def parse_pdf(pdf_path):
    """
    Parse a single PDF file and return cleaned text.

    Returns a dict:
    {
        "success": True/False,
        "text": "cleaned text...",
        "pages": 4,
        "scanned": False,
        "error": None
    }
    """
    try:
        doc = fitz.open(pdf_path)

        # Check if scanned
        if is_scanned_pdf(doc):
            doc.close()
            return {
                "success": False,
                "text": "",
                "pages": len(doc),
                "scanned": True,
                "error": "Scanned PDF — no extractable text"
            }

        # Extract text page by page
        pages_text = []
        for page_num in range(len(doc)):
            page = doc[page_num]

            # get_text("text") → plain text extraction
            # get_text("blocks") → preserves layout blocks (alternative)
            # We use "text" for simplicity — it handles most RBI circulars well
            raw_text = page.get_text("text")
            pages_text.append(raw_text)

        doc.close()

        # Join all pages with clear separator
        full_raw_text = "\n".join(pages_text)

        # Clean the text
        cleaned = clean_text(full_raw_text)

        # Sanity check — if cleaned text is very short, something went wrong
        if len(cleaned) < 100:
            return {
                "success": False,
                "text": cleaned,
                "pages": len(pages_text),
                "scanned": False,
                "error": f"Suspiciously short text: {len(cleaned)} chars"
            }

        return {
            "success": True,
            "text": cleaned,
            "pages": len(pages_text),
            "scanned": False,
            "error": None
        }

    except Exception as e:
        return {
            "success": False,
            "text": "",
            "pages": 0,
            "scanned": False,
            "error": str(e)
        }


# ─────────────────────────────────────────────
# MAIN PARSER PIPELINE
# ─────────────────────────────────────────────

def run_parser():
    """
    Parse all PDFs in data/raw/ and save text to data/processed/texts/
    Updates metadata.json with parsing results.
    """
    print("=" * 60)
    print("RBI Circular PDF Parser")
    print(f"Input:  {RAW_DIR}")
    print(f"Output: {TEXT_DIR}")
    print("=" * 60)

    # Load metadata
    with open(METADATA_FILE, "r", encoding="utf-8") as f:
        records = json.load(f)

    # Build filename → record lookup
    filename_to_record = {
        r["pdf_filename"]: r
        for r in records
        if r.get("pdf_filename")
    }

    # Create output directory
    os.makedirs(TEXT_DIR, exist_ok=True)

    # Get all PDFs
    pdf_files = sorted(Path(RAW_DIR).glob("*.pdf"))
    print(f"\n📂 Found {len(pdf_files)} PDFs to parse\n")

    # Tracking
    total_success  = 0
    total_scanned  = 0
    total_failed   = 0
    parse_log      = {}

    for i, pdf_path in enumerate(pdf_files):
        filename = pdf_path.name
        print(f"[{i+1}/{len(pdf_files)}] {filename[:60]}...")

        # Output text file path
        text_filename = pdf_path.stem + ".txt"
        text_path = os.path.join(TEXT_DIR, text_filename)

        # Skip if already parsed
        if os.path.exists(text_path):
            print(f"  ⏭️  Already parsed, skipping.")
            total_success += 1
            continue

        # Parse the PDF
        result = parse_pdf(str(pdf_path))

        if result["success"]:
            # Save the cleaned text
            with open(text_path, "w", encoding="utf-8") as f:
                f.write(result["text"])

            char_count = len(result["text"])
            print(f"  ✅ {result['pages']} pages → {char_count:,} chars")
            total_success += 1

            # Update metadata record
            if filename in filename_to_record:
                filename_to_record[filename]["text_filename"] = text_filename
                filename_to_record[filename]["char_count"] = char_count
                filename_to_record[filename]["pages"] = result["pages"]

        elif result["scanned"]:
            print(f"  🖼️  Scanned PDF — skipping (no OCR)")
            total_scanned += 1

            if filename in filename_to_record:
                filename_to_record[filename]["scanned"] = True

        else:
            print(f"  ❌ Failed: {result['error']}")
            total_failed += 1

        # Log result for every file
        parse_log[filename] = {
            "success": result["success"],
            "pages": result["pages"],
            "scanned": result["scanned"],
            "error": result["error"],
            "char_count": len(result["text"])
        }

    # Save updated metadata
    with open(METADATA_FILE, "w", encoding="utf-8") as f:
        json.dump(records, f, indent=2, ensure_ascii=False)

    # Save parse log
    with open(PARSE_LOG, "w", encoding="utf-8") as f:
        json.dump(parse_log, f, indent=2, ensure_ascii=False)

    # Summary
    print("\n" + "=" * 60)
    print("PARSING COMPLETE")
    print(f"  ✅ Successfully parsed: {total_success}")
    print(f"  🖼️  Scanned (skipped):  {total_scanned}")
    print(f"  ❌ Failed:              {total_failed}")
    print(f"  💾 Texts saved to:     {TEXT_DIR}")
    print("=" * 60)


# ─────────────────────────────────────────────
# RUN
# ─────────────────────────────────────────────

if __name__ == "__main__":
    run_parser()