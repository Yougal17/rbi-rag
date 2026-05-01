# ingestion/scraper.py

import os
import re
import json
import time
import random
import requests
from bs4 import BeautifulSoup
from datetime import datetime

# ─────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────

BASE_URL   = "https://www.rbi.org.in"
DETAIL_URL = "https://www.rbi.org.in/Scripts/BS_CircularIndexDisplay.aspx?Id={}"

RAW_DIR       = "data/raw"
PROCESSED_DIR = "data/processed"
METADATA_FILE = os.path.join(PROCESSED_DIR, "metadata.json")

ID_START = 12300
ID_END   = 13450

TARGET_YEARS = {"2022", "2023", "2024", "2025", "2026"}

# Increased delays — more human-like
MIN_DELAY     = 4.0
MAX_DELAY     = 8.0

# Extra long pause every N requests — mimics human taking a break
LONG_PAUSE_EVERY = 20
LONG_PAUSE_MIN   = 30.0
LONG_PAUSE_MAX   = 60.0

MAX_RETRIES  = 3
RETRY_BACKOFF = 10  # seconds to wait between retries

# Realistic browser headers
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;"
        "q=0.9,image/webp,*/*;q=0.8"
    ),
    "Accept-Language": "en-IN,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}

# ─────────────────────────────────────────────
# SESSION — reuse TCP connection like a browser
# ─────────────────────────────────────────────

def make_session():
    """Create a fresh requests Session with our headers."""
    session = requests.Session()
    session.headers.update(HEADERS)
    return session

# Global session — recreated periodically
SESSION = make_session()

# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────

def polite_sleep(long=False):
    if long:
        delay = random.uniform(LONG_PAUSE_MIN, LONG_PAUSE_MAX)
        print(f"  😴 Long pause: {delay:.0f}s (mimicking human break)...")
    else:
        delay = random.uniform(MIN_DELAY, MAX_DELAY)
        print(f"  ⏳ Waiting {delay:.1f}s...")
    time.sleep(delay)
def load_existing_metadata():
    """Load metadata.json — keyed by circular_id for fast lookup."""
    if os.path.exists(METADATA_FILE):
        with open(METADATA_FILE, "r", encoding="utf-8") as f:
            records = json.load(f)
        return {str(r["circular_id"]): r for r in records}
    return {}


def save_metadata(metadata_dict):
    os.makedirs(PROCESSED_DIR, exist_ok=True)
    records = list(metadata_dict.values())
    with open(METADATA_FILE, "w", encoding="utf-8") as f:
        json.dump(records, f, indent=2, ensure_ascii=False)


def make_filename(circular_id, circular_number):
    """Create a clean filename from the circular ID and number."""
    clean = re.sub(r"[^\w\-]", "_", circular_number)
    clean = re.sub(r"_+", "_", clean).strip("_")
    return f"{circular_id}_{clean}.pdf"


def extract_year(date_str):
    """
    Extract 4-digit year from date strings like:
    '27.4.2026', '02/05/2024', 'April 27, 2026'
    Returns year as string or None.
    """
    match = re.search(r'(20\d{2})', date_str)
    return match.group(1) if match else None


# ─────────────────────────────────────────────
# CORE: Parse a single circular detail page
# ─────────────────────────────────────────────

def parse_detail_page(circular_id):
    """
    Fetch and parse a single circular detail page by ID.

    Returns a dict with all metadata + pdf_url, or None if:
    - Page doesn't exist
    - No circular number found (invalid/empty page)
    - Year is outside TARGET_YEARS
    """
    url = DETAIL_URL.format(circular_id)

    try:
        response = requests.get(url, headers=HEADERS, timeout=20)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"  ❌ Request failed: {e}")
        return None

    soup = BeautifulSoup(response.text, "html.parser")
    full_text = soup.get_text(separator=" ", strip=True)

    # ── Extract circular number ──────────────────
    # Pattern: RBI/2024-25/73 or RBI/DOR/2024-25/36
    circ_match = re.search(
        r'(RBI/[A-Z]{0,6}/?20\d{2}-\d{2,4}/\d+\S*)',
        full_text
    )
    if not circ_match:
        # Page exists but has no circular — likely a deleted/invalid ID
        return None

    circular_number = circ_match.group(1).strip()

    # ── Extract date ─────────────────────────────
    date_match = re.search(r'(\d{1,2}[./]\d{1,2}[./]20\d{2})', full_text)
    date = date_match.group(1) if date_match else "unknown"

    # ── Year gate — skip if outside target range ─
    year = extract_year(date)
    if year and year not in TARGET_YEARS:
        print(f"  ⏭️  Year {year} outside target range — skipping.")
        return None

    # ── Extract department ───────────────────────
    # RBI pages have department in a consistent location
    dept_match = re.search(
        r'(Department of [A-Za-z\s&,]+|[A-Z][a-z]+ Markets [A-Za-z\s]+Department)',
        full_text
    )
    department = dept_match.group(1).strip() if dept_match else "Unknown"

    # ── Extract title ────────────────────────────
    # Title is usually in <title> tag or first <h2>/<h3>
    title = ""
    title_tag = soup.find("title")
    if title_tag:
        title = title_tag.get_text(strip=True)
        # RBI title tags look like "RBI | Master Direction on KYC"
        if "|" in title:
            title = title.split("|")[-1].strip()

    if not title:
        for tag in ["h2", "h3", "h4"]:
            found = soup.find(tag)
            if found:
                title = found.get_text(strip=True)
                break

    # ── Find PDF URL ─────────────────────────────
    pdf_url = None

    # Strategy 1: direct .pdf link
    for link in soup.find_all("a", href=True):
        href = link["href"]
        if href.lower().endswith(".pdf"):
            pdf_url = href if href.startswith("http") else BASE_URL + "/" + href.lstrip("/")
            break

    # Strategy 2: link with "PDF" in text
    if not pdf_url:
        for link in soup.find_all("a", href=True):
            if "pdf" in link.get_text(strip=True).lower():
                href = link["href"]
                pdf_url = href if href.startswith("http") else BASE_URL + "/" + href.lstrip("/")
                break

    return {
        "circular_id": circular_id,
        "circular_number": circular_number,
        "title": title,
        "date": date,
        "year": year or "unknown",
        "department": department,
        "detail_url": url,
        "pdf_url": pdf_url,
        "pdf_filename": None,  # filled after download
    }


# ─────────────────────────────────────────────
# DOWNLOAD PDF
# ─────────────────────────────────────────────

def download_pdf(pdf_url, filename):
    """Download PDF to data/raw/. Returns True on success."""
    filepath = os.path.join(RAW_DIR, filename)

    if os.path.exists(filepath):
        print(f"  ⏭️  Already exists: {filename}")
        return True

    try:
        response = requests.get(
            pdf_url, headers=HEADERS, timeout=60, stream=True
        )
        response.raise_for_status()

        os.makedirs(RAW_DIR, exist_ok=True)
        with open(filepath, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

        size_kb = os.path.getsize(filepath) / 1024
        print(f"  ✅ Downloaded: {filename} ({size_kb:.1f} KB)")
        return True

    except requests.RequestException as e:
        print(f"  ❌ Download failed: {e}")
        # Remove partial file if it exists
        if os.path.exists(filepath):
            os.remove(filepath)
        return False


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

def run_scraper():
    print("=" * 60)
    print("RBI Circular Scraper — ID Range Mode")
    print(f"ID range: {ID_START} → {ID_END}")
    print(f"Target years: {TARGET_YEARS}")
    print("=" * 60)

    metadata = load_existing_metadata()
    print(f"\n📂 Already have {len(metadata)} circulars in metadata.\n")

    total_downloaded = 0
    total_skipped    = 0
    total_failed     = 0
    total_invalid    = 0
    total_ids        = ID_END - ID_START + 1

    for circular_id in range(ID_START, ID_END + 1):
        progress = circular_id - ID_START + 1
        print(f"\n[{progress}/{total_ids}] Checking ID {circular_id}...")

        # Skip if already processed
        if str(circular_id) in metadata:
            print(f"  ⏭️  Already in metadata.")
            total_skipped += 1
            continue

        # Parse the detail page
        info = parse_detail_page(circular_id)
        polite_sleep()

        if not info:
            print(f"  ⚪ Invalid or out-of-range page.")
            total_invalid += 1
            continue

        print(f"  📄 {info['circular_number']} | {info['date']}")
        print(f"  📝 {info['title'][:70]}...")

        # Download PDF if URL found
        if info["pdf_url"]:
            filename = make_filename(circular_id, info["circular_number"])
            success = download_pdf(info["pdf_url"], filename)
            polite_sleep()

            if success:
                info["pdf_filename"] = filename
                metadata[str(circular_id)] = info
                save_metadata(metadata)
                total_downloaded += 1
            else:
                total_failed += 1
        else:
            # Save metadata even without PDF — still useful
            print(f"  ⚠️  No PDF found — saving metadata only.")
            metadata[str(circular_id)] = info
            save_metadata(metadata)

    # Summary
    print("\n" + "=" * 60)
    print("SCRAPING COMPLETE")
    print(f"  ✅ Downloaded:      {total_downloaded}")
    print(f"  ⏭️  Skipped:         {total_skipped}")
    print(f"  ⚪ Invalid/empty:   {total_invalid}")
    print(f"  ❌ Failed:          {total_failed}")
    print(f"  📂 Total metadata:  {len(metadata)}")
    print("=" * 60)

if __name__ == "__main__":
    run_scraper()