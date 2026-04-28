import os
import re
import json
import time
import random
import requests
from bs4 import BeautifulSoup
from datetime import datetime

BASE_URL = "https://www.rbi.org.in"
INDEX_URL = "https://www.rbi.org.in/Scripts/BS_CircularIndexDisplay.aspx"

RAW_DIR = "data/raw"
PROCESSED_DIR = "data/processed"
METADATA_FILE = os.path.join(PROCESSED_DIR, "metadata.json")

# Years to scrape — we decided on last 3 years in Step 2
YEARS_TO_SCRAPE = [2022, 2023, 2024, 2025, 2026]

# Random range so we don't look like a bot with perfectly timed requests
MIN_DELAY = 2.0
MAX_DELAY = 4.0

# Headers — we identify ourselves honestly
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (compatible; RBI-RAG-Research-Bot/1.0; "
        "Educational project — contact yougalattri17@gmail.com)"
    )
}

# ─────────────────────────────────────────────
# HELPER: polite sleep between requests
# ─────────────────────────────────────────────

def polite_sleep():
    """Sleep a random amount between MIN_DELAY and MAX_DELAY seconds."""
    delay = random.uniform(MIN_DELAY, MAX_DELAY)
    print(f"  ⏳ Waiting {delay:.1f}s...")
    time.sleep(delay)


# ─────────────────────────────────────────────
# HELPER: load existing metadata (avoid re-downloading)
# ─────────────────────────────────────────────

def load_existing_metadata():
    """
    Load metadata.json if it exists.
    Returns a dict keyed by circular_number for fast lookup.
    """
    if os.path.exists(METADATA_FILE):
        with open(METADATA_FILE, "r", encoding="utf-8") as f:
            records = json.load(f)
        # Key by circular_number for fast "already downloaded?" checks
        return {r["circular_number"]: r for r in records}
    return {}


def save_metadata(metadata_dict):
    """Save the full metadata dict back to metadata.json."""
    os.makedirs(PROCESSED_DIR, exist_ok=True)
    records = list(metadata_dict.values())
    with open(METADATA_FILE, "w", encoding="utf-8") as f:
        json.dump(records, f, indent=2, ensure_ascii=False)


# ─────────────────────────────────────────────
# STEP A: Get all circular links for a given year
# ─────────────────────────────────────────────

def get_circular_links_for_year(year):
    """
    Fetch the RBI circular index page for a given year.
    Returns a list of dicts:
      [{ title, date, circular_number, department, detail_url }, ...]
    """
    print(f"\n📅 Fetching circular index for year: {year}")

    # RBI uses a query parameter to filter by year
    params = {"Year": str(year)}

    try:
        response = requests.get(
            INDEX_URL,
            params=params,
            headers=HEADERS,
            timeout=30
        )
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"  ❌ Failed to fetch index for {year}: {e}")
        return []

    soup = BeautifulSoup(response.text, "html.parser")

    # Find the main content table
    # RBI's table has class 'tablebg' — inspect the page to confirm this
    table = soup.find("table", {"class": "tablebg"})

    if not table:
        # Fallback: try finding any table with circular data
        tables = soup.find_all("table")
        print(f"  ⚠️  Could not find 'tablebg' table. Found {len(tables)} tables total.")
        print("  ℹ️  You may need to inspect the page and update the table selector.")
        return []

    circulars = []
    rows = table.find_all("tr")

    for row in rows:
        cols = row.find_all("td")

        # Skip header rows or empty rows
        if len(cols) < 3:
            continue

        try:
            # Extract date (first column)
            raw_date = cols[0].get_text(strip=True)

            # Extract title and link (second column)
            link_tag = cols[1].find("a")
            if not link_tag:
                continue

            title = link_tag.get_text(strip=True)
            relative_url = link_tag.get("href", "")

            # Build full URL
            if relative_url.startswith("http"):
                detail_url = relative_url
            else:
                detail_url = BASE_URL + "/" + relative_url.lstrip("/")

            # Extract circular number (third column)
            circular_number = cols[2].get_text(strip=True)

            # Extract department (fourth column, if exists)
            department = cols[3].get_text(strip=True) if len(cols) > 3 else "Unknown"

            # Clean up the date string
            # RBI dates look like "Sep 15, 2024" or "15/09/2024"
            date_cleaned = raw_date.strip()

            circulars.append({
                "title": title,
                "date": date_cleaned,
                "circular_number": circular_number if circular_number else f"UNKNOWN_{year}_{len(circulars)}",
                "department": department,
                "detail_url": detail_url,
            })

        except Exception as e:
            print(f"  ⚠️  Error parsing row: {e}")
            continue

    print(f"  ✅ Found {len(circulars)} circulars for {year}")
    return circulars


# ─────────────────────────────────────────────
# STEP B: Get the PDF download URL from the detail page
# ─────────────────────────────────────────────

def get_pdf_url_from_detail_page(detail_url):
    """
    Visit the intermediate circular detail page.
    Find and return the direct PDF URL.
    Returns None if no PDF found.
    """
    try:
        response = requests.get(detail_url, headers=HEADERS, timeout=30)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"  ❌ Failed to fetch detail page: {e}")
        return None

    soup = BeautifulSoup(response.text, "html.parser")

    # Strategy 1: Find direct .pdf links
    for link in soup.find_all("a", href=True):
        href = link["href"]
        if href.lower().endswith(".pdf"):
            if href.startswith("http"):
                return href
            else:
                return BASE_URL + "/" + href.lstrip("/")

    # Strategy 2: Find links with "PDF" in the text
    for link in soup.find_all("a", href=True):
        link_text = link.get_text(strip=True).upper()
        if "PDF" in link_text:
            href = link["href"]
            if href.startswith("http"):
                return href
            else:
                return BASE_URL + "/" + href.lstrip("/")

    # Strategy 3: Check if the detail_url itself is a PDF
    if detail_url.lower().endswith(".pdf"):
        return detail_url

    return None


# ─────────────────────────────────────────────
# STEP C: Download a single PDF
# ─────────────────────────────────────────────

def download_pdf(pdf_url, filename):
    """
    Download a PDF from pdf_url and save it to data/raw/filename.
    Returns True on success, False on failure.
    """
    filepath = os.path.join(RAW_DIR, filename)

    # Skip if already downloaded — never re-download
    if os.path.exists(filepath):
        print(f"  ⏭️  Already exists, skipping: {filename}")
        return True

    try:
        response = requests.get(pdf_url, headers=HEADERS, timeout=60, stream=True)
        response.raise_for_status()

        # Verify it's actually a PDF
        content_type = response.headers.get("Content-Type", "")
        if "pdf" not in content_type.lower() and not pdf_url.lower().endswith(".pdf"):
            print(f"  ⚠️  Unexpected content type: {content_type}")

        os.makedirs(RAW_DIR, exist_ok=True)

        with open(filepath, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

        size_kb = os.path.getsize(filepath) / 1024
        print(f"  ✅ Downloaded: {filename} ({size_kb:.1f} KB)")
        return True

    except requests.RequestException as e:
        print(f"  ❌ Failed to download {filename}: {e}")
        return False


# ─────────────────────────────────────────────
# HELPER: Create a clean filename from circular number
# ─────────────────────────────────────────────

def make_filename(circular_number):
    """
    Convert 'RBI/2024-25/73' → 'RBI_2024-25_73.pdf'
    Removes characters that are invalid in filenames.
    """
    clean = re.sub(r"[^\w\-]", "_", circular_number)
    clean = re.sub(r"_+", "_", clean)  # collapse multiple underscores
    return f"{clean}.pdf"


# ─────────────────────────────────────────────
# MAIN SCRAPER FUNCTION
# ─────────────────────────────────────────────

def run_scraper():
    """
    Main function — orchestrates the full scraping pipeline.
    """
    print("=" * 60)
    print("RBI Circular Scraper")
    print(f"Years: {YEARS_TO_SCRAPE}")
    print(f"Saving PDFs to: {RAW_DIR}")
    print(f"Saving metadata to: {METADATA_FILE}")
    print("=" * 60)

    # Load existing metadata so we don't re-download
    metadata = load_existing_metadata()
    print(f"\n📂 Already have {len(metadata)} circulars in metadata.")

    total_downloaded = 0
    total_skipped = 0
    total_failed = 0

    for year in YEARS_TO_SCRAPE:

        # Get all circular listings for this year
        circulars = get_circular_links_for_year(year)
        polite_sleep()

        for i, circular in enumerate(circulars):
            circ_num = circular["circular_number"]
            print(f"\n[{i+1}/{len(circulars)}] {circ_num}")
            print(f"  📄 {circular['title'][:70]}...")

            # Skip if already in metadata (already downloaded before)
            if circ_num in metadata:
                print(f"  ⏭️  Already in metadata, skipping.")
                total_skipped += 1
                continue

            # Visit detail page to get PDF URL
            pdf_url = get_pdf_url_from_detail_page(circular["detail_url"])
            polite_sleep()

            if not pdf_url:
                print(f"  ❌ No PDF found for {circ_num}")
                total_failed += 1
                continue

            # Create a clean filename
            filename = make_filename(circ_num)

            # Download the PDF
            success = download_pdf(pdf_url, filename)
            polite_sleep()

            if success:
                # Save metadata for this circular
                metadata[circ_num] = {
                    "circular_number": circ_num,
                    "title": circular["title"],
                    "date": circular["date"],
                    "department": circular["department"],
                    "detail_url": circular["detail_url"],
                    "pdf_url": pdf_url,
                    "pdf_filename": filename,
                    "scraped_at": datetime.now().isoformat(),
                }

                # Save metadata after every download
                # (so progress isn't lost if scraper crashes)
                save_metadata(metadata)
                total_downloaded += 1
            else:
                total_failed += 1

    # Final summary
    print("\n" + "=" * 60)
    print("SCRAPING COMPLETE")
    print(f"  ✅ Downloaded: {total_downloaded}")
    print(f"  ⏭️  Skipped (already had): {total_skipped}")
    print(f"  ❌ Failed: {total_failed}")
    print(f"  📂 Total in metadata: {len(metadata)}")
    print("=" * 60)


# ─────────────────────────────────────────────
# RUN
# ─────────────────────────────────────────────

if __name__ == "__main__":
    run_scraper()