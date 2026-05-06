import re
import json
import time
import random
import requests
from bs4 import BeautifulSoup

METADATA_FILE = "data/processed/metadata.json"
DETAIL_URL = "https://www.rbi.org.in/Scripts/BS_CircularIndexDisplay.aspx?Id={}"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-IN,en;q=0.9",
}


def extract_date(soup, full_text):
    """Try all strategies to find the date."""

    # Strategy 1: <p align="right"> — most reliable
    for p in soup.find_all("p", align="right"):
        p_text = p.get_text(strip=True)
        if re.search(r'20\d{2}', p_text):
            return p_text

    # Strategy 2: DD.MM.YYYY or DD/MM/YYYY
    m = re.search(r'(\d{1,2}[./]\d{1,2}[./]20\d{2})', full_text)
    if m:
        return m.group(1)

    # Strategy 3: Month DD, YYYY
    m = re.search(
        r'(January|February|March|April|May|June|July|August|'
        r'September|October|November|December)\s+\d{1,2},\s+20\d{2}',
        full_text
    )
    if m:
        return m.group(0)

    return "unknown"


def extract_year(date_str):
    m = re.search(r'(20\d{2})', date_str)
    return m.group(1) if m else "unknown"


def main():
    with open(METADATA_FILE, "r", encoding="utf-8") as f:
        records = json.load(f)

    # Only fix records where date is unknown
    to_fix = [r for r in records if r.get("date") == "unknown"]
    print(f"Records to fix: {len(to_fix)} / {len(records)}")

    session = requests.Session()
    session.headers.update(HEADERS)

    fixed = 0
    failed = 0

    for i, record in enumerate(to_fix):
        cid = record.get("circular_id")
        print(f"[{i+1}/{len(to_fix)}] Fixing ID {cid}...", end=" ")

        url = DETAIL_URL.format(cid)
        try:
            response = session.get(url, timeout=20)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "html.parser")
            full_text = soup.get_text(separator=" ", strip=True)

            date = extract_date(soup, full_text)
            year = extract_year(date)

            record["date"] = date
            record["year"] = year
            print(f"→ {date} ({year})")
            fixed += 1

        except Exception as e:
            print(f"→ FAILED: {type(e).__name__}")
            failed += 1

        # Save progress every 10 records
        if (i + 1) % 10 == 0:
            with open(METADATA_FILE, "w", encoding="utf-8") as f:
                json.dump(records, f, indent=2, ensure_ascii=False)
            print(f"  💾 Progress saved ({i+1}/{len(to_fix)})")

        time.sleep(random.uniform(3.0, 5.0))

    # Final save
    with open(METADATA_FILE, "w", encoding="utf-8") as f:
        json.dump(records, f, indent=2, ensure_ascii=False)

    print(f"\n✅ Fixed: {fixed}")
    print(f"❌ Failed: {failed}")
    print(f"💾 Metadata saved.")


if __name__ == "__main__":
    main()