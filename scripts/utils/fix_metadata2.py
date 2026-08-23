# fix_metadata2.py
# Fixes title, date, and department for all 317 records
# Run once — then delete this file

import re
import json
import time
import random
import requests
from bs4 import BeautifulSoup

METADATA_FILE = "data/processed/metadata.json"
DETAIL_URL    = "https://www.rbi.org.in/Scripts/BS_CircularIndexDisplay.aspx?Id={}"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
}

# ─────────────────────────────────────────────
# EXTRACTION FUNCTIONS
# ─────────────────────────────────────────────

def extract_title(soup):
    """
    RBI detail pages always have exactly one <b> tag
    containing the circular title.
    """
    # Primary: single <b> tag — always contains the title
    b_tag = soup.find("b")
    if b_tag:
        title = b_tag.get_text(strip=True)
        if len(title) > 10:
            return title[:300]

    # Fallback: Subject: line
    full_text = soup.get_text(separator="\n", strip=True)
    subject_match = re.search(
        r'(?:Subject|Sub)\s*[:\-]\s*(.+?)(?:\n|$)',
        full_text, re.IGNORECASE
    )
    if subject_match:
        title = subject_match.group(1).strip()
        if len(title) > 10:
            return title[:300]

    return ""


def extract_date(soup, full_text):
    """
    Extract date using three strategies in order of reliability.
    """
    # Strategy 1: <p align="right"> — most reliable on RBI pages
    for p in soup.find_all("p", align="right"):
        p_text = p.get_text(strip=True)
        if re.search(r'20\d{2}', p_text):
            return p_text.strip()

    # Strategy 2: Month DD, YYYY format
    m = re.search(
        r'(January|February|March|April|May|June|July|August|'
        r'September|October|November|December)\s+\d{1,2},?\s+20\d{2}',
        full_text
    )
    if m:
        return m.group(0)

    # Strategy 3: DD.MM.YYYY — validate day <= 31 and month <= 12
    m = re.search(r'(\d{1,2}[./]\d{1,2}[./]20\d{2})', full_text)
    if m:
        parts = re.split(r'[./]', m.group(1))
        try:
            if int(parts[0]) <= 31 and int(parts[1]) <= 12:
                return m.group(1)
        except (ValueError, IndexError):
            pass

    return None


def extract_year(date_str):
    """Extract 4-digit year from any date string."""
    if not date_str:
        return "unknown"
    m = re.search(r'(20\d{2})', date_str)
    return m.group(1) if m else "unknown"


def extract_department(full_text):
    """Extract department name from page text."""
    patterns = [
        r'(Department of [A-Za-z\s&,\-]+?)(?:,|\n|Central Office)',
        r'(Financial [A-Za-z\s]+ Department)',
        r'([A-Z][a-z]+ Markets [A-Za-z\s]+Department)',
    ]
    for pattern in patterns:
        m = re.search(pattern, full_text)
        if m:
            candidate = m.group(1).strip()
            if len(candidate) > 5:
                return candidate[:100]
    return "Unknown"


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

def main():
    with open(METADATA_FILE, "r", encoding="utf-8") as f:
        records = json.load(f)

    print(f"Total records: {len(records)}")

    # Show current state before fixing
    bad_titles = [
        r for r in records
        if not r.get('title') or 'Index To RBI' in r.get('title', '')
    ]
    bad_dates = [
        r for r in records
        if not r.get('date') or
        r.get('date') == 'unknown' or
        re.match(r'^[3-5]\d\.', r.get('date', ''))
    ]
    unknown_depts = [r for r in records if r.get('department') == 'Unknown']

    print(f"Bad titles:        {len(bad_titles)}")
    print(f"Bad dates:         {len(bad_dates)}")
    print(f"Unknown depts:     {len(unknown_depts)}")
    print(f"\nFixing all {len(records)} records...")
    print("Estimated time: ~30 minutes\n")

    session = requests.Session()
    session.headers.update(HEADERS)

    fixed  = 0
    failed = 0

    for i, record in enumerate(records):
        cid  = record.get("circular_id")
        circ = record.get("circular_number", "")

        print(f"[{i+1}/{len(records)}] ID {cid} | {circ[:40]}", end=" → ")

        url = DETAIL_URL.format(cid)

        try:
            response  = session.get(url, timeout=20)
            response.raise_for_status()
            soup      = BeautifulSoup(response.text, "html.parser")
            full_text = soup.get_text(separator=" ", strip=True)

            # ── Fix title ────────────────────────
            new_title = extract_title(soup)
            if new_title:
                record["title"] = new_title
                print(f"{new_title[:60]}")
            else:
                print(f"(title not found)")

            # ── Fix date if bad ──────────────────
            current_date = record.get("date", "")
            is_bad_date  = (
                not current_date or
                current_date == "unknown" or
                bool(re.match(r'^[3-5]\d\.', current_date))
            )
            if is_bad_date:
                new_date = extract_date(soup, full_text)
                if new_date:
                    record["date"] = new_date
                    record["year"] = extract_year(new_date)
                    print(f"  Date fixed: {new_date}")

            # ── Fix unknown department ───────────
            if record.get("department") == "Unknown":
                new_dept = extract_department(full_text)
                if new_dept != "Unknown":
                    record["department"] = new_dept

            fixed += 1

        except Exception as e:
            print(f"FAILED: {type(e).__name__}: {str(e)[:60]}")
            failed += 1

            # Refresh session on network errors
            session = requests.Session()
            session.headers.update(HEADERS)

        # Save progress every 10 records
        if (i + 1) % 10 == 0:
            with open(METADATA_FILE, "w", encoding="utf-8") as f:
                json.dump(records, f, indent=2, ensure_ascii=False)
            print(f"  💾 Progress saved ({i+1}/{len(records)})")

        # Polite delay
        time.sleep(random.uniform(3.0, 5.0))

    # Final save
    with open(METADATA_FILE, "w", encoding="utf-8") as f:
        json.dump(records, f, indent=2, ensure_ascii=False)

    # ── Verification ─────────────────────────────
    print(f"\n{'='*50}")
    print("VERIFICATION AFTER FIX")
    print(f"{'='*50}")

    still_bad_titles = [
        r for r in records
        if not r.get('title') or 'Index To RBI' in r.get('title', '')
    ]
    still_bad_dates = [
        r for r in records
        if not r.get('date') or
        r.get('date') == 'unknown' or
        re.match(r'^[3-5]\d\.', r.get('date', ''))
    ]
    still_unknown_depts = [
        r for r in records
        if r.get('department') == 'Unknown'
    ]

    print(f"Fixed:              {fixed}")
    print(f"Failed:             {failed}")
    print(f"Still bad titles:   {len(still_bad_titles)}")
    print(f"Still bad dates:    {len(still_bad_dates)}")
    print(f"Still unknown dept: {len(still_unknown_depts)}")

    # Show sample of fixed records
    print(f"\nSample fixed records:")
    for r in records[:3]:
        print(f"  {r['circular_number']}")
        print(f"    Title: {r.get('title', '')[:70]}")
        print(f"    Date:  {r.get('date', '')}")
        print(f"    Dept:  {r.get('department', '')[:50]}")
        print()

    if still_bad_titles:
        print(f"Sample still-bad titles:")
        for r in still_bad_titles[:3]:
            print(f"  {r['circular_number']} → {r.get('title', 'EMPTY')[:60]}")


if __name__ == "__main__":
    main()