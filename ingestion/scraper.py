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


