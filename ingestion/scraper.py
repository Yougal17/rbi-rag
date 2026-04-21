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

