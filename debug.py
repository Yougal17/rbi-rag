# debug.py
import requests
from bs4 import BeautifulSoup
import time

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (compatible; RBI-RAG-Research-Bot/1.0; "
        "Educational project - contact yougalattri17@gmail.com)"
    )
}

BASE_URL = "https://www.rbi.org.in/Scripts/"

def check_circular_id(circular_id):
    url = f"{BASE_URL}BS_CircularIndexDisplay.aspx?Id={circular_id}"
    try:
        response = requests.get(url, headers=HEADERS, timeout=15)
        soup = BeautifulSoup(response.text, "html.parser")
        
        # Look for date on the page
        # RBI detail pages show the circular date prominently
        text = soup.get_text()
        
        # Find circular number pattern
        import re
        circ_match = re.search(r'RBI/(\d{4}-\d{2,4}/\d+)', text)
        date_match = re.search(r'(\d{1,2}[./]\d{1,2}[./]20\d{2})', text)
        
        circ_num = circ_match.group(0) if circ_match else "unknown"
        date = date_match.group(0) if date_match else "unknown"
        
        return circ_num, date
    except:
        return "error", "error"

# Binary search approach — check IDs at intervals to find 2022 boundary
# We know ~13400 = April 2026
# Let's probe backwards to find where 2022 starts

probe_ids = [13400, 13000, 12500, 12000, 11500, 11000, 10500, 10000]

for pid in probe_ids:
    circ_num, date = check_circular_id(pid)
    print(f"ID {pid}: {circ_num} | {date}")
    time.sleep(2)