import requests
from bs4 import BeautifulSoup
import re
import json

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}

# Load metadata and pick 3 different circular IDs to inspect
data = json.load(open('data/processed/metadata.json'))
samples = data[:3]

for record in samples:
    cid = record['circular_id']
    url = f"https://www.rbi.org.in/Scripts/BS_CircularIndexDisplay.aspx?Id={cid}"
    
    print(f"\n{'='*60}")
    print(f"ID: {cid} | Circular: {record['circular_number']}")
    print(f"{'='*60}")
    
    response = requests.get(url, headers=HEADERS, timeout=20)
    soup = BeautifulSoup(response.text, "html.parser")
    
    # Check <title> tag
    title_tag = soup.find("title")
    print(f"<title> tag: {title_tag.get_text(strip=True) if title_tag else 'NOT FOUND'}")
    
    # Check for Subject: line
    full_text = soup.get_text(separator="\n", strip=True)
    subject_match = re.search(
        r'(?:Subject|Sub)\s*[:\-]\s*(.+?)(?:\n|$)',
        full_text, re.IGNORECASE
    )
    print(f"Subject line: {subject_match.group(1)[:100] if subject_match else 'NOT FOUND'}")
    
    # Check h2/h3/h4
    for tag in ["h2", "h3", "h4"]:
        found = soup.find(tag)
        if found:
            print(f"<{tag}>: {found.get_text(strip=True)[:100]}")
    
    # Check bold tags
    bold_tags = soup.find_all("b")
    print(f"Bold tags found: {len(bold_tags)}")
    for b in bold_tags[:3]:
        print(f"  <b>: {b.get_text(strip=True)[:80]}")
    
    # Print first 30 lines of page text
    lines = [l.strip() for l in full_text.split('\n') if l.strip()]
    print(f"\nFirst 20 non-empty lines:")
    for i, line in enumerate(lines[:20], 1):
        print(f"  {i}: {line[:100]}")
    
    import time
    time.sleep(3)