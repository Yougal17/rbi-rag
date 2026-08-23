import json
from pathlib import Path

records = json.load(open('data/processed/metadata.json'))
raw_dir = Path('data/raw')

def is_real_pdf(p):
    try:
        return open(p, 'rb').read(4) == b'%PDF'
    except:
        return False

failed = [
    r for r in records
    if r.get('pdf_filename') and (
        not (raw_dir / r['pdf_filename']).exists() or
        not is_real_pdf(str(raw_dir / r['pdf_filename']))
    )
]

print(f"Failed: {len(failed)}\n")
for r in failed:
    print(f"Circular : {r['circular_number']}")
    print(f"Save as  : {r['pdf_filename']}")
    print(f"URL      : {r['pdf_url']}")
    print()