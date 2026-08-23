import json

with open("data/processed/metadata.json", "r", encoding="utf-8") as f:
    data = json.load(f)

for r in sorted(data, key=lambda x: len(x.get("title", "")), reverse=True)[:20]:
    print(
        f"{r['circular_number']} | {r['date']} | {r['title'][:70]}"
    )