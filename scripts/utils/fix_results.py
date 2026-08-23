import json

with open('evaluation/results.json', 'r') as f:
    results = json.load(f)

# Keep only successful answers
good = [r for r in results if r['answer'] and 'Generation error' not in r['answer']]
print(f"Good results: {len(good)}/{len(results)}")

with open('evaluation/results.json', 'w') as f:
    json.dump(good, f, indent=2)