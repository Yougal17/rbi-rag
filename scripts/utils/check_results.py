import json

with open('evaluation/results.json') as f:
    results = json.load(f)

print(f"Total results: {len(results)}")
print(f"\nSample answers:")
for i, r in enumerate(results[:5]):
    print(f"\n[{i+1}] Question: {r['question'][:60]}...")
    print(f"     Answer:   {repr(r['answer'][:100])}")
    print(f"     Contexts: {len(r['contexts'])} chunks")