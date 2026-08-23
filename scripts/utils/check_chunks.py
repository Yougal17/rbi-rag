import json
import random

chunks = json.load(open('data/processed/chunks.json'))

print(f"Total chunks: {len(chunks)}")
print(f"\n--- 3 Random Sample Chunks ---\n")

samples = random.sample(chunks, 3)
for i, c in enumerate(samples, 1):
    print(f"Sample {i}:")
    print(f"  Circular:   {c['circular_number']}")
    print(f"  Date:       {c['date']}")
    print(f"  Department: {c['department'][:50]}")
    print(f"  Child text: {c['child_text'][:200]}")
    print(f"  Parent size: {len(c['parent_text'].split())} words")
    print()