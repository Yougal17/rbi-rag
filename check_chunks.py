import json

chunks = json.load(open('data/processed/chunks.json'))

parent_sizes = [len(c['parent_text'].split()) for c in chunks]

over_800  = sum(1 for s in parent_sizes if s > 800)
under_800 = sum(1 for s in parent_sizes if s <= 800)
avg_size  = sum(parent_sizes) // len(parent_sizes)
max_size  = max(parent_sizes)
min_size  = min(parent_sizes)

print(f"Total chunks:       {len(chunks)}")
print(f"Over 800 words:     {over_800}")
print(f"Under 800 words:    {under_800}")
print(f"Average parent:     {avg_size} words")
print(f"Largest parent:     {max_size} words")
print(f"Smallest parent:    {min_size} words")