import os
import random

text_dir = "data/processed/texts"
files = os.listdir(text_dir)

# Check 3 random files
samples = random.sample(files, 3)

for fname in samples:
    path = os.path.join(text_dir, fname)
    content = open(path, encoding="utf-8").read()
    print(f"{'='*50}")
    print(f"File: {fname}")
    print(f"Length: {len(content):,} chars")
    print(f"Preview:\n{content[:300]}")
    print()