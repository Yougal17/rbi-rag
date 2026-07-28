FROM python:3.11-slim

WORKDIR /app

# System dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    git \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.cloud.txt .
RUN pip install --no-cache-dir -r requirements.cloud.txt

# Pre-download ML models during build
# This bakes them into the image — no download at runtime
RUN python -c "
from sentence_transformers import SentenceTransformer, CrossEncoder
import os
print('Downloading embedding model...')
SentenceTransformer('sentence-transformers/all-mpnet-base-v2')
print('Downloading cross-encoder...')
CrossEncoder('cross-encoder/ms-marco-MiniLM-L-6-v2')
print('All models downloaded.')
"

# Copy application code
COPY backend/ ./backend/
COPY generation/ ./generation/
COPY retrieval/ ./retrieval/
COPY vectordb/ ./vectordb/
COPY data/processed/chunks.json ./data/processed/chunks.json
COPY data/processed/metadata.json ./data/processed/metadata.json

# Cloud Run uses PORT environment variable
ENV PORT=8080
EXPOSE 8080

CMD uvicorn backend.main:app --host 0.0.0.0 --port $PORT --workers 1