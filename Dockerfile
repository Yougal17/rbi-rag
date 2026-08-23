FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y gcc g++ git && rm -rf /var/lib/apt/lists/*

# Install PyTorch first separately — it's the largest package
RUN pip install --no-cache-dir --timeout=300 torch==2.3.1 --index-url https://download.pytorch.org/whl/cpu

# Install remaining dependencies
COPY requirements.cloud.txt .
RUN pip install --no-cache-dir --timeout=300 -r requirements.cloud.txt

# Pre-download ML models
RUN python -c "from sentence_transformers import SentenceTransformer, CrossEncoder; SentenceTransformer('sentence-transformers/all-mpnet-base-v2'); CrossEncoder('cross-encoder/ms-marco-MiniLM-L-6-v2'); print('Models ready.')"

COPY backend/ ./backend/
COPY generation/ ./generation/
COPY retrieval/ ./retrieval/
COPY vectordb/ ./vectordb/
COPY data/processed/chunks.json ./data/processed/chunks.json
COPY data/processed/metadata.json ./data/processed/metadata.json

ENV PORT=8080
EXPOSE 8080

CMD uvicorn backend.main:app --host 0.0.0.0 --port $PORT --workers 1