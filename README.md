# RBI Circular Intelligence System

A production-grade Retrieval-Augmented Generation (RAG) system that answers natural language questions about Reserve Bank of India circulars with verifiable, cited answers.

**Live Demo:** [rbi-circular-intelligence.vercel.app](https://rbi-circular-intelligence.vercel.app)  
**API Docs:** [rbi-backend.koyeb.app/docs](https://rbi-backend.koyeb.app/docs)

---

## What It Does

Ask questions like:
- *"What are the KYC requirements for doorstep banking services?"*
- *"What change did RBI make to the Urban Co-operative Bank tier framework?"*
- *"What penalties can RBI impose on banks for currency chest violations?"*

Get cited answers pointing to specific circular numbers, dates, departments, and source URLs — all verifiable at rbi.org.in.

---

## Evaluation Results

Evaluated on 30 questions across 12 regulatory categories:

| Metric | Score |
|--------|-------|
| Answer Relevancy | **0.96** |
| Context Precision | **0.91** |
| Context Recall | **0.73** |
| Faithfulness | **0.50** |
| **Overall Average** | **0.775** |

> Faithfulness score reflects data coverage gaps (some questions reference circulars outside the 2022–2024 dataset), not hallucination.

---

## Architecture

```
User Question
     ↓
React Frontend (Vite)
     ↓
FastAPI Backend
     ↓
┌─────────────────────────────────────────┐
│           Retrieval Pipeline            │
│                                         │
│  Dense Retrieval (Qdrant + mpnet-base)  │
│               +                         │
│  Sparse Retrieval (BM25)                │
│               ↓                         │
│  Reciprocal Rank Fusion (RRF)           │
│               ↓                         │
│  Cross-Encoder Reranking                │
└─────────────────────────────────────────┘
     ↓
Top 5 Relevant Chunks
     ↓
Gemini 2.5 Flash (citation-grounded prompt)
     ↓
Cited Answer + Source Cards
```

---

## Tech Stack

| Component | Technology |
|-----------|-----------|
| Scraping | Python, Requests, BeautifulSoup, Playwright |
| PDF Parsing | PyMuPDF |
| Chunking | Custom hierarchical chunker |
| Embeddings | sentence-transformers/all-mpnet-base-v2 |
| Vector Database | Qdrant |
| Sparse Retrieval | BM25 (rank_bm25) |
| Reranking | cross-encoder/ms-marco-MiniLM-L-6-v2 |
| LLM | Google Gemini 2.5 Flash |
| Backend | FastAPI + Uvicorn |
| Frontend | React + Vite |
| Evaluation | Ragas (manual sequential) |

---

## Dataset

- **Source:** rbi.org.in (public government website)
- **Coverage:** 317 circulars from 2022–2024
- **Distribution:** 2022 (115) | 2023 (127) | 2024 (75)
- **Chunks:** 4,368 hierarchical chunks
- **Vectors:** 4,368 × 768 dimensions

---

## Project Structure

```
rbi-rag/
├── backend/
│   └── main.py                  # FastAPI application
├── generation/
│   ├── prompt.py                # Prompt templates + citation logic
│   └── generator.py             # Gemini API integration
├── retrieval/
│   ├── dense.py                 # Vector similarity search
│   ├── sparse.py                # BM25 keyword search
│   ├── hybrid.py                # RRF fusion
│   └── reranker.py              # Cross-encoder reranking
├── ingestion/
│   ├── scraper.py               # RBI website scraper
│   ├── pdf_downloader.py        # Playwright PDF downloader
│   ├── parser.py                # PDF text extraction
│   └── chunker.py               # Hierarchical chunking
├── embeddings/
│   └── embedder.py              # Vector embedding generation
├── vectordb/
│   └── qdrant_store.py          # Qdrant storage + indexing
├── evaluation/
│   ├── testset.json             # 30 test questions
│   ├── evaluate_ragas.py        # Official Ragas (10 questions)
│   └── evaluate_manual.py       # Manual evaluation (30 questions)
├── frontend/
│   └── src/
│       ├── App.jsx              # Main React component
│       └── App.css              # Styles
├── scripts/
│   ├── migrate_to_cloud.py      # Migrate Qdrant to cloud
│   └── utils/                   # Development utility scripts
├── data/
│   ├── raw/                     # Downloaded PDFs (git-ignored)
│   └── processed/
│       ├── metadata.json        # Circular metadata
│       ├── chunks.json          # All chunks
│       └── texts/               # Parsed text files (git-ignored)
├── Dockerfile                   # Cloud deployment
├── requirements.txt             # Full dependencies
├── requirements.cloud.txt       # Cloud deployment dependencies
└── .env                         # API keys (git-ignored)
```

---

## Setup Instructions

### Prerequisites

- Python 3.11+
- Node.js 18+
- Docker Desktop
- Git

### 1. Clone the Repository

```bash
git clone https://github.com/YOUR-USERNAME/rbi-rag.git
cd rbi-rag
```

### 2. Create Virtual Environment

```bash
python -m venv venv

# Windows
venv\Scripts\activate

# Mac/Linux
source venv/bin/activate
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

### 4. Configure Environment Variables

Create a `.env` file in the project root:

```env
# Required
GEMINI_API_KEY=your-gemini-api-key-here
GEMINI_MODEL=gemini-2.5-flash

# For local development (Docker Qdrant)
# Leave QDRANT_URL empty to use local Docker

# For cloud deployment (Qdrant Cloud)
QDRANT_URL=https://your-cluster.cloud.qdrant.io
QDRANT_API_KEY=your-qdrant-api-key
COLLECTION_NAME=qdrant-rbi
```

Get your Gemini API key at: https://aistudio.google.com/app/apikey

### 5. Start Qdrant (Local Development)

```bash
docker run -d \
  --name qdrant-rbi \
  -p 6333:6333 \
  -v qdrant_storage:/qdrant/storage \
  qdrant/qdrant
```

On Windows PowerShell:
```powershell
docker run -d --name qdrant-rbi -p 6333:6333 -v qdrant_storage:/qdrant/storage qdrant/qdrant
```

### 6. Collect Data (Skip if using pre-built data)

> **Note:** The `data/raw/` and `data/processed/texts/` folders are git-ignored due to size. You need to either run the data pipeline or obtain the pre-built `chunks.json` and `metadata.json` files.

```bash
# Step 1: Scrape RBI website
python ingestion/scraper.py

# Step 2: Download PDFs (Playwright-based, bypasses Cloudflare)
python ingestion/pdf_downloader.py

# Step 3: Parse PDFs to text
python ingestion/parser.py

# Step 4: Create hierarchical chunks
python ingestion/chunker.py
```

### 7. Generate Embeddings

```bash
python embeddings/embedder.py
```

> Takes ~27 minutes on CPU. Run once — results cached in `embeddings.npz`.

### 8. Load into Qdrant

```bash
python vectordb/qdrant_store.py
```

### 9. Run the Backend

```bash
python backend/main.py
```

API will be available at: http://localhost:8000  
Interactive docs at: http://localhost:8000/docs

### 10. Run the Frontend

```bash
cd frontend
npm install
npm run dev
```

Frontend will be available at: http://localhost:5173

---

## Quick Start (If You Have Pre-built Data)

If you have `chunks.json`, `metadata.json`, and `embeddings.npz`:

```bash
# 1. Start Qdrant
docker start qdrant-rbi

# 2. Load into Qdrant (if not already loaded)
python vectordb/qdrant_store.py

# 3. Start backend
python backend/main.py

# 4. Start frontend (new terminal)
cd frontend && npm run dev
```

---

## API Reference

### POST /query

Ask a question about RBI circulars.

**Request:**
```json
{
  "question": "What are the KYC requirements for doorstep banking?",
  "year_filter": "2022",
  "top_k": 5
}
```

**Response:**
```json
{
  "answer": "As per RBI circular RBI/2022-23/66 dated June 8, 2022...",
  "sources": [
    {
      "circular_number": "RBI/2022-23/66",
      "title": "Section 23 of the Banking Regulation Act...",
      "date": "June 8, 2022",
      "department": "Department of Supervision",
      "url": "https://www.rbi.org.in/..."
    }
  ],
  "total_sources": 1,
  "timing": {"retrieval": 1.2, "generation": 4.5, "total": 5.7},
  "query": "What are the KYC requirements for doorstep banking?"
}
```

### GET /health

```json
{
  "status": "healthy",
  "model": "gemini-2.5-flash",
  "chunks": 4368,
  "circulars": 317,
  "uptime": 142.3
}
```

### GET /stats

```json
{
  "total_chunks": 4368,
  "total_circulars": 317,
  "years_covered": ["2022", "2023", "2024"],
  "model": "gemini-2.5-flash",
  "retrieval": "Dense + BM25 + RRF + Cross-Encoder"
}
```

---

## Evaluation

Run the evaluation suite on 30 test questions:

```bash
# Manual sequential evaluation (recommended — works with free API tier)
python evaluation/evaluate_manual.py

# Official Ragas evaluation (10 questions)
python evaluation/evaluate_ragas.py
```

Results saved to `evaluation/manual_report.json` and `evaluation/ragas_10_report.json`.

---

## Cloud Deployment

### Qdrant Cloud
1. Create free cluster at https://cloud.qdrant.io
2. Run migration: `python scripts/migrate_to_cloud.py`

### Backend (Koyeb / Cloud Run)
```bash
# Build Docker image
docker build -t rbi-backend .

# Push to registry and deploy
# See deployment guide in scripts/deploy.md
```

### Frontend (Vercel)
```bash
cd frontend
vercel --prod
```

---

## Key Engineering Decisions

**Why hierarchical chunking over fixed-size?**  
RBI circulars have explicit numbered sections. Splitting on section boundaries preserves regulatory context. Child chunks (~120 words) enable precise retrieval; parent chunks (~600 words) provide full context for generation.

**Why hybrid retrieval (Dense + BM25)?**  
Dense retrieval finds semantically similar content but misses exact legal terms. BM25 catches exact matches like "UAPA section 51A" that dense retrieval would miss. RRF merges both without requiring score normalization across different scales.

**Why cross-encoder reranking?**  
Bi-encoders encode query and chunk independently — losing interaction signals. Cross-encoders read both together, providing significantly more accurate relevance scoring. Running on only 15 candidates (not all 4,368) keeps latency acceptable.

**Why refuse to answer out-of-scope questions?**  
For regulatory compliance, a confident wrong answer is more dangerous than an honest "I don't know." The system cites every claim — if the source circular isn't in the dataset, no answer is generated.

---

## Known Limitations

- Dataset covers 2022–2024 only (317 circulars). Pre-2022 circulars and most Master Directions are not included.
- Faithfulness score (0.50) reflects data coverage gaps, not hallucination — the system correctly refuses to answer when data is insufficient.
- Cold start on cloud deployment takes 45–90 seconds as ML models load into memory.
- Hindi text in circulars is stripped during parsing (extracted as garbled characters due to PDF font encoding).

---

## Author

**Yougal Attri**  
[LinkedIn](https://linkedin.com/in/yougal-attri) | [GitHub](https://github.com/YOUR-USERNAME)

---

## License

This project is for educational and portfolio purposes.  
RBI circular data is sourced from rbi.org.in (public government data).
