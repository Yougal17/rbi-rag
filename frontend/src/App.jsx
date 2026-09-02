import { useState } from "react";
import axios from "axios";
import "./App.css";

const API_URL = import.meta.env.VITE_API_URL || "http://localhost:8000" || "https://rbi-proxy.yougalattri17.workers.dev";

function App() {
  const [question, setQuestion] = useState("");
  const [yearFilter, setYearFilter] = useState("");
  const [loading, setLoading] = useState(false);
  const [result, setResult] = useState(null);
  const [error, setError] = useState(null);

  const handleSubmit = async (e) => {
    e.preventDefault();
    if (!question.trim() || question.trim().length < 10) {
      setError("Please enter a question with at least 10 characters.");
      return;
    }

    setLoading(true);
    setError(null);
    setResult(null);

    try {
      const response = await axios.post(`${API_URL}/query`, {
        question: question.trim(),
        year_filter: yearFilter || null,
        top_k: 5,
      });
      setResult(response.data);
    } catch (err) {
      if (err.response?.data?.detail) {
        setError(err.response.data.detail);
      } else {
        setError("Failed to connect to the API. Make sure the backend is running.");
      }
    } finally {
      setLoading(false);
    }
  };

  const handleExampleClick = (q) => {
    setQuestion(q);
  };

  const examples = [
    "What are the KYC requirements for doorstep banking services?",
    "What change did RBI make to the Urban Co-operative Bank tier framework?",
    "What penalties can RBI impose on banks for currency chest violations?",
    "What is the purpose of the Line of Credit extended to the Government of Maldives?",
  ];

  return (
    <div className="app">
      {/* Header */}
      <header className="header">
        <div className="header-content">
          <div className="logo">
            <span className="logo-icon">🏛️</span>
            <div>
              <h1>RBI Circular Intelligence</h1>
              <p>Ask questions about RBI circulars (2022–2024)</p>
            </div>
          </div>
          <div className="stats">
            <span>317 Circulars</span>
            <span>4,368 Chunks</span>
            <span>2022–2024</span>
          </div>
        </div>
      </header>

      <main className="main">
        {/* Search Form */}
        <section className="search-section">
          <form onSubmit={handleSubmit} className="search-form">
            <div className="search-row">
              <input
                type="text"
                value={question}
                onChange={(e) => setQuestion(e.target.value)}
                placeholder="Ask a question about RBI circulars..."
                className="search-input"
                disabled={loading}
              />
              <select
                value={yearFilter}
                onChange={(e) => setYearFilter(e.target.value)}
                className="year-select"
                disabled={loading}
              >
                <option value="">All Years</option>
                <option value="2022">2022</option>
                <option value="2023">2023</option>
                <option value="2024">2024</option>
              </select>
              <button
                type="submit"
                className="search-btn"
                disabled={loading}
              >
                {loading ? "Searching..." : "Search"}
              </button>
            </div>
          </form>

          {/* Example Questions */}
          {!result && !loading && (
            <div className="examples">
              <p className="examples-label">Try an example:</p>
              <div className="examples-list">
                {examples.map((ex, i) => (
                  <button
                    key={i}
                    className="example-btn"
                    onClick={() => handleExampleClick(ex)}
                  >
                    {ex}
                  </button>
                ))}
              </div>
            </div>
          )}
        </section>

        {/* Loading */}
        {loading && (
          <div className="loading">
            <div className="spinner"></div>
            <p>Searching through RBI circulars...</p>
            <p className="loading-sub">
              If the server was idle, first request may take 45-60 seconds to warm up.
              Subsequent queries will be fast.
              <br>
              Running hybrid retrieval → cross-encoder reranking → generating answer
              </br>
            </p>
          </div>
        )}

        {/* Error */}
        {error && (
          <div className="error">
            <span>⚠️</span>
            <p>{error}</p>
          </div>
        )}

        {/* Result */}
        {result && (
          <div className="result">
            {/* Answer */}
            <section className="answer-section">
              <div className="section-header">
                <h2>Answer</h2>
                <span className="timing">
                  ⏱ {result.timing?.total || 0}s
                </span>
              </div>
              <div className="answer-text">
                {result.answer}
              </div>
            </section>

            {/* Sources */}
            {result.sources && result.sources.length > 0 && (
              <section className="sources-section">
                <h2>Sources ({result.total_sources})</h2>
                <div className="sources-grid">
                  {result.sources.map((source, i) => (
                    <div key={i} className="source-card">
                      <div className="source-header">
                        <span className="source-number">
                          {source.circular_number}
                        </span>
                        <span className="source-date">{source.date}</span>
                      </div>
                      <p className="source-title">{source.title}</p>
                      {source.department && source.department !== "Unknown" && (
                        <p className="source-dept">{source.department}</p>
                      )}

                      <a
                        href={source.url}
                        target="_blank"
                        rel="noopener noreferrer"
                        className="source-link"
                      >
                        View Circular →
                      </a>
                    </div>
                  ))}
                </div>
              </section>
            )}

            {/* Ask Another */}
            <button
              className="reset-btn"
              onClick={() => {
                setResult(null);
                setQuestion("");
                setError(null);
              }}
            >
              Ask Another Question
            </button>
          </div>
        )}
      </main>

      {/* Footer */}
      <footer className="footer">
        <p>
          Built with Dense + BM25 + RRF + Cross-Encoder retrieval ·
          Powered by Gemini 2.5 Flash ·
          Data from RBI.org.in (2022–2024) ·
          Built by Yougal Attri ⭐
        </p>
      </footer>
    </div>
  );
}

export default App;