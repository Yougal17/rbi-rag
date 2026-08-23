# Official Ragas evaluation on 10 carefully selected questions

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json
import time
from dotenv import load_dotenv
from datasets import Dataset
from ragas import evaluate
from ragas.metrics import (
    faithfulness,
    answer_relevancy,
    context_precision,
    context_recall,
)
from ragas.llms import LangchainLLMWrapper
from ragas.embeddings import LangchainEmbeddingsWrapper
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_huggingface import HuggingFaceEmbeddings
from generation.generator import RBIGenerator

load_dotenv()

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
GEMINI_MODEL   = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")
RESULTS_FILE   = "evaluation/ragas_10_results.json"
REPORT_FILE    = "evaluation/ragas_10_report.json"

# ── 10 selected questions ────────────────────
SELECTED_QUESTIONS = [
    {
        "question": "What percentage of the contract value must be supplied from India under the Line of Credit for the Government of Maldives?",
        "ground_truth": "At least 75% of the contract price must consist of goods, works, and services supplied from India.",
        "source_circular": "RBI/2022-23/179",
        "category": "procurement"
    },
    {
        "question": "What is the amount of the Government of India-supported Line of Credit provided to the Government of the Kingdom of Eswatini?",
        "ground_truth": "The Government of India-supported Line of Credit provided to the Government of the Kingdom of Eswatini is USD 108.28 million.",
        "source_circular": "RBI/2022-23/114",
        "category": "line_of_credit"
    },
    {
        "question": "How much advance notice must a District Central Co-operative Bank give to customers before shifting or closing a branch?",
        "ground_truth": "The bank must inform customers at least two months in advance before shifting or closing a branch.",
        "source_circular": "RBI/2023-24/78",
        "category": "customer_notification"
    },
    {
        "question": "To what date did RBI extend the implementation timeline for certain provisions of the Master Direction on Credit Card and Debit Card Issuance and Conduct Directions 2022?",
        "ground_truth": "RBI extended the implementation timeline for the specified provisions from July 1 2022 to October 1 2022.",
        "source_circular": "RBI/2022-23/74",
        "category": "implementation"
    },
    {
        "question": "What is the maximum residual maturity allowed for receivables to qualify for exemption from the Minimum Holding Period requirement?",
        "ground_truth": "The residual maturity of the receivables must not be more than 90 days at the time of transfer.",
        "source_circular": "RBI/2023-24/99",
        "category": "eligibility"
    },
    {
        "question": "What change did RBI make to the regulatory categorization framework for Urban Co-operative Banks?",
        "ground_truth": "RBI replaced the existing two-tier regulatory framework with a four-tier regulatory framework for categorizing Urban Co-operative Banks based on their deposit size.",
        "source_circular": "RBI/2022-23/144",
        "category": "regulatory_framework"
    },
    {
        "question": "What should regulated entities do when an individual is designated as a terrorist under the Unlawful Activities Prevention Act UAPA 1967?",
        "ground_truth": "Regulated entities must follow the procedures prescribed under the Master Direction on KYC and the UAPA Order, report matching accounts to FIU-IND and the Ministry of Home Affairs, and ensure compliance with the updated UAPA schedules.",
        "source_circular": "RBI/2023-24/133",
        "category": "aml_kyc"
    },
    {
        "question": "What was the purpose of the EUR 100 million Short-Term Line of Credit extended by Exim Bank to Banco Exterior de Cuba?",
        "ground_truth": "The EUR 100 million Short-Term Line of Credit was provided to finance the procurement of rice from India by the Republic of Cuba.",
        "source_circular": "RBI/2022-23/133",
        "category": "line_of_credit"
    },
    {
        "question": "What change has RBI made regarding the types of instruments that can be invested in through overseas investment funds?",
        "ground_truth": "RBI has clarified that investments may be made not only in units but also in any other instrument by whatever name called issued by a duly regulated overseas investment fund.",
        "source_circular": "RBI/2024-25/41",
        "category": "overseas_investment"
    },
    {
        "question": "How should co-operative banks present unclaimed liabilities transferred to the Depositor Education and Awareness Fund in their financial statements?",
        "ground_truth": "Co-operative banks should present unclaimed liabilities transferred to the Depositor Education and Awareness Fund under Contingent Liabilities Others in their financial statements.",
        "source_circular": "RBI/2023-24/71",
        "category": "financial_reporting"
    },
]


def run_pipeline(generator):
    """Run RAG pipeline on 10 questions."""

    # Load existing if available
    if os.path.exists(RESULTS_FILE):
        with open(RESULTS_FILE) as f:
            existing = json.load(f)
        answered = {r["question"] for r in existing
                    if r.get("answer") and "Generation error" not in r["answer"]}
        if len(answered) >= len(SELECTED_QUESTIONS):
            print(f"✅ All 10 questions already answered")
            return existing
    else:
        existing = []
        answered = set()

    results  = list(existing)
    remaining = [q for q in SELECTED_QUESTIONS if q["question"] not in answered]

    print(f"Running pipeline on {len(remaining)} questions...")

    for i, item in enumerate(remaining):
        print(f"\n[{i+1}/{len(remaining)}] {item['question'][:65]}...")

        for attempt in range(3):
            try:
                result = generator.answer(query=item["question"])
                answer = result.get("answer", "")

                if not answer or "Generation error" in answer:
                    raise ValueError(f"Bad answer: {answer[:60]}")

                contexts = [c["child_text"] for c in result["chunks"]]

                results.append({
                    "question":        item["question"],
                    "answer":          answer,
                    "contexts":        contexts if contexts else [""],
                    "ground_truth":    item["ground_truth"],
                    "source_circular": item["source_circular"],
                    "category":        item["category"],
                })

                print(f"  ✅ {answer[:80]}...")
                break

            except Exception as e:
                if "429" in str(e):
                    wait = 60 * (attempt + 1)
                    print(f"  ⏳ Rate limited — waiting {wait}s...")
                    time.sleep(wait)
                else:
                    print(f"  ❌ {str(e)[:80]}")
                    break

        with open(RESULTS_FILE, "w") as f:
            json.dump(results, f, indent=2, ensure_ascii=False)

        if i < len(remaining) - 1:
            print(f"  ⏳ Waiting 45s...")
            time.sleep(45)

    return results


def run_ragas(results):
    """Run official Ragas evaluation."""

    print(f"\n{'='*60}")
    print("Running Official Ragas Evaluation (10 questions)...")
    print(f"{'='*60}\n")

    valid = [r for r in results
             if r.get("answer") and "Generation error" not in r["answer"]]

    print(f"Valid results: {len(valid)}")

    dataset = Dataset.from_dict({
        "question":     [r["question"]     for r in valid],
        "answer":       [r["answer"]       for r in valid],
        "contexts":     [r["contexts"]     for r in valid],
        "ground_truth": [r["ground_truth"] for r in valid],
    })

    llm = LangchainLLMWrapper(
        ChatGoogleGenerativeAI(
            model=GEMINI_MODEL,
            google_api_key=GEMINI_API_KEY,
            temperature=0.1,
        )
    )
    embeddings = LangchainEmbeddingsWrapper(
        HuggingFaceEmbeddings(
            model_name="sentence-transformers/all-mpnet-base-v2"
        )
    )

    faithfulness.llm            = llm
    answer_relevancy.llm        = llm
    answer_relevancy.embeddings = embeddings
    context_precision.llm       = llm
    context_recall.llm          = llm

    scores = evaluate(
        dataset=dataset,
        metrics=[
            faithfulness,
            answer_relevancy,
            context_precision,
            context_recall,
        ],
        raise_exceptions=False,
    )

    return scores


def print_report(scores):
    """Print and save scores."""
    import numpy as np

    df = scores.to_pandas()
    print("\nPer-question scores:")
    print(df[["faithfulness", "answer_relevancy",
              "context_precision", "context_recall"]].to_string())

    metrics = {}
    for col in ["faithfulness", "answer_relevancy",
                "context_precision", "context_recall"]:
        if col in df.columns:
            val = float(np.nanmean(df[col].values))
            metrics[col] = val if not np.isnan(val) else 0.0
        else:
            metrics[col] = 0.0

    avg = sum(v for v in metrics.values() if v > 0) / max(
        sum(1 for v in metrics.values() if v > 0), 1
    )

    print(f"\n{'='*60}")
    print("OFFICIAL RAGAS SCORES (10 questions)")
    print(f"{'='*60}")

    labels = {
        "faithfulness":      "Faithfulness",
        "answer_relevancy":  "Answer Relevancy",
        "context_precision": "Context Precision",
        "context_recall":    "Context Recall",
    }
    for key, label in labels.items():
        score = metrics[key]
        bar   = "█" * int(score * 20)
        empty = "░" * (20 - int(score * 20))
        print(f"  {label:22s}: {score:.4f} [{bar}{empty}]")

    print(f"\n  {'Overall Average':22s}: {avg:.4f}")

    report = {
        "method":              "Official Ragas",
        "questions_evaluated": 10,
        "scores":              metrics,
        "overall_average":     avg,
    }
    with open(REPORT_FILE, "w") as f:
        json.dump(report, f, indent=2)

    print(f"\n✅ Saved to {REPORT_FILE}")
    return metrics


def main():
    generator = RBIGenerator()
    results   = run_pipeline(generator)
    scores    = run_ragas(results)
    print_report(scores)


if __name__ == "__main__":
    main()