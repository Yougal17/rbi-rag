import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json
import time
from dotenv import load_dotenv
from datasets import Dataset
from ragas import evaluate
from ragas.llms import LangchainLLMWrapper
from ragas.embeddings import LangchainEmbeddingsWrapper
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_huggingface import HuggingFaceEmbeddings
from generation.generator import RBIGenerator

load_dotenv()

# ─────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────

TESTSET_FILE   = "evaluation/testset.json"
RESULTS_FILE   = "evaluation/results.json"
REPORT_FILE    = "evaluation/report.json"
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
GEMINI_MODEL   = os.getenv("GEMINI_MODEL", "gemini-flash-lite-latest")

# 45 seconds between questions — well within free tier limits
QUESTION_DELAY = 45

# ─────────────────────────────────────────────
# STEP 1: RUN PIPELINE ON ALL TEST QUESTIONS
# ─────────────────────────────────────────────

def run_pipeline_on_testset(generator, testset, existing_results):
    """
    Run RAG pipeline on test questions.
    Skips questions already answered successfully.
    """
    # Build set of already answered questions
    answered = {
        r["question"] for r in existing_results
        if r.get("answer") and
        "Generation error" not in r["answer"] and
        "getaddrinfo" not in r["answer"]
    }

    print(f"\n✅ Already answered: {len(answered)}/{len(testset)}")
    print(f"⏳ Remaining:        {len(testset) - len(answered)}\n")

    results = list(existing_results)  # start with existing

    # Remove failed results so we retry them
    results = [
        r for r in results
        if r.get("answer") and
        "Generation error" not in r["answer"] and
        "getaddrinfo" not in r["answer"]
    ]

    remaining = [q for q in testset if q["question"] not in answered]

    if not remaining:
        print("✅ All questions already answered successfully.")
        return results

    print(f"Running pipeline on {len(remaining)} questions...")
    print(f"Delay between questions: {QUESTION_DELAY}s")
    print(f"Estimated time: ~{len(remaining) * QUESTION_DELAY // 60} minutes\n")

    for i, item in enumerate(remaining):
        question     = item["question"]
        ground_truth = item["ground_truth"]

        print(f"[{i+1}/{len(remaining)}] {question[:65]}...")

        success = False
        for attempt in range(3):
            try:
                result = generator.answer(query=question)

                answer = result.get("answer", "")
                if not answer or "Generation error" in answer:
                    raise ValueError(f"Bad answer: {answer[:80]}")

                contexts = [c["child_text"] for c in result["chunks"]]
                if not contexts:
                    contexts = [""]

                results.append({
                    "question":            question,
                    "answer":              answer,
                    "contexts":            contexts,
                    "ground_truth":        ground_truth,
                    "source_circular":     item.get("source_circular", ""),
                    "category":            item.get("category", ""),
                    "difficulty":          item.get("difficulty", ""),
                    "retrieved_circulars": [c["circular_number"] for c in result["chunks"]],
                    "timing":              result["timing"],
                })

                print(f"  ✅ {answer[:80]}...")
                success = True
                break

            except Exception as e:
                err = str(e)
                if "429" in err:
                    wait = 60 * (attempt + 1)
                    print(f"  ⏳ Rate limited (attempt {attempt+1}/3) — waiting {wait}s...")
                    time.sleep(wait)
                elif "getaddrinfo" in err or "timeout" in err.lower():
                    print(f"  ⚠️  Network error (attempt {attempt+1}/3) — retrying in 30s...")
                    time.sleep(30)
                else:
                    print(f"  ❌ Error: {err[:100]}")
                    break

        if not success:
            print(f"  ❌ All attempts failed — skipping this question")

        # Save after every question
        with open(RESULTS_FILE, "w", encoding="utf-8") as f:
            json.dump(results, f, indent=2, ensure_ascii=False)

        if i < len(remaining) - 1:
            print(f"  ⏳ Waiting {QUESTION_DELAY}s...\n")
            time.sleep(QUESTION_DELAY)

    print(f"\n✅ Pipeline complete: {len(results)} successful answers")
    return results


# ─────────────────────────────────────────────
# STEP 2: RUN RAGAS EVALUATION
# ─────────────────────────────────────────────

def run_ragas_evaluation(results):
    """Run Ragas on successfully answered questions."""

    print(f"\n{'='*60}")
    print("Running Ragas Evaluation...")
    print(f"{'='*60}\n")

    if not results:
        print("❌ No valid results to evaluate.")
        return None

    ragas_data = {
        "question":     [r["question"]     for r in results],
        "answer":       [r["answer"]       for r in results],
        "contexts":     [r["contexts"]     for r in results],
        "ground_truth": [r["ground_truth"] for r in results],
    }
    dataset = Dataset.from_dict(ragas_data)

    from ragas import evaluate
    from ragas.metrics import (
        faithfulness,
        answer_relevancy,
        context_precision,
        context_recall,
    )
    from langchain_google_genai import ChatGoogleGenerativeAI
    from langchain_huggingface import HuggingFaceEmbeddings
    from ragas.llms import LangchainLLMWrapper
    from ragas.embeddings import LangchainEmbeddingsWrapper

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

    # Set LLM on each metric explicitly
    faithfulness.llm             = llm
    answer_relevancy.llm         = llm
    answer_relevancy.embeddings  = embeddings
    context_precision.llm        = llm
    context_recall.llm           = llm

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
# ─────────────────────────────────────────────
# STEP 3: PRINT AND SAVE REPORT
# ─────────────────────────────────────────────

def print_report(scores, results):
    """Print evaluation report and save to JSON."""

    import numpy as np
    df = scores.to_pandas()

    print("\nRaw scores dataframe:")
    print(df.to_string())

    metric_cols = [
        "faithfulness",
        "answer_relevancy",
        "context_precision",
        "context_recall",
    ]

    metrics = {}
    for col in metric_cols:
        if col in df.columns:
            # Use nanmean — ignores NaN values
            val = float(np.nanmean(df[col].values))
            metrics[col] = val if not np.isnan(val) else 0.0
        else:
            metrics[col] = 0.0

    avg = sum(v for v in metrics.values() if v > 0) / max(
        sum(1 for v in metrics.values() if v > 0), 1
    )

    print(f"\n{'='*60}")
    print("RAGAS EVALUATION REPORT")
    print(f"{'='*60}")
    print(f"Questions evaluated: {len(results)}")
    print(f"\nScores:")
    print(f"{'='*60}")

    labels = {
        "faithfulness":      "Faithfulness",
        "answer_relevancy":  "Answer Relevancy",
        "context_precision": "Context Precision",
        "context_recall":    "Context Recall",
    }

    for key, label in labels.items():
        score = metrics[key]
        if score > 0:
            bar   = "█" * int(score * 20)
            empty = "░" * (20 - int(score * 20))
            print(f"  {label:22s}: {score:.4f} [{bar}{empty}]")
        else:
            print(f"  {label:22s}: N/A (all evaluations timed out)")

    print(f"\n  {'Overall Average':22s}: {avg:.4f}")

    # Save report
    report = {
        "questions_evaluated": len(results),
        "scores":              metrics,
        "overall_average":     avg,
    }
    with open(REPORT_FILE, "w") as f:
        json.dump(report, f, indent=2)

    print(f"\n✅ Report saved to {REPORT_FILE}")
    print(f"{'='*60}")

    return metrics

# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

def main():
    print("📂 Loading testset...")
    with open(TESTSET_FILE, "r", encoding="utf-8") as f:
        testset = json.load(f)
    print(f"  ✅ {len(testset)} questions")

    # Load existing results if any
    existing = []
    if os.path.exists(RESULTS_FILE):
        with open(RESULTS_FILE) as f:
            existing = json.load(f)
        valid_existing = [
            r for r in existing
            if r.get("answer") and
            "Generation error" not in r["answer"] and
            "getaddrinfo" not in r["answer"]
        ]
        print(f"  ✅ {len(valid_existing)} valid existing answers found")
    else:
        valid_existing = []

    # Check if all questions answered
    answered_questions = {r["question"] for r in valid_existing}
    all_answered = all(q["question"] in answered_questions for q in testset)

    if not all_answered:
        generator = RBIGenerator()
        results   = run_pipeline_on_testset(generator, testset, valid_existing)
    else:
        print("✅ All questions already answered — skipping pipeline")
        results = valid_existing

    if not results:
        print("❌ No valid results — check API quota and try again tomorrow")
        return

    # Run Ragas
    scores = run_ragas_evaluation(results)

    if scores is not None:
        print_report(scores, results)
    else:
        print("❌ Ragas evaluation failed")


if __name__ == "__main__":
    main()