# Manual sequential evaluation on all 36 questions
# Broader coverage, same metrics, no parallel API calls
# Run: python evaluation/evaluate_manual.py

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json
import time
import re
from dotenv import load_dotenv
from langchain_google_genai import ChatGoogleGenerativeAI
from generation.generator import RBIGenerator

load_dotenv()

GEMINI_API_KEY  = os.getenv("GEMINI_API_KEY")
GEMINI_MODEL    = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")
TESTSET_FILE    = "evaluation/testset.json"
PIPELINE_FILE   = "evaluation/manual_pipeline_results.json"
EVAL_FILE       = "evaluation/manual_eval_scores.json"
REPORT_FILE     = "evaluation/manual_report.json"

QUESTION_DELAY  = 45   # between pipeline calls
EVAL_DELAY      = 8    # between metric calls


def run_pipeline(generator, testset):
    """Run RAG pipeline on all questions. Resumable."""

    existing = []
    if os.path.exists(PIPELINE_FILE):
        try:
            with open(PIPELINE_FILE, encoding="utf-8") as f:
                existing = json.load(f)
        except (json.JSONDecodeError, ValueError):
            print("  ⚠️  Corrupted pipeline file detected — starting fresh")
            existing = []   

    answered = {
        r["question"] for r in existing
        if r.get("answer") and "Generation error" not in r["answer"]
        and "getaddrinfo" not in r.get("answer", "")
    }

    print(f"Already answered: {len(answered)}/{len(testset)}")
    results   = [r for r in existing if r["question"] in answered]
    remaining = [q for q in testset if q["question"] not in answered]

    if not remaining:
        print("✅ All questions already answered")
        return results

    print(f"Running pipeline on {len(remaining)} remaining questions...")

    for i, item in enumerate(remaining):
        print(f"\n[{i+1}/{len(remaining)}] {item['question'][:65]}...")

        success = False
        for attempt in range(3):
            try:
                result  = generator.answer(query=item["question"])
                answer  = result.get("answer", "")

                if not answer or "Generation error" in answer:
                    raise ValueError(f"Bad answer")

                contexts = [c["child_text"] for c in result["chunks"]]
                results.append({
                    "question":        item["question"],
                    "answer":          answer,
                    "contexts":        contexts if contexts else [""],
                    "ground_truth":    item["ground_truth"],
                    "source_circular": item.get("source_circular", ""),
                    "category":        item.get("category", ""),
                    "difficulty":      item.get("difficulty", ""),
                })
                print(f"  ✅ {answer[:80]}...")
                success = True
                break

            except Exception as e:
                err = str(e)
                if "429" in err:
                    wait = 60 * (attempt + 1)
                    print(f"  ⏳ Rate limited — waiting {wait}s...")
                    time.sleep(wait)
                elif "getaddrinfo" in err:
                    print(f"  ⚠️  Network error — waiting 30s...")
                    time.sleep(30)
                else:
                    print(f"  ❌ {err[:80]}")
                    break

        if not success:
            print(f"  ❌ Skipped after 3 attempts")

        with open(PIPELINE_FILE, "w", encoding="utf-8") as f:
            json.dump(results, f, indent=2, ensure_ascii=False)

        if i < len(remaining) - 1:
            print(f"  ⏳ Waiting {QUESTION_DELAY}s...")
            time.sleep(QUESTION_DELAY)

    return results


def evaluate_single(llm, question, answer, contexts, ground_truth):
    """Evaluate one question across all 4 metrics."""
    context_text = "\n\n".join(contexts[:3])[:1500]
    scores = {}

    prompts = {
        "faithfulness": f"""Is the answer fully supported by the context? Reply with only a decimal between 0 and 1.
Context: {context_text}
Answer: {answer[:500]}
Score:""",

        "answer_relevancy": f"""Does the answer directly address the question? Reply with only a decimal between 0 and 1.
Question: {question}
Answer: {answer[:500]}
Score:""",

        "context_precision": f"""Is the retrieved context relevant to the question? Reply with only a decimal between 0 and 1.
Question: {question}
Context: {context_text}
Score:""",

        "context_recall": f"""Does the context contain enough information to produce the correct answer? Reply with only a decimal between 0 and 1.
Question: {question}
Correct Answer: {ground_truth[:300]}
Context: {context_text}
Score:""",
    }

    for metric, prompt in prompts.items():
        try:
            response = llm.invoke(prompt)
            # Handle both string and list response formats
            content = response.content
            if isinstance(content, list):
                text = " ".join(
                    part.get("text", "") if isinstance(part, dict) else str(part)
                    for part in content
                ).strip()
            else:
                text = str(content).strip()
            match    = re.search(r'0?\.\d+|1\.0|^[01]$', text)
            score    = float(match.group()) if match else 0.5
            score    = max(0.0, min(1.0, score))
            scores[metric] = score
            time.sleep(EVAL_DELAY)
        except Exception as e:
            print(f"    ❌ {metric}: {type(e).__name__}: {str(e)[:100]}")
            scores[metric] = None
            time.sleep(15)

    return scores


def run_evaluation(results):
    """Run manual evaluation on all results. Resumable."""

    # Load existing eval scores
    existing_evals = {}
    if os.path.exists(EVAL_FILE):
        with open(EVAL_FILE) as f:
            existing_evals = json.load(f)

    llm = ChatGoogleGenerativeAI(
        model=GEMINI_MODEL,
        google_api_key=GEMINI_API_KEY,
        temperature=0.0,
    )

    print(f"\nEvaluating {len(results)} questions...")
    print(f"Already evaluated: {len(existing_evals)}")

    all_scores = dict(existing_evals)

    for i, r in enumerate(results):
        q = r["question"]

        if q in all_scores:
            print(f"[{i+1}/{len(results)}] ⏭️  Already evaluated")
            continue

        print(f"[{i+1}/{len(results)}] {q[:60]}...")

        scores = evaluate_single(
            llm,
            q,
            r["answer"],
            r["contexts"],
            r["ground_truth"],
        )

        all_scores[q] = scores
        def fmt(v):
            return f"{v:.2f}" if v is not None else "ERR"

        print(f"  F:{fmt(scores.get('faithfulness'))} "
              f"R:{fmt(scores.get('answer_relevancy'))} "
              f"P:{fmt(scores.get('context_precision'))} "
              f"Rc:{fmt(scores.get('context_recall'))}")
        
        with open(EVAL_FILE, "w", encoding="utf-8") as f:
            json.dump(all_scores, f, indent=2, ensure_ascii=False)

        if i < len(results) - 1:
            time.sleep(5)

    return all_scores


def print_report(all_scores, results):
    """Compute and print final averages."""

    def safe_mean(values):
        valid = [v for v in values if v is not None and not (isinstance(v, float) and v != v)]
        return sum(valid) / len(valid) if valid else 0.0

    metrics = {
        "faithfulness":      safe_mean([s.get("faithfulness")      for s in all_scores.values()]),
        "answer_relevancy":  safe_mean([s.get("answer_relevancy")  for s in all_scores.values()]),
        "context_precision": safe_mean([s.get("context_precision") for s in all_scores.values()]),
        "context_recall":    safe_mean([s.get("context_recall")    for s in all_scores.values()]),
    }

    avg = sum(metrics.values()) / len(metrics)

    print(f"\n{'='*60}")
    print("MANUAL EVALUATION REPORT (36 questions)")
    print(f"{'='*60}")
    print(f"Questions evaluated: {len(all_scores)}")
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
        bar   = "█" * int(score * 20)
        empty = "░" * (20 - int(score * 20))
        print(f"  {label:22s}: {score:.4f} [{bar}{empty}]")

    print(f"\n  {'Overall Average':22s}: {avg:.4f}")

    report = {
        "method":              "Manual Sequential",
        "questions_evaluated": len(all_scores),
        "scores":              metrics,
        "overall_average":     avg,
    }
    with open(REPORT_FILE, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)

    print(f"\n✅ Saved to {REPORT_FILE}")
    return metrics


def main():
    # Load testset
    with open(TESTSET_FILE) as f:
        testset = json.load(f)
    print(f"Testset: {len(testset)} questions")

    # Stage 1: Run pipeline
    generator = RBIGenerator()
    results   = run_pipeline(generator, testset)
    print(f"\n✅ Pipeline done: {len(results)} answers")

    # Stage 2: Evaluate
    all_scores = run_evaluation(results)

    # Stage 3: Report
    print_report(all_scores, results)


if __name__ == "__main__":
    main()