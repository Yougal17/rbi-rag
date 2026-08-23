import json
import pandas as pd
from datasets import Dataset
from ragas import evaluate
from ragas.metrics import faithfulness, answer_relevancy, context_precision, context_recall
from ragas.llms import LangchainLLMWrapper
from ragas.embeddings import LangchainEmbeddingsWrapper
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_huggingface import HuggingFaceEmbeddings
from dotenv import load_dotenv
import os

load_dotenv()

# Load results
with open('evaluation/results.json') as f:
    results = json.load(f)

valid = [r for r in results if r['answer'] and 'Generation error' not in r['answer']]
print(f"Valid results: {len(valid)}/{len(results)}")

ragas_data = {
    "question":     [r["question"]     for r in valid],
    "answer":       [r["answer"]       for r in valid],
    "contexts":     [r["contexts"]     for r in valid],
    "ground_truth": [r["ground_truth"] for r in valid],
}
dataset = Dataset.from_dict(ragas_data)

llm = LangchainLLMWrapper(
    ChatGoogleGenerativeAI(
        model=os.getenv("GEMINI_MODEL", "gemini-2.5-flash"),
        google_api_key=os.getenv("GEMINI_API_KEY"),
        temperature=0.1,
    )
)
embeddings = LangchainEmbeddingsWrapper(
    HuggingFaceEmbeddings(model_name="sentence-transformers/all-mpnet-base-v2")
)

scores = evaluate(
    dataset=dataset,
    metrics=[faithfulness, answer_relevancy, context_precision, context_recall],
    llm=llm,
    embeddings=embeddings,
    raise_exceptions=False,
)

df = scores.to_pandas()
print("\n" + "="*50)
print("RAGAS SCORES")
print("="*50)
for col in ["faithfulness", "answer_relevancy", "context_precision", "context_recall"]:
    if col in df.columns:
        val = df[col].mean()
        print(f"  {col:25s}: {val:.4f}")

avg = df[["faithfulness","answer_relevancy","context_precision","context_recall"]].mean().mean()
print(f"\n  {'Overall Average':25s}: {avg:.4f}")

# Save
report = {"scores": {col: float(df[col].mean()) for col in df.columns if col in ["faithfulness","answer_relevancy","context_precision","context_recall"]}, "overall_average": float(avg)}
json.dump(report, open('evaluation/report.json','w'), indent=2)
print("\n✅ Saved to evaluation/report.json")