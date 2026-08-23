import os
from dotenv import load_dotenv
from google import genai
from google.genai import types

load_dotenv()
api_key = os.getenv("GEMINI_API_KEY")
client  = genai.Client(api_key=api_key)

# Test confirmed working model
print("Testing gemini-flash-lite-latest...")
response = client.models.generate_content(
    model="gemini-flash-lite-latest",
    contents="What is the Reserve Bank of India? Answer in one sentence.",
    config=types.GenerateContentConfig(
        temperature=0.1,
        max_output_tokens=100,
    )
)
print(f"✅ gemini-flash-lite-latest works")
print(f"Response: {response.text}")

# Also test gemini-2.5-flash with a real prompt
print("\nTesting gemini-2.5-flash with real prompt...")
try:
    response2 = client.models.generate_content(
        model="gemini-2.5-flash",
        contents="What is the Reserve Bank of India? Answer in one sentence.",
        config=types.GenerateContentConfig(
            temperature=0.1,
            max_output_tokens=100,
        )
    )
    if response2.text:
        print(f"✅ gemini-2.5-flash works: {response2.text}")
    else:
        print(f"❌ gemini-2.5-flash returned empty response")
except Exception as e:
    print(f"❌ gemini-2.5-flash failed: {str(e)[:100]}")