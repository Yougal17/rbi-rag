import urllib.request
import json

payload = {
    "question": "What does one do if he has old money and want to change it",
    "year_filter": None,
    "top_k": 5,
}

data = json.dumps(payload).encode("utf-8")
request = urllib.request.Request(
    "http://127.0.0.1:8000/query",
    data=data,
    headers={"Content-Type": "application/json"},
    method="POST",
)

try:
    with urllib.request.urlopen(request) as response:
        print(response.status)
        print(response.read().decode("utf-8"))
except urllib.error.HTTPError as e:
    print("status", e.code)
    print(e.read().decode("utf-8"))
