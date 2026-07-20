
import json
import sys

params = json.loads(input())
print(f"Processing {params}...", flush=True)
print(json.dumps({"result": params["city"], "status": "ok"}), flush=True)
