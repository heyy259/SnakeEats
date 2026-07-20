
import json
import time

params = json.loads(input())
for i in range(10):
    print(f"Working {i}", flush=True)
    time.sleep(0.3)
print(json.dumps({"done": True}), flush=True)
