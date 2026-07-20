
import json
import time

params = json.loads(input())
print("Starting...", flush=True)

# 长时间运行
for i in range(100):
    print(f"Working... {i}", flush=True)
    time.sleep(0.5)

print(json.dumps({"result": "done"}), flush=True)
