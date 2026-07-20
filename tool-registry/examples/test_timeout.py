
import json
import time

params = json.loads(input())
print("Starting long task...", flush=True)
time.sleep(100)  # 很长的任务
print(json.dumps({"result": "done"}), flush=True)
