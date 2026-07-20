
import json
import sys

params = json.loads(input())
print(json.dumps({"done": True}), flush=True)
# 工具自动退出，不需要用户干预
