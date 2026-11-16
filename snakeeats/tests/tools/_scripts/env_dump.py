"""Child-process env probe: dump os.environ to ``env_out`` then optionally hold.

Used by behavior tests to observe the *real* environment a tool subprocess
receives (not the in-process dict construction). ``seconds`` keeps the process
alive so tests can inspect token state while the child is still running.
"""

import json
import os
import sys
import time

params = json.loads(sys.stdin.read() or "{}")
out = params.get("env_out")
if out:
    with open(out, "w", encoding="utf-8") as fh:
        json.dump(dict(os.environ), fh, ensure_ascii=True)

seconds = params.get("seconds", 0)
if seconds:
    time.sleep(float(seconds))

print(json.dumps({"ok": True}))
