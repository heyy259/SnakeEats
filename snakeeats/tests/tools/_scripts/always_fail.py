import sys, json
params = json.loads(sys.stdin.read() or '{}')
print(json.dumps({'ok': True}))
