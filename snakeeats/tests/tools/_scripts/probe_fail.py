import sys, json
params = json.loads(sys.stdin.read())
print(json.dumps({'ok': True}))
