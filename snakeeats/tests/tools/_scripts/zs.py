import sys, json, time
params = json.loads(sys.stdin.read())
time.sleep(params.get('seconds', 1))
print(json.dumps({'done': True}))
