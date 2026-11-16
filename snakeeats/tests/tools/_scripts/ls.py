import sys, json, time
params = json.loads(sys.stdin.read())
time.sleep(params.get('seconds', 60))
print(json.dumps({'done': True}))
