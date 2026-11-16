import sys,json,time
params=json.loads(sys.stdin.read())
time.sleep(0.1)
print(json.dumps({'done':True}))
