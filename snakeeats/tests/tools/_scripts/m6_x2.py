import sys, json
params = json.loads(sys.stdin.read())
print(json.dumps({'echo': params.get('text', '')}, ensure_ascii=False))
