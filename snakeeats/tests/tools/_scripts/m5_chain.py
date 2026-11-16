import sys, json
params = json.loads(sys.stdin.read() or '{}')
print(json.dumps({'echo': params.get('text', '')}, ensure_ascii=False))
