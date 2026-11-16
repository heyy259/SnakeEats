import sys, json, os, time
params = json.loads(sys.stdin.read())
with open(params['token_file'], 'w', encoding='utf-8') as fh:
    fh.write(os.environ.get('SCAN_TOKEN', ''))
time.sleep(float(params.get('seconds', 0)))
print(json.dumps({'done': True}))
