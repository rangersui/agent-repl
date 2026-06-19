# k-kernel: Persistent REPL Patterns

## Core Principle

bash_tool is curl. k is a socket.

Every bash_tool call spawns a new process, runs a command, returns output, and dies. No state survives. To pass information between steps, you write files.

k keeps a process alive inside tmux. Variables, imports, connections, cwd, env — everything persists across cells. The process IS the workspace. Files become backups, not the primary medium.

```
bash_tool (stateless, one-shot)
  └─ k run/fire/poll (stateless CLI, the "launcher")
       └─ tmux session (stateful, persistent)
            └─ bash / python / node / R REPL (stateful, persistent)
```

The launcher is stateless. The target is stateful. This is exactly how curl talks to a server — the client forgets, the server remembers.

---

## Context Loading: source & exec

A REPL is blank memory. `source` (bash) and `exec` (python) inject a snapshot into it. After loading, every cell runs inside that context.

### Bash: source

```bash
# write a context file
cat > /tmp/my_ctx.sh << 'EOF'
export API_URL="https://api.example.com"
export API_KEY="sk-..."
export DB_HOST="prod-db.internal"

request() {
    curl -s -H "Authorization: Bearer $API_KEY" "$API_URL/$1"
}

dbquery() {
    PGPASSWORD=$DB_PASS psql -h "$DB_HOST" -U app -d main -c "$1"
}

echo "ctx loaded: request <path> | dbquery <sql>"
EOF

# load it into a persistent session
k new work bash
k run work "source /tmp/my_ctx.sh"
# → ctx loaded: request <path> | dbquery <sql>

# now use it — functions and env vars persist
k run -j work "request users/me"
k run -j work "dbquery 'SELECT count(*) FROM orders'"
# everything works, nothing re-imported
```

### Python: exec

```bash
# write a context file
cat > /tmp/my_ctx.py << 'PYEOF'
import json
import time
import websocket

# connections
ws = None
db = None

def connect_ws(url):
    global ws
    ws = websocket.create_connection(url)
    print(f'ws connected to {url}')

def connect_db(dsn):
    global db
    import psycopg2
    db = psycopg2.connect(dsn)
    db.autocommit = False
    print(f'db connected')

def query(sql, *args):
    cur = db.cursor()
    cur.execute(sql, args)
    rows = cur.fetchall()
    for r in rows:
        print(r)
    return rows

def commit():
    db.commit()
    print('committed')

def rollback():
    db.rollback()
    print('rolled back')

print('ctx loaded: connect_ws() connect_db() query() commit() rollback()')
PYEOF

# load it
k new py python3 -i
k run -j py "exec(open('/tmp/my_ctx.py').read())"
# → ctx loaded

# connect
k run -j py "connect_db('postgresql://app:pass@localhost/main')"
k run -j py "query('SELECT * FROM users LIMIT 3')"
# → rows printed, connection alive, transaction open
```

### Context Recovery

Session died? New REPL, one line, back to work:

```bash
k kill py
k new py python3 -i
k run -j py "exec(open('/tmp/my_ctx.py').read())"
# everything is back
```

### State Checkpoint

Save current state, restore later:

```bash
k run -j py "
with open('/tmp/checkpoint.py', 'w') as f:
    f.write(f'radius = {repr(radius)}\n')
    f.write(f'data = {repr(data)}\n')
    f.write(f'results = {repr(results)}\n')
print('checkpointed')
"

# later, in a new session
k run -j py "exec(open('/tmp/checkpoint.py').read())"
# radius, data, results all restored
```

### Hot Patching

Found a bug? Redefine the function in the next cell. No re-source needed:

```bash
# original function is broken
k run -j work "cb_price"
# → error

# fix it live — only this function, everything else untouched
k run -j work '
cb_price() {
    (echo "$CB_SUB"; sleep 3) \
        | websocat "$CB_URL" 2>/dev/null \
        | grep "\"type\":\"ticker\"" \
        | head -1 \
        | python3 -c "import sys,json; t=json.load(sys.stdin); print(t[\"product_id\"],t[\"price\"])"
}
echo "fixed"'
# → fixed

k run -j work "cb_price"
# → works now. env vars, other functions, cwd — all untouched
```

### Painless Trial and Error

One-shot: error = scorched earth. Process dies, imports gone, variables gone, connections closed, start from zero.

REPL: error = one failed cell. Everything else survives.

```bash
k run -j py "import pandas as pd; df = pd.read_csv('big_data.csv')"
# → ok, df loaded (took 30 seconds)

k run -j py "df.groupby('category').agg({'revenue': 'sum'}).sort_values('revnue')"
# → error: KeyError 'revnue' — typo

# one-shot: re-import pandas, re-read csv (30 seconds), fix typo, try again
# REPL: just fix the typo. df is still there. pandas is still imported.

k run -j py "df.groupby('category').agg({'revenue': 'sum'}).sort_values('revenue')"
# → works. zero re-setup cost.
```

This compounds. In a 10-step workflow, step 7 fails:

```
one-shot:  redo steps 1-6 (imports, connections, data loading) → fix step 7 → retry
REPL:      fix step 7 → retry. steps 1-6 are still in memory.
```

Database connection still open. SSH still connected. WebSocket still subscribed. Model still in GPU. You only redo the thing that broke.

```bash
k run -j py "conn.execute('SLECT * FROM users')"
# → error: syntax error at "SLECT"
# connection is still alive. transaction is still open.

k run -j py "conn.execute('SELECT * FROM users')"
# → works. same connection, same transaction, no reconnect.
```

The REPL is a safety net. Try things, break things, fix things. The cost of failure is one cell, not the entire session.

---

## Pattern: Persistent Connections

The fundamental insight: **building a connection is expensive, using it is cheap.** One-shot CLI pays the build cost every time. REPL pays once.

### Database: Interactive Transactions

```bash
k new py python3 -i
k run -j py "exec(open('/tmp/db_ctx.py').read())"
k run -j py "connect_db('postgresql://...')"

# explore
k run -j py "query('SELECT * FROM users LIMIT 5')"
# AI sees the data, decides what to do

# modify — inside a transaction
k run -j py "query('ALTER TABLE users ADD COLUMN risk_score FLOAT')"
k run -j py "query('UPDATE users SET risk_score = 0.5 WHERE signup_date < %s', '2024-01-01')"

# inspect
k run -j py "query('SELECT id, name, risk_score FROM users WHERE risk_score > 0 LIMIT 5')"

# not right? rollback. connection still alive
k run -j py "rollback()"

# try again with different logic...
k run -j py "query('UPDATE users SET risk_score = ...')"
k run -j py "commit()"
```

One-shot can't do this. The transaction exists only inside the connection. Kill the process, lose the transaction.

### WebSocket: Subscribe/Unsubscribe

```bash
k run -j py "connect_ws('wss://ws-feed.exchange.coinbase.com')"
# → connected

k run -j py "ws.send(json.dumps({'type':'subscribe','channels':[{'name':'ticker','product_ids':['BTC-USD']}]}))"
# → subscribed

k run -j py "
for i in range(3):
    t = json.loads(ws.recv())
    if t['type'] == 'ticker':
        print(t['product_id'], t['price'])
"
# → BTC-USD 64290.82
# → BTC-USD 64291.86
# → BTC-USD 64290.83

k run -j py "ws.send(json.dumps({'type':'unsubscribe','channels':['ticker']}))"
# → unsubscribed. connection still alive, just silent

k run -j py "ws.close()"
# → closed when YOU say so
```

The connection is a variable. `send()` and `recv()` are method calls. Subscribe and unsubscribe are just messages on the same socket. No process restart needed.

### SSH: Persistent Remote Session

k doesn't need a "remote" feature. SSH is just a command:

```bash
k new remote "ssh -o StrictHostKeyChecking=no user@prod-server"

# state persists on the REMOTE machine
k run -j remote "cd /opt/app && export ENV=production"
k run -j remote "tail -50 logs/error.log"
# AI reads logs, forms hypothesis
k run -j remote "grep 'OOM' /var/log/syslog | tail -10"
# confirmed — fix it
k run -j remote "sed -i 's/MaxHeap=512/MaxHeap=1024/' config.yaml"
k run -j remote "systemctl restart app"
k run -j remote "tail -20 logs/error.log"
# verify fix worked
```

cd, env, everything persists on the remote side. No re-login between steps. The SSH connection lives in tmux, k's prompt detection works transparently over it — the remote shell's prompt flows back through SSH, k detects completion the same way.

Nothing installed on the remote. Just SSH and bash.

### CDP: Chrome DevTools Protocol

CDP is a WebSocket to Chrome's internals:

```bash
# launch headless chrome
k run -j work "google-chrome --headless --remote-debugging-port=9222 &"

# connect via CDP
k run -j py "
import websocket, json
# get the page's debugger URL
import urllib.request
targets = json.loads(urllib.request.urlopen('http://localhost:9222/json').read())
ws_url = targets[0]['webSocketDebuggerUrl']
cdp = websocket.create_connection(ws_url)
print('CDP connected')
"

# navigate
k run -j py "
cdp.send(json.dumps({'id':1, 'method':'Page.navigate', 'params':{'url':'https://example.com'}}))
print(json.loads(cdp.recv()))
"

# execute JS in the page
k run -j py "
cdp.send(json.dumps({'id':2, 'method':'Runtime.evaluate', 'params':{'expression':'document.title'}}))
result = json.loads(cdp.recv())
print(result['result']['result']['value'])
"

# intercept network requests
k run -j py "
cdp.send(json.dumps({'id':3, 'method':'Fetch.enable', 'params':{'patterns':[{'urlPattern':'*api*'}]}}))
"
# every API call now passes through AI's hands

# modify DOM
k run -j py "
cdp.send(json.dumps({'id':4, 'method':'Runtime.evaluate',
    'params':{'expression': 'document.querySelector(\"h1\").textContent = \"AI was here\"'}}))
"
```

One WebSocket connection. Full browser control. JS execution, DOM modification, network interception, performance profiling — all via `cdp.send()` / `cdp.recv()`.

### Message Queue: AI as a Live Node

```bash
k run -j py "
from kafka import KafkaConsumer, KafkaProducer
consumer = KafkaConsumer('events', bootstrap_servers='kafka:9092', auto_offset_reset='latest')
producer = KafkaProducer(bootstrap_servers='kafka:9092')
print('kafka connected')
"

# AI sits in the event stream
k run -j -t 30 py "
for msg in consumer:
    event = json.loads(msg.value)
    print(event['type'], event.get('user_id'))
    if event['type'] == 'anomaly':
        producer.send('alerts', json.dumps({'source': 'ai', 'event': event}).encode())
        print('  → alert sent')
        break
"
```

AI is a participant in the distributed system. Not analyzing logs after the fact — sitting inside the stream, reading events, making decisions, publishing reactions. The consumer group offset persists. Disconnect and reconnect picks up where it left off.

### Debugger: Interactive Investigation

```bash
k new dbg python3 -i

# attach to a running process (or start one under debug)
k run -j dbg "
import pdb
import importlib
mod = importlib.import_module('myapp.worker')
# set a breakpoint
pdb.run('mod.process_batch()')
"

# AI steps through, cell by cell
k run -j dbg "n"          # next
k run -j dbg "p self.queue"  # inspect
k run -j dbg "p len(self.queue)"
# AI sees the state, forms hypothesis
k run -j dbg "p self.config"
# found the bug
k run -j dbg "!self.config['max_retries'] = 5"  # fix in-memory
k run -j dbg "c"          # continue
```

Debugging is the most context-dependent activity. Break the session, lose the callstack, lose the variable state, start over. REPL keeps the debug session alive across cells.

### GPU: Load Once, Use Forever

```bash
k new gpu python3 -i

k run -j -t 300 gpu "
import torch
from transformers import AutoModelForCausalLM, AutoTokenizer
model = AutoModelForCausalLM.from_pretrained('meta-llama/Llama-2-7b', device_map='auto')
tokenizer = AutoTokenizer.from_pretrained('meta-llama/Llama-2-7b')
print(f'loaded on {model.device}, {torch.cuda.memory_allocated()/1e9:.1f}GB')
"
# → loaded on cuda:0, 13.2GB (took 3 minutes)

# now inference is instant — model stays in VRAM
k run -j gpu "
inputs = tokenizer('Hello, my name is', return_tensors='pt').to(model.device)
out = model.generate(**inputs, max_new_tokens=50)
print(tokenizer.decode(out[0]))
"

# change parameters without reloading
k run -j gpu "model.config.temperature = 0.3"

# load a LoRA adapter on top
k run -j gpu "
from peft import PeftModel
model = PeftModel.from_pretrained(model, '/tmp/my_lora')
print('adapter loaded')
"
# model still in VRAM, adapter added, no restart
```

Loading a model = minutes. Inference = seconds. One-shot pays the load cost every time. REPL pays once.

---

## Pattern: REPL as Live Server

The REPL isn't just a client that holds connections. It can BE the server. Run a web server in a background thread, and the REPL becomes the control plane.

### Hot-Patchable Web Server

```bash
k new py python3 -i
k new work bash

# start server
k run -j py "from flask import Flask, jsonify; import threading"
k run -j py "app = Flask(__name__); RESPONSE = {'version': 'v1'}"
k run -j py "
@app.route('/')
def index():
    return jsonify(RESPONSE)
"
k run -j py "threading.Thread(target=lambda: app.run(port=8080, use_reloader=False), daemon=True).start(); import time; time.sleep(1); print('server up')"

# test it
k run -j work "curl -s localhost:8080"
# → {"version": "v1"}

# hot-patch: change response data — just dict mutation, no restart
k run -j py "RESPONSE['version'] = 'v2'; RESPONSE['feature'] = 'hot-patched'"
k run -j work "curl -s localhost:8080"
# → {"version": "v2", "feature": "hot-patched"}

# hot-patch: swap the entire handler
k run -j py "
def index():
    from flask import jsonify, request
    return jsonify(version='v3', your_ip=request.remote_addr, method=request.method)
app.view_functions['index'] = index
print('handler swapped')
"

k run -j work "curl -s localhost:8080"
# → {"version": "v3", "your_ip": "127.0.0.1", "method": "GET"}
# server never restarted
```

### Quantum Maze: Observation Changes State

A maze where every visit mutates the structure. The AI watches and intervenes.

```bash
# source the maze server
k run -j py "exec(open('/tmp/quantum_maze.py').read())"
k run -j py "start(8888)"

# visitor navigates — maze mutates on each visit
# AI watches the visit log
k run -j py "print(len(VISIT_LOG), 'visits so far')"

# AI intervenes: rewrite a room
k run -j py "
ROOMS['void']['desc'] = 'THE OBSERVER HAS BEEN DETECTED.'
ROOMS['void']['exits'] = {'down': 'trap'}
"
# next visitor to void sees the AI's message. no restart.
```

The server and AI share the same process memory. `ROOMS` is just a dict. The AI reads `VISIT_LOG`, mutates `ROOMS`, swaps handlers — all while the server keeps handling requests.

### Adaptive Honeypot: Tarpit + AI Generation

```bash
# architecture:
# 1. request comes in → handler starts slow response (tarpit)
# 2. AI sees the access in VISIT_LOG
# 3. AI generates fake content, writes to GENERATED dict
# 4. handler picks up generated content, serves it

k run -j py "GENERATED = {}; VISIT_LOG = []"
k run -j py "
import asyncio, random
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
app = FastAPI()

@app.get('/{path:path}')
async def honeypot(path: str, request: Request):
    VISIT_LOG.append({'path': path, 'ip': request.client.host})
    await asyncio.sleep(random.uniform(2, 5))
    content = GENERATED.get(path, '<h1>Loading...</h1>')
    return HTMLResponse(content)
"

# attacker hits /admin → AI sees it, generates fake admin page
k run -j py "
GENERATED['/admin'] = '''
<html><body>
<h1>Admin Dashboard</h1>
<p>Users: 14,293</p>
<p>Revenue: $2.3M</p>
<a href=\"/admin/users\">Manage Users</a>
<a href=\"/admin/settings\">Settings</a>
</body></html>'''
print('trap set for /admin')
"

# attacker goes to /admin/users → AI generates fake user list
k run -j py "
GENERATED['/admin/users'] = '''...fake user table with canary tokens...'''
"
# every path is generated on demand. no fingerprint. no fixed script.
```

The tarpit covers AI generation latency (slow = normal for a "struggling server"). The AI generates content between requests. The attacker sees a convincing, unique environment that never matches any known honeypot signature.

---

## Pattern: Cross-Session Workflows

Multiple sessions can share data through the filesystem or through k notify.

### Bash Captures, Python Analyzes

```bash
# bash session: pull data
k run -j work "(echo '{\"type\":\"subscribe\",...}'; sleep 5) \
    | websocat wss://ws-feed.exchange.coinbase.com \
    | head -10 > /tmp/market.jsonl"

# python session: analyze it
k run -j py "
import json
ticks = [json.loads(l) for l in open('/tmp/market.jsonl') if '\"ticker\"' in l]
prices = [float(t['price']) for t in ticks]
print(f'range: ${min(prices):,.2f} - ${max(prices):,.2f}')
print(f'spread: ${max(prices)-min(prices):,.2f}')
"
```

### Notify Across Sessions

```bash
# session A fires a long task
k fire work "make build && k notify work 'build done'"

# session B monitors
# km work -1  ← waits for any event, including the notify
# or check from python:
k run -j py "
import subprocess, json
r = subprocess.run(['k', 'poll', 'work'], capture_output=True, text=True)
print(json.loads(r.stdout)['status'])
"
```

---

## Principle Summary

| One-shot (bash_tool) | Persistent (k REPL) |
|---|---|
| Every call = new process | Process stays alive |
| State via files | State in memory |
| Connection per call | Connection per session |
| Import every time | Import once |
| Error = total reset | Error = keep going |
| Cold start every step | Warm context always |
| curl | socket |

### When to use k

Use k when you need:
- **Persistence**: variables, imports, connections surviving across steps
- **Connections**: database, websocket, SSH, CDP, message queue
- **Transactions**: database transactions that span multiple decisions
- **Interactive control**: subscribe/unsubscribe, step debugger, REPL exploration
- **Live server**: hot-patchable web server, adaptive systems
- **Cross-session**: bash + python + remote working together

### When bash_tool is enough

- One-off commands: `ls`, `cat`, `grep`
- File creation and editing
- Package installation
- Anything truly stateless

### The mental model

REPL is not "a better terminal." REPL is a **live process with memory**. Every cell is a conversation turn with that process. The process accumulates knowledge: imports, variables, connections, functions, state. It's the difference between sending letters (one-shot) and having a phone call (persistent session). The call stays connected. Context builds up. You don't re-introduce yourself every sentence.

k is the phone line. The REPL is the other person. They remember everything.
