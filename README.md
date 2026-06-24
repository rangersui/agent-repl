# pythond

**sshd gives you a shell. pythond gives you Python.**

Persistent Python daemon with named sessions, WebSocket protocol, and human attach.
Connect to a live Python namespace — variables, connections, threads still running from last time.

```
pip install pythond
```

## Quick start

```bash
# Terminal 1: start daemon
pythond daemon

# Terminal 2: use it
pysh new work
pysh run work "x = 42"
pysh run work "x + 1"          # → 43 (state persists)
pysh attach work               # → real Python REPL (Ctrl-] to detach)
```

## Three commands

| Command | Role | Analogy |
|---------|------|---------|
| `pythond` | daemon process | `sshd` |
| `pysh` | session client | `ssh` |
| `pyctl` | daemon control | `systemctl` |

## Commands

```
pysh new <name>              create session
pysh run <name> "code"       sync exec → raw output
pysh fire <name> "code"      async (thread) → shares namespace, can't kill C
pysh fork <name> "code"      async process (POSIX only) → killable, pickles vars back
pysh poll <name> [cell_id]   check async result
pysh attach <name>           human REPL (readline, colors, Ctrl-C)
pysh int <name>              best-effort interrupt (fire=best effort, fork=kill)
pysh kill <name>             terminate session
pysh ls                      list sessions
pysh status <name>           session health (JSON)
pysh vars <name>             namespace names (JSON)
pysh complete <name> "text"  tab completion (JSON)

pyctl start [--listen HOST:PORT] [--tls]   start daemon in foreground
pyctl stop                                 stop daemon
pyctl status                               daemon info
pyctl connect <name> <host:port> <token> [--tls]   proxy to remote pythond
pyctl disconnect <name>                            drop remote proxy
pyctl cert                                 show/generate machine cert
pyctl trust <cert.pem>                     let this client connect (server-side)
pyctl pin <cert.pem>                       verify this server is real (client-side)
```

Session names are canonical lowercase: `a-z`, `0-9`, `_`, or `-`, 1-80
characters. Names with uppercase letters or dots are rejected. Windows device
names such as `con`, `nul`, `prn`, `aux`, `com1`, and `lpt1` are also rejected.

## Stateful First

Most command tools are intentionally stateless: fork, run, die. That is simple
for humans, but wasteful for agents. Agents repeat imports, reopen connections,
re-parse configs, and rebuild intermediate data because the process disappears
after every call.

pythond flips that default. The process is the workspace. State is not a
cleanup problem first; it is addressable memory.

Things that stay alive:

- Python variables, imports, compiled regexes, parsed configs, DataFrames, and
  models,
- database handles, HTTP sessions, WebSockets, TCP sockets, SSH tunnels, and
  browser/CDP sessions,
- Flask apps, local servers, file watchers, monitors, and other daemon threads,
- live control-plane decisions such as feature flags, rate limits, blocked IP
  sets, routing weights, and circuit breaker state.

Static config can become a Python variable. Patch one cell; the next request
sees it. No restart is needed for logic that already lives inside the session.

## Two Channels, One Namespace

The agent channel is structured: source code in, captured text/JSON out. It does
not need to parse ANSI escape sequences, cursor movement, prompts, or screen
redraws to know when a cell finished.

The human channel is interactive: `pysh attach` connects to the same process
through PTY or WinPTY. A human can inspect variables, interrupt with Ctrl-C, or
detach with `Ctrl-]` without discarding the namespace.

That split is the core design: pure data for agents, real terminal ergonomics
for humans, one shared runtime underneath.

## Why pythond exists

```
pysh run work "x = 42"
pysh run work "x + 1"    # → 43
```

Code in, result out. Variables survive between calls.
No terminal. No ANSI. No parsing. Function-call API to a persistent Python namespace.

AI agents use it as their Python runtime. Humans use `pysh attach` for an interactive REPL into the same namespace. Both see the same objects.

## Why two daemons (remote proxy)

AI can't ssh. A human would `ssh server` then `python -i` — done. An AI agent can only do one-shot `bash_tool` calls, so it can't hold an SSH session open.

The two-daemon pattern solves this: the local daemon holds the connection the AI can't hold. `pyctl connect` tells the local daemon to proxy to a remote daemon. By default the proxy name is also the remote session name, so remote use looks local: `pyctl connect work ...` then `pysh run work "code"`. For advanced routing, use explicit proxy form: `pysh run server work "code"`.

## fire vs fork

Both run code asynchronously. The difference is the execution model.

```
pysh fire work "model = train(data)"    # thread — shares namespace
pysh fork work "model = train(data)"    # process — killable, pickles back
```

**fire** (threading.Thread): Code runs in a thread that shares the session namespace. Exec is serialized (one cell at a time) — async to the client, not parallel. Variables set by fire'd code are immediately visible to later calls. Cannot be force-killed when stuck in C code (requests.get, time.sleep). `pysh kill` (whole session) is the escape.

**fork** (`os.fork()`, POSIX only): Code runs in a child process with a copy of the namespace. New/changed variables are pickled back and merged when done. `pysh int` kills it (SIGKILL). Unpicklable objects (sockets, locks, CUDA tensors) are skipped -- the poll response tells you what didn't come back. In-place mutations (`list.append`, `dict[k]=v`) won't merge -- use assignment (`x = new_value`). Failed forks do not merge. Merge is last-writer-wins: a completed fork may overwrite variables changed in the parent while running.

```json
// poll after fork completes
{"cell_id": "abc", "status": "done", "output": "...",
 "merged": ["model", "results"], "skipped": ["db_conn"]}
```

## Protocol

WebSocket with newline-separated fields. Python code is never JSON-escaped.

```
ws.send("run work\nprint('hello')")     → "hello"
ws.send("fire work\ntrain(epochs=50)")  → {"cell_id":"..."}
ws.send("fork work\ntrain(epochs=50)")  → {"cell_id":"..."}
ws.send("ls")                           → "  work: alive pid=123"
```

The protocol supports multiple commands on one WebSocket. The normal `pysh`
CLI opens a short connection per command; `pyctl connect` keeps a remote proxy
connection alive inside the local daemon.

## Transport

| Mode | URL | Auth | Use case |
|------|-----|------|----------|
| Local POSIX | `ws://` over AF_UNIX | socket perms | default |
| Local Windows | `ws://127.0.0.1:7399` | token | default |
| Remote | `wss://host:7399` | token plus pinned self-signed server cert; optionally mTLS | `--listen --tls` |

## Remote access

```bash
# Server
pip install pythond
pyctl start --listen 0.0.0.0:7399 --tls --show-token
# prints token and fingerprint

# Client: copy server ~/.pythond/tls/cert.pem to client as ~/server_cert.pem.
# Remote TLS uses a self-signed cert, so pin before connecting.
pyctl pin ~/server_cert.pem
export PYTHOND_HOST=10.0.0.5:7399 PYTHOND_TOKEN=abc... PYTHOND_TLS=1
pysh new work
pysh run work "import platform; platform.node()"
```

### mTLS plus token

```bash
# Client: generate client cert
pyctl cert
# copy client ~/.pythond/tls/cert.pem to server as ~/client_cert.pem

# Server: generate server cert, trust client cert
pyctl cert
pyctl trust ~/client_cert.pem
# copy server ~/.pythond/tls/cert.pem to client as ~/server_cert.pem
pyctl start --listen 0.0.0.0:7399 --tls --show-token
# cert is required and token is still required

# Client: pin server cert, then connect (client cert sent automatically)
pyctl pin ~/server_cert.pem
export PYTHOND_HOST=10.0.0.5:7399 PYTHOND_TOKEN=<printed-token> PYTHOND_TLS=1
pysh run work "x"
```

### SSH tunnel

```bash
ssh -L 7399:localhost:7399 user@server "pythond daemon --listen 127.0.0.1:7399 --show-token"
# local:
export PYTHOND_HOST=127.0.0.1:7399 PYTHOND_TOKEN=<printed-token>
pysh run work "x"
```

## Remote proxy

Local daemon maintains connection to remote daemon. Agent just talks to local.

```bash
pythond daemon                                    # local daemon
pyctl connect work 10.0.0.5:7399 <token> --tls    # proxy alias = remote session
pysh run work "x = 42"                            # forwarded to remote work
pysh run work "x"                                 # → 42 (remote state)
pyctl disconnect work
```

One local proxy can also address a different remote session explicitly:

```bash
pyctl connect server 10.0.0.5:7399 <token> --tls  # proxy alias
pysh run server gpu "x = 42"                      # remote session = gpu
```

## Auto-checkpoint

Successful synchronous `run` cells are saved to `~/.pythond/sessions/<name>/history.py`.
Successful async `fire`/`fork` cells are saved when `poll` observes completion.
Errors go to `session.log` but not `history.py`.

Like shell history and environment variables under SSH, pythond session history,
logs, and live namespaces can expose secrets. `history.py` and `session.log` may
contain executed Python source and captured output. Variables assigned in a
session remain in that live Python process until overwritten or the session is
killed. Do not paste API keys, passwords, tokens, or other secrets into cells
unless you are willing for them to persist in that session and its local files.

```bash
# Process died? Replay:
pysh new work
pysh run work "exec(open(os.path.expanduser('~/.pythond/sessions/work/history.py')).read())"
```

## Security

The security model mirrors SSH:

| pythond | SSH equivalent |
|---------|---------------|
| token in `daemon.json` | private key in `~/.ssh/` |
| `pyctl trust cert.pem` | adding a line to `authorized_keys` |
| `pyctl pin cert.pem` | adding a line to `known_hosts` |
| authenticated client | logged-in user |

Once authenticated, a client has full access to all sessions — there is no per-session permission isolation. This is the same as SSH: once you log in, you are that user with all their permissions.

- **Not a sandbox**: code runs with the daemon user's OS permissions
- **Local POSIX**: AF_UNIX socket with `0o600` permissions
- **Local Windows**: OWNER RIGHTS DACL via `icacls` — owner-level isolation (comparable to Unix `chmod 700`)
- **Remote**: pinned self-signed TLS cert + token auth, with optional mTLS client cert
- **Access logs**: daemon writes `ACCESS` lines to runtime `access.log` and mirrors them to daemon stderr for supervisors; logs include `conn_id`, peer, `cmd`, session, status, and `body_bytes`, but never token or code body
- **Crash isolation**: 5-layer try/except + process isolation — exec errors never kill daemon

## Operations

Run the daemon in the foreground under your supervisor:

```bash
pythond daemon
# or
pyctl start --listen 0.0.0.0:7399 --tls
```

Operational signals:

```bash
pyctl status          # daemon endpoint metadata and liveness
pysh ls               # sessions known to the daemon
pysh status work      # one session's worker health
pyctl stop            # graceful daemon shutdown
```

Logs:

- `ACCESS ...` lines are mirrored to daemon stderr for systemd/supervisor/journald.
- The same access events are appended to the runtime `access.log`.
- Interactive `pysh run/fire/fork` also echoes submitted code, errors, and raw
  `run` output to the client terminal's stderr. That is operator feedback, not
  daemon access logging.
- Per-session activity goes to `~/.pythond/sessions/<name>/session.log`.
- Successful replayable sync execs go to `~/.pythond/sessions/<name>/history.py`.
- Successful async execs go there when `poll` observes completion.

Access logs are for daemon operations: connection id, peer, cmd, session,
status, and body size. They deliberately do not record tokens or Python source.
Use `session.log` when you need the executed code and output.

Runtime files and durable state live in different places:

| Purpose | Windows | POSIX |
|---------|---------|-------|
| daemon metadata/logs | `%LOCALAPPDATA%\pythond\daemon.json`, `%LOCALAPPDATA%\pythond\access.log` | `$XDG_RUNTIME_DIR/pythond/` or `/tmp/pythond-$UID/` |
| session state/certs | `~\.pythond\sessions\...`, `~\.pythond\tls\...` | `~/.pythond/sessions/...`, `~/.pythond/tls/...` |

## Cross-platform

| Platform | PTY | Transport | Notes |
|----------|-----|-----------|-------|
| Linux/macOS | `pty.openpty()` | AF_UNIX WS | full featured |
| Windows | `pywinpty` | TCP WS | `pip install pywinpty` |
| WSL | same as Linux | AF_UNIX WS | full featured |

## Architecture

```
agent (one-shot bash_tool)
  ↓ ws://unix socket or wss://tcp
daemon process (WebSocket server, keep-alive connections)
  ├── session "work" (subprocess, isolated)
  │     ├── persistent namespace (variables live forever)
  │     ├── AI channel: JSON lines over socketpair
  │     └── human channel: real PTY (readline, colors)
  ├── session "gpu" (another subprocess)
  └── remote "server" (WebSocket proxy to remote daemon)
```

## Design

**exec() is the core insight.** Old agent-terminal tools parse ANSI escape sequences
from TTY byte streams to detect when commands finish. pythond uses `exec(code, namespace)` —
source code in, captured output out, function call semantics. No ANSI parsing. No frame detection.

**Connection ≠ state.** SSH conflates them — disconnect kills the shell. pythond separates
them — the WebSocket is transport, the namespace is state. Disconnect and reconnect; state survives.

**Write-file-then-exec.** Complex code with quotes and f-strings? Write a file, then
`exec(open('/tmp/task.py').read())`. The file is transport; the namespace is the workspace.

## REPL Patterns

Because the session is a Python REPL, ordinary Python patterns become agent
operations:

- Prefix tax: import what you use once, then call shorter names in later cells.
- Print tax: expression results display automatically; the last expression does
  not need `print()`.
- Hot reload: use `exec(open("module.py").read())` or `importlib.reload(m)` to
  update code without losing process state.
- Incremental execution: split a long script into cells. If step 3 fails, fix
  step 3; steps 1 and 2 still exist in memory.
- Catch, fix, retry: read the traceback, patch a function, and run again in the
  same namespace.
- Host commands: use `subprocess.run(..., capture_output=True, text=True)` from
  inside the session when you need the OS.

### Persistent subprocess

The session can host long-lived child processes. A persistent bash inside the
persistent Python REPL gives you shell state (cd, env vars, aliases) that
survives across agent turns:

```bash
pysh run work "
from subprocess import Popen, PIPE, STDOUT
import queue, threading, time

shell = Popen(['bash'], stdin=PIPE, stdout=PIPE, stderr=STDOUT, text=True, bufsize=1)
_q = queue.Queue()
threading.Thread(target=lambda: [_q.put(l) for l in shell.stdout], daemon=True).start()

def sh(cmd, timeout=5):
    marker = f'__DONE_{time.monotonic_ns()}__'
    shell.stdin.write(f'{cmd}\necho {marker}\n')
    shell.stdin.flush()
    lines, deadline = [], time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            line = _q.get(timeout=0.1)
            if line is None or marker in line: break
            lines.append(line.rstrip())
        except queue.Empty: continue
    return chr(10).join(lines)
"
```

Now bash state persists:

```bash
pysh run work "print(sh('cd /tmp && pwd'))"
# /tmp

pysh run work "print(sh('pwd'))"
# /tmp  ← cd persisted

pysh run work "print(sh('export SECRET=hunter2'))"
pysh run work "print(sh('echo \$SECRET'))"
# hunter2  ← env var persisted
```

The same pattern works for any interactive subprocess: node, gdb, redis-cli,
psql. The Python session is the host; everything else lives inside it.

## Tests

Static syntax check:

```bash
python -B -m py_compile pythond.py test_pythond.py
```

Run test suite:

```bash
python -B test_pythond.py
```

## Dependencies

```
pythond              websockets, wsproto, cryptography, pywinpty (Windows only)
```

## License

MIT
