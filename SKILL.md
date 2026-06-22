# agent-tty skill

Use `k` when you need live Python process state across turns:
imports, variables, connections, servers, sockets, cached data, and decisions
that should stay addressable.

`k` is a function-call surface into a persistent Python process: source code in,
captured output out. Do not treat it as a prompt-scraping terminal transcript.

Requires Python 3.10+. The CLI is installed by the package:

```bash
python -m pip install agent-tty
k --version
```

From a source checkout, use editable install once:

```bash
python -m pip install -e .
k --version
```

The code help is authoritative:

```bash
k --help
```

## Mental Model

One named session is one Python child process with one persistent namespace.

Your cells run through Python `eval`/`exec`. Human attach connects to the same
namespace. You and the human are operating the same live runtime.

## Default Work Surface

Use the live session as the default work surface. Once a daemon/session exists,
run task commands through `k run` or `k fire`, including small checks such as
`ls`, `pwd`, and `git status`.

The host shell is plumbing for starting the daemon, editing transport files,
loading large cells from disk, or repairing a broken session. Project work goes
through the session so cwd, env, imports, sockets, variables, and visible history
stay together.

Keep state in the Python namespace when you will need it again:

- parsed data, config, DataFrames, compiled regexes, imported modules,
- database handles, HTTP sessions, WebSockets, sockets, SSH tunnels,
- local servers, file watchers, background daemon threads,
- live decisions such as blocked IPs, feature flags, rate limits, and routing
  weights.

Do not reopen or recompute these on every turn unless the task requires a fresh
state. Read, patch, and inspect them by name in later cells.

Host tools are called from Python inside the session:

```python
import subprocess
subprocess.run(["git", "status"])
```

For complex cells, write a Python file and load it:

```bash
cat > /tmp/agent_tty_task.py << 'PY'
import subprocess
result = subprocess.run(["git", "status"], text=True, capture_output=True)
print(result.stdout)
PY

k run work "exec(open('/tmp/agent_tty_task.py').read())"
```

## When To Use The Host Shell

Use the host shell tool for: daemon lifecycle (`k daemon`, stop with Ctrl-C),
writing Python files to disk before loading them, and package management
(`pip install`). Everything else goes through `k run` or `k fire`.

## First Steps

Before starting, check if a daemon and session already exist:

```bash
k ls
```

If `k ls` responds with session names, the daemon is running. Skip to `k run`.
If it errors or shows `(no sessions)`, start the daemon and create a session.

Start the daemon:

```bash
k daemon
```

Stop the daemon with `Ctrl-C` in the daemon terminal. Shutdown terminates owned
sessions and closes the control socket.

Create a session and prove state persists:

```bash
k new work
k run work "x = 41"
k run work "x + 1"
# 42
```

Run async work:

```bash
k fire work "import time; time.sleep(2); y = x + 1"
# {"cell_id":"...","status":"fired"}

k poll work
# {"cell_id":"...","status":"running|done","output":"..."}
```

Attach as a human:

```bash
k attach work
```

PTY and WinPTY sessions detach with `Ctrl-]` and keep the session alive.
`exit()` ends the session process. Socket-console sessions detach when stdin
ends.

## Session Modes

agent-tty uses the best local console surface available:

| mode | platform | human attach |
| --- | --- | --- |
| POSIX PTY | Linux, macOS, WSL | raw terminal, readline, tab, arrows, Ctrl-C |
| WinPTY | Windows with `pywinpty` | raw terminal through WinPTY |
| socket console | fallback | line-based `InteractiveConsole` over local TCP |

Windows uses TCP transport. The daemon prints a token; set `K_TOKEN` or use a
filled-in `k.py` wrapper before running client commands or `attach`.

## Commands

```text
k daemon                  start daemon in foreground
k new <name>              create a Python session
k int <name>              interrupt running async cells
k kill <name>             terminate session process and forget it
k run <name> "code"       sync Python eval/exec, print raw output
k fire <name> "code"      async queued eval/exec, print JSON cell_id
k poll <name> [cell_id]   print JSON cell result
k status <name>           print JSON session state
k vars <name>             print JSON list of public namespace names
k complete <name> "text"  print JSON Python completion candidates
k ls                      list sessions
k attach <name>           attach human REPL to the session
k --version|-V|version    print version
```

In TCP mode, copy `k.py.template` to `k.py`, paste the daemon token into it,
then use:

```bash
python k.py ...
```

## Output Formats

| command | format | shape |
| --- | --- | --- |
| `k daemon` | process | foreground daemon; startup line on stderr |
| `k new` | text | `OK <name> pid=<pid> ...` or `ERR ...` |
| `k int` | text | `OK interrupted <name> (N cells)` or `ERR ...` |
| `k kill` | text | `OK killed <name>` or `ERR ...` |
| `k ls` | text | one line per session, or `(no sessions)` |
| `k run` | raw text | captured stdout/stderr from the cell |
| `k fire` | JSON | `{"cell_id":"...","status":"fired"}` |
| `k poll` | JSON | `{"cell_id":"...","status":"running|done|error","output":"..."}` |
| `k status` | JSON | `{"state":"idle|running","running":[],"vars":N,"cells":N}` |
| `k vars` | JSON | `{"vars":["name", ...]}` |
| `k complete` | JSON | `{"matches":["os.path", ...]}` |
| `k attach` | stream | interactive console |
| `k --version` | text | `agent-tty 0.2.0`; aliases: `k -V`, `k version` |

`k run` prints expression results. Strings print as raw text; other values use
`repr`. Assignments normally produce empty output.

## REPL Patterns

- Import once, then use shorter names in later cells.
- Use expression results directly: `k run work "len(items)"`.
- For complex code, write a file and load it with
  `exec(open('/tmp/name.py').read())`.
- For host commands, call `subprocess.run(..., capture_output=True, text=True)`
  inside the session so the output is a string you can parse.
- For hot reload, use `exec(open(...).read())` or `importlib.reload(module)`.
- If a cell fails, fix the function or data and retry in the same namespace.
- Split long workflows into small cells so successful prior state is retained.

## Async Rules

`k fire` is queued per session. Multiple fired cells in one session execute
serially under one lock. Use multiple sessions for parallel execution.

`k poll <session> <cell_id>` reads a specific cell.

`k poll <session>` reads the most recent cell, or `{"status":"idle"}` if none
exist.

## Transport

AF_UNIX mode uses filesystem-local `K_SOCK`, default `/tmp/k.sock`.

TCP mode uses `127.0.0.1:K_PORT` (default 7399) and requires `K_TOKEN`. The
daemon prints the token at startup (`set K_TOKEN=...` on Windows,
`export K_TOKEN=...` on Linux/macOS). Attach uses the same token.
`k.py.template` is the local wrapper template for storing that token in an
ignored `k.py` file.

On Windows, WinPTY mode requires `pywinpty`. With `pywinpty` available the daemon
prints `mode=winpty`; otherwise it falls back to `mode=socket`.
