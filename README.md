# agent-tty

Persistent Python runtime for your AI agent.

Each session is a long-lived Python namespace in a child process. Agent cells
enter that namespace through `eval`/`exec`, so imports, variables, open
connections, servers, and in-memory decisions survive across agent turns.

`bash_tool` is curl: every call forks a process, runs, and dies. `k` is a
socket: one process stays alive, and every call is a function invocation inside
it. The agent sends source code through a structured pipe and receives captured
output. Humans attach through a real terminal with readline, tab completion,
colors, and Ctrl-C. Both channels share the same namespace.

Requires Python 3.10+.

Install the CLI:

```bash
python -m pip install agent-tty
k --version
```

From a source checkout, install editable once so `k` resolves to this tree:

```bash
python -m pip install -e .
k --version
```

The code help is the single source of truth:

```bash
k --help
```

## Quick Start

Start the daemon in one terminal:

```bash
k daemon
```

Stop the daemon with `Ctrl-C` in that terminal. Daemon shutdown terminates owned
sessions and closes the control socket.

Use the client from another terminal:

```bash
k new work
k run work "x = 41"
k run work "x + 1"
# 42
```

Async cells:

```bash
k fire work "import time; time.sleep(2); y = x + 1"
# {"cell_id": "a1b2c3d4e5f6", "status": "fired"}

k poll work a1b2c3d4e5f6
# {"cell_id": "a1b2c3d4e5f6", "status": "done", "output": ""}

k run work "y"
# 42
```

Human attach:

```bash
k attach work
```

On PTY and WinPTY sessions, `Ctrl-]` detaches and leaves the session alive.
`exit()` exits the session process. On socket-console sessions, ending stdin
detaches.

## Runtime Model

`k daemon` runs a foreground daemon. The daemon owns named session processes
and a local control socket.

Each session is a Python process with:

- one persistent namespace,
- one execution lock,
- one cell table for async `fire` results,
- one human attach surface.

AI commands use a separate control path and share the same namespace as the
human console.

## Stateful First

Most command tools are intentionally stateless: fork, run, die. That is simple
for humans, but wasteful for agents. Agents repeat imports, reopen connections,
re-parse configs, and rebuild intermediate data because the process disappears
after every call.

agent-tty flips that default. The process is the workspace. State is not a
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

The human channel is interactive: `k attach` connects to the same process
through PTY, WinPTY, or socket-console mode. A human can inspect variables,
interrupt with Ctrl-C, or detach with `Ctrl-]` without discarding the namespace.

That split is the core design: pure data for agents, real terminal ergonomics
for humans, one shared runtime underneath.

## Session Modes

agent-tty uses the best local console surface available:

| mode | platform | human attach |
| --- | --- | --- |
| POSIX PTY | Linux, macOS, WSL | raw terminal, readline, tab, arrows, Ctrl-C |
| WinPTY | Windows with `pywinpty` | raw terminal through WinPTY |
| socket console | fallback | line-based `InteractiveConsole` over local TCP |

PTY and WinPTY sessions support `Ctrl-]` to detach while the Python session
keeps running. `exit()` exits the session process. Socket-console sessions
detach when stdin ends.

## Default Work Surface

Default to the live session for project work. Once a daemon/session exists,
commands that affect the task should go through `k run` or `k fire`, even when
the command is as simple as `ls`, `pwd`, or `git status`.

That keeps the work in one inspectable runtime: cwd changes, environment
mutations, imports, open sockets, cached data, and command history all stay
with the session the human can attach to.

Use the host shell as plumbing:

- start or stop the daemon,
- copy `k.py.template` to `k.py` and fill in the token,
- write larger Python cells to files before loading them,
- inspect or repair the repository when no session is available.

Inside the session, call host commands through Python:

```bash
k run work "import subprocess; subprocess.run(['git', 'status'])"
```

## Command Reference

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

`k new <name>` creates a Python session. Put host commands inside Python cells
with `subprocess`:

```bash
k run work "import subprocess; subprocess.run(['git', 'status'])"
```

For larger code, write a Python file and load it into the live session:

```bash
cat > /tmp/agent_tty_task.py << 'PY'
import subprocess
result = subprocess.run(["git", "status"], text=True, capture_output=True)
print(result.stdout)
PY

k run work "exec(open('/tmp/agent_tty_task.py').read())"
```

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

The REPL is Turing complete. File watchers, completion callbacks, local
monitors, proxy servers, and control loops do not need to be built into
agent-tty; the session can build them as Python code.

## Output Formats

Each command has a fixed output style:

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

`k run` prints expression results like a Python REPL: strings print as raw text;
other values use `repr`.

Assignments usually produce no output:

```bash
k run work "x = 1"
# empty output
```

Expressions print:

```bash
k run work "x + 1"
# 2
```

## Async Cells

`fire` starts a background cell and returns immediately. Cells inside one
session execute serially under the session lock. Create multiple sessions for
parallel work.

```bash
k new a
k new b
k fire a "import time; time.sleep(5); result = 'A'"
k fire b "import time; time.sleep(5); result = 'B'"
```

`poll` with a cell id returns that cell. `poll` without a cell id returns the
most recent cell in the session, or `{"status":"idle"}` if no cells exist.

## Transport

AF_UNIX mode uses `K_SOCK`, defaulting to `/tmp/k.sock`.

TCP mode uses `127.0.0.1:K_PORT` (default 7399) and requires `K_TOKEN`. Native
Windows uses TCP mode. The daemon prints the token on startup:

```text
k daemon pid=12345 127.0.0.1:7399 mode=winpty token=abc123...
set K_TOKEN=abc123...
```

On Linux/macOS (when using TCP explicitly):

```text
export K_TOKEN=abc123...
```

Attach uses the same token in TCP mode, so the `k attach` client or `k.py`
wrapper must have `K_TOKEN` set.

Use `k.py.template` to make TCP commands short:

```bash
cp k.py.template k.py
# paste K_TOKEN and K_PORT into k.py
python k.py ls
python k.py new work
```

`k.py` is listed in `.gitignore` because it contains a live local daemon token.

On Windows, WinPTY mode requires `pywinpty`. With `pywinpty` available the daemon
prints `mode=winpty`; otherwise it falls back to `mode=socket`.

## Tests

Static syntax check:

```bash
python -B -m py_compile agent_tty.py k.py.template tests/test_pty_posix.py tests/test_tcp_windows.py
```

POSIX PTY regression suite:

```bash
python3 -B tests/test_pty_posix.py
```

The POSIX suite starts a daemon with a Unix socket, creates a PTY session, and
checks `new`, `ls`, `run`, `fire`, `poll`, `status`, `complete`, and `kill`.

Windows TCP regression suite:

```powershell
python -B tests/test_tcp_windows.py
```

The Windows suite starts a daemon on loopback TCP, reads the daemon token from
stderr, sets `K_TOKEN`/`K_PORT`, then exercises the real CLI client path. It
works with WinPTY when `pywinpty` is installed and with socket-console fallback
otherwise.
