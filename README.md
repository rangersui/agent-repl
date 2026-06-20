# agent-tty

Persistent REPL for AI agents. Shared live terminal for humans.

bash_tool runs a command and forgets. agent-tty keeps a persistent TTY
inside tmux — variables, cwd, imports, connections, SSH sessions, and
debugger state survive across agent turns. The human watches the same
terminal live, can interrupt with `k int`, or take over with `tmux attach`.

The package is `agent-tty`. The CLI command is `k`, intentionally short to minimise token overhead in agent tool calls. `km` is the companion event monitor.

**Requires POSIX + tmux 3.0+** — k drives tmux, tail, and POSIX signals; it does not bundle or replace them.

## Why agent-tty

`bash_tool` is curl. `k` is a socket.

Use `k` when the process must keep memory between commands: Python imports,
database connections, browser/CDP sockets, remote shells, debuggers, running
servers. `k watch` gives a human the same live filtered view — cell markers,
completion ticks, frame noise hidden. `tmux attach` is native raw takeover.

Use `km` when a long cell should call the agent back on completion.
Use `k poll` only as a simple fallback for scripts or agent runtimes without a
monitor/interrupt path.

## Quick Start

```bash
k new work bash
k run -j work "echo hello"
# {"cell_id":"...","status":"done","output":"hello"}

k new py python3 -i                         # Python 3.12 and below
k new py "env PYTHON_BASIC_REPL=1 python3 -i"  # Python 3.13+ (disables _pyrepl auto-indent)
k run -j py "print(42)"
```

## Install

Requires: **POSIX**, **Python 3.10+**, **tmux 3.0+**

```bash
pip install agent-tty            # → k, km, agent-tty in PATH
```

To refresh a stale `k`/`km` entry point, reinstall in the same shell environment
that will run it, then verify the resolved command:

```bash
python -m pip install --upgrade --force-reinstall agent-tty
k --version
km --version
agent-tty --version
python -m agent_tty --version
command -v k    # use: where.exe k  (PowerShell)
```

Or without pip:

```bash
git clone <repo> && cd agent-tty
./scripts/k --help               # works immediately (dev shim)
```

Or symlink into PATH:

```bash
ln -sf "$(pwd)/scripts/k"  /usr/local/bin/k
ln -sf "$(pwd)/scripts/km" /usr/local/bin/km
```

## Commands

```
k new    <session> [cmd...] [--prompt="x"]     spawn (default: bash)
k new    <session> <cmd> --prompt=./hook        hook mode
k fire   [-t N] [session] <code>               async fire (default 300s)
k poll   [session] [cell_id]                   poll (O(1))
k run    [-j] [-t N] [session] <code>          sync (default 30s)
k await  ...                                   alias for run
k notify [session] <message>                   notification (direct to log)
k int    [session]                             ctrl-c (+ re-frame in repeat mode)
k kill   <session>                             kill + cleanup
k ls                                           list sessions
k status [session]                             health + next action
k watch  [session]                             live filtered view
k history [-n N] [session]                     last N×5 lines (default 5)
k --version                                    print agent-tty version
                                                aliases: k -V, k version
```

Session resolves: explicit arg > K_SESSION env > auto-detect (single session).

`k status work` repairs the log pipe if needed and prints the next useful command:

```bash
OK work pipe=ok state=running cell=a1b2c3d4e5f6 next='k poll work a1b2c3d4e5f6 or k int work'
```

## Frame Detection

Three modes via `--prompt`:

| --prompt=     | mode   | how                                         |
| ------------- | ------ | ------------------------------------------- |
| *(not set)* | repeat | 5 empty Enters → 5 identical lines → done |
| `"(gdb)"`   | exact  | match prompt string                         |
| `./hook.py` | hook   | stdin lines → hook exit → done            |

Hook protocol: k feeds ANSI-stripped lines to stdin. Hook exits = frame end. Hook paths must include a path separator (`/`). Path is canonicalised to absolute at `k new` time; hook must exist and be executable.

## How It Works

```
k fire "echo hello"
  |
  +-- acquires lock (rejected fire = zero side effects)
  +-- sends code via paste-buffer (atomic)
      bash multiline: writes 0600 temp script, sends "source <script>"
  +-- sends 5 frame Enters (repeat mode only)
  +-- starts background stream processor
  |
  stream processor tails log:
    ECHOING: skip echo_count lines
    OUTPUT:  collect lines
    DONE:    5 identical lines / prompt match / hook exit
  |
  writes result file -> exits
  |
k poll
  +-- checks result file (O(1))
  +-- returns JSON
```

## Safety

| invariant                | mechanism                                                                                                   |
| ------------------------ | ----------------------------------------------------------------------------------------------------------- |
| one cell per session     | O_EXCL lock, acquired before send                                                                           |
| timeout keeps lock       | lock marked `timed_out`; subsequent polls say `use k int or k kill`                                     |
| completed-cell recovery  | bg watcher marks `completed`; next fire/run can clear a done-lock without losing the result file           |
| orphan recovery          | bg process group in lock, poll checks `os.killpg(pgid, 0)` (POSIX)                                        |
| no line-wrap skew        | tmux width 10000                                                                                            |
| atomic send              | per-session named paste-buffer `k_{session}`                                                              |
| bash multiline state     | private per-cell script + `source`, so cd/env/functions persist without interleaved prompt echoes          |
| ctrl-c safe              | kills watcher, writes `{"status": "error", "output": "interrupted"}`, re-sends frame enters (repeat only) |
| session name validation  | `[A-Za-z0-9_.-]+`, no `..`, no path traversal                                                           |
| idempotent pipe restart  | pipe-pane replaced on every fire/run                                                                        |
| atomic result writes     | tmp + fsync +`os.replace` — poll never reads partial JSON                                                |
| no output classification | "done" = prompt appeared, not success                                                                       |

## JSON Schema (k)

```
fired:        {"cell_id": "...", "status": "fired"}
running:      {"cell_id": "...", "status": "running"}
done:         {"cell_id": "...", "status": "done", "output": "..."}
timeout:      {"cell_id": "...", "status": "timeout", "output": ""}
timeout(2+):  {"cell_id": "...", "status": "timeout", "output": "use k int or k kill"}
error:        {"status": "error", "output": "..."}
cell error:   {"cell_id": "...", "status": "error", "output": "..."}
```

JSON errors without `cell_id`: `no session 'x'; use k new x bash`, `active cell 'x'`, `pipe failed: ...`, `send failed: ...`, `no active cell on 'x'`, `invalid cell_id`.
JSON errors with `cell_id`: `interrupted`, `unknown cell`, `watcher died`, `result missing`, `lock update failed; use k int or k kill`, `lock release failed`, `interrupt failed; use k kill`.
Text-only errors: `no session found; use k ls or k new <session> bash`, `no log for 'x'; use k status x`, `watcher kill failed; use k kill`.

## Metadata on Disk

```
$XDG_RUNTIME_DIR/k_cells/<session>/    (or /tmp/k_cells_<uid>/<session>/)
  _session.json       {name} or {name, prompt}
  _lock.json          {cell_id, log_offset, echo_count, bg_pgid, completed?, timed_out?, timeout_polled?, terminal_status?}
  _output.log         pipe-pane stream (append-only)
  <cell_id>_result.json  stream processor output (deleted after poll)
```

## Known Limitations

agent-tty is POSIX-only: it requires tmux, tail, and POSIX process signals.
WSL is fine; native Windows fails fast.

**Frame collision (repeat mode)**: if output contains 5+ consecutive identical non-empty lines, the stream processor falsely detects completion. Extremely rare — 5 identical lines = zero information entropy.

**echo_count heuristic**: generic REPL mode assumes 1 sent line = 1 echoed line. Bash multiline cells avoid this by sourcing a private per-cell script; other REPLs still rely on prompt filtering or hook/exact prompt mode.

**Hook mode**: no `...` filtering (user takes full control). Hook paths must include a path separator to distinguish them from string prompts.

**Python 3.13+ `_pyrepl`**: The new Python REPL auto-indents pasted code, doubling indentation on multi-line blocks. Workaround: `k new py "env PYTHON_BASIC_REPL=1 python3 -i"`. Single-line code is unaffected.

## km — event monitor

Callback-style completion for persistent TTY cells. `km` tails the session log via pipe-pane and emits one JSON line per event to stdout. No polling, no sleep loops.

Each stdout line is a JSON event. Works with any agent host that has background-notification support: Claude Code's Monitor tool can read each line as an interrupt, Codex App Server can consume it through `vendor/codex_bridge.py`, and plain subprocess readers work the same way.

```
km <session> [cell_id] [-1]
```

`-1` exits after first completion — one-shot `.then()` for agent orchestration.

### Why km after k

`k` is the stateful terminal. `km` is the callback channel for long-running cells.
Background task support alone is not enough when the process state matters;
`km` lets the persistent TTY keep running and wakes the agent when the cell
finishes. `k poll` is O(1) and still useful for simple scripts, but poll loops
waste tokens and add latency:

```bash
# poll loop: agent burns a tool call every N seconds
# k poll → "running" → k poll → "running" → k poll → "done"

# km: one tool call, block until done
km work -1
# {"cell_id": "...", "session": "work", "status": "done", "ts": "..."}
```

With `km -1`, the agent fires a long task, starts `km` as a background monitor, and gets interrupted exactly once when the task completes. Zero wasted calls.

### Continuous mode

Without `-1`, `km` runs indefinitely — every fired/done/notify event streams as a JSON line. Useful for multi-cell orchestration where the agent needs to react to each completion in sequence.

### Codex bridge (experimental)

`vendor/codex_bridge.py` is a local experiment for hosts that expose Codex App Server.
It reads `km` stdout, polls completed cells with `k poll`, and starts a visible
Codex turn with `turn/start` when the target thread is idle. If the thread is
already active, events are queued and batched into one visible turn after the
thread becomes idle. That makes the event visible as a normal Codex turn instead
of hiding it in prompt history.

Important caveat from local testing on 2026-06-20: Codex App Server does not
expose a single Monitor-like primitive. `thread/inject_items` persists data for
later turns but does not wake an agent; `turn/steer` needs an active turn and
expected turn id; `turn/interrupt` stops work but does not carry payload; and
`turn/start` wakes an idle thread but can create a parallel side turn if another
turn is still active. The bridge owns that state machine.

Codex Desktop may also fail to live-refresh turns started by another app-server
client. Treat this bridge as better suited to headless/remote automation and
external sinks such as tmux, a web UI, or email. It is not a guarantee that the
Desktop UI will update in real time.

```bash
# find candidates first
python vendor/codex_bridge.py --list-threads --thread-cwd . --thread-search "agent tty"
python vendor/codex_bridge.py --list-loaded

# run the bridge daemon
python vendor/codex_bridge.py --session work --thread-id THREAD_ID
```

The bridge is deliberately type-sealed: km lines must parse into `KmEvent`,
thread ids must become `ThreadHandle`, thread runtime must become
`ThreadRuntimeStatus`, idle delivery must become `IdleThread`, and visible
`turn/start` calls only accept an `IdleThread` plus an `EventPrompt` derived
from a validated event and optional `PollResult`. The static check is
`python tests/test_bridge_contracts.py`.

### Events

```
fired:       {"cell_id": "...", "session": "...", "status": "fired",       "ts": "..."}
done:        {"cell_id": "...", "session": "...", "status": "done",        "ts": "..."}
timeout:     {"cell_id": "...", "session": "...", "status": "timeout",     "ts": "..."}
interrupted: {"cell_id": "...", "session": "...", "status": "interrupted", "ts": "..."}
notify:      {"session": "...", "status": "notify", "from": "...", "message": "...", "ts": "..."}
closed:      {"session": "...", "status": "closed", "ts": "..."}
error:       {"session": "...", "status": "error",  "message": "...", "ts": "..."}
```

## Testing

```bash
python tests/test_contracts.py      # static code contracts, no tmux
python tests/test_bridge_contracts.py # Codex bridge type-seal contracts, no app-server
python tests/test_docs.py           # docs/package drift, no tmux
python -m mypy --platform linux src vendor tests # POSIX type surface
bash tests/test.sh                  # 66 tests (64 without gdb), runtime smoke suite
python tests/test_regressions.py    # targeted audit regressions
python tests/run_all.py             # all suites
```

## Files

```
src/agent_tty/cli.py       k — main script
src/agent_tty/monitor.py   km — event monitor
scripts/k, scripts/km      dev shims (no pip install needed)
vendor/codex_bridge.py     experimental km → Codex App Server bridge
pyproject.toml             pip install agent-tty → agent-tty, k, km in PATH
man/agent-tty.1            man page source
tests/test.sh              runtime smoke suite
tests/*.py                 static, docs, and regression suites
SKILL.md                   agent reference
EXAMPLES.md                patterns + philosophy
```
