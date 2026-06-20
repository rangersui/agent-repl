# agent-tty — persistent TTY for AI agents, shared live terminal for humans

## Install

```bash
pip install agent-tty            # → k, km, agent-tty in PATH
```

If `k --version` is stale or PATH is shadowed, reinstall in the active environment:
`python -m pip install --upgrade --force-reinstall agent-tty`, then run `k --version`,
`km --version`, `agent-tty --version`, and `python -m agent_tty --version`.

Requires POSIX, Python 3.10+, and tmux 3.0+. Or without pip: `./scripts/k` works immediately (dev shim, no install needed).

## When to use

Use k when the process must keep memory between agent turns: live connections, imported modules, cwd/env, running servers, SSH sessions, browser/CDP sockets, or debugger state. The session is a real tmux TTY — the human can watch (`k watch`), interrupt (`k int`), or take over (`tmux attach`) without losing state. Use km for callback-style completion of long cells. Use k poll only as a simple fallback for scripts or agent runtimes without a monitor/interrupt path. Use bash_tool for one-shot commands.

## First Steps

```bash
k new work bash
k run -j work "echo hello"
# {"cell_id":"...","status":"done","output":"hello"}

k new py python3 -i
k run -j py "x = 42"
k run -j py "print(x)"              # state persists: x is still alive

k new dbg "gdb -q ./app" --prompt="(gdb)"
k run -j dbg "break main"
```

Zero config for bash/python. `--prompt` for exact match or custom hook.

## Commands

```
k new    <session> [cmd...] [--prompt="x"]     spawn session (default: bash)
k new    <session> <cmd> --prompt=./hook        hook mode
k fire   [-t N] [session] <code>               async fire (default 300s)
k poll   [session] [cell_id]                   poll async result
k run    [-j] [-t N] [session] <code>          sync (default 30s)
k await  ...                                   alias for run
k notify [session] <message>                   notification event
k int    [session]                             interrupt active cell
k kill   <session>                             kill + cleanup
k ls                                           list tmux sessions
k status [session]                             health + next action
k watch  [session]                             live filtered view
k history [-n N] [session]                     last N×5 lines (default 5)
k --version                                    print agent-tty version
                                                aliases: k -V, k version
```

Session resolves: explicit arg > K_SESSION env > auto-detect (single session).

Use `k status <session>` when stuck. It repairs the log pipe if needed and prints the next useful command.

`k history` shows the last few results inline — quick glance without switching to the terminal.

**Frame detection** has three modes via `--prompt`:

| --prompt= | mode | how it works |
|-----------|------|-------------|
| *(not set)* | repeat | works for ordinary bash/python prompts |
| `"string"` | exact | match prompt string exactly |
| `./file` | hook | stdin lines → hook exit = frame end |

## Frame Detection

### Default: repeated prompt lines (zero config)

Works after `cd`, venv activation, prompt theme changes, and ordinary bash/python prompts.

Bash multiline cells preserve state: `cd`, exported env vars, shell functions, and aliases remain in the same persistent shell.

### Exact match: `--prompt="(gdb)"`

For REPLs where empty Enter has side effects (gdb repeats last command).

### Hook: `--prompt=./detect.py`

k feeds ANSI-stripped, whitespace-stripped output lines to the hook's stdin. Hook exit means the frame is done; k treats the line that triggered the exit as the frame boundary and removes it from output. Hook paths must include a path separator (`/`). The path is canonicalised at `k new` time; the hook file must exist and be executable (`chmod +x`).

```python
#!/usr/bin/env python3
import sys, re
while True:
    line = sys.stdin.readline()
    if not line: break
    if re.match(r'.*[#$]\s*$', line.strip()):
        sys.exit(0)
```

In hook mode, k does not filter `...` continuation prompts; the hook owns frame detection.

## Sync Mode

```bash
k run -j work "echo hello"
# {"cell_id":"...","status":"done","output":"hello"}
```

k does not classify command output. If the REPL returned to its prompt, status is "done" regardless of whether the command succeeded or failed. Agent reads output and decides.

## Async Mode

```bash
k fire work "make build"
# {"cell_id":"a1b2c3d4e5f6","status":"fired"}

k poll work
# {"cell_id":"a1b2c3d4e5f6","status":"running"}

k poll work
# {"cell_id":"a1b2c3d4e5f6","status":"done","output":"..."}
```

Use `k poll` for quick async checks. For long-running work, prefer `km -1` so the agent is woken on completion instead of polling.

## Timeout

On timeout, the lock is NOT released — the REPL command may still be running. Subsequent polls return `status: "timeout"` with a hint to use `k int` or `k kill`. Only explicit recovery releases the lock.

```
k fire work "make build -j8"   # takes too long
k poll work                    # → {"status": "timeout", ...}
k poll work                    # → {"status": "timeout", "output": "use k int or k kill"}
k int work                     # sends Ctrl-C, writes result, releases lock
k poll work                    # → {"status": "error", "output": "interrupted"}
```

## ctrl-c

`k int` interrupts the active cell, writes an `error`/`interrupted` result for it, and releases the session so new commands can run.

## JSON Schema

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

## Known Limitations

agent-tty is POSIX-only. It requires tmux, tail, and POSIX process signals.
WSL is fine; native Windows fails fast.

**Frame collision (repeat mode)**: if output contains 5+ consecutive identical non-empty lines, k may falsely detect completion. Extremely rare — 5 identical lines = zero information entropy.

**Echoed input**: some unusual REPLs echo pasted input differently. If output framing looks wrong, use exact prompt mode or hook mode.

**Hook mode**: no `...` filtering (user takes full control). Hook paths must include a path separator to distinguish them from string prompts.

**Line-protocol REPLs only — not TUIs**: agent-tty works with line-protocol
REPLs (bash, python3 -i classic, sqlite3, node) that emit mostly clean text with
stable prompts. TUIs — programs that paint the screen with cursor positioning and
redraws, including IPython and Python 3.13+'s `_pyrepl` — are designed for human
eyes, not programmatic consumption. Their terminal stream is VT100 draw
instructions, not text; framing breaks. This is an impedance mismatch, not a bug.
`PYTHON_BASIC_REPL=1` switches Python 3.13+ back to line-protocol mode — always
use it for agent work surfaces.

## Python Multi-line

Multi-line blocks work naturally. The trailing newline from shell quoting closes Python blocks:

```bash
k run -j py "
def factorial(n):
    if n <= 1:
        return 1
    return n * factorial(n-1)
"
k run -j py "print(factorial(10))"
# 3628800
```

## Everything Runs in k

Default to k. A k session is the shared working terminal — the human can watch it, attach to it, interrupt it, and see the same cwd/env/history/output the agent sees. Even commands that look stateless (`ls`, `pwd`, `git status`) belong in k because they are part of the shared terminal narrative. Live connections, sockets, database handles, imported modules, SSH sessions — these exist only in the REPL's memory. Splitting work between k and shell tools scatters history and makes the human watch the wrong place.

Shell tools are plumbing, not the work surface. Use them only for: writing quote-safe transport files, starting/stopping helper daemons, checking the host environment before a k session exists, or repairing a broken session.

**Complex code: write to file, then load into k.**

The heredoc (`<< 'EOF'`) preserves file content literally. `source`/`exec` loads it into the live session with zero escaping problems:

```bash
# bash: write + source
cat > /tmp/task.sh << 'EOF'
for f in *.log; do
    grep -q "ERROR" "$f" && echo "$f has errors"
done
EOF
k run -j work "source /tmp/task.sh"

# python: write + exec
cat > /tmp/task.py << 'EOF'
d = {"product_id": "BTC", "price": "104.50"}
print(f"{d.get('product_id')} = ${d.get('price')}")
EOF
k run -j py "exec(open('/tmp/task.py').read())"
```

**Simple code: inline directly in k run.**

```bash
k run -j work "echo hello && ls -la"
k run -j py "print(42)"
k run -j py "
def factorial(n):
    if n <= 1:
        return 1
    return n * factorial(n-1)
"
```

The rule: if the code has `"` or `'` or `\` or `$` inside string literals, write it to a file and load through k. Do not try to hand-escape complex code inline — the quoting layers (shell → k → tmux → REPL) will fight you.

Bash multiline is handled automatically — k writes a cell-id-named script and sources it. For Python and other REPLs, use a unique path (e.g. `/tmp/k_task_<descriptive>_<nonce>.py`) to avoid collisions when multiple agents or sessions are active.

## Language Notes

k is REPL-agnostic. Any program with a readline prompt works:

```bash
k new work bash                                # zero config (repeat mode)
k new py python3 -i                            # zero config (repeat mode)
k new dbg "gdb -q ./app" --prompt="(gdb)"      # exact match
k new custom ./repl --prompt=./detect.py        # hook
k new redis redis-cli                          # zero config
k new remote "ssh prod"                        # zero config
```

## km — event monitor

Callback-style completion for persistent TTY cells. Each stdout line is one JSON event.

Each stdout line is a JSON event. Any host with background-notification support (Claude Code's Monitor tool, Codex's `notify` callback, or a plain subprocess reader) can consume them directly.

```
km <session> [cell_id] [-1]
```

`-1` exits after first completion (one-shot `.then()`). With a cell_id, waits for
that specific cell. Without one, pre-scans the log and returns the most recent
completion — be aware it may return an older cell if nothing new has completed.

### Persistent state plus monitor

k is the stateful terminal. km is the callback channel for long-running cells. Background task support alone is not enough when the process state matters; km lets the persistent TTY keep running and wakes the agent when the cell finishes. Poll loops waste tokens and add latency — every `k poll` is a tool call that returns "running" and accomplishes nothing.

```bash
# poll loop: burns a tool call every N seconds
# k poll → "running" → k poll → "running" → k poll → "done"

# km: one tool call, block until done
km work -1
# {"cell_id": "...", "session": "work", "status": "done", "ts": "..."}
```

Use `km -1` when the task takes longer than a few seconds — fire, start monitor, get interrupted on completion. Use `k poll` for quick checks, shell scripts, or agent frameworks without a monitor/interrupt path.

### Continuous mode

Without `-1`, `km` streams all events indefinitely. For multi-cell orchestration where the agent reacts to each completion.

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
