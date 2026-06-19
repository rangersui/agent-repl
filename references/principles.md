# Agent Terminal Principles

## Persistent Over One-Shot

One-shot rebuilds the world:
```bash
python3 -c "import os; os.chdir('/project'); import pandas; df = pandas.read_csv('x.csv'); print(df.head())"
```

Persistent sends deltas:
```bash
t w 'import os; os.chdir("/project")'
t w 'import pandas as pd'
t w 'df = pd.read_csv("x.csv")'
t w 'print(df.head())'
```

The first form repeats setup every time. The second keeps cwd, env, imports, and variables alive. Token savings compound over a session.

## Import Once, Use Forever

The real savings show at the Nth command. Without persistent REPL, every call repeats the full setup:

```
Call 1: python3 -c "import os; import pandas; df = pd.read_csv('x.csv'); print(df.head())"
Call 2: python3 -c "import os; import pandas; df = pd.read_csv('x.csv'); print(df.describe())"
Call 3: python3 -c "import os; import pandas; df = pd.read_csv('x.csv'); print(df.columns)"
```

3 calls × full setup = 3× the import/load tokens.

With persistent REPL, setup is paid once:

```
t w 'import os, pandas as pd'       # once
t w 'df = pd.read_csv("x.csv")'     # once
t w 'df.head()'                       # just this
t w 'df.describe()'                   # just this
t w 'df.columns'                      # just this
```

By call 10, you've saved 9× the setup cost. By call 100, 99×. The longer the session, the bigger the savings.

## REPL As External Memory

The REPL state is machine fact. Chat context is lossy summary.

Recover state from the REPL:
```bash
t w 'locals()'          # Python
t w 'Get-Variable'      # PowerShell
t w 'ls()'              # R
t w 'Object.keys(this)' # Node
t r 20
```

Do not reconstruct from memory what the REPL can answer directly.

## Bounded Reads

`t r 5` costs 5 lines of tokens. `t r 500` costs 500. Ask for what you need.

- After a short command: `t r 5`
- After test output: `t r 30`
- Full screen check: `t r screen 40`
- Never: `t r 99999`

## Shortest Command Wins

Agent tool calls are repeated hundreds of times per session. Character count matters.

```
t w 'x'    — 6 chars
t r 5      — 4 chars
t status   — 8 chars
```

This is why the binary is `t`, not `agent-terminal-controller`.

## Physics Not Policy

tmux/psmux provides:
- PTY persistence (process stays alive)
- send-keys (write to process)
- capture-pane (read from process)
- session multiplexing (multiple REPLs)
- detach/reattach (survive disconnects)

websocat provides:
- WebSocket listener (network transport)
- stdin/stdout bridge (pipe commands)

No framework, no runtime, no daemon. One multiplexer, optional websocat, one shell script.

## Don't Kill Live Sessions

When something goes wrong:
1. Try `t k ctrl-c` first
2. Spawn a separate test session if needed
3. Only `t kill` sessions you created
4. Never kill a session the user is working in
5. State is expensive to rebuild — preserve it

## Shell Boundaries Are Real

tmux/psmux send-keys has quoting rules. Shells eat special characters. These aren't bugs — they're protocol boundaries.

When output looks wrong, test which layer changed the bytes:
```bash
t w 'echo $SHELL'        # did the inner shell expand $?
t w "echo \$SHELL"       # did the outer shell expand $?
t w 'echo test'          # baseline: does plain text survive?
```

Use the shortest command that isolates the boundary.
