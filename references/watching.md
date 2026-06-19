# Watching Agent Sessions

## Attach

```bash
tmux attach -t repl          # attach to the agent's session
psmux attach -t repl          # same on Windows
```

You see exactly what the agent sees — every command, every output, in real time.

## Detach

Press `Ctrl-b d` to detach. The session keeps running. The agent keeps working.

## List Sessions

```bash
tmux ls                       # or: psmux ls
```

Shows all active sessions. The agent typically uses names like `repl`, `py`, `sh`, `js`.

## Watch Without Interfering

Attach is read-write — you can type into the session. If you just want to watch:

```bash
# read-only on tmux (no input accepted)
tmux attach -t repl -r
```

On psmux, `-r` may not be available. Just attach and don't type.

## Type While Agent Works

You can type into the session while the agent is using it. Both inputs go to the same REPL. Use this to:

- Fix a typo the agent made
- Import something the agent forgot
- Set a variable the agent needs
- `Ctrl-C` to interrupt a stuck command

The agent will see your changes on its next `t r` read.

## Multiple Observers

tmux/psmux supports multiple clients on the same session:

```bash
# terminal 1: agent is working via send-keys
# terminal 2: you attach and watch
# terminal 3: colleague attaches and watches
```

All see the same pane content.

## Watch a Specific Session

When the agent runs multiple sessions:

```bash
tmux ls                       # Linux/macOS
psmux ls                      # Windows
# py: 1 windows (created ...)
# sh: 1 windows (created ...)
# js: 1 windows (created ...)

tmux attach -t py             # Linux/macOS
psmux attach -t py            # Windows
```

## Scroll Back

Inside an attached session, enter scroll mode:

```
Ctrl-b [                      # enter copy/scroll mode
↑/↓ or PgUp/PgDn             # scroll through history
q                             # exit scroll mode
```

## Quick Peek (No Attach)

If you just want to see the last few lines without attaching:

```bash
tmux capture-pane -t repl -p | tail -20    # Linux/macOS
psmux capture-pane -t repl -p | tail -20   # Windows
```

Same as what the agent sees with `t r 20`.

## Kill a Runaway Session

If the agent left a session running and you want to clean up:

```bash
tmux kill-session -t repl     # Linux/macOS
psmux kill-session -t repl    # Windows
```

The agent will get `ERR: no session 'repl'` on its next command and can respawn.
