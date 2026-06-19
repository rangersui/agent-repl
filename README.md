# k-kernel

Structured async execution over PTY for AI agents. REPL-agnostic. Zero config.

Agent fires code, polls for output, gets JSON. REPL stays alive between cells. Any readline prompt works. Frame delimiter = repeated prompt lines (no prompt detection needed).

## Quick Start

```bash
k new work bash
k run -j work "echo hello"
# {"cell_id":"...","status":"done","output":"hello"}

k new py python3 -i
k run -j py "print(42)"
```

## Commands

```
k new    <session> <cmd...> [--prompt="x"]  spawn
k fire   [session] <code> [-t N]            async fire
k poll   [session] [cell_id]                poll (O(1))
k run    [session] <code>                   sync (fire+poll)
k run -j [session] <code>                   sync, JSON
k run -j -t N [session] <code>              sync, timeout
k notify [session] <message>                notification
k int    [session]                          ctrl-c + re-frame
k kill   <session>                          cleanup
k ls / k status / k watch / k history
```

## How It Works

```
k fire "echo hello"
  |
  +-- sends: echo hello + 5 empty Enters (one tmux call, atomic)
  +-- starts: background stream processor
  |
  stream processor tails log:
    ECHOING: skip echo_count lines
    OUTPUT:  collect lines
    DONE:    5 consecutive identical lines (= prompt redrawn)
  |
  writes result file -> exits
  |
k poll
  +-- checks result file (O(1))
  +-- returns JSON
```

## Architecture

| component | role |
|-----------|------|
| pipe-pane | captures PTY output to log (lossless) |
| batch send-keys | sends all code in one tmux call |
| stream processor | state machine: ECHOING -> OUTPUT -> DONE |
| bg watcher | one per cell, writes result, exits when done |
| O_EXCL lock | one cell per session, stores bg PID |
| frame enters | 5 empty Enters -> 5 identical prompt lines -> frame end |

## Safety

| invariant | mechanism |
|-----------|-----------|
| one cell per session | O_EXCL atomic lock (fire + run both lock) |
| orphan recovery | bg PID in lock, poll checks /proc |
| no line-wrap skew | tmux width 10000 |
| atomic send | batch send-keys (single fork) |
| ctrl-c safe | re-sends frame enters after SIGINT |
| no output classification | status = "done" always |

## Testing

```bash
bash test.sh           # 34 tests, covers all edge cases
```

## Files

```
scripts/k      568 lines  main script
scripts/km     219 lines  event monitor
test.sh        204 lines  test suite (34 tests)
SKILL.md                  agent reference
EXAMPLES.md               patterns + philosophy
```
