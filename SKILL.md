# k-kernel — REPL-agnostic cell execution for AI agents

## When to use

When the agent needs persistent REPL state across tool calls: live connections, imported modules, running servers, debug sessions. Use k when the process must stay alive between commands. Use bash_tool for one-shot commands.

## First Steps

```bash
k new work bash
k run -j work "echo hello"
# {"cell_id":"...","status":"done","output":"hello"}

k new py python3 -i
k run -j py "print(42)"

k new dbg "gdb -q ./app" --prompt="(gdb)"
k run -j dbg "break main"
```

Zero config for bash/python. `--prompt` for REPLs where empty Enter doesn't redisplay the prompt (gdb, node).

## Commands

```
k new    <session> <cmd...> [--prompt="x"]  spawn session
k fire   [session] <code> [-t N]            async fire (default timeout 300s)
k poll   [session] [cell_id]                poll result (O(1))
k run    [session] <code>                   sync (fire + poll inline)
k run -j [session] <code>                   sync, JSON output
k run -j -t N [session] <code>              sync, custom timeout
k notify [session] <message>                notification (direct to log)
k int    [session]                          ctrl-c + re-send frame
k kill   <session>                          kill + cleanup
k ls                                        list sessions
k status [session]                          health check
k watch  [session]                          live filtered view
k history [session] [-n N]                  last N cells
```

Session resolves: explicit arg > K_SESSION env > auto-detect (single session).

## Architecture

```
k new   -> spawn tmux (width 10000) -> start pipe-pane
k fire  -> batch send-keys (code + frame enters) -> bg watcher starts
k poll  -> check result file (O(1)) -> return output or "running"
k run   -> send code + run stream processor inline (blocking)
```

**Frame delimiter**: after code, k sends FRAME_ENTERS (5) empty Enters. The REPL redraws its prompt 5 times. The stream processor detects 5 consecutive identical lines = completion. No prompt detection needed.

**Stream processor**: state machine (ECHOING -> OUTPUT -> DONE). Tails the log in real-time. Classifies each line as it arrives. Writes result file when done.

**Background watcher**: fire spawns a Python subprocess per cell. It runs the stream processor and writes the result. poll reads the result file. O(1).

## Frame Delimiter

```
agent sends: echo hello + 5 empty Enters
log shows:
  echo hello              <- echo (skipped by echo_count)
  hello                   <- output (collected)
  root@vm:/#              <- prompt 1 (from command)
  root@vm:/#              <- prompt 2 (from Enter)
  root@vm:/#              <- prompt 3 (from Enter)
  root@vm:/#              <- prompt 4 (from Enter)
  root@vm:/#              <- prompt 5 (from Enter)
                           <- 5 identical = DONE

stream processor: sees 5 consecutive identical lines -> removes them from output -> done
```

Works after cd, venv activation, prompt theme change. The repeated lines are always identical to each other regardless of what the prompt looks like.

## Sync Mode

```bash
k run -j work "echo hello"
# {"cell_id":"...","status":"done","output":"hello"}
```

Status is always "done". k detects completion, not errors. Agent reads output and decides.

## Async Mode

```bash
k fire work "make build"
# {"cell_id":"abc123","status":"fired"}

k poll work
# {"cell_id":"abc123","status":"running"}

k poll work
# {"cell_id":"abc123","status":"done","output":"..."}
```

poll is O(1): checks if the background watcher wrote a result file.

## ctrl-c

`k int` sends SIGINT then re-sends frame enters (SIGINT clears readline's typeahead buffer). The background watcher detects the new prompts and resolves the cell.

## JSON Schema

```
fired:    {"cell_id": "...", "status": "fired"}
running:  {"cell_id": "...", "status": "running"}
done:     {"cell_id": "...", "status": "done", "output": "..."}
error:    {"status": "error", "output": "no session 'x'"}
timeout:  {"cell_id": "...", "status": "timeout", "output": ""}
```

## Safety Invariants

- One cell per session (O_EXCL lock). Second fire/run refused.
- Lock stores bg watcher PID. poll detects orphaned watchers (OOM/crash).
- Batch send-keys: all code lines sent in one tmux call (atomic, no race).
- tmux width 10000: prevents line wrapping that would skew echo_count.
- k does not classify output. Status always "done". Agent decides.

## Metadata on Disk

```
/tmp/k_cells/<session>/
  _session.json       {name} or {name, prompt}
  _lock.json          {cell_id, log_offset, echo_count, bg_pid}
  _output.log         pipe-pane stream (append-only)
  <cell_id>_result.json  stream processor output (deleted after poll)
```

## Known Limitations

**Frame collision**: if output contains 5+ consecutive identical non-empty lines, the stream processor falsely detects completion. Extremely rare in practice.

**echo_count heuristic**: assumes 1 sent line = 1 echoed line. Mitigated by tmux width 10000 (no wrapping) and continuation prompt filtering.

**--prompt mode**: for REPLs where empty Enter has side effects (gdb repeats last command, node prints undefined). Uses exact prompt matching instead of repeat detection.

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

## Language Notes

k is REPL-agnostic. Any program with a readline prompt works:

```bash
k new work bash                                # zero config
k new py python3 -i                            # zero config
k new dbg "gdb -q ./app" --prompt="(gdb)"      # explicit prompt
k new redis redis-cli                          # zero config
k new remote "ssh prod"                        # zero config
```

## Testing

```bash
bash test.sh           # 34 tests
bash test.sh ./scripts/k  # custom path
```
