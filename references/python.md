# Python REPL Reference

## Spawn

```bash
t new repl python3 -i
# or with specific version / venv
t new repl /path/to/venv/bin/python -i
```

## State Inspection

```bash
t w 'print({k:v for k,v in locals().items() if not k.startswith("_")})'
t w 'print(dir())'
t w 'import sys; print(sys.modules.keys())'
```

## Dump / Restore

Uses `dill` (install: `pip install dill`).

```bash
t dump              # → dill.dump_session("/tmp/t_session_repl.pkl")
t restore           # → dill.load_session(...)
```

What survives: variables, imports, data structures, class instances, closures, lambdas.
What doesn't: cwd (`os.chdir` — re-run after restore), open files, sockets, threads, generators mid-yield.

### Manual checkpoint

```bash
t w 'import dill; dill.dump_session("/tmp/checkpoint_v2.pkl")'
# later
t w 'import dill; dill.load_session("/tmp/checkpoint_v2.pkl")'
```

## Multiline

Python needs a blank Enter after indented blocks (for/if/def/class). Use:

```bash
t w 'for i in range(5): print(i)'    # one-liner — works
t w 'def f(x):'                       # multiline — needs extra Enter
t w '    return x * 2'
t k enter                             # close block
```

Do not send the next unrelated command while the prompt is `...`; Python will treat it as part of the unfinished block. Close the block with `t k enter`, or abort it with `t k ctrl-c` if the indentation state is wrong.

## Common Patterns

```bash
# data exploration
t w 'import pandas as pd'
t w 'df = pd.read_csv("data.csv")'
t w 'print(df.shape)'
t w 'print(df.describe())'

# debugging
t w 'import traceback'
t w 'try: risky_call()'
t w 'except: traceback.print_exc()'
t k enter

# long task
t w 'result = train_model(epochs=100); print("TRAIN_DONE")'
t W 'TRAIN_DONE'                          # wait for explicit completion marker
t w 'print(result.metrics)'              # inspect
```
