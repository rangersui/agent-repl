# Bash REPL Reference

## Spawn

```bash
t new sh bash
# or with specific shell
t new sh zsh
t new sh fish
```

## State Inspection

```bash
T_SESSION=sh t w 'env | sort'           # environment variables
T_SESSION=sh t w 'declare -p'           # all variables with types
T_SESSION=sh t w 'alias'                # aliases
T_SESSION=sh t w 'type -a funcname'     # function definitions
T_SESSION=sh t w 'pwd'                  # working directory
```

## Dump / Restore

Bash state is process-level. No single serializer like dill. Export what you can:

### Environment variables
```bash
T_SESSION=sh t w 'export -p > /tmp/bash_env.sh'
# restore
T_SESSION=sh t w 'source /tmp/bash_env.sh'
```

### Functions
```bash
T_SESSION=sh t w 'declare -f > /tmp/bash_funcs.sh'
# restore
T_SESSION=sh t w 'source /tmp/bash_funcs.sh'
```

### Combined dump
```bash
T_SESSION=sh t w '{ export -p; declare -f; alias -p; echo "cd $(pwd)"; } > /tmp/bash_state.sh'
# restore
T_SESSION=sh t w 'source /tmp/bash_state.sh'
```

What survives: env vars, functions, aliases, cwd (via cd command).
What doesn't: job table, open fds, shell options (set -o), process substitutions.

## Persistent State Advantages

```bash
T_SESSION=sh t w 'cd /project && export RUST_LOG=debug'
T_SESSION=sh t w 'cargo test'           # cwd and env already set
T_SESSION=sh t w 'cargo test --release' # still set, no rebuild
```

## Common Patterns

```bash
# build + test
T_SESSION=sh t w 'cd /repo'
T_SESSION=sh t w 'make -j$(nproc)'
T_SESSION=sh t w 'make test'

# long running process
T_SESSION=sh t w './train.sh &'
T_SESSION=sh t w 'jobs'
T_SESSION=sh t W 'Done'

# pipe chains
T_SESSION=sh t w 'cat log.txt | grep ERROR | wc -l'
```
