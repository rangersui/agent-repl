# Node.js REPL Reference

## Spawn

```bash
t new js node -i
# or with experimental flags
t new js node --experimental-vm-modules -i
```

## State Inspection

```bash
T_SESSION=js t w 'Object.keys(global).filter(k => !k.startsWith("_"))'
T_SESSION=js t w 'console.log(require.cache && Object.keys(require.cache).length)'
T_SESSION=js t w 'process.cwd()'
T_SESSION=js t w 'process.memoryUsage()'
```

## Dump / Restore

No universal serializer like Python's dill. Strategies:

### JSON-serializable state
```bash
T_SESSION=js t w 'const state = { x, y, config }'
T_SESSION=js t w 'require("fs").writeFileSync("/tmp/node_state.json", JSON.stringify(state))'
# restore
T_SESSION=js t w 'const restored = JSON.parse(require("fs").readFileSync("/tmp/node_state.json"))'
T_SESSION=js t w 'Object.assign(global, restored)'
```

### Require cache persists
Modules loaded with `require()` stay cached across commands. No need to dump/restore imports.

What survives (via JSON): plain objects, arrays, strings, numbers, booleans.
What doesn't: functions, class instances, Buffers, Streams, Promises, closures.

## Common Patterns

```bash
# package exploration
T_SESSION=js t w 'const fs = require("fs")'
T_SESSION=js t w 'const path = require("path")'
T_SESSION=js t w 'fs.readdirSync(".").forEach(f => console.log(f))'

# async work
T_SESSION=js t w 'const res = await fetch("https://api.example.com/data")'
T_SESSION=js t w 'const data = await res.json()'
T_SESSION=js t w 'console.log(data)'

# module testing
T_SESSION=js t w 'const myMod = require("./src/module")'
T_SESSION=js t w 'console.log(myMod.transform("test input"))'
```
