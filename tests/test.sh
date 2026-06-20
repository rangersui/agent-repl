#!/bin/bash
# agent-tty test suite
# Usage: bash tests/test.sh [path/to/k]

K="${1:-scripts/k}"
if [[ "$K" == */* ]]; then
    K_ABS=$(python3 -c 'import os,sys; print(os.path.abspath(sys.argv[1]))' "$K")
else
    K_ABS=$(command -v "$K")
fi
if [[ "$K" == */k ]]; then
    KM="${K%/k}/km"
else
    KM="km"
fi
VERSION=$(python3 -c 'import pathlib,re; print(re.search(r"^version = \"([^\"]+)\"", pathlib.Path("pyproject.toml").read_text(), re.M).group(1))')
PASS=0
FAIL=0

# per-user state dir (must match cli.py _cell_dir logic)
if [ -n "$XDG_RUNTIME_DIR" ]; then
    CELL_DIR="$XDG_RUNTIME_DIR/k_cells"
else
    CELL_DIR="/tmp/k_cells_$(id -u)"
fi

check() {
    local name="$1" expect="$2" actual="$3"
    if echo "$actual" | grep -qF "$expect"; then
        PASS=$((PASS+1))
    else
        echo "  ✗ $name"
        echo "    expect: $expect"
        echo "    actual: $(echo "$actual" | head -1 | cut -c1-80)"
        FAIL=$((FAIL+1))
    fi
}

check_exact() {
    local name="$1" expect="$2" actual="$3"
    if [[ "$actual" == "$expect" ]]; then
        PASS=$((PASS+1))
    else
        echo "  ✗ $name"
        echo "    expect: $(printf '%q' "$expect")"
        echo "    actual: $(printf '%q' "$actual")"
        FAIL=$((FAIL+1))
    fi
}

out() { python3 -c "import sys,json;print(json.load(sys.stdin)['output'])" 2>/dev/null; }
cid() { python3 -c "import sys,json;print(json.load(sys.stdin)['cell_id'])" 2>/dev/null; }

cleanup() { for s in "$@"; do $K kill "$s" 2>/dev/null; done; }
reset() { for s in w p d h missing; do rm -rf "$CELL_DIR/$s"; tmux kill-session -t $s 2>/dev/null; done; }

# ═══════════════════════════════════════════
echo "═══ agent-tty test suite ═══"
echo ""

# ── BASH BASICS ──
reset
echo "── bash basics ──"
check_exact "k-version" "agent-tty $VERSION" "$($K --version)"
check_exact "k-version-short" "agent-tty $VERSION" "$($K -V)"
check_exact "k-version-word" "agent-tty $VERSION" "$($K version)"
check_exact "km-version" "agent-tty $VERSION" "$($KM --version)"
check_exact "km-version-short" "agent-tty $VERSION" "$($KM -V)"
check_exact "km-version-word" "agent-tty $VERSION" "$($KM version)"
check "reject-dot-session" "invalid session name" "$($K new . bash 2>&1 || true)"
check "poll-missing-session" "no session 'missing'; use k new missing bash" "$($K poll missing 2>&1 || true)"
check "int-missing-session" "ERR no session 'missing'; use k new missing bash" "$($K int missing 2>&1 || true)"
$K new w bash >/dev/null; sleep 1
check "new-idempotent" "OK w (alive)" "$($K new w bash)"
check "ls" "w" "$($K ls)"
check "status-idle" "state=idle" "$($K status w)"
check "run-bad-timeout" "ERR -t must be a positive integer" "$($K run -t abc w 'echo nope' 2>&1 || true)"
check "run-zero-timeout" "ERR -t must be a positive integer" "$($K run -t 0 w 'echo nope' 2>&1 || true)"
check "fire-negative-timeout" "ERR -t must be a positive integer" "$($K fire -t -5 w 'echo nope' 2>&1 || true)"
check "fire-no-code" "usage: k fire" "$($K fire -t 5 2>&1 || true)"
check "history-bad-n" "ERR -n must be a positive integer" "$($K history -n xyz w 2>&1 || true)"
check "echo"        "hello"     "$($K run -j w 'echo hello')"
check_exact "run-text" "textmode" "$($K run w 'echo textmode')"
check "empty-out"   '"output": ""'   "$($K run -j w 'true')"
check "multi-cmd"   "world"     "$($K run -j w 'echo hello && echo world')"
check "unicode"     "你好"      "$($K run -j w 'echo 你好')"
check "invalid-cell-id" "invalid cell_id" "$($K poll w not-a-cell)"
check "notify" "OK notified: deploy complete" "$($K notify w 'deploy complete')"
check "history" "deploy complete" "$($K history -n 5 w)"
WATCH_CAPTURE=$(python3 - "$K_ABS" <<'PY'
import signal
import subprocess
import sys
import time

k = sys.argv[1]
p = subprocess.Popen([k, "watch", "w"], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
time.sleep(0.5)
subprocess.run([k, "notify", "w", "watch_ping"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
time.sleep(0.5)
p.send_signal(signal.SIGINT)
try:
    out, _ = p.communicate(timeout=3)
except subprocess.TimeoutExpired:
    p.kill()
    out, _ = p.communicate()
print(out, end="")
PY
)
check "watch" "watch_ping" "$WATCH_CAPTURE"

# ── BASH PERSISTENCE ──
echo "── bash persistence ──"
$K run -j w "export FOO=bar" >/dev/null
check "export"      "bar"       "$($K run -j w 'echo $FOO')"
$K run -j w "cd /tmp" >/dev/null
check "cd"          "/tmp"      "$($K run -j w 'pwd')"
$K run -j w "cd /var" >/dev/null
check "cd-again"    "/var"      "$($K run -j w 'pwd')"
$K run -j w "myfn() { echo fn_works; }" >/dev/null
check "function"    "fn_works"  "$($K run -j w 'myfn')"
cleanup w

# ── BASH MULTI-LINE ──
reset
echo "── bash multi-line ──"
$K new w bash >/dev/null; sleep 1
OUT="$($K run -j w $'echo first\necho second' | out)"
check_exact "two-line-output" $'first\nsecond' "$OUT"
$K run -j w $'export MLINE=sticky\nmkdir -p /tmp/agent_tty_multiline\ncd /tmp/agent_tty_multiline\nmlfn(){ echo fn:$MLINE; }' >/dev/null
OUT="$($K run -j w $'pwd\necho env:$MLINE\nmlfn' | out)"
check_exact "multiline-state" $'/tmp/agent_tty_multiline\nenv:sticky\nfn:sticky' "$OUT"
check "for" "c" "$($K run -j w '
for i in a b c; do
    echo $i
done
')"
check "if" "yes" "$($K run -j w '
if [ 1 -eq 1 ]; then
    echo yes
fi
')"
check "while" "3" "$($K run -j w '
x=0
while [ $x -lt 3 ]; do
    x=$((x+1))
done
echo $x
')"
cleanup w

# ── PYTHON ──
reset
echo "── python ──"
$K new p env PYTHON_BASIC_REPL=1 python3 -i >/dev/null; sleep 1
check "py-print"    "42"        "$($K run -j p 'print(42)')"
check "py-error"    "Traceback" "$($K run -j p '1/0')"
check "py-survives" "alive"     "$($K run -j p "print('alive')")"

# ── PYTHON MULTI-LINE (echo_count) ──
echo "── python multi-line ──"
$K run -j p '
def factorial(n):
    if n <= 1:
        return 1
    return n * factorial(n-1)
' >/dev/null
check "py-def"      "120"       "$($K run -j p 'print(factorial(5))')"

$K run -j p '
total = 0
for i in range(10):
    total += i
' >/dev/null
check "py-for"      "45"        "$($K run -j p 'print(total)')"

$K run -j p '
class Dog:
    def speak(self):
        return "woof"
' >/dev/null
check "py-class"    "woof"      "$($K run -j p 'print(Dog().speak())')"
cleanup p

# ── FIRE / POLL ──
reset
echo "── fire/poll ──"
$K new w bash >/dev/null; sleep 1
CID=$($K fire w "sleep 1 && echo ASYNC" | cid)
check "running"     "running"   "$($K poll w "$CID")"
check "status-running" "next='k poll w $CID or k int w'" "$($K status w)"
sleep 2
KM_DONE=$($KM w "$CID" -1)
check "km-oneshot-done" '"status": "done"' "$KM_DONE"
check "km-oneshot-filter" "\"cell_id\": \"$CID\"" "$KM_DONE"
check "done"        "ASYNC"     "$($K poll w "$CID")"

# ── LOCK ──
echo "── lock ──"
$K fire w "sleep 3" >/dev/null
check "fire-locked" "active cell" "$($K fire w 'nope')"
check "run-locked"  "active cell" "$($K run -j -t 1 w 'nope')"
sleep 4; $K poll w >/dev/null
cleanup w

# ── CTRL-C ──
reset
echo "── ctrl-c ──"
$K new w bash >/dev/null; sleep 1
CID=$($K fire w "sleep 30" | cid)
sleep 0.5
$K int w >/dev/null
# poll until resolved (bg watcher needs time after ctrl-c)
for i in 1 2 3 4 5; do
    R=$($K poll w "$CID")
    echo "$R" | grep -q '"done"\|"error"' && break
    sleep 1
done
check "int-resolves" "interrupted" "$R"
check "int-recover"  "ok"       "$($K run -j w 'echo ok')"
cleanup w

# ── REPEATED OUTPUT LINES (data preservation) ──
reset
echo "── repeated output ──"
$K new w bash >/dev/null; sleep 1
OUT=$($K run -j w "echo same; echo same; echo end" | out)
check "2-repeat"    "same"      "$OUT"
N=$(echo "$OUT" | grep -c "same")
[ "$N" = "2" ] && PASS=$((PASS+1)) || { echo "  ✗ 2-repeat-count (got $N, want 2)"; FAIL=$((FAIL+1)); }

OUT=$($K run -j w "echo r; echo r; echo r; echo r; echo end" | out)
check "4-repeat"    "end"       "$OUT"
N=$(echo "$OUT" | grep -c "^r$")
[ "$N" = "4" ] && PASS=$((PASS+1)) || { echo "  ✗ 4-repeat-count (got $N, want 4)"; FAIL=$((FAIL+1)); }
cleanup w

# ── LONG OUTPUT ──
reset
echo "── long output ──"
$K new w bash >/dev/null; sleep 1
check "seq-500"     "500"       "$($K run -j -t 60 w 'seq 1 500 | tail -1')"
check "seq-count"   "1000"      "$($K run -j -t 60 w 'seq 1 1000 | wc -l')"
cleanup w

# ── FIRE TIMEOUT ──
reset
echo "── fire timeout ──"
$K new w bash >/dev/null; sleep 1
FIRE_OUT=$($K fire -t 1 w 'sleep 30')
CID=$(echo "$FIRE_OUT" | cid)
check "fire-t"      "fired"     "$FIRE_OUT"
sleep 2
check "timeout-first" '"status": "timeout"' "$($K poll w "$CID")"
check "timeout-hint" "use k int or k kill" "$($K poll w "$CID")"
$K int w >/dev/null
sleep 1
check "timeout-recover" "after_timeout" "$($K run -j -t 5 w 'echo after_timeout')"
cleanup w

# ── HOOK RELATIVE PATH ──
reset
echo "── hook relative path ──"
HOOK_TMP=$(mktemp -d)
cat > "$HOOK_TMP/detect.sh" <<'HOOK'
#!/bin/sh
while IFS= read -r line; do
    [ "$line" = "HOOK_DONE" ] && exit 0
done
exit 1
HOOK
chmod +x "$HOOK_TMP/detect.sh"
(cd "$HOOK_TMP" && "$K_ABS" new h bash --prompt=./detect.sh >/dev/null)
sleep 1
check "hook-relative" "HOOK_OK" "$($K run -j h 'echo HOOK_OK; echo HOOK_DONE')"
cleanup h
rm -rf "$HOOK_TMP"

# ── ORPHAN DETECTION ──
reset
echo "── orphan detection ──"
$K new w bash >/dev/null; sleep 1
CID=$($K fire w "sleep 60" | cid)
sleep 0.5
BG_PGID=$(python3 -c "import json;print(json.load(open('$CELL_DIR/w/_lock.json'))['bg_pgid'])")
kill -9 "-$BG_PGID" 2>/dev/null
sleep 1
check "orphan"      "watcher died" "$($K poll w "$CID")"
check "orphan-locked" "active cell" "$($K run -j -t 1 w 'echo nope')"
check "orphan-timeout-hint" "use k int or k kill" "$($K poll w "$CID")"
# sleep 60 is still running in REPL — need to cancel it
$K int w >/dev/null
sleep 1
check "orphan-recv" "after"     "$($K run -j -t 5 w 'echo after')"
cleanup w

# ── GDB (--prompt) ──
reset
echo "── gdb ──"
if which gdb >/dev/null 2>&1 && [ -f /tmp/test ]; then
    $K new d "gdb -q /tmp/test" --prompt="(gdb)" >/dev/null; sleep 2
    check "gdb-break"  "Breakpoint"  "$($K run -j d 'break main')"
    check "gdb-run"    "main"        "$($K run -j d 'run')"
    cleanup d
else
    echo "  (skipped — gdb or test binary not available)"
fi

# ═══════════════════════════════════════════
echo ""
echo "═══ $PASS passed, $FAIL failed ═══"
[ "$FAIL" -eq 0 ] && exit 0 || exit 1
