#!/bin/bash
# k-kernel test suite
# Usage: bash test.sh [path/to/k]

K="${1:-scripts/k}"
PASS=0
FAIL=0

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

out() { python3 -c "import sys,json;print(json.load(sys.stdin)['output'])" 2>/dev/null; }
cid() { python3 -c "import sys,json;print(json.load(sys.stdin)['cell_id'])" 2>/dev/null; }

cleanup() { for s in "$@"; do $K kill "$s" 2>/dev/null; done; }
reset() { rm -rf /tmp/k_cells; tmux kill-server 2>/dev/null; }

# ═══════════════════════════════════════════
echo "═══ k-kernel test suite ═══"
echo ""

# ── BASH BASICS ──
reset
echo "── bash basics ──"
$K new w bash >/dev/null; sleep 1
check "echo"        "hello"     "$($K run -j w 'echo hello')"
check "empty-out"   '"output": ""'   "$($K run -j w 'true')"
check "multi-cmd"   "world"     "$($K run -j w 'echo hello && echo world')"
check "unicode"     "你好"      "$($K run -j w 'echo 你好')"

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
$K new p python3 -i >/dev/null; sleep 1
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
sleep 2
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
check "int-resolves" "done" "$R"
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
check "seq-500"     "500"       "$($K run -j w 'seq 1 500 | tail -1')"
check "seq-count"   "1000"      "$($K run -j -t 10 w 'seq 1 1000 | wc -l')"
cleanup w

# ── FIRE TIMEOUT ──
reset
echo "── fire timeout ──"
$K new w bash >/dev/null; sleep 1
check "fire-t"      "fired"     "$($K fire -t 600 w 'echo long_job')"
sleep 2; $K poll w >/dev/null
cleanup w

# ── ORPHAN DETECTION ──
reset
echo "── orphan detection ──"
$K new w bash >/dev/null; sleep 1
CID=$($K fire w "sleep 60" | cid)
sleep 0.5
BG_PID=$(python3 -c "import json;print(json.load(open('/tmp/k_cells/w/_lock.json'))['bg_pid'])")
kill -9 "$BG_PID" 2>/dev/null
sleep 1
check "orphan"      "watcher died" "$($K poll w "$CID")"
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
