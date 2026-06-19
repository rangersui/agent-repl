#!/bin/bash
# monitor.sh — Monitor-ready watcher for agent terminal
# Each stdout line → one Monitor notification. Exit ends the watch.
#
# Usage:
#   monitor.sh [-s session] [-1] [pattern]
#
#   -s SESSION   target session (default: $T_SESSION or "repl")
#   -1           exit after first match (one-shot)
#   pattern      ERE regex to filter output (default: common signals)
#
# Examples:
#   monitor.sh -1 'result='
#   monitor.sh 'Error|Traceback'
#   T_SESSION=py monitor.sh -1 '>>>'
set -euo pipefail

# --- parse args ---
SESSION="${T_SESSION:-repl}"
ONESHOT=false
tail_pid=""
fifo=""
while [ $# -gt 0 ]; do
    case "$1" in
        -s) SESSION="$2"; shift 2;;
        -1) ONESHOT=true; shift;;
        --)  shift; break;;
        *)  break;;
    esac
done
PATTERN="${1:->>>|Error|Traceback|FAILED|Done|complete}"
export T_SESSION="$SESSION"

# --- start pipe ---
logfile=$(t pipe)
cleanup() {
    if [ -n "${tail_pid:-}" ] && kill -0 "$tail_pid" 2>/dev/null; then
        kill "$tail_pid" 2>/dev/null || true
    fi
    [ -n "${fifo:-}" ] && rm -f "$fifo"
    T_SESSION="$SESSION" t unpipe >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

# --- stream ---
if $ONESHOT; then
    # one-shot: FIFO + read loop for clean exit after first match
    fifo="/tmp/t_monitor_$$"
    rm -f "$fifo"; mkfifo "$fifo"
    tail -f "$logfile" > "$fifo" &
    tail_pid=$!
    while IFS= read -r line; do
        clean=$(printf '%s' "$line" | sed 's/\x1b\[[0-9;]*[a-zA-Z]//g; s/\r//g')
        if [ -n "$clean" ] && printf '%s' "$clean" | grep -qE "$PATTERN"; then
            printf '%s\n' "$clean"
            exit 0
        fi
    done < "$fifo"
else
    # continuous: stream all matching lines until stopped
    tail -f "$logfile" \
        | sed -u 's/\x1b\[[0-9;]*[a-zA-Z]//g; s/\r//g' \
        | grep --line-buffered -E "$PATTERN"
fi
