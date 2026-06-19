#!/bin/bash
# t — persistent REPL terminal over tmux/psmux
# async write/read, multi-session, dump/restore, language-aware
set -euo pipefail

T_SESSION="${T_SESSION:-repl}"
T_STRIP_ANSI="${T_STRIP_ANSI:-1}"

# --- auto-remote: if T_REMOTE is set, all commands go through WebSocket ---
if [ -n "${T_REMOTE:-}" ] && [ "${1:-}" != "remote" ] && [ "${1:-}" != "serve" ]; then
    shift_cmd="${1:-status}"; shift || true
    echo "$shift_cmd $*" | websocat "$T_REMOTE" --text -n1
    exit $?
fi

if command -v tmux >/dev/null 2>&1; then
    MUX=tmux
elif command -v psmux >/dev/null 2>&1; then
    MUX=psmux
else
    echo "ERR: tmux or psmux required"; exit 1
fi
# detect psmux (pipe-pane is stub, resize-window is stub)
T_IS_PSMUX=false
$MUX --help 2>&1 | grep -qi psmux && T_IS_PSMUX=true

strip() {
    if [ "$T_STRIP_ANSI" = 1 ]; then
        sed 's/\x1b\[[0-9;]*[a-zA-Z]//g; s/\x1b\[<[0-9;]*[mM]//g; s/\x1b\[?[0-9;]*[hlsr]//g; s/[0-9]\{1,4\};[0-9]\{1,4\};[0-9]\{1,4\}[mM]//g; s/\r//g'
    else
        cat
    fi
}
pane_alive() { $MUX has-session -t "$T_SESSION" 2>/dev/null; }
detect_lang() {
    local cmd
    cmd="$($MUX display-message -t "$T_SESSION" -p '#{pane_current_command}' 2>/dev/null || true)"
    case "$cmd" in
        python*) echo python;;
        node*)   echo node;;
        R|r)     echo r;;
        bash|zsh|sh|fish) echo bash;;
        *)       echo unknown;;
    esac
}
# FIFO-based wait needs mkfifo + Unix cat — only works on real tmux.
# psmux: always poll (mkfifo unreliable on Windows, PowerShell cat != Unix cat).
# `t pipe` on psmux uses capture-pane polling; see the pipe) case below.
_has_pipe_pane() { ! $T_IS_PSMUX; }

_validate_wait_mode() {
    case "${T_WAIT_MODE:-}" in
        ""|poll) return 0;;
        fifo)
            if _has_pipe_pane; then
                return 0
            fi
            echo "ERR: T_WAIT_MODE=fifo requires tmux pipe-pane; psmux supports poll only" >&2
            return 2
            ;;
        *)
            echo "ERR: T_WAIT_MODE must be 'poll' or 'fifo'" >&2
            return 2
            ;;
    esac
}

# blocking wait via FIFO + pipe-pane (preferred, no sleep)
_wait_for_fifo() {
    local needle="$1" timeout="${2:-30}" mode="${3:-text}"
    local fifo="/tmp/t_fifo_${T_SESSION}_$$"
    if ! _has_pipe_pane; then
        echo "ERR: T_WAIT_MODE=fifo requires tmux pipe-pane; psmux supports poll only" >&2
        return 2
    fi
    rm -f "$fifo"
    if ! mkfifo "$fifo"; then
        echo "ERR: mkfifo failed; use T_WAIT_MODE=poll on this platform" >&2
        rm -f "$fifo"
        return 2
    fi
    if ! exec 3<>"$fifo"; then
        echo "ERR: could not open wait FIFO; use T_WAIT_MODE=poll on this platform" >&2
        rm -f "$fifo"
        return 2
    fi
    trap 'trap - RETURN; $MUX pipe-pane -t "$T_SESSION" 2>/dev/null || true; exec 3>&- 2>/dev/null || true; rm -f "$fifo"' RETURN
    if ! $MUX pipe-pane -t "$T_SESSION" "cat > '$fifo'"; then
        echo "ERR: tmux pipe-pane failed; use T_WAIT_MODE=poll on this platform" >&2
        return 2
    fi
    local rc=1
    SECONDS=0
    while [ $SECONDS -lt "$timeout" ]; do
        if IFS= read -r -t 1 line <&3; then
            clean=$(printf '%s' "$line" | sed 's/\x1b\[[0-9;]*[a-zA-Z]//g; s/\r//g; s/[[:space:]]*$//')
            case "$mode" in
                line) [ "$clean" = "$needle" ] && { rc=0; break; };;
                *)    printf '%s' "$clean" | grep -qF "$needle" && { rc=0; break; };;
            esac
        fi
    done
    return $rc
}

# fallback: capture-pane polling (for psmux / no pipe-pane)
_wait_for_poll() {
    local needle="$1" timeout="${2:-30}" mode="${3:-text}"
    local rc=1
    SECONDS=0
    while [ $SECONDS -lt "$timeout" ]; do
        local snap
        snap=$($MUX capture-pane -t "$T_SESSION" -p 2>/dev/null | sed 's/\x1b\[[0-9;]*[a-zA-Z]//g; s/\r//g; s/[[:space:]]*$//')
        case "$mode" in
            line) printf '%s\n' "$snap" | grep -qxF "$needle" && { rc=0; break; };;
            *)    printf '%s\n' "$snap" | grep -qF "$needle" && { rc=0; break; };;
        esac
        sleep 0.2
    done
    return $rc
}

# auto-select: FIFO if pipe-pane works, otherwise poll
_wait_for() {
    _validate_wait_mode || return $?
    case "${T_WAIT_MODE:-}" in
        poll) _wait_for_poll "$@";;
        fifo) _wait_for_fifo "$@";;
        *)    if _has_pipe_pane; then _wait_for_fifo "$@"; else _wait_for_poll "$@"; fi;;
    esac
}

case "${1:-status}" in

# --- write (async, fire-and-forget) ---
w)  shift
    pane_alive || { echo "ERR: no session '$T_SESSION'"; exit 1; }
    $MUX send-keys -t "$T_SESSION" -l "$*"
    $MUX send-keys -t "$T_SESSION" Enter
    ;;
w!) shift
    pane_alive || { echo "ERR: no session '$T_SESSION'"; exit 1; }
    $MUX send-keys -t "$T_SESSION" -l "$*"
    ;;

# --- execute (sync, waits for completion via sentinel) ---
x)  shift
    pane_alive || { echo "ERR: no session '$T_SESSION'"; exit 1; }
    sentinel="__T_DONE_${RANDOM}${RANDOM}__"
    lang="$(detect_lang)"
    case "$lang" in
        python) $MUX send-keys -t "$T_SESSION" -l "$*"
                $MUX send-keys -t "$T_SESSION" Enter
                $MUX send-keys -t "$T_SESSION" -l 'print("'"$sentinel"'")'
                $MUX send-keys -t "$T_SESSION" Enter;;
        node)   $MUX send-keys -t "$T_SESSION" -l "$*"
                $MUX send-keys -t "$T_SESSION" Enter
                $MUX send-keys -t "$T_SESSION" -l 'console.log("'"$sentinel"'")'
                $MUX send-keys -t "$T_SESSION" Enter;;
        r)      $MUX send-keys -t "$T_SESSION" -l "$*"
                $MUX send-keys -t "$T_SESSION" Enter
                $MUX send-keys -t "$T_SESSION" -l 'cat("'"$sentinel"'\n")'
                $MUX send-keys -t "$T_SESSION" Enter;;
        *)      $MUX send-keys -t "$T_SESSION" -l "$*; echo $sentinel"
                $MUX send-keys -t "$T_SESSION" Enter;;
    esac
    # t x prints its own sentinel before waiting. Polling can see the
    # current screen/scrollback; FIFO pipe-pane would miss fast sentinels
    # emitted before the pipe is armed.
    if _wait_for_poll "$sentinel" "${T_EXEC_TIMEOUT:-30}" line; then
        $MUX capture-pane -t "$T_SESSION" -p | strip | grep -v '^$' | grep -v "$sentinel" | tail -20
    else
        echo "TIMEOUT: command did not complete in ${T_EXEC_TIMEOUT:-30}s"
        $MUX capture-pane -t "$T_SESSION" -p | strip | grep -v '^$' | tail -10
        exit 1
    fi
    ;;

# --- read ---
r)  shift
    pane_alive || { echo "ERR: no session '$T_SESSION'"; exit 1; }
    mode="history"
    N=20
    if [ "${1:-}" = "screen" ]; then
        mode="screen"; shift; N="${1:-40}"
    else
        N="${1:-20}"
    fi
    if [ "$mode" = "screen" ]; then
        $MUX capture-pane -t "$T_SESSION" -p | strip | grep -v '^$' | tail -"$N"
    else
        $MUX capture-pane -t "$T_SESSION" -p -S -"$N" | strip | grep -v '^$'
    fi
    ;;

# --- wait for text ---
W)  shift; needle="$1"; timeout="${2:-30}"
    pane_alive || { echo "ERR: no session '$T_SESSION'"; exit 1; }
    _validate_wait_mode || exit $?
    if _wait_for "$needle" "$timeout"; then
        echo "OK: found '$needle'"
    else
        echo "TIMEOUT: '$needle' not found after ${timeout}s"; exit 1
    fi
    ;;

# --- send key ---
k)  shift
    pane_alive || { echo "ERR: no session '$T_SESSION'"; exit 1; }
    case "$1" in
        ctrl-c)    $MUX send-keys -t "$T_SESSION" C-c;;
        ctrl-d)    $MUX send-keys -t "$T_SESSION" C-d;;
        ctrl-z)    $MUX send-keys -t "$T_SESSION" C-z;;
        ctrl-l)    $MUX send-keys -t "$T_SESSION" C-l;;
        enter)     $MUX send-keys -t "$T_SESSION" Enter;;
        escape|esc)$MUX send-keys -t "$T_SESSION" Escape;;
        backspace) $MUX send-keys -t "$T_SESSION" BSpace;;
        tab)       $MUX send-keys -t "$T_SESSION" Tab;;
        up)        $MUX send-keys -t "$T_SESSION" Up;;
        down)      $MUX send-keys -t "$T_SESSION" Down;;
        *)         $MUX send-keys -t "$T_SESSION" "$1";;
    esac
    ;;

# --- resize ---
z)  shift
    pane_alive || { echo "ERR: no session '$T_SESSION'"; exit 1; }
    if $T_IS_PSMUX; then
        $MUX resize-pane -t "$T_SESSION" -x "${1:-200}" -y "${2:-50}"
    else
        $MUX resize-window -t "$T_SESSION" -x "${1:-200}" -y "${2:-50}"
    fi
    ;;

# --- session management ---
new)shift; name="${1:?name required}"; shift
    if [ $# -gt 0 ]; then
        if $T_IS_PSMUX; then
            # psmux needs -- before command; resolve to Windows path
            cmd="$1"; shift
            resolved="$(command -v "$cmd" 2>/dev/null || echo "$cmd")"
            if command -v cygpath >/dev/null 2>&1; then
                resolved="$(cygpath -w "$resolved")"
            fi
            $MUX new-session -d -s "$name" -x 200 -y 50 -- "$resolved" "$@"
        else
            $MUX new-session -d -s "$name" -x 200 -y 50 "$@"
        fi
    else
        $MUX new-session -d -s "$name" -x 200 -y 50
    fi
    $MUX set -t "$name" mouse off 2>/dev/null || true
    T_SESSION="$name"
    echo "OK: session '$name' started"
    ;;
ls) $MUX list-sessions 2>/dev/null || echo "no sessions"
    ;;
@*) T_SESSION="${1#@}"
    export T_SESSION
    echo "OK: use T_SESSION='$T_SESSION' t <cmd> to target this session"
    ;;
kill)
    pane_alive && $MUX kill-session -t "$T_SESSION" && echo "OK: killed '$T_SESSION'"
    ;;
clear)
    pane_alive || { echo "ERR: no session '$T_SESSION'"; exit 1; }
    $MUX clear-history -t "$T_SESSION"
    echo "OK: cleared '$T_SESSION' scrollback"
    ;;

# --- dump/restore state ---
dump)
    pane_alive || { echo "ERR: no session '$T_SESSION'"; exit 1; }
    lang="$(detect_lang)"
    sentinel="DUMP_OK_${RANDOM}"
    case "$lang" in
        python)
            file="${2:-/tmp/t_session_${T_SESSION}.pkl}"
            $MUX send-keys -t "$T_SESSION" -l "import dill; dill.dump_session('$file'); print('$sentinel')"
            $MUX send-keys -t "$T_SESSION" Enter
            ;;
        bash)
            file="${2:-/tmp/t_session_${T_SESSION}.sh}"
            $MUX send-keys -t "$T_SESSION" -l "{ export -p; declare -f; alias -p; echo \"cd \$(pwd)\"; } > '$file' && echo '$sentinel'"
            $MUX send-keys -t "$T_SESSION" Enter
            ;;
        r)
            file="${2:-/tmp/t_session_${T_SESSION}.RData}"
            $MUX send-keys -t "$T_SESSION" -l "save.image('$file'); cat('$sentinel\\n')"
            $MUX send-keys -t "$T_SESSION" Enter
            ;;
        node)
            file="${2:-/tmp/t_session_${T_SESSION}.json}"
            $MUX send-keys -t "$T_SESSION" -l "require('fs').writeFileSync('$file',JSON.stringify(Object.fromEntries(Object.entries(global).filter(([k])=>!k.startsWith('_')&&typeof global[k]!=='function')))); console.log('$sentinel')"
            $MUX send-keys -t "$T_SESSION" Enter
            ;;
        *)  echo "WARN: unknown REPL '$lang' — dump not supported"; exit 1;;
    esac
    if _wait_for_poll "$sentinel" 30 line; then
        echo "DUMP OK: $file"
    else
        echo "TIMEOUT: dump did not complete in 30s"; exit 1
    fi
    ;;
restore)
    pane_alive || { echo "ERR: no session '$T_SESSION'"; exit 1; }
    lang="$(detect_lang)"
    sentinel="RESTORE_OK_${RANDOM}"
    case "$lang" in
        python)
            file="${2:-/tmp/t_session_${T_SESSION}.pkl}"
            [ -f "$file" ] || { echo "ERR: no snapshot at $file"; exit 1; }
            $MUX send-keys -t "$T_SESSION" -l "import dill; dill.load_session('$file'); print('$sentinel')"
            $MUX send-keys -t "$T_SESSION" Enter
            ;;
        bash)
            file="${2:-/tmp/t_session_${T_SESSION}.sh}"
            [ -f "$file" ] || { echo "ERR: no snapshot at $file"; exit 1; }
            $MUX send-keys -t "$T_SESSION" -l "source '$file' && echo '$sentinel'"
            $MUX send-keys -t "$T_SESSION" Enter
            ;;
        r)
            file="${2:-/tmp/t_session_${T_SESSION}.RData}"
            [ -f "$file" ] || { echo "ERR: no snapshot at $file"; exit 1; }
            $MUX send-keys -t "$T_SESSION" -l "load('$file'); cat('$sentinel\\n')"
            $MUX send-keys -t "$T_SESSION" Enter
            ;;
        node)
            file="${2:-/tmp/t_session_${T_SESSION}.json}"
            [ -f "$file" ] || { echo "ERR: no snapshot at $file"; exit 1; }
            $MUX send-keys -t "$T_SESSION" -l "Object.assign(global,JSON.parse(require('fs').readFileSync('$file'))); console.log('$sentinel')"
            $MUX send-keys -t "$T_SESSION" Enter
            ;;
        *)  echo "WARN: unknown REPL '$lang' — restore not supported"; exit 1;;
    esac
    if _wait_for_poll "$sentinel" 30 line; then
        echo "RESTORE OK: $file"
    else
        echo "TIMEOUT: restore did not complete in 30s"; exit 1
    fi
    ;;

# --- health ---
status)
    if pane_alive; then
        echo "OK: '$T_SESSION' alive"
        $MUX capture-pane -t "$T_SESSION" -p | strip | grep -v '^$' | tail -3
    else
        echo "ERR: no session '$T_SESSION'"
    fi
    ;;

# --- serve over websocket ---
serve)
    port="${2:-9002}"
    bind="${T_SERVE_BIND:-127.0.0.1}"
    token="${T_SERVE_TOKEN:-}"
    echo "listening on ws://$bind:$port"
    [ -n "$token" ] && echo "auth: token required"
    [ "$bind" = "0.0.0.0" ] && echo "WARNING: binding 0.0.0.0 — any host can execute commands. Set T_SERVE_TOKEN."
    t_bin="$(which t)"
    websocat ws-l:"$bind":"$port" sh-c:'
        set -f
        while IFS= read -r line; do
            if [ -n "'"$token"'" ]; then
                auth=$(echo "$line" | cut -d" " -f1)
                if [ "$auth" != "'"$token"'" ]; then
                    echo "ERR: auth failed"
                    continue
                fi
                line=$(echo "$line" | cut -d" " -f2-)
            fi
            cmd="${line%% *}"
            case "$cmd" in
                w|w!|x|r|W|k|z|new|ls|kill|clear|dump|restore|status|pipe|unpipe|@*)
                    '"$t_bin"' $line 2>&1;;
                *) echo "ERR: unknown command '\''$cmd'\''";;
            esac
        done
    ' --text
    ;;

# --- pipe (stream pane output for Monitor/interrupt) ---
pipe)
    pane_alive || { echo "ERR: no session '$T_SESSION'"; exit 1; }
    logfile="/tmp/t_pipe_${T_SESSION}.log"
    pidfile="/tmp/t_pipe_${T_SESSION}.pid"
    # Kill any existing poller for this session before starting a new one,
    # otherwise the old pid is lost and keeps appending forever.
    if [ -f "$pidfile" ]; then
        kill "$(cat "$pidfile")" 2>/dev/null
        rm -f "$pidfile"
    fi
    > "$logfile"
    if ! $T_IS_PSMUX; then
        # Real tmux: native pipe-pane, always works
        $MUX pipe-pane -t "$T_SESSION" -o "cat >> '$logfile'"
    else
        # psmux: capture-pane polling fallback. Works on both stock and
        # patched builds. Once upstream merges pipe-pane data forwarding
        # (https://github.com/psmux/psmux), uncomment the native path:
        #
        #   logfile_win="$logfile"
        #   command -v cygpath >/dev/null 2>&1 && logfile_win="$(cygpath -w "$logfile")"
        #   $MUX pipe-pane -t "$T_SESSION" -o "\$input | Out-File -FilePath '$logfile_win' -Encoding utf8 -Append"
        #
        # Note: polling appends whole-screen snapshots on change, not
        # incremental output. Monitors may see duplicate matches for lines
        # that stay on screen across refreshes. The native pipe-pane path
        # above produces true incremental output — prefer it once available.
        (
            prev=""
            while true; do
                snap=$($MUX capture-pane -t "$T_SESSION" -p 2>/dev/null) || break
                if [ "$snap" != "$prev" ]; then
                    printf '%s\n' "$snap" >> "$logfile"
                    prev="$snap"
                fi
                sleep 0.3
            done
        ) </dev/null >/dev/null 2>&1 &
        echo $! > "$pidfile"
    fi
    echo "$logfile"
    ;;
unpipe)
    # Stop pipe-pane (real tmux); ignore errors (session may already be gone)
    $MUX pipe-pane -t "$T_SESSION" 2>/dev/null || true
    # Stop poll-backed pipe (psmux)
    pidfile="/tmp/t_pipe_${T_SESSION}.pid"
    if [ -f "$pidfile" ]; then
        kill "$(cat "$pidfile")" 2>/dev/null
        rm -f "$pidfile"
    fi
    rm -f "/tmp/t_pipe_${T_SESSION}.log"
    echo "OK: pipe stopped"
    ;;

# --- remote (one-shot, agent-friendly) ---
remote)
    shift; url="${1:?url required}"; shift
    echo "$*" | websocat "$url" --text -n1
    ;;

# --- connect to remote (interactive, human-friendly) ---
connect)
    url="${2:?url required}"
    echo "connected to $url — type t commands"
    websocat "$url"
    ;;

*) cat <<'USAGE'
t w <cmd>         write + Enter
t x <cmd>         best-effort sync shortcut (known REPLs only)
t w! <text>       write without Enter
t r [N]           read last N lines (default 20)
t r screen [N]    read visible pane, no blanks
t W <needle> [s]  wait for text (timeout default 30s)
t k <key>         send key: ctrl-c ctrl-d ctrl-z ctrl-l enter escape/esc tab up down backspace
t z <cols> <rows> resize
t new <name> <cmd...>  spawn session
t ls              list sessions
t @<name>         show session targeting hint (non-persistent)
t kill            kill current session
t clear           clear current session scrollback
t dump [file]     snapshot REPL state (language-aware)
t restore [file]  restore REPL state
t status          health check
t pipe            tap pane output to file (for Monitor)
t unpipe          stop pane tap
t serve [port]    listen on WebSocket (default 9002)
t connect <url>   connect to remote t server

notes:
  Prefer t w + t W/monitor.sh + t r for reliable completion detection.
  t x injects an internal sentinel; reliable language-aware support is
  Python/Node/R/bash/zsh/sh/fish on real tmux. Unknown REPLs get the
  shell fallback: <cmd>; echo <sentinel>. t x always polls so it cannot
  miss its own sentinel; dump/restore sentinels poll for the same reason.
  T_WAIT_MODE applies only to t W.
  Probe first, or use t w + t W.

env:
  T_SESSION        active session name (default repl)
  T_STRIP_ANSI     strip terminal control sequences from reads (default 1)
  T_REMOTE         send commands to remote t server; does not inject T_SERVE_TOKEN
  T_WAIT_MODE      force t W backend: poll, or fifo on real tmux only
  T_EXEC_TIMEOUT   t x timeout seconds (default 30)
  T_SERVE_BIND     t serve bind address (default 127.0.0.1)
  T_SERVE_TOKEN    optional server token; clients prefix commands with it
USAGE
    ;;
esac
