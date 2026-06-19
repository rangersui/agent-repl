#!/usr/bin/env python3
"""
agent-tty / k -- persistent terminal sessions for AI agents

Usage:
  k new    <session> [cmd...] [--prompt="x"]  spawn session (default: bash)
  k new    <session> <cmd> --prompt=./hook     hook mode (custom frame detect)
  k fire   [-t N] [session] <code>             async fire (default 300s)
  k poll   [session] [cell_id]                poll result (O(1))
  k run    [-j] [-t N] [session] <code>       sync (default 30s)
  k await  ...                                alias for run
  k notify [session] <message>                notification
  k int    [session]                          ctrl-c
  k kill   <session>                          kill + cleanup
  k ls                                        list sessions
  k status [session]                          health check
  k watch  [session]                          live filtered view
  k history [-n N] [session]                  last N*5 lines (default 5)

Session resolves: explicit arg > K_SESSION env > auto-detect.

Frame detection (--prompt):
  not set      -> 5 empty Enters, detect repeated prompt lines (zero config)
  "string"     -> exact prompt match (e.g. --prompt="(gdb)")
  ./file       -> stdin hook: k feeds lines, hook exit = frame end
               hook path canonicalised to absolute at k new time; must exist and be executable

JSON output (-j / fire / poll):
  fired:        {"cell_id": "...", "status": "fired"}
  running:      {"cell_id": "...", "status": "running"}
  done:         {"cell_id": "...", "status": "done", "output": "..."}
  timeout:      {"cell_id": "...", "status": "timeout", "output": ""}
  timeout(2+):  {"cell_id": "...", "status": "timeout", "output": "use k int or k kill"}
  error:        {"status": "error", "output": "..."}
  cell error:   {"cell_id": "...", "status": "error", "output": "..."}

  Errors without cell_id: no session, active cell, pipe failed, send failed, no active cell
  Errors with cell_id:    interrupted, unknown cell, watcher died, lock update failed, interrupt failed

Timeout: lock is NOT released (command may still be running).
  Only k int or k kill releases. k int sends ctrl-c, writes interrupted, releases lock.

Monitor (separate command):
  km <session> [cell_id] [-1]    event stream -- each stdout line is one JSON event
                                 -1 = exit after first completion (one-shot)
  Events: fired, done, notify, closed, error (all include "ts" field)
"""
import json, os, re, signal, shutil, subprocess, sys, time, uuid

TMUX = shutil.which("tmux") or "tmux"
CELL_DIR = "/tmp/k_cells"
FRAME_ENTERS = 5  # consecutive identical lines to detect frame end

ANSI_RE = re.compile(
    r"\x1b\[[0-9;]*[a-zA-Z]|\x1b\[<[0-9;]*[mM]|\x1b\[\?[0-9;]*[hlsr]"
    r"|\x1b\][^\x07]*\x07|\x1b\][^\x1b]*\x1b\\|\x1b[()][0-9A-B]"
    r"|\x1b[>=]|\x1b\x50[^\x1b]*\x1b\\|\x08|\r"
)


# ═══════════════════════════════════════════
# TMUX
# ═══════════════════════════════════════════

class T:
    @staticmethod
    def spawn(s, cmd):
        subprocess.run([TMUX, "new-session", "-d", "-s", s, "-x", "10000", "-y", "50"]
                       + ([cmd] if cmd else []), check=True)
    @staticmethod
    def has(s):
        return subprocess.run([TMUX, "has-session", "-t", s], capture_output=True).returncode == 0
    @staticmethod
    def kill(s):
        subprocess.run([TMUX, "kill-session", "-t", s], capture_output=True)
    @staticmethod
    def send(s, text):
        subprocess.run([TMUX, "send-keys", "-t", s, text, "Enter"], check=True)
    @staticmethod
    def send_enter(s):
        subprocess.run([TMUX, "send-keys", "-t", s, "", "Enter"], check=True)
    @staticmethod
    def send_int(s):
        subprocess.run([TMUX, "send-keys", "-t", s, "C-c"], check=True)
    @staticmethod
    def ls():
        r = subprocess.run([TMUX, "list-sessions", "-F", "#{session_name}"],
                           capture_output=True, text=True)
        return r.stdout.strip()
    @staticmethod
    def pipe_start(s, logfile):
        open(logfile, "a").close()
        subprocess.run([TMUX, "pipe-pane", "-t", s, f"cat >> '{logfile}'"], check=True)
    @staticmethod
    def pipe_stop(s):
        subprocess.run([TMUX, "pipe-pane", "-t", s], capture_output=True)


# ═══════════════════════════════════════════
# PATHS + HELPERS
# ═══════════════════════════════════════════

def _meta(s):   return os.path.join(CELL_DIR, s, "_session.json")
def _lock(s):   return os.path.join(CELL_DIR, s, "_lock.json")
def _log(s):    return os.path.join(CELL_DIR, s, "_output.log")
def _result(s, cid): return os.path.join(CELL_DIR, s, f"{cid}_result.json")

def _log_size(s):
    try: return os.path.getsize(_log(s))
    except FileNotFoundError: return 0

def _ensure_pipe(s):
    """(Re)start pipe-pane. Idempotent — replaces dead/existing pipe."""
    logpath = _log(s)
    os.makedirs(os.path.join(CELL_DIR, s), exist_ok=True)
    T.pipe_start(s, logpath)

def _log_event(s, event):
    try:
        with open(_log(s), "a") as f: f.write(f"\n{event}\n")
    except OSError: pass

def _resolve(explicit=None):
    if explicit:
        _validate_name(explicit)
        return explicit
    env = os.environ.get("K_SESSION")
    if env:
        _validate_name(env)
        return env
    if os.path.isdir(CELL_DIR):
        ss = [d for d in os.listdir(CELL_DIR) if os.path.isfile(os.path.join(CELL_DIR, d, "_session.json"))]
        if len(ss) == 1:
            _validate_name(ss[0])
            return ss[0]
    return None

def _json(d): print(json.dumps(d, ensure_ascii=False))

def _emit(json_out, data, text=None):
    """Unified output: JSON mode → _json(data), text mode → print(text)."""
    if json_out: _json(data)
    else: print(text if text is not None else data.get("output", ""))

def _kill_watcher(meta):
    """SIGTERM bg watcher if present. Returns True if killed."""
    if "bg_pid" not in meta: return False
    try: os.kill(meta["bg_pid"], signal.SIGTERM)
    except OSError: return False
    return True

def _write_result(session, cell_id, result):
    """Atomic result write: tmp + fsync + os.replace. No partial reads."""
    rpath = _result(session, cell_id)
    tmp = rpath + ".tmp"
    with open(tmp, "w") as f:
        json.dump(result, f)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, rpath)

def _update_lock(session, **kw):
    """Read-modify-write lock file. Returns True on success, False on failure."""
    try:
        with open(_lock(session), "r+") as f:
            meta = json.load(f)
            meta.update(kw)
            f.seek(0); f.truncate()
            json.dump(meta, f)
        return True
    except Exception:
        return False

_SAFE_NAME = re.compile(r'^[A-Za-z0-9_.-]+$')
def _validate_name(s):
    """Reject path traversal / injection in session names."""
    if not s or not _SAFE_NAME.match(s) or '..' in s:
        print(f"ERR invalid session name: {s!r}"); sys.exit(1)


# ═══════════════════════════════════════════
# SESSION
# ═══════════════════════════════════════════

def _create(session, cmd, prompt=None):
    T.spawn(session, cmd)
    os.makedirs(os.path.join(CELL_DIR, session), exist_ok=True)
    _ensure_pipe(session)
    time.sleep(1.0)
    meta = {"name": session}
    if prompt:
        meta["prompt"] = prompt  # explicit prompt → exact match mode
    with open(_meta(session), "w") as f:
        json.dump(meta, f)

def _session_exists(session):
    return T.has(session) and os.path.exists(_meta(session))

def _session_prompt(session):
    """Returns explicit prompt if set, None for default repeat-detection."""
    try:
        with open(_meta(session)) as f: return json.load(f).get("prompt")
    except Exception: return None


# ═══════════════════════════════════════════
# LOCK = CELL METADATA
# ═══════════════════════════════════════════

def _acquire(session, cell_id, log_offset, echo_count):
    lock = _lock(session)
    meta = {"cell_id": cell_id, "log_offset": log_offset, "echo_count": echo_count}
    try:
        fd = os.open(lock, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
        os.write(fd, json.dumps(meta).encode())
        os.close(fd)
        return None
    except FileExistsError:
        try:
            with open(lock) as f: return json.load(f).get("cell_id", "?")
        except Exception: return "?"

def _load_cell(session):
    try:
        with open(_lock(session)) as f: return json.load(f)
    except Exception: return None

def _release(session, cell_id):
    try:
        with open(_lock(session)) as f:
            if json.load(f).get("cell_id") == cell_id: os.unlink(_lock(session))
    except Exception: pass


def _send_interrupt(session):
    """Send Ctrl-C to REPL + re-send frame enters in repeat mode.
    Returns True if Ctrl-C was delivered (or session is already dead).
    Returns False if Ctrl-C failed but session is still alive — caller must not release.
    """
    prompt = _session_prompt(session)
    try:
        T.send_int(session)
    except Exception:
        # Ctrl-C didn't reach REPL. If session is dead, nothing is running → safe.
        # If session is alive, command may still be running → unsafe to release.
        return not T.has(session)
    time.sleep(0.3)
    # re-frame is best-effort (Ctrl-C already delivered)
    if not prompt:
        try:
            _send_frame_enters(session)
        except Exception:
            pass
    return True


class CellBusy(Exception):
    """Raised by CellLock when the session already has an active cell."""
    def __init__(self, held_id):
        self.held_id = held_id


class CellLock:
    """RAII lock for cell lifecycle. Three states via sent/keep:
      pre-send (default)  → release on any exit
      post-send (sent)    → interrupt recovery on exception, release on normal exit
      keep (timeout/fire) → lock stays held, no cleanup
    """
    def __init__(self, session, cell_id, log_offset, echo_count):
        self.session = session
        self.cell_id = cell_id
        self.sent = False
        self.keep = False
        self.interrupt_failed = False
        held = _acquire(session, cell_id, log_offset, echo_count)
        if held:
            raise CellBusy(held)

    def __enter__(self):
        return self

    def mark_sent(self):
        self.sent = True

    def mark_keep(self):
        self.keep = True

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.keep:
            return False
        if exc_type is not None and self.sent:
            if not _send_interrupt(self.session):
                # Ctrl-C didn't reach REPL but session is alive — keep lock
                # (same reasoning as timeout: command may still be running)
                self.interrupt_failed = True
                return False
        _release(self.session, self.cell_id)
        # sync mode cleanup: remove result file (nobody will poll it)
        try:
            rpath = _result(self.session, self.cell_id)
            if os.path.exists(rpath): os.unlink(rpath)
        except OSError: pass
        return False


# ═══════════════════════════════════════════
# STREAM PROCESSOR
# Frame delimiter: N consecutive identical non-empty lines
# (= REPL redrawing prompt on empty Enter)
# ═══════════════════════════════════════════

def _stream_process(session, cell_id, log_offset, echo_count, timeout=None, prompt=None):
    """
    Stream processor with three modes:
      prompt=None      → frame = N consecutive identical lines (default)
      prompt="string"  → frame = exact prompt match
      prompt="./file"  → frame = hook process (stdin lines, exit=done)
    """
    logpath = _log(session)
    state = "OUTPUT" if echo_count <= 0 else "ECHOING"
    remaining = echo_count
    output = []
    deadline = time.monotonic() + timeout if timeout else None
    repeat_count = 0
    last_clean = None

    # start hook process if prompt is an absolute file path (canonicalised by cmd_new)
    hook = None
    if prompt and os.path.isabs(prompt) and os.path.isfile(prompt):
        hook = subprocess.Popen(
            [prompt], stdin=subprocess.PIPE, stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL, text=True
        )
        prompt = None  # don't also do string matching

    last_appended = False  # tracks if last line was appended (not filtered)
    timed_out = False

    try:
        with open(logpath, "r", errors="replace") as f:
            f.seek(log_offset)
            while True:
                if deadline and time.monotonic() > deadline:
                    timed_out = True
                    break

                line = f.readline()
                if not line:
                    if hook and hook.poll() is not None:
                        # hook exited — pop last line only if it was appended
                        if output and last_appended:
                            output.pop()
                        break
                    time.sleep(0.05)
                    continue

                clean = ANSI_RE.sub("", line).strip()
                if not clean:
                    continue

                if clean.startswith("── cell:") or clean.startswith("── notify "):
                    continue

                if state == "ECHOING":
                    remaining -= 1
                    if remaining <= 0:
                        state = "OUTPUT"

                elif state == "OUTPUT":
                    if hook:
                        # hook mode: feed line, exit = frame end
                        # NO filtering — hook user takes full control of output
                        try:
                            hook.stdin.write(clean + "\n")
                            hook.stdin.flush()
                        except (BrokenPipeError, OSError):
                            if output and last_appended:
                                output.pop()
                            break
                        output.append(clean)
                        last_appended = True
                        time.sleep(0.01)
                        if hook.poll() is not None:
                            output.pop()
                            break
                    elif prompt:
                        # string mode: exact match
                        if clean == prompt:
                            break
                        if clean == "..." or clean.startswith("... "):
                            continue
                        output.append(clean)
                    else:
                        # repeat mode: N consecutive identical lines
                        if clean == "..." or clean.startswith("... "):
                            last_clean = clean
                            continue

                        if clean == last_clean:
                            repeat_count += 1
                        else:
                            repeat_count = 0

                        output.append(clean)

                        if repeat_count >= FRAME_ENTERS - 1:
                            for _ in range(repeat_count + 1):
                                output.pop()
                            break
                    last_clean = clean
    finally:
        if hook:
            if hook.poll() is None:
                hook.kill()
            hook.wait()

    result = {
        "cell_id": cell_id,
        "status": "timeout" if timed_out else "done",
        "output": "" if timed_out else "\n".join(output)
    }

    _write_result(session, cell_id, result)
    if not timed_out:
        _log_event(session, f"── cell:{cell_id} done ──")

    return result


def _echo_count(code):
    """Count how many lines the REPL will echo (= non-trailing-blank lines)."""
    code_lines = code.lstrip().split("\n")
    count = len(code_lines)
    while count > 0 and not code_lines[count - 1].strip():
        count -= 1
    return count


def _send_frame_enters(session):
    """Send FRAME_ENTERS empty Enters via send-keys (repeat-mode framing)."""
    args = [TMUX, "send-keys", "-t", session]
    for _ in range(FRAME_ENTERS):
        args.extend(["", "Enter"])
    subprocess.run(args, check=True)


def _send_code(session, code, prompt=None):
    """Send code via paste-buffer (no per-char echo) + frame enters."""
    code_lines = code.lstrip().split("\n")

    # paste-buffer: entire text arrives as one write → readline redraws once
    text = "\n".join(code_lines) + "\n"
    buf = f"k_{session}"
    subprocess.run([TMUX, "load-buffer", "-b", buf, "-"], input=text.encode(), check=True)
    subprocess.run([TMUX, "paste-buffer", "-b", buf, "-d", "-t", session], check=True)

    if not prompt:
        _send_frame_enters(session)


# ═══════════════════════════════════════════
# COMMANDS
# ═══════════════════════════════════════════

def cmd_new(session, cmd_parts, prompt=None):
    _validate_name(session)
    if T.has(session):
        print(f"OK {session} (alive)")
        return 0
    # hook mode: path contains / or \ → canonicalize and fail early if missing
    if prompt and (os.sep in prompt or "/" in prompt):
        prompt = os.path.abspath(os.path.expanduser(prompt))
        if not os.path.isfile(prompt):
            print(f"ERR hook not found: {prompt}"); return 1
        if not os.access(prompt, os.R_OK):
            print(f"ERR hook not readable: {prompt}"); return 1
        if not os.access(prompt, os.X_OK):
            print(f"ERR hook not executable: {prompt}"); return 1
    cmd = " ".join(cmd_parts) if cmd_parts else "bash"
    _create(session, cmd, prompt)
    if prompt:
        print(f"OK {session} prompt={repr(prompt)}")
    else:
        print(f"OK {session}")
    return 0


def cmd_fire(session, code, timeout=300):
    if not _session_exists(session):
        _json({"status": "error", "output": f"no session '{session}'"}); return 1

    cell_id = uuid.uuid4().hex[:12]
    prompt = _session_prompt(session)
    echo_count = _echo_count(code)
    log_offset = _log_size(session)

    try:
        lock = CellLock(session, cell_id, log_offset, echo_count)
    except CellBusy as e:
        _json({"status": "error", "output": f"active cell '{e.held_id}'"}); return 1

    try:
        with lock:
            try:
                _ensure_pipe(session)
            except Exception as e:
                _json({"status": "error", "output": f"pipe failed: {e}"}); return 1

            try:
                _send_code(session, code, prompt)
            except Exception as e:
                _json({"status": "error", "output": f"send failed: {e}"}); return 1

            lock.mark_sent()
            _log_event(session, f"── cell:{cell_id} fired ──")

            bg_args = [sys.executable, os.path.abspath(__file__), "_bg",
                       session, cell_id, str(log_offset), str(echo_count), str(timeout)]
            if prompt:
                bg_args.append(prompt)

            bg = subprocess.Popen(bg_args, start_new_session=True,
                             stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

            # store bg PID in lock — without it orphan detection is blind
            if not _update_lock(session, bg_pid=bg.pid):
                try: bg.kill()
                except OSError: pass
                raise RuntimeError("lock update failed")

            lock.mark_keep()  # bg process owns the lock now
    except (Exception, KeyboardInterrupt):
        msg = "interrupt failed; use k kill" if lock.interrupt_failed else "interrupted"
        _json({"cell_id": cell_id, "status": "error", "output": msg})
        return 1

    _json({"cell_id": cell_id, "status": "fired"})
    return 0


def cmd_poll(session, cell_id=None):
    if cell_id is None:
        meta = _load_cell(session)
        if not meta:
            _json({"status": "error", "output": f"no active cell on '{session}'"}); return 1
        cell_id = meta["cell_id"]

    rpath = _result(session, cell_id)
    if os.path.exists(rpath):
        try:
            with open(rpath) as f: result = json.load(f)
        except (json.JSONDecodeError, OSError):
            # atomic writes make this near-impossible; if it happens,
            # do NOT release lock — state is unknown, let user k int / k kill
            _json({"cell_id": cell_id, "status": "running"})
            return 0
        if result.get("status") == "timeout":
            # mark lock BEFORE consuming result — if this fails, keep result for retry
            if not _update_lock(session, timed_out=True):
                _json({"cell_id": cell_id, "status": "error", "output": "lock update failed; use k int or k kill"})
                return 1
        # safe to consume result now (timed_out written, or non-timeout)
        try: os.unlink(rpath)
        except OSError: pass
        if result.get("status") != "timeout":
            _release(session, cell_id)
        _json(result)
        return 0

    # check lock state
    meta = _load_cell(session)

    # no lock, or lock is for a different cell → this cell_id is unknown
    if not meta or meta.get("cell_id") != cell_id:
        _json({"cell_id": cell_id, "status": "error", "output": "unknown cell"})
        return 1

    # timed_out: command may still be running — only k int / k kill can release
    if meta.get("timed_out"):
        _json({"cell_id": cell_id, "status": "timeout", "output": "use k int or k kill"})
        return 1

    # check if bg process died (orphaned lock)
    if "bg_pid" in meta:
        pid = meta["bg_pid"]
        try:
            os.kill(pid, 0)  # POSIX: check process exists (no signal sent)
            alive = True
        except OSError:
            alive = False
        if not alive:
            _release(session, cell_id)
            _json({"cell_id": cell_id, "status": "error", "output": "watcher died"})
            return 1

    _json({"cell_id": cell_id, "status": "running"})
    return 0


def cmd_run(session, code, timeout=30, json_out=False):
    if not _session_exists(session):
        _emit(json_out, {"status": "error", "output": f"no session '{session}'"})
        return 1

    prompt = _session_prompt(session)
    cell_id = uuid.uuid4().hex[:12]
    echo_count = _echo_count(code)
    log_offset = _log_size(session)

    try:
        lock = CellLock(session, cell_id, log_offset, echo_count)
    except CellBusy as e:
        _emit(json_out, {"status": "error", "output": f"active cell '{e.held_id}'"})
        return 1

    try:
        with lock:
            try:
                _ensure_pipe(session)
            except Exception as e:
                _emit(json_out, {"status": "error", "output": f"pipe failed: {e}"})
                return 1

            try:
                _send_code(session, code, prompt)
            except Exception as e:
                _emit(json_out, {"status": "error", "output": f"send failed: {e}"})
                return 1

            lock.mark_sent()
            result = _stream_process(session, cell_id, log_offset, echo_count, timeout, prompt)

            if result.get("status") == "timeout":
                lock.mark_keep()
    except (Exception, KeyboardInterrupt):
        # CellLock.__exit__ handled cleanup (interrupt recovery or lock kept)
        msg = "interrupt failed; use k kill" if lock.interrupt_failed else "interrupted"
        _emit(json_out, {"cell_id": cell_id, "status": "error", "output": msg})
        return 1

    _emit(json_out, result)
    return 0


def cmd_notify(session, message):
    if not _session_exists(session):
        print(f"ERR no session '{session}'"); return 1
    try: parent = open(f"/proc/{os.getppid()}/comm").read().strip()
    except Exception: parent = "?"
    _log_event(session, f"── notify [{parent}@k:{os.getpid()}] {message} ──")
    print(f"OK notified: {message}")
    return 0


def cmd_int(s):
    if not _send_interrupt(s):
        print("ERR interrupt failed; use k kill"); return 1
    # kill bg watcher (if any) before releasing lock
    # prevents old watcher from consuming new cell's output
    meta = _load_cell(s)
    if meta:
        cell_id = meta["cell_id"]
        if _kill_watcher(meta):
            time.sleep(0.2)  # let watcher exit
        # write result so poll finds closure — overwrites timeout result too
        _write_result(s, cell_id, {"cell_id": cell_id, "status": "error", "output": "interrupted"})
        _release(s, cell_id)
    print("OK"); return 0

def cmd_kill(s):
    # kill bg watcher if running
    meta = _load_cell(s)
    if meta:
        _kill_watcher(meta)
    T.pipe_stop(s); T.kill(s)
    d = os.path.join(CELL_DIR, s)
    if os.path.isdir(d): shutil.rmtree(d, ignore_errors=True)
    print(f"OK killed {s}"); return 0

def cmd_ls():
    s = T.ls(); print(s if s else "no sessions"); return 0

def cmd_status(session):
    if not _session_exists(session): print(f"ERR no session '{session}'"); return 1
    logpath = _log(session)
    pipe_ok = False
    if os.path.exists(logpath):
        before = os.path.getsize(logpath)
        subprocess.run([TMUX, "send-keys", "-t", session, " ", "BSpace"], capture_output=True)
        time.sleep(0.2)
        pipe_ok = (os.path.getsize(logpath) > before)
    if not pipe_ok:
        T.pipe_start(session, logpath)
        print(f"OK {session} pipe=repaired")
    else:
        print(f"OK {session} pipe=ok")
    return 0


# ═══════════════════════════════════════════
# WATCH / HISTORY
# ═══════════════════════════════════════════

_NOTIFY_RE = re.compile(r"^── notify \[(.+?)\] (.+) ──$")
_CELL_RE = re.compile(r"^── cell:([0-9a-f]{12}) (fired|done) ──$")

def _filter_line(raw_line):
    clean = ANSI_RE.sub("", raw_line).strip()
    if not clean: return None
    m = _NOTIFY_RE.match(clean)
    if m: return f"\033[33m📢 {m.group(2)}\033[0m \033[2m({m.group(1)})\033[0m"
    m = _CELL_RE.match(clean)
    if m:
        if m.group(2) == "fired": return f"\033[2;36m── {m.group(1)[:8]} ──\033[0m"
        else: return f"\033[2;32m── ✓ ──\033[0m"
    if clean == "..." or clean.startswith("... "): return None
    return ANSI_RE.sub("", raw_line).rstrip()

def cmd_watch(session):
    if not _session_exists(session): print(f"ERR no session '{session}'"); return 1
    logpath = _log(session)
    if not os.path.exists(logpath): print(f"ERR no log"); return 1
    print(f"\033[2mwatching {session} (ctrl-c to stop)\033[0m\n")
    try:
        proc = subprocess.Popen(["tail", "-n", "0", "-f", logpath], stdout=subprocess.PIPE, text=True)
        last_printed = None
        for raw_line in proc.stdout:
            r = _filter_line(raw_line)
            if r is not None and r != last_printed:
                print(r)
                last_printed = r
    except KeyboardInterrupt: print(f"\n\033[2mstopped\033[0m")
    finally:
        if proc.poll() is None: proc.kill(); proc.wait()
    return 0

def cmd_history(session, n=5):
    if not _session_exists(session): print(f"ERR no session '{session}'"); return 1
    logpath = _log(session)
    if not os.path.exists(logpath): print(f"ERR no log"); return 1
    with open(logpath, "r", errors="replace") as f: raw_lines = f.readlines()
    filtered = [r for line in raw_lines if (r := _filter_line(line)) is not None]
    # dedup consecutive identical lines (frame delimiter noise)
    deduped = []
    for line in filtered:
        if not deduped or line.strip() != deduped[-1].strip():
            deduped.append(line)
    for line in deduped[-n * 5:]: print(line)
    return 0


# ═══════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════

def main():
    args = sys.argv[1:]
    if not args or args[0] in ("-h", "--help"):
        print(__doc__.strip()); return 0
    verb, rest = args[0], args[1:]

    if verb == "_bg" and len(rest) >= 5:
        session, cell_id, offset, echo, tout = rest[:5]
        _validate_name(session)
        prompt = rest[5] if len(rest) > 5 else None
        _stream_process(session, cell_id, int(offset), int(echo), timeout=int(tout), prompt=prompt)
        return 0

    if verb == "new" and rest:
        prompt = None; cmd_parts = []
        for a in rest[1:]:
            if a.startswith("--prompt="): prompt = a[len("--prompt="):]
            else: cmd_parts.append(a)
        return cmd_new(rest[0], cmd_parts, prompt)
    if verb == "kill" and rest:
        _validate_name(rest[0]); return cmd_kill(rest[0])
    if verb == "ls": return cmd_ls()

    if verb in ("run", "await"):
        timeout, json_out = 30, False
        while rest and rest[0].startswith("-"):
            if rest[0] == "-t":
                if len(rest) < 2: print("usage: k run [-j] [-t N] [session] <code>"); return 1
                timeout = int(rest[1]); rest = rest[2:]
            elif rest[0] == "-j": json_out = True; rest = rest[1:]
            else: break
        if len(rest) >= 2: s, c = rest[0], rest[1]; _validate_name(s)
        elif len(rest) == 1: s, c = _resolve(), rest[0]
        else: print("usage: k run [-j] [-t N] [session] <code>"); return 1
        if not s: print("ERR: no session found."); return 1
        return cmd_run(s, c, timeout, json_out)

    if verb == "fire" and rest:
        timeout = 300
        while rest and rest[0].startswith("-"):
            if rest[0] == "-t":
                if len(rest) < 2: print("usage: k fire [-t N] [session] <code>"); return 1
                timeout = int(rest[1]); rest = rest[2:]
            else: break
        if len(rest) >= 2: s, c = rest[0], rest[1]; _validate_name(s)
        else: s, c = _resolve(), rest[0]
        if not s: print("ERR: no session found."); return 1
        return cmd_fire(s, c, timeout)

    if verb == "poll":
        s = _resolve(rest[0] if rest else None)
        if not s: print("ERR: no session found."); return 1
        return cmd_poll(s, rest[1] if len(rest) >= 2 else None)

    if verb == "notify" and rest:
        if len(rest) >= 2 and T.has(rest[0]):
            _validate_name(rest[0]); s, msg = rest[0], " ".join(rest[1:])
        else: s, msg = _resolve(), " ".join(rest)
        if not s: print("ERR: no session found."); return 1
        return cmd_notify(s, msg)

    if verb == "int":
        s = _resolve(rest[0] if rest else None)
        if not s: print("ERR: no session found."); return 1
        return cmd_int(s)
    if verb == "status":
        s = _resolve(rest[0] if rest else None)
        if not s: print("ERR: no session found."); return 1
        return cmd_status(s)
    if verb == "watch":
        s = _resolve(rest[0] if rest else None)
        if not s: print("ERR: no session found."); return 1
        return cmd_watch(s)
    if verb == "history":
        n = 5
        if rest and rest[0] == "-n":
            if len(rest) < 2: print("usage: k history [-n N] [session]"); return 1
            n = int(rest[1]); rest = rest[2:]
        s = _resolve(rest[0] if rest else None)
        if not s: print("ERR: no session found."); return 1
        return cmd_history(s, n)

    print(__doc__.strip()); return 1

if __name__ == "__main__": sys.exit(main())
