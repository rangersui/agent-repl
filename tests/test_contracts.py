#!/usr/bin/env python3
"""Static contract tests for k/km. No tmux session is started."""

import ast
from pathlib import Path
from typing import Any, cast

ROOT = Path(__file__).resolve().parents[1]
K_PATH = ROOT / "src" / "agent_tty" / "cli.py"
KM_PATH = ROOT / "src" / "agent_tty" / "monitor.py"
SHARED_PATH = ROOT / "src" / "agent_tty" / "_shared.py"
K_SRC = K_PATH.read_text(encoding="utf-8")
KM_SRC = KM_PATH.read_text(encoding="utf-8")
SHARED_SRC = SHARED_PATH.read_text(encoding="utf-8")
FAILURES: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    if not cond:
        FAILURES.append(f"{name}: {detail}".rstrip(": "))


def parse(path: Path, src: str) -> ast.Module:
    try:
        return ast.parse(src, filename=str(path))
    except SyntaxError as exc:
        FAILURES.append(f"{path.name}: syntax error: {exc}")
        return ast.Module(body=[], type_ignores=[])


K_TREE = parse(K_PATH, K_SRC)
KM_TREE = parse(KM_PATH, KM_SRC)
SHARED_TREE = parse(SHARED_PATH, SHARED_SRC)


def function(tree: ast.Module, name: str) -> ast.FunctionDef | None:
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name == name:
            return node
    FAILURES.append(f"missing function {name}")
    return None


def klass(tree: ast.Module, name: str) -> ast.ClassDef | None:
    for node in tree.body:
        if isinstance(node, ast.ClassDef) and node.name == name:
            return node
    FAILURES.append(f"missing class {name}")
    return None


def segment(src: str, node: ast.AST | None) -> str:
    if node is None:
        return ""
    lines = src.splitlines()
    positioned = cast(Any, node)
    return "\n".join(lines[positioned.lineno - 1 : positioned.end_lineno])


def call_lines(node: ast.FunctionDef | None, name: str) -> list[int]:
    if node is None:
        return []
    out: list[int] = []
    for child in ast.walk(node):
        if isinstance(child, ast.Call):
            func = child.func
            if isinstance(func, ast.Name) and func.id == name:
                out.append(child.lineno)
            elif isinstance(func, ast.Attribute) and func.attr == name:
                out.append(child.lineno)
    return sorted(out)


def check_no_except_pass(name: str, tree: ast.Module) -> None:
    for node in ast.walk(tree):
        if isinstance(node, ast.ExceptHandler) and any(isinstance(n, ast.Pass) for n in node.body):
            FAILURES.append(f"{name}:{node.lineno}: except handler must not pass silently")


def check_function_annotations(label: str, tree: ast.Module) -> None:
    for node in ast.walk(tree):
        if not isinstance(node, ast.FunctionDef):
            continue
        args = list(node.args.posonlyargs) + list(node.args.args) + list(node.args.kwonlyargs)
        if node.args.vararg is not None:
            args.append(node.args.vararg)
        if node.args.kwarg is not None:
            args.append(node.args.kwarg)
        missing = [arg.arg for arg in args if arg.arg != "self" and arg.annotation is None]
        if missing:
            FAILURES.append(f"{label}:{node.lineno}: missing arg annotations on {node.name}: {', '.join(missing)}")
        if node.returns is None:
            FAILURES.append(f"{label}:{node.lineno}: missing return annotation on {node.name}")


def with_body_segment(src: str, node: ast.FunctionDef | None, guard_name: str) -> str:
    if node is None:
        return ""
    lines = src.splitlines()
    for child in ast.walk(node):
        if not isinstance(child, ast.With):
            continue
        for item in child.items:
            expr = item.context_expr
            if (
                isinstance(expr, ast.Call)
                and isinstance(expr.func, ast.Name)
                and expr.func.id == guard_name
                and child.body
            ):
                return "\n".join(lines[child.body[0].lineno - 1 : child.body[-1].end_lineno])
    return ""


check_no_except_pass("cli.py", K_TREE)
check_no_except_pass("monitor.py", KM_TREE)
check_no_except_pass("_shared.py", SHARED_TREE)
check_function_annotations("cli.py", K_TREE)
check_function_annotations("monitor.py", KM_TREE)
check_function_annotations("_shared.py", SHARED_TREE)

cmd_fire = function(K_TREE, "cmd_fire")
cmd_run = function(K_TREE, "cmd_run")
cmd_poll = function(K_TREE, "cmd_poll")
cmd_int = function(K_TREE, "cmd_int")
stream_process = function(K_TREE, "_stream_process")
cmd_new = function(K_TREE, "cmd_new")
main = function(K_TREE, "main")
cmd_watch = function(K_TREE, "cmd_watch")
cmd_status = function(K_TREE, "cmd_status")
cmd_notify = function(K_TREE, "cmd_notify")
cmd_ls = function(K_TREE, "cmd_ls")
cmd_history = function(K_TREE, "cmd_history")
resolve_fn = function(K_TREE, "_resolve")
session_dir_fn = function(K_TREE, "_session_dir")
release_fn = function(K_TREE, "_release_unlocked")
release_if_current_fn = function(K_TREE, "_release_if_current")
watcher_alive_fn = function(K_TREE, "_watcher_alive")
acquire_unlocked_fn = function(K_TREE, "_acquire_unlocked")
should_source_bash_fn = function(K_TREE, "_should_source_bash")
write_input_script_fn = function(K_TREE, "_write_input_script")
commit_terminal_fn = function(K_TREE, "_commit_terminal_result")
shared_open_private_fn = function(SHARED_TREE, "open_private")
ensure_private_dir_fn = function(SHARED_TREE, "ensure_private_dir")
notify_event_fn = function(SHARED_TREE, "notify_event")
parse_positive_int_fn = function(K_TREE, "_parse_positive_int")
create_fn = function(K_TREE, "_create")

# ── CellLock RAII structure ──
cell_lock_cls = klass(K_TREE, "CellLock")
check("CellLock: class exists", cell_lock_cls is not None)
if cell_lock_cls:
    methods = {n.name for n in cell_lock_cls.body if isinstance(n, ast.FunctionDef)}
    for m in ("__init__", "__enter__", "__exit__", "mark_sent", "mark_keep"):
        check(f"CellLock: has {m}", m in methods)
    cl_seg = segment(K_SRC, cell_lock_cls)
    init_seg = segment(K_SRC, next((n for n in cell_lock_cls.body if isinstance(n, ast.FunctionDef) and n.name == "__init__"), None))
    enter_seg = segment(K_SRC, next((n for n in cell_lock_cls.body if isinstance(n, ast.FunctionDef) and n.name == "__enter__"), None))
    check("CellLock: __init__ does not acquire lock", "_acquire(" not in init_seg)
    check("CellLock: __enter__ acquires lock", "_acquire(" in enter_seg)
    check("CellLock: __exit__ releases lock", "_release(" in cl_seg)
    check("CellLock: __exit__ sends interrupt", "_send_interrupt(" in cl_seg)
    check("CellLock: __exit__ keeps lock on failed interrupt", "interrupt_failed" in cl_seg)
    check("CellLock: tracks acquired state", "self.acquired" in cl_seg)

lock_guard_cls = klass(K_TREE, "LockGuard")
check("LockGuard: class exists", lock_guard_cls is not None)
if lock_guard_cls:
    lg_methods = {n.name for n in lock_guard_cls.body if isinstance(n, ast.FunctionDef)}
    check("LockGuard: has __enter__", "__enter__" in lg_methods)
    check("LockGuard: has __exit__", "__exit__" in lg_methods)
    lg_seg = segment(K_SRC, lock_guard_cls)
    check("LockGuard: enter locks", "fcntl.flock" in lg_seg and "LOCK_EX" in lg_seg)
    check("LockGuard: exit unlocks", "fcntl.flock" in lg_seg and "LOCK_UN" in lg_seg)
    check("LockGuard: supports nonblocking", "LOCK_NB" in lg_seg and "LockBusy" in K_SRC)

send_int_fn = function(K_TREE, "_send_interrupt")
si_seg = segment(K_SRC, send_int_fn)
check("_send_interrupt: sends ctrl-c", "send_int(" in si_seg)
check("_send_interrupt: returns bool", "return True" in si_seg and "return not" in si_seg)
check("_send_interrupt: checks session alive on failure", "T.has(" in si_seg)
check("_send_interrupt: re-frames via helper", "_send_frame_enters(" in si_seg)

for fn_name, fn in (("cmd_fire", cmd_fire), ("cmd_run", cmd_run)):
    seg = segment(K_SRC, fn)
    ensure = call_lines(fn, "_ensure_pipe")
    send = call_lines(fn, "_send_code")
    check(f"{fn_name}: uses CellLock", "CellLock(" in seg)
    check(f"{fn_name}: no raw _acquire", "_acquire(" not in seg)
    check(f"{fn_name}: no raw _release", "_release(" not in seg)
    check(f"{fn_name}: ensure_pipe exists", bool(ensure))
    check(f"{fn_name}: send_code exists", bool(send))
    check(f"{fn_name}: calls mark_sent", "mark_sent()" in seg)
    check(f"{fn_name}: pipe failure path", "pipe failed" in seg)
    if fn is None:
        continue
    # lock (CellLock) before pipe/send
    cl_lines = [child.lineno for child in ast.walk(fn)
                if isinstance(child, ast.Call)
                and isinstance(child.func, ast.Name)
                and child.func.id == "CellLock"]
    if cl_lines and ensure and send:
        check(f"{fn_name}: lock before pipe/send",
              min(cl_lines) < min(ensure) < min(send),
              f"CellLock={cl_lines}, _ensure_pipe={ensure}, _send_code={send}")

check("cmd_fire: calls mark_keep", "mark_keep()" in segment(K_SRC, cmd_fire))
check("cmd_run: calls mark_keep", "mark_keep()" in segment(K_SRC, cmd_run))

poll_seg = segment(K_SRC, cmd_poll)
check("poll: validates explicit cell_id", "validate_cell_id(" in poll_seg and "invalid cell_id" in poll_seg)
check("poll: validates session and fails missing session", "validate_name(session)" in poll_seg and "_no_session_output(session)" in poll_seg)
check("poll: timeout marks lock", "_mark_terminal(" in poll_seg and "TIMEOUT" in poll_seg)
check("poll: timeout checks update success", "if not _update_lock(" in poll_seg)
check("poll: timeout second poll hints recovery", "timeout_polled" in poll_seg and "use k int or k kill" in poll_seg)
check("poll: timed_out blocks orphan release", 'meta.get("timed_out")' in poll_seg and "use k int or k kill" in poll_seg)
check("poll: completed done-lock can be released", 'meta.get("completed")' in poll_seg and 'terminal_status") == DONE' in poll_seg)
check("poll: old result releases only current lock", "_release_if_current(" in poll_seg)
check("poll: wrong explicit cell is unknown", 'meta.get("cell_id") != cell_id' in poll_seg and '"unknown cell"' in poll_seg)
check("poll: no bare except on result read", "except: pass" not in poll_seg)
check("poll: no release on decode error", "corrupt" not in poll_seg,
      "JSONDecodeError must not release lock")

int_seg = segment(K_SRC, cmd_int)
check("int: uses _send_interrupt", "_send_interrupt(" in int_seg)
check("int: bails on failed interrupt", "interrupt failed" in int_seg)
check("int: fails missing session before lock", "_session_exists(s)" in int_seg and "_no_session_output(s)" in int_seg)
check("int: holds LockGuard across interrupt", "with LockGuard(s):" in int_seg and "_load_cell(s)" in int_seg)
int_guard_seg = with_body_segment(K_SRC, cmd_int, "LockGuard")
for needle in ("_load_cell(s)", "_send_interrupt(s)", "_kill_watcher(meta)",
               '_write_result(s, cell_id', "_release_unlocked(s, cell_id)"):
    check(f"int: guarded {needle}", needle in int_guard_seg)
check("int: writes ERROR/interrupted", '_write_result(s, cell_id' in int_seg and '"status": ERROR' in int_seg and '"output": "interrupted"' in int_seg)
check("int: overwrites stale timeout result", "not os.path.exists(rpath)" not in int_seg)
check("int: kills watcher before active release", "_watcher_pgid(meta) and not _kill_watcher(meta)" in int_seg)
check("int: releases without nested guard", "_release_unlocked(" in int_seg and "_release(s" not in int_seg)

release_seg = segment(K_SRC, release_fn)
check("_release: no silent broad pass", "except Exception: pass" not in release_seg)
check("_release: returns failure on unexpected error", "except Exception as e:" in release_seg and "return False" in release_seg)
check("_release: mismatch fails loud", "return False" in release_seg and "meta.get(\"cell_id\") == cell_id" in release_seg)
check("_release_if_current: mismatch is not an error", release_if_current_fn is not None and "meta.get(\"cell_id\") != cell_id" in segment(K_SRC, release_if_current_fn))

new_seg = segment(K_SRC, cmd_new)
stream_seg = segment(K_SRC, stream_process)
parse_positive_int_seg = segment(K_SRC, parse_positive_int_fn)
create_seg = segment(K_SRC, create_fn)
check("hook: canonicalises path", "os.path.abspath(os.path.expanduser(prompt))" in new_seg)
check("hook: checks executable", "os.access(prompt, os.X_OK)" in new_seg)
check("hook: runtime uses absolute file path", "os.path.isabs(prompt)" in stream_seg and '"/" in prompt' not in stream_seg)
check("prompt: string mode strips whitespace", "prompt = prompt.strip()" in new_seg)
check("prompt: empty after strip is rejected", "ERR empty prompt" in new_seg)
check("_create: no redundant prompt strip", "prompt.strip()" not in create_seg,
      "_create should store prompt as-is; normalisation belongs in cmd_new")
check("exact match: capture-pane fallback for pipe buffering",
      "_pane_shows_prompt(session, prompt)" in stream_seg,
      "exact match needs capture-pane polling; pipe-pane buffers prompts without trailing newline")
check("exact match: capture-pane is rate-limited",
      "_PANE_POLL_INTERVAL" in K_SRC and "last_pane_poll" in stream_seg,
      "capture-pane polling must be throttled to avoid hammering tmux")

main_seg = segment(K_SRC, main)
check("new: reports create failure without traceback", "ERR create failed" in new_seg)
check("_create: rolls back spawned tmux on setup failure", "except BaseException" in create_seg and "T.kill(session)" in create_seg)
check("_create: rolls back session directory on setup failure", "shutil.rmtree(created_dir)" in create_seg)
check("_parse_positive_int: rejects invalid input", "except ValueError" in parse_positive_int_seg and "must be a positive integer" in parse_positive_int_seg)
check("_parse_positive_int: rejects zero and negative", "value <= 0" in parse_positive_int_seg)
check("main: parses user numbers via helper", "_parse_positive_int(rest[1], \"-t\"" in main_seg and "_parse_positive_int(rest[1], \"-n\"" in main_seg)
check("main: no bare int() on user rest values", "timeout = int(" not in main_seg and "n = int(" not in main_seg)
check("_stream_process: timeout zero is not infinite", "timeout if timeout is not None else None" in stream_seg and "deadline is not None" in stream_seg)
check("sentinel exceptions: warn before returning fallback", all(needle in K_SRC for needle in (
    "_warn(f\"lock read failed",
    "_warn(f\"lock write failed",
    "_warn(f\"lock file missing",
    "_warn(f\"corrupt lock shape",
    "_warn(f\"session metadata prompt read failed",
    "_warn(f\"session metadata command read failed",
    "_warn(f\"active lock read failed",
    "_warn(f\"corrupt lock JSON",
    "_warn(f\"lock read IO error",
    "_warn(f\"lock release check failed",
    "_warn(f\"lock release failed",
)))
check("session: _bg validates session", 'verb == "_bg"' in main_seg and "validate_name(session)" in main_seg)
check("session: notify direct path validates session", 'verb == "notify"' in main_seg and "validate_name(rest[0])" in main_seg)
check("shared: _SAFE_NAME defined", "_SAFE_NAME" in SHARED_SRC and "def validate_name" in SHARED_SRC)
check("shared: validate_name rejects dot", 'name == "."' in SHARED_SRC)
check("k: imports validate_name", "validate_name" in K_SRC and "validate_name(" in K_SRC)
check("km: imports validate_name", "validate_name" in KM_SRC and "validate_name(" in KM_SRC)

check("pipe-pane: k replace mode", '"-o"' not in K_SRC)
check("pipe-pane: km replace mode", '"-o"' not in KM_SRC)

# cmd_kill must terminate bg watcher before killing session
cmd_kill = function(K_TREE, "cmd_kill")
kill_seg = segment(K_SRC, cmd_kill)
check("kill: self-validates session", "validate_name(s)" in kill_seg)
check("kill: terminates bg watcher", "_kill_watcher(" in kill_seg)

# _kill_watcher helper must terminate the watcher process group
kw_fn = function(K_TREE, "_kill_watcher")
kw_seg = segment(K_SRC, kw_fn)
check("_kill_watcher: sends SIGTERM", "signal.SIGTERM" in kw_seg)
check("_kill_watcher: uses process group", "os.killpg(" in kw_seg)
check("_kill_watcher: escalates SIGKILL", "signal.SIGKILL" in kw_seg)
wa_seg = segment(K_SRC, watcher_alive_fn)
check("_watcher_alive: probes process group", "os.killpg(" in wa_seg and ", 0)" in wa_seg)

# _send_frame_enters helper must use FRAME_ENTERS
sfe_fn = function(K_TREE, "_send_frame_enters")
sfe_seg = segment(K_SRC, sfe_fn)
check("_send_frame_enters: uses FRAME_ENTERS", "FRAME_ENTERS" in sfe_seg)
send_code_fn = function(K_TREE, "_send_code")
send_code_seg = segment(K_SRC, send_code_fn)
check("_send_code: hook prompt gets frame enters", "_is_hook_prompt(prompt)" in send_code_seg and "_send_frame_enters(" in send_code_seg)

# _write_result helper: atomic write via os.replace
wr_fn = function(K_TREE, "_write_result")
wr_seg = segment(K_SRC, wr_fn)
check("_write_result: uses os.replace", "os.replace(" in wr_seg)
check("_write_result: uses fsync", "os.fsync(" in wr_seg)
check("_write_result: uses private open", "_open_private(" in wr_seg)

# _update_lock helper exists
ul_fn = function(K_TREE, "_update_lock")
check("_update_lock: exists", ul_fn is not None)

# cmd_fire must store bg_pgid in lock for orphan detection
fire_seg = segment(K_SRC, cmd_fire)
check("fire: stores bg_pgid in lock", "_update_lock(" in fire_seg and "bg_pgid" in fire_seg)

mark_terminal = function(K_TREE, "_mark_terminal")
mt_seg = segment(K_SRC, mark_terminal)
terminal_fields_fn = function(K_TREE, "_terminal_fields")
tf_seg = segment(K_SRC, terminal_fields_fn)
check("_terminal_fields: records completed done", '"completed": True' in tf_seg and '"terminal_status": DONE' in tf_seg)
check("_terminal_fields: records timeout", '"timed_out": True' in tf_seg and '"terminal_status": TIMEOUT' in tf_seg)
check("_mark_terminal: uses terminal fields", "_terminal_fields(status)" in mt_seg)
ct_seg = segment(K_SRC, commit_terminal_fn)
check("_commit_terminal_result: exists", commit_terminal_fn is not None)
check("_commit_terminal_result: nonblocking lock proof", "blocking=blocking" in ct_seg and "LockBusy" in ct_seg)
check("_commit_terminal_result: writes result under lock", "_update_lock_unlocked(" in ct_seg and "_write_result(" in ct_seg)
check("_commit_terminal_result: logs terminal event", "cell_event(cell_id, DONE)" in ct_seg and "cell_event(cell_id, TIMEOUT)" in ct_seg)
check("_stream_process: commits terminal result", "_commit_terminal_result(" in stream_seg)
check("_stream_process: failed commit becomes interrupted error", '"status": ERROR' in stream_seg and '"output": "interrupted"' in stream_seg)
check("_stream_process: private log read", "_open_private(logpath, os.O_RDONLY" in stream_seg)
check("_stream_process: event filtering uses shared regex", "CELL_EVENT_RE.match(clean)" in stream_seg and "NOTIFY_EVENT_RE.match(clean)" in stream_seg)

check("shared: POSIX-only fail-fast", 'os.name != "posix"' in SHARED_SRC and "requires POSIX" in SHARED_SRC)
check("shared: tmux fail-fast no bare fallback", '_require_executable("tmux"' in SHARED_SRC and 'or "tmux"' not in SHARED_SRC)
check("shared: tail fail-fast no bare fallback", '_require_executable("tail"' in SHARED_SRC and 'or "tail"' not in SHARED_SRC)
check("shared: missing dependency exits human-readable", "requires {name} in PATH" in SHARED_SRC and "sys.exit(1)" in SHARED_SRC)
check("shared: tmux 3.0+ checked", "_require_tmux_version" in SHARED_SRC and "requires tmux {required}+" in SHARED_SRC)
check("shared: tmux version parser exists", "_tmux_version_tuple" in SHARED_SRC and "tmux\\s+" in SHARED_SRC)
check("k: version before dependency checks",
      "__version__" in K_SRC and K_SRC.index("__version__") < K_SRC.index("from agent_tty._shared import"))
check("km: version before dependency checks",
      "__version__" in KM_SRC and KM_SRC.index("__version__") < KM_SRC.index("from agent_tty._shared import"))
check("k: version aliases", '"--version", "-V", "version"' in K_SRC)
check("km: version aliases", '"--version", "-V", "version"' in KM_SRC)
check("k: imports shared before fcntl",
      K_SRC.index("from agent_tty._shared import") < K_SRC.index("import fcntl"))
check("shared: validate_cell_id exists", "def validate_cell_id" in SHARED_SRC and "CELL_ID_RE" in SHARED_SRC)
check("shared: CELL_ID_RE exact", "CELL_ID_RE = re.compile(r'^[0-9a-f]{12}$')" in SHARED_SRC)
cell_event_fn = function(SHARED_TREE, "cell_event")
cell_event_seg = segment(SHARED_SRC, cell_event_fn)
check("shared: cell_event validates cell_id", "validate_cell_id(cell_id)" in cell_event_seg)
check("shared: cell_event rejects bad status", "raise ValueError" in cell_event_seg)
check("shared: CELL_END_RE generated from TERMINAL", '"|".join(sorted(TERMINAL))' in SHARED_SRC)
open_private_seg = segment(SHARED_SRC, shared_open_private_fn)
check("shared: open_private uses O_NOFOLLOW", "O_NOFOLLOW" in open_private_seg)
check("shared: open_private uses fchmod", "os.fchmod" in open_private_seg)
ensure_private_seg = segment(SHARED_SRC, ensure_private_dir_fn)
check("shared: ensure_private_dir rejects symlink", "os.path.islink" in ensure_private_seg)
check("shared: ensure_private_dir rejects non-directory", "os.path.lexists" in ensure_private_seg and "not a directory" in ensure_private_seg)
check("shared: ensure_private_dir uses lstat", "os.lstat" in ensure_private_seg)
check("shared: ensure_private_dir checks owner", "st.st_uid != os.getuid()" in ensure_private_seg)
check("shared: ensure_private_dir gives recovery hint", "set XDG_RUNTIME_DIR" in ensure_private_seg)
check("shared: ensure_private_dir enforces 0700", "st.st_mode & 0o077" in ensure_private_seg and "os.chmod(path, 0o700)" in ensure_private_seg)
notify_event_seg = segment(SHARED_SRC, notify_event_fn)
check("shared: notify_event one-line seal", "_one_line(who)" in notify_event_seg and "_one_line(message)" in notify_event_seg)
check("shared: notify regex accepts empty message", "NOTIFY_EVENT_RE" in SHARED_SRC and "(.*)" in SHARED_SRC)

# ── dedup invariants: helpers used, not inlined ──
# only _send_frame_enters and _send_code should reference FRAME_ENTERS directly
for fn_name, fn in (("cmd_fire", cmd_fire), ("cmd_run", cmd_run),
                     ("cmd_int", cmd_int), ("_send_interrupt", send_int_fn)):
    seg = segment(K_SRC, fn)
    check(f"{fn_name}: no inline frame enters", "FRAME_ENTERS" not in seg,
          "should use _send_frame_enters()")
# terminal writers must use _write_result for atomic writes
for fn_name, fn in (("_commit_terminal_result", commit_terminal_fn), ("cmd_int", cmd_int)):
    seg = segment(K_SRC, fn)
    check(f"{fn_name}: uses _write_result", "_write_result(" in seg,
          "should use _write_result() for atomic writes")
    check(f"{fn_name}: no inline json.dump to result", "json.dump(result, f)" not in seg,
          "should use _write_result()")
# only _kill_watcher should contain os.kill + SIGTERM
for fn_name, fn in (("cmd_int", cmd_int), ("cmd_kill", cmd_kill)):
    seg = segment(K_SRC, fn)
    check(f"{fn_name}: no inline os.kill for watcher", "signal.SIGTERM" not in seg,
          "should use _kill_watcher()")

# ANSI_RE lives in _shared.py — k and km import it (no local override)
check("shared: ANSI_RE defined", "ANSI_RE = re.compile(" in SHARED_SRC)
check("k: no local ANSI_RE", "ANSI_RE = re.compile(" not in K_SRC,
      "should import ANSI_RE from _shared")
check("km: no local ANSI_RE", "ANSI_RE = re.compile(" not in KM_SRC,
      "should import ANSI_RE from _shared")

for status_name in ("FIRED", "DONE", "TIMEOUT", "INTERRUPTED", "RUNNING", "ERROR", "NOTIFY", "CLOSED"):
    check(f"k: no local {status_name}", f"\n{status_name} =" not in "\n" + K_SRC)
    check(f"km: no local {status_name}", f"\n{status_name} =" not in "\n" + KM_SRC)

# event wire format type seal: cell_event + regexes in _shared
check("shared: cell_event constructor", "def cell_event(" in SHARED_SRC)
check("shared: CELL_END_RE derived from TERMINAL", "TERMINAL" in SHARED_SRC and "CELL_END_RE" in SHARED_SRC)
check("shared: CELL_DIR defined", "CELL_DIR" in SHARED_SRC)
check("k: _result validates cell_id", "def _result" in K_SRC and "validate_cell_id(cid)" in K_SRC)
check("k: uses lock guard", "class LockGuard" in K_SRC and "fcntl.flock" in K_SRC)
check("k: imports shared private open", "open_private as _open_private" in K_SRC)
check("k: no raw os.open outside shared helper", "os.open(" not in K_SRC)
check("k: session_dir validates and privatizes", "validate_name(s)" in segment(K_SRC, session_dir_fn) and "ensure_private_dir" in segment(K_SRC, session_dir_fn))
check("k: poll result read is private", "_open_private(rpath, os.O_RDONLY" in poll_seg)
check("k: history log read is private", "_open_private(logpath, os.O_RDONLY" in segment(K_SRC, cmd_history))
check("k: bash multiline source wrapper", "def _should_source_bash" in K_SRC and "def _source_command" in K_SRC)
ssb_seg = segment(K_SRC, should_source_bash_fn)
check("k: bash source requires multiline and bash", "_has_multiple_code_lines" in ssb_seg and "_looks_like_bash" in ssb_seg)
acquire_unlocked_seg = segment(K_SRC, acquire_unlocked_fn)
check("k: acquire creates lock via private open", "_open_private(lock, os.O_WRONLY | os.O_CREAT | os.O_EXCL" in acquire_unlocked_seg)
check("k: acquire clears done-lock through release helper", "_release_unlocked" in acquire_unlocked_seg)
check("k: input scripts are private", "_open_private(" in segment(K_SRC, write_input_script_fn))
check("k: input scripts are cleaned", "_cleanup_input_script(session, cell_id)" in K_SRC)
notify_seg = segment(K_SRC, cmd_notify)
check("k: notify uses notify_event constructor", "notify_event(" in notify_seg)
check("k: notify closes proc comm file", "with open(f\"/proc/{os.getppid()}/comm\") as f" in notify_seg)
check("km: notify uses NOTIFY constant", "status\": NOTIFY" in KM_SRC)
check("km: signal handler does not cleanup inline", "def request_stop" in KM_SRC and "raise KeyboardInterrupt" in KM_SRC)
check("km: imports shared private open", "open_private" in KM_SRC and "def open_private" not in KM_SRC)
check("km: private log open", "open_private(logfile, os.O_WRONLY" in KM_SRC and "open_private(logfile, os.O_RDONLY" in KM_SRC)
check("km: cell_id uses shared validator", "CELL_ID_RE.match" in KM_SRC and "validate_cell_id(arg)" in KM_SRC)
check("km: one-shot tails from scan offset even when empty", "scan_offset = None" in KM_SRC and "if scan_offset is not None" in KM_SRC)
check("km: one-shot pre-scans without cell_id", "if oneshot:" in KM_SRC and "last_completion" in KM_SRC)
check("km: tail uses checked executable", "TAIL" in KM_SRC and "tail_cmd = [TAIL" in KM_SRC)
check("km: tail replaces decode errors", 'errors="replace"' in KM_SRC)
check("k: no local cell event format", '── cell:' not in K_SRC or 'cell_event(' in K_SRC,
      "should use cell_event() from _shared")
check("km: no local cell event regex", "re.compile.*cell:" not in KM_SRC,
      "should import CELL_START_RE/CELL_END_RE from _shared")

resolve_seg = segment(K_SRC, resolve_fn)
check("_resolve: validates K_SESSION", "env = os.environ.get(\"K_SESSION\")" in resolve_seg and "validate_name(env)" in resolve_seg)

for fn_name, fn in (("_resolve", resolve_fn), ("cmd_watch", cmd_watch), ("cmd_status", cmd_status),
                    ("cmd_notify", cmd_notify), ("cmd_ls", cmd_ls)):
    check(f"{fn_name}: covered by contract", fn is not None)

status_seg = segment(K_SRC, cmd_status)
check("status: prints next action", "next=" in status_seg)
check("status: reports idle/running/timeout", "state=idle" in status_seg and "state=running" in status_seg and "state=timeout" in status_seg)
check("status: checks active cell", "_load_cell(session)" in status_seg)
check("status: detects dead watcher", "_watcher_alive(meta)" in status_seg and "state=watcher-dead" in status_seg)
check("status: pipe failure has recovery hint", "ERR pipe failed" in status_seg and "use k kill {session}" in status_seg)

watch_seg = segment(K_SRC, cmd_watch)
history_seg = segment(K_SRC, cmd_history)
check("watch/history: no-log helper", "def _no_log_output" in K_SRC and "_no_log_output(session)" in watch_seg and "_no_log_output(session)" in history_seg)
check("watch: tail process initialised before try", "proc = None" in watch_seg)
check("watch: tail uses checked executable", "[TAIL," in watch_seg)
check("watch: tail decode replaces bad bytes", 'errors="replace"' in watch_seg)
check("watch: tail launch fails cleanly", "except OSError as e" in watch_seg and "ERR watch failed" in watch_seg)

if FAILURES:
    print("contract failures:")
    for failure in FAILURES:
        print(f"  - {failure}")
    raise SystemExit(1)

print("contract tests passed")
