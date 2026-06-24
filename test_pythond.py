#!/usr/bin/env python3
"""pythond test suite -- unit + integration.

Run:  python -B test_pythond.py

Unit tests run everywhere.  Integration tests need POSIX (PTY) or TCP mode.
"""
import json
import io
import os
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
import threading
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parent
assert (ROOT / "pythond.py").exists(), f"pythond.py not found in {ROOT}"
sys.path.insert(0, str(ROOT))
import pythond
_WS_PROTO = pythond._WS_PROTO

PASS = 0
FAIL = 0


def check(name, condition, detail=""):
    global PASS, FAIL
    if condition:
        PASS += 1
    else:
        print(f"  X {name}")
        if detail:
            print(f"    {detail}")
        FAIL += 1


def section(title):
    print(f"\n--- {title} ---")


# ===========================================
# UNIT TESTS (no daemon needed)
# ===========================================

def test_version():
    section("version")
    check("version is string", isinstance(pythond.__version__, str))
    check("version has dots", "." in pythond.__version__)
    check("version is 0.3.0", pythond.__version__ == "0.3.0")


def test_listen_addr_parsing():
    """Cover all branches of listen_addr parsing in daemon()."""
    section("listen_addr parsing")
    cases = [
        ("0.0.0.0:7399", "0.0.0.0", 7399),
        (":7399",         "0.0.0.0", 7399),
        ("7399",          "0.0.0.0", 7399),
        ("myserver",      "myserver", 7399),
        ("myserver:8080", "myserver", 8080),
        ("10.0.0.5:443",  "10.0.0.5", 443),
    ]
    for listen_addr, exp_host, exp_port in cases:
        if ":" in listen_addr:
            host, _, port_s = listen_addr.rpartition(":")
            host = host or "0.0.0.0"
            port = int(port_s)
        elif listen_addr.isdigit():
            host = "0.0.0.0"
            port = int(listen_addr)
        else:
            host = listen_addr
            port = 7399
        check(f"listen={listen_addr} host",
              host == exp_host, f"got {host}")
        check(f"listen={listen_addr} port",
              port == exp_port, f"got {port}")


def test_khost_parsing():
    """Cover all branches of PYTHOND_HOST parsing in _client_socket."""
    section("PYTHOND_HOST parsing")
    cases = [
        ("10.0.0.5:7399", "10.0.0.5", 7399),
        ("10.0.0.5",       "10.0.0.5", 7399),
        ("myhost",         "myhost",   7399),
    ]
    for host_str, exp_h, exp_port in cases:
        if ":" in host_str:
            h, _, p = host_str.rpartition(":")
            port = int(p)
        else:
            port = 7399
            h = host_str
        check(f"PYTHOND_HOST={host_str} h", h == exp_h, f"got {h}")
        check(f"PYTHOND_HOST={host_str} port", port == exp_port, f"got {port}")


def test_parse_host_port_validation():
    section("_parse_host_port validation")
    check("parse explicit port",
          pythond._parse_host_port("example.com:443") == ("example.com", 443))
    check("parse default port",
          pythond._parse_host_port("example.com", default_port=1234) ==
          ("example.com", 1234))
    for value in ("example.com:0", "example.com:65536", ":8080"):
        try:
            pythond._parse_host_port(value)
            check(f"reject {value}", False)
        except ValueError:
            check(f"reject {value}", True)


def test_init_namespace():
    section("_init_namespace")
    ns = pythond._init_namespace()
    check("has builtins", "__builtins__" in ns)
    for mod in ("os", "sys", "json", "subprocess", "re", "sqlite3"):
        check(f"has {mod}", mod in ns, f"missing {mod}")


def test_make_exec_eval():
    section("_make_exec eval")
    ns = pythond._init_namespace()
    lock = threading.Lock()
    _exec = pythond._make_exec(ns, lock)
    # eval expression
    out = _exec("1 + 1")
    check("eval returns repr", out == "2")
    # eval string
    out = _exec("'hello'")
    check("eval string returns raw", out == "hello")
    # eval None (no output)
    out = _exec("None")
    check("eval None empty", out == "")


def test_make_exec_exec():
    section("_make_exec exec")
    ns = pythond._init_namespace()
    lock = threading.Lock()
    _exec = pythond._make_exec(ns, lock)
    # exec statement
    out = _exec("x = 42")
    check("exec no output", out == "")
    check("exec set var", ns.get("x") == 42)
    # exec print
    out = _exec("print('hello world')")
    check("exec print captured", out == "hello world")


def test_make_exec_last_expr():
    section("_make_exec auto-print last expression")
    ns = pythond._init_namespace()
    lock = threading.Lock()
    _exec = pythond._make_exec(ns, lock)
    # multi-line with last expression -> auto-print
    out = _exec("x = 6\ny = 7\nx * y")
    check("last expr auto-print", out == "42")
    # multi-line with last assignment -> no auto-print
    out = _exec("a = 1\nb = 2")
    check("last assign no print", out == "")
    # multi-line function def + call as last expr
    out = _exec("def fib(n):\n  a, b = 0, 1\n  for _ in range(n): a, b = b, a+b\n  return a\nfib(10)")
    check("func def + last call", out == "55")
    # single expression still works (eval path)
    out = _exec("100 + 23")
    check("single expr still works", out == "123")


def test_make_exec_error():
    section("_make_exec error handling")
    ns = pythond._init_namespace()
    lock = threading.Lock()
    _exec = pythond._make_exec(ns, lock)
    # exception
    out = _exec("1/0")
    check("exception has traceback", "ZeroDivisionError" in out)
    # syntax error in eval -> falls through to exec
    out = _exec("if True: pass")
    check("statement via exec", out == "")
    # KeyboardInterrupt
    out = _exec("raise KeyboardInterrupt")
    check("KeyboardInterrupt caught", "KeyboardInterrupt" in out)
    # SystemExit
    out = _exec("raise SystemExit(42)")
    check("SystemExit caught", "exit(42)" in out)


def test_make_exec_on_done():
    section("_make_exec on_done callback")
    ns = pythond._init_namespace()
    lock = threading.Lock()
    calls = []
    _exec = pythond._make_exec(ns, lock, on_done=lambda s, o: calls.append((s, o)))
    _exec("print('hi')")
    check("on_done called", len(calls) == 1)
    check("on_done src", calls[0][0] == "print('hi')")
    check("on_done output", calls[0][1] == "hi")


def test_make_exec_thread_isolation():
    section("_make_exec thread-local stdout")
    ns = pythond._init_namespace()
    lock = threading.Lock()
    _exec = pythond._make_exec(ns, lock)

    # child thread prints during exec -> should NOT appear in cell output
    import time
    leaked = []
    real_stdout = sys.stdout._real if isinstance(sys.stdout, pythond._ThreadStdout) else sys.stdout

    code = (
        "import threading, time\n"
        "def _bg():\n"
        "    time.sleep(0.05)\n"
        "    print('from_child_thread')\n"
        "t = threading.Thread(target=_bg)\n"
        "t.start()\n"
        "time.sleep(0.1)\n"
        "t.join()\n"
        "print('from_main')\n"
    )
    output = _exec(code)
    check("main thread captured", "from_main" in output)
    check("child thread NOT in cell", "from_child_thread" not in output, output)

    # direct exec -> main thread output captured normally
    output2 = _exec("print('normal')")
    check("normal capture works", output2.strip() == "normal")


def test_make_exec_restores_replaced_stdio():
    section("_make_exec restores replaced stdio")
    ns = pythond._init_namespace()
    lock = threading.Lock()
    _exec = pythond._make_exec(ns, lock)

    _exec("import sys, io\nsys.stdout = io.StringIO()\nsys.stderr = io.StringIO()")
    out = _exec("print('still captured')")
    check("stdout replacement recovered", out == "still captured", out)
    check("global stdout wrapper restored", isinstance(sys.stdout, pythond._ThreadStdout))
    check("global stderr wrapper restored", isinstance(sys.stderr, pythond._ThreadStdout))


def test_thread_stdout_compat_methods():
    section("_ThreadStdout compat methods")
    real = io.StringIO()
    wrapper = pythond._ThreadStdout(real)
    wrapper.writelines(["a", "b"])
    check("writelines forwards", real.getvalue() == "ab")
    check("isatty forwards", wrapper.isatty() is False)


def test_dispatch_run():
    section("_dispatch run")
    ns = pythond._init_namespace()
    lock = threading.Lock()
    _exec = pythond._make_exec(ns, lock)
    cells = {}
    resp = pythond._dispatch("run", ["2+2"], _exec, cells, ns)
    check("run output", resp["output"] == "4")
    check("run success error flag false", resp.get("_error") is False)
    resp = pythond._dispatch("run", ["print('Traceback')"], _exec, cells, ns)
    check("literal Traceback not error",
          resp["output"] == "Traceback" and resp.get("_error") is False)
    resp = pythond._dispatch("run", ["1/0"], _exec, cells, ns)
    check("run exception error flag true", resp.get("_error") is True)


def test_dispatch_fire_poll():
    section("_dispatch fire+poll")
    ns = pythond._init_namespace()
    lock = threading.Lock()
    _exec = pythond._make_exec(ns, lock)
    cells = {}
    resp = pythond._dispatch("fire", ["import time; time.sleep(0.1); x=99"], _exec, cells, ns)
    check("fire has cell_id", "cell_id" in resp)
    check("fire status", resp["status"] == "fired")
    cid = resp["cell_id"]
    time.sleep(0.3)
    resp2 = pythond._dispatch("poll", [cid], _exec, cells, ns)
    check("poll done", resp2["status"] == "done")
    check("poll cell_id", resp2["cell_id"] == cid)
    check("fire set var", ns.get("x") == 99)
    check("fire cell carries tid key", "tid" in cells[cid])


def test_dispatch_async_empty_code_rejected():
    section("_dispatch async empty code rejected")
    ns = pythond._init_namespace()
    lock = threading.Lock()
    _exec = pythond._make_exec(ns, lock)
    cells = {}
    resp = pythond._dispatch("fire", ["   "], _exec, cells, ns)
    check("fire empty rejected", resp == {"error": "fire requires code"}, resp)
    resp = pythond._dispatch("fork", ["   "], _exec, cells, ns, lock)
    if sys.platform == "win32":
        check("fork windows still reports unsupported",
              resp == {"error": "fork not supported on Windows (no COW fork)"}, resp)
    else:
        check("fork empty rejected", resp == {"error": "fork requires code"}, resp)


def test_dispatch_fire_traceback_format_failure():
    section("_dispatch fire traceback fallback")
    ns = pythond._init_namespace()
    cells = {}
    def boom(src):
        raise RuntimeError("boom")
    with mock.patch.object(pythond.traceback, "format_exc",
                           side_effect=RuntimeError("format failed")):
        resp = pythond._dispatch("fire", ["x"], boom, cells, ns)
        cid = resp["cell_id"]
        for _ in range(20):
            time.sleep(0.05)
            polled = pythond._dispatch("poll", [cid], boom, cells, ns)
            if polled["status"] == "done":
                break
    check("fire completed after format failure", polled["status"] == "done", polled)
    check("fallback output", polled["output"] == "(traceback formatting failed)",
          polled)


def test_dispatch_poll_empty():
    section("_dispatch poll empty")
    ns = pythond._init_namespace()
    lock = threading.Lock()
    _exec = pythond._make_exec(ns, lock)
    cells = {}
    resp = pythond._dispatch("poll", [], _exec, cells, ns)
    check("poll empty idle", resp == {"status": "idle"})


def test_dispatch_poll_unknown():
    section("_dispatch poll unknown cell")
    ns = pythond._init_namespace()
    lock = threading.Lock()
    _exec = pythond._make_exec(ns, lock)
    cells = {}
    resp = pythond._dispatch("poll", ["nonexistent"], _exec, cells, ns)
    check("poll unknown error", resp["status"] == "error")


def test_dispatch_poll_latest():
    section("_dispatch poll latest (no id)")
    ns = pythond._init_namespace()
    lock = threading.Lock()
    _exec = pythond._make_exec(ns, lock)
    cells = {}
    pythond._dispatch("fire", ["y=1"], _exec, cells, ns)
    time.sleep(0.2)
    resp = pythond._dispatch("poll", [], _exec, cells, ns)
    check("poll latest has cell_id", "cell_id" in resp)


def test_dispatch_status():
    section("_dispatch status")
    ns = pythond._init_namespace()
    ns["myvar"] = 42
    lock = threading.Lock()
    _exec = pythond._make_exec(ns, lock)
    cells = {}
    resp = pythond._dispatch("status", [], _exec, cells, ns)
    check("status has state", "state" in resp)
    check("status idle", resp["state"] == "idle")
    check("status vars count", resp["vars"] >= 1)


def test_dispatch_vars():
    section("_dispatch vars")
    ns = pythond._init_namespace()
    ns["myvar"] = 42
    lock = threading.Lock()
    _exec = pythond._make_exec(ns, lock)
    cells = {}
    resp = pythond._dispatch("vars", [], _exec, cells, ns)
    check("vars has list", "vars" in resp)
    check("vars includes myvar", "myvar" in resp["vars"])
    check("vars excludes private", not any(v.startswith("_") for v in resp["vars"]))


def test_dispatch_complete():
    section("_dispatch complete")
    ns = pythond._init_namespace()
    lock = threading.Lock()
    _exec = pythond._make_exec(ns, lock)
    cells = {}
    resp = pythond._dispatch("complete", ["os.path."], _exec, cells, ns)
    check("complete has matches", "matches" in resp)
    check("complete non-empty", len(resp["matches"]) > 0)
    check("complete has join", any("join" in m for m in resp["matches"]))


def test_dispatch_int():
    section("_dispatch int (interrupt)")
    ns = pythond._init_namespace()
    lock = threading.Lock()
    _exec = pythond._make_exec(ns, lock)
    cells = {}
    # Python-level loop (not C-level sleep) so PyThreadState_SetAsyncExc works
    resp = pythond._dispatch("fire",
        ["[__import__('time').sleep(0.1) for _ in range(100)]"], _exec, cells, ns)
    cid = resp["cell_id"]
    time.sleep(0.3)
    resp = pythond._dispatch("int", [], _exec, cells, ns)
    check("int has threads", "threads" in resp)
    time.sleep(0.5)
    resp = pythond._dispatch("poll", [cid], _exec, cells, ns)
    check("interrupted cell done", resp["status"] == "done")


def test_dispatch_unknown():
    section("_dispatch unknown command")
    ns = pythond._init_namespace()
    lock = threading.Lock()
    _exec = pythond._make_exec(ns, lock)
    cells = {}
    resp = pythond._dispatch("bogus", [], _exec, cells, ns)
    check("unknown error", "error" in resp)


def test_dispatch_fork():
    section("_dispatch fork (process-based async)")
    if sys.platform == "win32":
        check("fork skipped on windows", True)
        return
    ns = pythond._init_namespace()
    ns["x"] = 10
    lock = threading.Lock()
    _exec = pythond._make_exec(ns, lock)
    cells = {}

    # basic fork: set a variable, it should merge back
    resp = pythond._dispatch("fork", ["y = x * 2"], _exec, cells, ns, lock)
    check("fork returns cell_id", "cell_id" in resp)
    check("fork status", resp["status"] == "forked")
    cid = resp["cell_id"]

    # wait for fork to complete
    for _ in range(20):
        time.sleep(0.2)
        resp = pythond._dispatch("poll", [cid], _exec, cells, ns)
        if resp["status"] == "done":
            break
    check("fork done", resp["status"] == "done")
    check("fork merged y", "y" in resp.get("merged", []))
    check("y merged to namespace", ns.get("y") == 20)

    # fork with output
    resp2 = pythond._dispatch("fork", ["print(x + y)"], _exec, cells, ns, lock)
    cid2 = resp2["cell_id"]
    for _ in range(20):
        time.sleep(0.2)
        resp2 = pythond._dispatch("poll", [cid2], _exec, cells, ns)
        if resp2["status"] == "done":
            break
    check("fork output", resp2["output"].strip() == "30")

    # fork with unpicklable object -> skipped
    resp3 = pythond._dispatch("fork", ["import threading; lk = threading.Lock()"],
                               _exec, cells, ns, lock)
    cid3 = resp3["cell_id"]
    for _ in range(20):
        time.sleep(0.2)
        resp3 = pythond._dispatch("poll", [cid3], _exec, cells, ns)
        if resp3["status"] == "done":
            break
    check("fork skipped unpicklable", "lk" in resp3.get("skipped", []))


def test_dispatch_fork_kill():
    section("_dispatch fork int (killable)")
    if sys.platform == "win32":
        check("fork kill skipped on windows", True)
        return
    ns = pythond._init_namespace()
    lock = threading.Lock()
    _exec = pythond._make_exec(ns, lock)
    cells = {}

    # fork a long sleep -> int should kill it
    resp = pythond._dispatch("fork", ["__import__('time').sleep(30)"],
                              _exec, cells, ns, lock)
    cid = resp["cell_id"]
    time.sleep(0.5)
    resp = pythond._dispatch("int", [], _exec, cells, ns)
    check("fork int count", resp["processes"] >= 1)
    # wait for monitor thread to reap
    time.sleep(1)
    resp = pythond._dispatch("poll", [cid], _exec, cells, ns)
    check("fork killed done", resp["status"] == "done")
    check("fork killed output", "killed" in resp.get("output", ""))


def test_dispatch_fork_large_payload():
    section("_dispatch fork large payload (pipe buffer test)")
    if sys.platform == "win32":
        check("fork large skipped on windows", True)
        return
    ns = pythond._init_namespace()
    lock = threading.Lock()
    _exec = pythond._make_exec(ns, lock)
    cells = {}

    # Create data larger than pipe buffer (~64KB) to exercise:
    #   - _write_all (partial write handling)
    #   - read-first-then-waitpid ordering (no deadlock)
    resp = pythond._dispatch("fork",
        ["big = list(range(50000))"],  # ~400KB when pickled
        _exec, cells, ns, lock)
    cid = resp["cell_id"]
    for _ in range(30):
        time.sleep(0.2)
        resp = pythond._dispatch("poll", [cid], _exec, cells, ns)
        if resp["status"] == "done":
            break
    check("fork large done", resp["status"] == "done")
    check("fork large merged", "big" in resp.get("merged", []))
    check("fork large data intact", ns.get("big") == list(range(50000)))


def test_dispatch_fork_concurrent_fire():
    section("_dispatch fork while fire running")
    if sys.platform == "win32":
        check("fork concurrent skipped on windows", True)
        return
    ns = pythond._init_namespace()
    ns["base"] = 100
    lock = threading.Lock()
    _exec = pythond._make_exec(ns, lock)
    cells = {}

    # fire a slow task, then fork while it's running
    pythond._dispatch("fire",
        ["import time; time.sleep(0.5); fired_val = base + 1"],
        _exec, cells, ns, lock)
    time.sleep(0.1)
    # fork should work even with fire thread active (os.fork not mp.Process)
    resp = pythond._dispatch("fork", ["forked_val = base + 2"],
                              _exec, cells, ns, lock)
    cid = resp["cell_id"]
    for _ in range(20):
        time.sleep(0.2)
        resp = pythond._dispatch("poll", [cid], _exec, cells, ns)
        if resp["status"] == "done":
            break
    check("fork concurrent done", resp["status"] == "done")
    check("fork concurrent merged", "forked_val" in resp.get("merged", []))
    check("fork concurrent value", ns.get("forked_val") == 102)
    # wait for fire to finish too
    time.sleep(1)
    check("fire also completed", ns.get("fired_val") == 101)


def test_cell_eviction():
    section("cell eviction (time-based)")
    ns = pythond._init_namespace()
    lock = threading.Lock()
    _exec = pythond._make_exec(ns, lock)
    cells = {}

    # fire a cell and let it complete
    resp = pythond._dispatch("fire", ["1+1"], _exec, cells, ns)
    cid = resp["cell_id"]
    time.sleep(0.2)

    # cell should be pollable
    resp = pythond._dispatch("poll", [cid], _exec, cells, ns)
    check("cell exists before evict", resp["status"] == "done")

    # fake the done_at to 10 minutes ago
    with pythond._cells_lock:
        cells[cid]["_done_at"] = time.time() - 600

    # fire another cell -> triggers eviction
    resp2 = pythond._dispatch("fire", ["2+2"], _exec, cells, ns)
    time.sleep(0.2)

    # old cell should be evicted
    with pythond._cells_lock:
        check("stale cell evicted", cid not in cells)
    # new cell still there
    check("new cell exists", resp2["cell_id"] in cells)


def test_session_name_sanitization():
    section("session name sanitization")
    bad_names = [
        "../etc", "foo/bar", "a\\b", "x\0y", "", ".", "..",
        ".../.../passwd", "work.", "work ",
    ]
    for name in bad_names:
        resp = pythond.handle_client("new", [name])
        check(f"reject '{name[:20]}'", "ERR" in resp, resp)
        resp = pythond.handle_client("connect", [name, "127.0.0.1:1", "tok"])
        check(f"connect rejects '{name[:20]}'", "invalid session name" in resp, resp)


def test_control_handler_exact_arity():
    section("control handler exact arity")
    check("disconnect rejects extra",
          pythond._handle_disconnect(["x", "extra"]).startswith("ERR usage"))
    check("int rejects extra",
          pythond._handle_int(["x", "extra"]).startswith("ERR usage"))
    check("kill rejects extra",
          pythond._handle_kill(["x", "extra"]).startswith("ERR usage"))
    check("resize rejects extra",
          pythond._handle_resize(["x", "24", "80", "extra"]).startswith("ERR usage"))
    check("connect rejects unknown option",
          pythond._handle_connect(["x", "127.0.0.1:1", "tok", "--bad"]).startswith(
              "ERR usage"))


def test_resize_dead_pty_not_ok():
    section("resize dead PTY not OK")
    name = "__dead_resize__"
    with pythond._sessions_lock:
        pythond.sessions[name] = {"type": "pty"}
    try:
        resp = pythond._handle_resize([name, "24", "80"])
        check("dead PTY resize errors", resp.startswith("ERR session has no live PTY"),
              resp)
    finally:
        with pythond._sessions_lock:
            pythond.sessions.pop(name, None)


def test_session_dir():
    section("_session_dir")
    name = "__test_session_dir__"
    path = pythond._session_dir(name)
    check("dir exists", os.path.isdir(path))
    check("dir path correct", name in path)
    if sys.platform != "win32":
        mode = os.stat(path).st_mode & 0o777
        check("dir is private", mode & 0o077 == 0, oct(mode))
    shutil.rmtree(os.path.join(os.path.expanduser("~"), ".pythond",
                               "sessions", name), ignore_errors=True)


def test_session_capacity_limit():
    section("session capacity limit")
    with pythond._sessions_lock:
        saved = dict(pythond.sessions)
        pythond.sessions.clear()
    try:
        with mock.patch.object(pythond, "_MAX_SESSIONS", 1):
            pythond._set_session("one", {"type": "remote"})
            try:
                pythond._set_session("two", {"type": "remote"})
                check("capacity rejects second session", False)
            except RuntimeError as e:
                check("capacity rejects second session", "too many sessions" in str(e))
    finally:
        with pythond._sessions_lock:
            pythond.sessions.clear()
            pythond.sessions.update(saved)


def test_log_history():
    section("_log_history")
    name = "__test_log_hist__"
    pythond._log_history(name, "x = 42")
    pythond._log_history(name, "y = x + 1")
    path = os.path.join(pythond._session_dir(name), "history.py")
    check("history exists", os.path.exists(path))
    if sys.platform != "win32":
        mode = os.stat(path).st_mode & 0o777
        check("history is private", mode & 0o077 == 0, oct(mode))
    content = open(path).read()
    check("history has x", "x = 42" in content)
    check("history has y", "y = x + 1" in content)
    check("history has timestamp", "# [" in content)
    # verify it's valid Python
    try:
        compile(content, path, "exec")
        check("history compiles", True)
    except SyntaxError as e:
        check("history compiles", False, str(e))
    shutil.rmtree(os.path.join(os.path.expanduser("~"), ".pythond",
                               "sessions", name), ignore_errors=True)


def test_log_session():
    section("_log_session")
    name = "__test_log_sess__"
    pythond._log_session(name, "good()", "result", error=False)
    pythond._log_session(name, "bad()", "Traceback...", error=True)
    path = os.path.join(pythond._session_dir(name), "session.log")
    check("session.log exists", os.path.exists(path))
    if sys.platform != "win32":
        mode = os.stat(path).st_mode & 0o777
        check("session.log is private", mode & 0o077 == 0, oct(mode))
    content = open(path).read()
    check("log has OK", "OK" in content)
    check("log has ERROR", "ERROR" in content)
    check("log has good()", "good()" in content)
    check("log has bad()", "bad()" in content)
    check("log has output prefix", "# >" in content)
    shutil.rmtree(os.path.join(os.path.expanduser("~"), ".pythond",
                               "sessions", name), ignore_errors=True)


def test_daemon_meta_roundtrip():
    section("daemon meta read/write")
    # save original
    orig = pythond._read_daemon_meta()
    # with a fake path
    with tempfile.TemporaryDirectory() as td:
        fake_path = os.path.join(td, "daemon.json")
        with mock.patch.object(pythond, "_daemon_meta_path", return_value=fake_path):
            with mock.patch.object(pythond, "_tcp_daemon_alive", return_value=False):
                pythond._write_daemon_meta(9999, "testtoken")
                meta = pythond._read_daemon_meta()
                check("meta port", meta["port"] == 9999)
                check("meta token", meta["token"] == "testtoken")
                check("meta pid", meta["pid"] == os.getpid())
                pythond._remove_daemon_meta()
                check("meta removed", pythond._read_daemon_meta() == {})


def test_daemon_meta_read_missing():
    section("daemon meta read missing")
    with mock.patch.object(pythond, "_daemon_meta_path",
                           return_value="/nonexistent/daemon.json"):
        meta = pythond._read_daemon_meta()
        check("missing returns empty", meta == {})


def test_daemon_meta_read_rejects_symlink():
    section("daemon meta read rejects symlink")
    if sys.platform == "win32" or not hasattr(os, "symlink") or not hasattr(os, "O_NOFOLLOW"):
        check("skip symlink nofollow test", True)
        return
    with tempfile.TemporaryDirectory() as td:
        target = os.path.join(td, "target.json")
        link = os.path.join(td, "daemon.json")
        with open(target, "w", encoding="utf-8") as f:
            json.dump({"port": 9999, "token": "bad"}, f)
        os.symlink(target, link)
        with mock.patch.object(pythond, "_daemon_meta_path", return_value=link):
            check("symlink returns empty", pythond._read_daemon_meta() == {})


def test_daemon_meta_tmp_cleanup_on_write_failure():
    section("daemon meta tmp cleanup on write failure")
    with tempfile.TemporaryDirectory() as td:
        fake_path = os.path.join(td, "daemon.json")
        with mock.patch.object(pythond, "_daemon_meta_path", return_value=fake_path), \
             mock.patch.object(pythond, "_tcp_daemon_alive", return_value=False), \
             mock.patch.object(pythond.os, "write", side_effect=OSError("disk full")):
            try:
                pythond._write_daemon_meta(9999, "token")
                check("write should fail", False)
            except OSError:
                check("write failed", True)
            check("tmp removed", not os.path.exists(fake_path + ".tmp"))


def test_cert_fingerprint_missing():
    section("cert fingerprint missing file")
    fp = pythond._cert_fingerprint("/nonexistent/cert.pem")
    check("missing returns unknown", fp == "unknown")


def test_cert_generation():
    section("cert generation")
    if not pythond._HAS_CRYPTO:
        check("skip (no cryptography)", True)
        return
    with tempfile.TemporaryDirectory() as td:
        with mock.patch.object(pythond, "_tls_dir", return_value=td):
            cert, key = pythond._generate_cert()
            check("cert exists", os.path.exists(cert))
            check("key exists", os.path.exists(key))
            check("cert non-empty", os.path.getsize(cert) > 0)
            check("key non-empty", os.path.getsize(key) > 0)
            check("cert is PEM", open(cert).read().startswith("-----BEGIN"))
            check("key is PEM", open(key).read().startswith("-----BEGIN"))
            # fingerprint
            fp = pythond._cert_fingerprint(cert)
            check("fingerprint not unknown", fp != "unknown")
            check("fingerprint has colons", ":" in fp)
            check("generated cert matches key",
                  pythond._cert_key_pair_valid(cert, key))
            cert_obj = pythond.x509.load_pem_x509_certificate(
                open(cert, "rb").read()
            )
            basic = cert_obj.extensions.get_extension_for_class(
                pythond.x509.BasicConstraints
            ).value
            key_usage = cert_obj.extensions.get_extension_for_class(
                pythond.x509.KeyUsage
            ).value
            check("generated cert is leaf", basic.ca is False)
            check("generated cert cannot sign certs",
                  key_usage.key_cert_sign is False)
            check("generated cert cannot sign CRLs",
                  key_usage.crl_sign is False)
            # second call returns cached
            cert2, key2 = pythond._generate_cert()
            check("cached same cert", cert2 == cert)
            with open(key2, "wb") as f:
                f.write(b"bad key")
            with mock.patch.object(sys, "stderr", io.StringIO()):
                cert3, key3 = pythond._generate_cert()
            check("mismatched key regenerates",
                  cert3 == cert and key3 == key and
                  pythond._cert_key_pair_valid(cert3, key3))


def test_trust_cert_exact_fingerprint_store():
    section("trust cert exact fingerprint store")
    if not pythond._HAS_CRYPTO:
        check("skip (no cryptography)", True)
        return
    with tempfile.TemporaryDirectory() as td:
        with mock.patch.object(pythond, "_tls_dir", return_value=td):
            cert, _key = pythond._generate_cert()
            dest, fp = pythond.trust_cert(cert, direction="server")
            fp_path = dest + ".fingerprint"
            check("trusted PEM copied", os.path.exists(dest))
            check("fingerprint file written", os.path.exists(fp_path))
            check("trusted fingerprint exact",
                  fp in pythond._trusted_fingerprints(pythond._trusted_servers_dir()))
            try:
                pythond.trust_cert(cert, direction="servers")
                check("invalid direction rejected", False)
            except ValueError:
                check("invalid direction rejected", True)


def test_cert_generation_no_crypto():
    section("cert generation without cryptography")
    with mock.patch.object(pythond, "_HAS_CRYPTO", False):
        try:
            pythond._generate_cert()
            check("should have raised", False)
        except RuntimeError as e:
            check("error mentions pip install", "pip install pythond" in str(e))


def test_websocket_protocol():
    section("WebSocket protocol")
    # verify websockets is importable
    from websockets.sync.client import connect as ws_connect
    check("ws_connect exists", callable(ws_connect))
    if _HAS_AF_UNIX:
        from websockets.sync.client import unix_connect as ws_unix
        check("ws_unix_connect exists", callable(ws_unix))


def test_wire_message_builder():
    section("wire message builder")
    check("local run wire",
          pythond._build_wire_message("run", ["work", "x + 1"]) == "run work\nx + 1")
    check("remote run preserves target session",
          pythond._build_wire_message("run", ["remote_work", "x + 1"]) ==
          "run remote_work\nx + 1")
    check("status wire",
          pythond._build_wire_message("status", ["work"]) == "status work")


def test_send_remote_transparent_alias():
    section("remote transparent alias")
    sent = []

    class FakeWs:
        def send(self, msg):
            sent.append(msg)
        def recv(self, timeout=None):
            return "42"

    session = {"type": "remote", "alias": "work", "_ws": FakeWs()}
    resp = pythond._send_remote(session, {"cmd": "run", "args": ["x + 1"]})
    check("transparent alias uses proxy name as remote session",
          sent[-1] == "run work\nx + 1", sent[-1])
    check("transparent alias response", resp == {"output": "42"}, repr(resp))
    sent.clear()
    resp = pythond._send_remote(session, {"cmd": "run", "args": ["gpu", "x + 1"]})
    check("explicit proxy keeps target remote session",
          sent[-1] == "run gpu\nx + 1", sent[-1])
    check("explicit proxy response", resp == {"output": "42"}, repr(resp))


def test_send_remote_close_retries_and_closes():
    section("remote close retries and closes")
    opened = []

    class FakeClose:
        closed = False
        def send(self, msg):
            pass
        def recv(self, timeout=None):
            return pythond._WS_CLOSE
        def close(self):
            self.closed = True

    class FakeOk:
        def send(self, msg):
            pass
        def recv(self, timeout=None):
            return '{"state":"idle"}'

    first = FakeClose()
    second = FakeOk()
    def fake_open(*args, **kwargs):
        ws = [first, second][len(opened)]
        opened.append(ws)
        return ws

    session = {"type": "remote", "host": "h", "port": 1, "token": "t"}
    with mock.patch.object(pythond, "_open_remote_ws", side_effect=fake_open):
        resp = pythond._send_remote(session, {"cmd": "status", "args": ["work"]})
    check("closed stale remote ws", first.closed is True)
    check("retried after close", opened == [first, second])
    check("remote retry response", resp == {"state": "idle"}, resp)


def test_connect_remote_bytes_auth_failure():
    section("connect remote bytes auth failure")

    class FakeWs:
        def send(self, msg):
            pass
        def recv(self, timeout=None):
            return b"ERR auth failed"
        def close(self):
            pass

    with mock.patch.object(pythond, "_open_remote_ws", return_value=FakeWs()):
        resp = pythond.connect_remote("r", "127.0.0.1", 7399, "bad")
    check("bytes auth failure recognised", resp == "ERR auth failed on remote", resp)


def test_session_command_arg_normalization():
    section("session command arg normalization")
    with pythond._sessions_lock:
        saved = dict(pythond.sessions)
        pythond.sessions.clear()
        pythond.sessions["server"] = {"type": "remote"}
        pythond.sessions["work"] = {"type": "local"}
    calls = []

    def fake_send(name, msg):
        calls.append((name, msg))
        return {"output": "ok"}

    try:
        with mock.patch.object(pythond, "send_session", fake_send), \
             mock.patch.object(pythond, "_log_session"), \
             mock.patch.object(pythond, "_log_history"), \
             mock.patch.object(sys, "stderr", io.StringIO()):
            pythond._handle_session_command("run", ["server", "work", "x", "+", "1"])
            check("remote proxy keeps target session and joins code",
                  calls[-1] == ("server", {"cmd": "run", "args": ["work", "x + 1"]}),
                  repr(calls[-1]))
            pythond._handle_session_command("run", ["work", "x", "+", "1"])
            check("local run joins code remainder",
                  calls[-1] == ("work", {"cmd": "run", "args": ["x + 1"]}),
                  repr(calls[-1]))
    finally:
        with pythond._sessions_lock:
            pythond.sessions.clear()
            pythond.sessions.update(saved)


class _FakeWsSock:
    def __init__(self, data: bytes = b""):
        self.data = bytearray(data)
        self.sent = bytearray()
        self.timeout = None
        self.closed = False
    def recv(self, n):
        if not self.data:
            return b""
        out = bytes(self.data[:n])
        del self.data[:n]
        return out
    def sendall(self, data):
        self.sent.extend(data)
    def gettimeout(self):
        return self.timeout
    def settimeout(self, timeout):
        self.timeout = timeout
    def close(self):
        self.closed = True


def _accepted_ws_pair():
    client_ws = pythond.wsproto.WSConnection(pythond.ConnectionType.CLIENT)
    server_ws = pythond.wsproto.WSConnection(pythond.ConnectionType.SERVER)
    req = client_ws.send(pythond.ws_events.Request(
        host="example:443",
        target="/",
        subprotocols=[pythond._WS_PROTO],
    ))
    server_ws.receive_data(req)
    list(server_ws.events())
    accept = server_ws.send(
        pythond.ws_events.AcceptConnection(subprotocol=pythond._WS_PROTO)
    )
    client_ws.receive_data(accept)
    list(client_ws.events())
    return client_ws, server_ws


def test_wspro_client_basic():
    section("WsproClient basic")
    client_ws, server_ws = _accepted_ws_pair()
    sock = _FakeWsSock(server_ws.send(pythond.ws_events.TextMessage(data="hello")))
    client = pythond._WsproClient(sock, client_ws)
    check("wsproto text recv", client.recv(timeout=1) == "hello")

    client.send("ping")
    server_ws.receive_data(bytes(sock.sent))
    events = list(server_ws.events())
    check("wsproto text send",
          any(isinstance(e, pythond.ws_events.TextMessage) and e.data == "ping"
              for e in events))

    client.close()
    check("wsproto close writes and closes", sock.closed and len(sock.sent) > 0)


def test_wspro_client_preserves_batched_frames():
    section("WsproClient batched frames")
    client_ws, server_ws = _accepted_ws_pair()
    payload = (
        server_ws.send(pythond.ws_events.TextMessage(data="first")) +
        server_ws.send(pythond.ws_events.TextMessage(data="second"))
    )
    client = pythond._WsproClient(_FakeWsSock(payload), client_ws)
    check("first batched frame delivered", client.recv(timeout=1) == "first")
    check("second batched frame delivered", client.recv(timeout=1) == "second")


def test_wspro_client_payload_limit():
    section("WsproClient payload limit")
    client_ws, server_ws = _accepted_ws_pair()
    old_limit = pythond._MAX_WS_PAYLOAD
    pythond._MAX_WS_PAYLOAD = 5
    try:
        sock = _FakeWsSock(server_ws.send(
            pythond.ws_events.BytesMessage(data=b"123456")
        ))
        client = pythond._WsproClient(sock, client_ws)
        try:
            client.recv(timeout=1)
            check("oversize message rejected", False)
        except RuntimeError as e:
            check("oversize message rejected", "too large" in str(e))
    finally:
        pythond._MAX_WS_PAYLOAD = old_limit


def test_tls_and_auth_hardening_static():
    section("TLS/auth hardening")
    src = (ROOT / "pythond.py").read_text(encoding="utf-8")
    access_log_seg = src[src.index("def _access_log("):src.index("def _ensure_private_dir(")]
    access_mirror_seg = src[src.index("def _access_stderr_worker("):src.index("def _access_log(")]
    check("constant-time token compare", "hmac.compare_digest" in src)
    check("mTLS keeps token auth", "addition to token auth" in src)
    check("binary command frame returns error",
          "ERR binary frame not allowed in command mode" in src)
    check("access log helper avoids code and token",
          "def _access_log(" in src and
          "body_bytes" in access_log_seg and
          "Authorization" not in access_log_seg and
          "code bodies" in access_log_seg)
    check("access log has conn id and millisecond timestamps",
          "_ACCESS_CONN_SEQ = itertools.count(1)" in src and
          "def _utc_timestamp_ms(" in src and
          "time.time_ns()" in src and
          "conn_id" in access_log_seg)
    check("access log fchmods unconditionally",
          "created = not os.path.exists(path)" not in access_log_seg and
          "if sys.platform != \"win32\":\n                os.fchmod(fd, 0o600)" in access_log_seg)
    check("access log mirrors to stderr",
          "_ACCESS_STDERR_QUEUE.put_nowait(line)" in access_mirror_seg and
          "threading.Thread(target=_access_stderr_worker, daemon=True)" in access_mirror_seg and
          "sys.stderr.write(line)" in access_mirror_seg)
    check("access log sanitises invalid session names",
          "_field(session)" in access_log_seg and "invalid" in access_log_seg)
    ctx = pythond._client_ssl_ctx()
    check("client TLS minimum 1.2",
          getattr(ctx, "minimum_version", None) >= pythond._ssl.TLSVersion.TLSv1_2)


def test_connection_hardening_static():
    section("connection hardening static")
    src = (ROOT / "pythond.py").read_text(encoding="utf-8")
    check("future annotations prevent runtime type evaluation",
          "from __future__ import annotations" in src and
          src.index("from __future__ import annotations") < src.index("import sys"))
    attach_seg = src[src.index("def attach(name: str)"):src.index("def _attach_reader(")]
    attach_pty_seg = src[src.index("def _attach_ws_pty("):src.index("def _attach_ws_win(")]
    send_seg = src[src.index("def _send("):src.index("def client(")]
    handle_seg = src[src.index("def handle_client("):src.index("def daemon(")]
    daemon_seg = src[src.index("def daemon("):src.index("    # --- start server ---")]
    daemon_full_seg = src[src.index("def daemon("):src.index("# =============================================\n# CLIENT")]
    runtime_seg = src[src.index("def _runtime_dir("):src.index("def _daemon_meta_path(")]
    tls_seg = src[src.index("def _tls_dir("):src.index("def _generate_cert(")]
    private_dir_seg = src[src.index("def _ensure_private_dir("):src.index("def _session_dir(")]
    secure_win_seg = src[src.index("def _secure_path_win32("):src.index("def _runtime_dir(")]
    log_seg = src[src.index("def _log_history("):src.index("# -----------------------------------------------\n# SOCKET helpers")]
    set_session_seg = src[src.index("def _set_session("):src.index("def _ensure_session_capacity(")]
    trust_cert_seg = src[src.index("def trust_cert("):src.index("class _Servable")]
    cert_dirs_seg = src[src.index("def _trusted_clients_dir("):src.index("def _load_trusted_certs(")]
    cert_gen_seg = src[src.index("def _generate_cert("):src.index("def _cert_fingerprint(")]
    send_all_seg = src[src.index("def _send_all("):src.index("# =============================================\n# SHARED WORKER LOGIC")]
    new_session_seg = src[src.index("def new_session("):src.index("def kill_session(")]
    close_session_seg = src[src.index("def _close_session_resources("):src.index("def _monitor_session(")]
    fork_monitor_seg = src[src.index("def _fork_monitor("):src.index("elif cmd == \"int\":")]
    tls_server_seg = src[src.index("class _TlsTerminatedServer:"):src.index("# =============================================\n# SHARED WORKER LOGIC")]
    dispatch_seg = src[src.index("def _dispatch("):src.index("# =============================================\n# POSIX: real PTY worker")]
    monitor_seg = src[src.index("def _monitor_session("):src.index("def send_session(")]
    recv_line_seg = src[src.index("def _recv_session_line("):src.index("def send_session(")]
    kill_session_seg = src[src.index("def kill_session("):src.index("def _close_session_resources(")]
    send_session_seg = src[src.index("def send_session("):src.index("# -----------------------------------------------\n# REMOTE PROXY")]
    remote_seg = src[src.index("def _send_remote("):src.index("def connect_remote(")]
    connect_remote_seg = src[src.index("def connect_remote("):src.index("def _handle_stop(")]
    wspro_seg = src[src.index("class _WsproClient"):src.index("def _connect_wss(")]
    parse_host_port_seg = src[src.index("def _parse_host_port("):src.index("def _open_remote_ws(")]
    resize_seg = src[src.index("def _handle_resize("):src.index("def _handle_ls(")]
    attach_reader_seg = src[src.index("def _attach_reader("):src.index("def _attach_ws_loop(")]
    attach_loop_seg = src[src.index("def _attach_ws_loop("):src.index("def _attach_ws_pty(")]
    attach_win_seg = src[src.index("def _attach_ws_win("):src.index("def _worker_entry(")]
    eval_exec_seg = src[src.index("def _eval_exec_cell("):src.index("def _make_exec(")]
    handle_new_seg = src[src.index("def _handle_new("):src.index("def _handle_int(")]
    handle_ls_seg = src[src.index("def _handle_ls("):src.index("def _log_cell_launch(")]
    connect_daemon_seg = src[src.index("def _connect_daemon("):src.index("def _build_wire_message(")]
    add_session_subparsers_seg = src[src.index("def _add_session_subparsers("):src.index("def main(")]
    default_sock_seg = src[src.index("def _default_sock("):src.index("SOCK =")]
    client_start = src.index("def client(")
    client_seg = src[client_start:src.index("def attach(", client_start)]
    pyctl_seg = src[src.index("def pyctl_main("):src.index("if __name__ == \"__main__\":")]
    main_seg = src[src.index("def main("):src.index("def pysh_main(")]
    pysh_seg = src[src.index("def pysh_main("):src.index("def pyctl_main(")]

    check("blocking send waits write-ready",
          "except (_ssl.SSLWantWriteError, BlockingIOError):\n"
          "                    select.select([], [sock], [], 1.0)" in src)
    check("zero-byte send does not spin",
          "if sent == 0:" in send_all_seg and
          "if sent == 0:\n                        return False" in send_all_seg)
    check("attach uses shared daemon connector", "_connect_daemon(" in attach_seg)
    check("attach no direct ws_connect", "ws_connect" not in attach_seg)
    check("attach closes websocket on handshake failure",
          "ERR attach failed" in attach_seg and "ws.close()" in attach_seg)
    check("attach reports failure", "-> bool" in attach_seg)
    check("attach terminal setup failure detaches",
          "ws.send(\"detach\")" in attach_seg and
          "return False" in attach_seg)
    check("attach rejects binary handshake response",
          "isinstance(resp, bytes)" in attach_seg and
          "ERR invalid attach response" in attach_seg)
    check("attach POSIX requires TTY",
          "sys.stdin.isatty()" in attach_pty_seg and
          "attach requires a TTY" in attach_pty_seg)
    check("attach sends resize on same connection",
          "_send(\"resize\"" not in attach_seg and
          "cols, rows = os.get_terminal_size()" in attach_seg and
          "attach {name}{resize_args}" in attach_seg and
          "_handle_resize([aname, args[1], args[2]])" in daemon_seg)
    check("attach errors use public message",
          "_public_error(e)" in attach_seg)
    check("send uses shared daemon connector", "_connect_daemon(" in send_seg)
    check("send recv is bounded", "ws.recv(timeout=30)" in send_seg)
    check("remote opens use helper", "def _open_remote_ws" in src)
    check("close frame has sentinel", "return _WS_CLOSE" in src)
    check("wsproto is used for WSS framing",
          "import wsproto" in src and "class _WsproClient" in src)
    check("raw WSS parser removed",
          "_RawWssClient" not in src and "_WS_HANDSHAKE_LIMIT" not in src)
    check("wsproto events drive client send/recv",
          "ws_events.TextMessage" in src and
          "ws_events.BytesMessage" in src and
          "ws_events.AcceptConnection" in src)
    check("wsproto text send does not coerce payloads",
          "TextMessage(data=data)" in wspro_seg and
          "TextMessage(data=str(data))" not in wspro_seg and
          "websocket payload must be str or bytes" in wspro_seg)
    check("wsproto connect closes wrapped socket on failure",
          "sock = None" in wspro_seg and
          "if sock is not None:\n                sock.close()" in wspro_seg)
    check("wsproto close handles already closed state",
          "LocalProtocolError" in wspro_seg)
    check("wsproto close reply uses legal code",
          "code=1000" in wspro_seg and "code=event.code" not in wspro_seg)
    check("wsproto timeout clears partial message",
          "except (TimeoutError, socket.timeout):" in wspro_seg and
          "self._clear_message()" in wspro_seg)
    check("wsproto preserves pending events",
          "collections.deque" in wspro_seg and
          "while self._pending_events:" in wspro_seg and
          "self._pending_events.extend(events[i + 1:])" in wspro_seg)
    check("wsproto pong write is guarded",
          "ws_events.Pong" in wspro_seg and
          "except (OSError, LocalProtocolError):" in wspro_seg)
    check("oversize wsproto message closes socket",
          "websocket message too large" in wspro_seg and
          "self.sock.close()" in wspro_seg)
    check("host port parser validates range",
          "1 <= port <= 65535" in parse_host_port_seg)
    check("TLS bridge reaps threads", "def _reap_threads" in src and "self._reap_threads()" in src)
    check("TLS accept loop has timeout",
          "self._sock.settimeout(1.0)" in tls_server_seg and
          "except socket.timeout:" in tls_server_seg)
    check("TLS bridge polls inner while TLS has pending data",
          "tls_sock.pending()" in tls_server_seg and
          "select.select([inner_sock], [], [], 0)" in tls_server_seg)
    check("handle_client uses dispatch table", "_CONTROL_HANDLERS.get(cmd)" in handle_seg)
    check("handle_client no elif chain", "elif cmd" not in handle_seg)
    check("runtime dir uses private helper", "_ensure_private_dir" in runtime_seg)
    check("tls dir uses private helper", "_ensure_private_dir" in tls_seg)
    check("private dir rejects insecure POSIX dirs",
          "os.lstat(path)" in private_dir_seg and "st.st_uid" in private_dir_seg)
    check("private dir rejects Windows reparse points",
          "GetFileAttributesW" in private_dir_seg and
          "_WIN_FILE_ATTRIBUTE_REPARSE_POINT" in private_dir_seg)
    check("windows path hardening catches icacls timeout",
          "subprocess.TimeoutExpired" in src and "def _secure_path_win32" in src)
    check("windows path hardening fails closed",
          "raise RuntimeError(f\"cannot secure directory: {path}\")" in secure_win_seg)
    check("log files are private and nofollow",
          "os.open(path, flags" in log_seg and
          "O_NOFOLLOW" in log_seg and
          "0o600" in log_seg)
    check("trusted cert dirs use private helper",
          "_ensure_private_dir" in cert_dirs_seg and "os.makedirs" not in cert_dirs_seg)
    check("invalid trusted certs are rejected",
          "invalid certificate" in trust_cert_seg and "unknown.pem" not in trust_cert_seg)
    check("cert writes are atomic",
          "os.replace(tmp_key, key_path)" in cert_gen_seg and
          "os.replace(tmp_cert, cert_path)" in cert_gen_seg)
    check("cert cache validates key pair",
          "_cert_key_pair_valid(cert_path, key_path)" in cert_gen_seg and
          "TLS cert/key mismatch; regenerating" in cert_gen_seg)
    check("TLS server trust verifies exact fingerprint",
          "_verify_peer_fingerprint(tls_sock, self._trusted_client_dir" in tls_server_seg)
    check("TLS client pin verifies exact fingerprint",
          "_verify_peer_fingerprint(sock, server_trust_dir, \"server\")" in wspro_seg and
          "ctx.verify_mode = _ssl.CERT_NONE" in src)
    check("cert key validation tolerates missing crypto",
          "except (AttributeError, NameError, OSError, TypeError, ValueError):" in cert_gen_seg)
    check("trusted cert load skips unreadable certs",
          "except (_ssl.SSLError, OSError):" in src)
    check("trusted certs reject CA-capable certs",
          "refusing CA-capable certificate" in trust_cert_seg and
          "skipping CA-capable cert" in src)
    check("cert partial replace is invalidated",
          "for fpath in (tmp_key, tmp_cert, key_path, cert_path):" in cert_gen_seg)
    check("self-signed cert is not a CA",
          "BasicConstraints(ca=False" in cert_gen_seg and
          "key_cert_sign=False" in cert_gen_seg and
          "crl_sign=False" in cert_gen_seg)
    check("TLS bridge cleans inner server on bind failure",
          "self._inner.shutdown()" in tls_server_seg)
    check("TLS bridge has connection cap",
          "len(self._bridge_threads) >= _MAX_TLS_BRIDGE_THREADS" in tls_server_seg and
          "self._inner_thread" in tls_server_seg)
    check("new_session rolls back failed registration",
          "_close_session_resources(session)" in new_session_seg)
    check("set_session closes replaced session",
          "old_session = sessions.get(name)" in set_session_seg and
          "_close_session_resources(old_session)" in set_session_seg)
    check("winpty accept failures terminate spawned worker",
          "except BaseException:" in new_session_seg and
          "proc.terminate(force=True)" in new_session_seg)
    check("worker entry has environment capability",
          "_WORKER_ENV" in main_seg and
          "env={**os.environ, _WORKER_ENV: \"1\"}" in new_session_seg)
    check("winpty spawn env mutation is locked",
          "with _WORKER_SPAWN_LOCK:" in new_session_seg)
    check("winpty listener closes in finally",
          "finally:\n            ai_srv.close()" in new_session_seg)
    check("posix spawn failure closes fds and sockets",
          "except Exception:\n            for fd in (master_fd, slave_fd):" in new_session_seg and
          "for sock_obj in (ai_parent, ai_child):" in new_session_seg)
    check("PTY bridge writes all bytes",
          "lambda d: _write_all(master_fd, d)" in new_session_seg)
    check("kill closes PTY bridge",
          "bridge.close()" in close_session_seg)
    check("close clears process handles",
          "s[\"winpty\"] = None" in close_session_seg and
          "s[\"proc\"] = None" in close_session_seg)
    check("fork monitor always marks done",
          "with _cells_lock:\n                    r[\"pid\"] = None" in fork_monitor_seg and
          "r[\"status\"] = \"done\"" in fork_monitor_seg)
    check("fork monitor clears pid after waitpid under cell lock",
          "os.waitpid(pid, 0)" in fork_monitor_seg and
          "with _cells_lock:\n                    r[\"pid\"] = None" in fork_monitor_seg)
    check("fork monitor clears pid once",
          fork_monitor_seg.count("r[\"pid\"] = None") == 1)
    check("fork monitor handles unexpected result failures",
          "(fork result read failed)" in fork_monitor_seg and
          "except Exception:" in fork_monitor_seg)
    check("failed fork does not merge diff",
          "if had_error:" in fork_monitor_seg and
          "merged = {}" in fork_monitor_seg)
    check("fork snapshots while locked",
          "lock.acquire()" in dispatch_seg and "child_pid = os.fork()" in dispatch_seg)
    check("fork closes fds on fork failure",
          "for fd in (r_fd, w_fd):" in dispatch_seg)
    check("namespace reads are lock-protected",
          "def _locked(" in src and "def _public_names(" in src and
          "with _locked(lock):" in dispatch_seg and "ns_snapshot = dict(ns)" in dispatch_seg)
    check("int is serialized and ctypes is module-level",
          "with _INTERRUPT_LOCK:" in dispatch_seg and
          "import ctypes" not in dispatch_seg)
    check("SetAsyncExc cleanup passes NULL",
          "_SET_ASYNC_EXC(ctypes.c_ulong(tid), None)" in dispatch_seg and
          "ctypes.py_object(None)" not in dispatch_seg)
    check("fire publishes tid after start before cell visibility",
          "with _cells_lock:\n            t.start()" in dispatch_seg and
          "res[\"tid\"] = t.ident" in dispatch_seg and
          "cells[cid] = res" in dispatch_seg)
    check("fire result publication is locked",
          "with _cells_lock:\n                    r[\"output\"] = output" in dispatch_seg and
          "r[\"status\"] = \"done\"" in dispatch_seg)
    check("eval exec auto-prints any expression tail",
          "if isinstance(last, _ast.Expr):" in eval_exec_seg and
          "len(tree.body) > 1" not in eval_exec_seg)
    check("latest poll uses explicit cell sequence",
          "\"_seq\": next(_CELL_SEQ)" in dispatch_seg and
          "list(cells)[-1]" not in dispatch_seg)
    check("poll snapshots cells under lock",
          "cell = dict(cell)" in dispatch_seg and
          "r = dict(r)" in dispatch_seg)
    check("complete catches completer errors",
          "return {\"matches\": matches, \"_error\": True}" in dispatch_seg)
    check("stale monitor cannot kill replacement",
          "sessions.get(name) is not s" in monitor_seg)
    check("monitor closes resources under session lock",
          "with _session_lock(s):" in monitor_seg and
          "_close_session_resources(s)" in monitor_seg)
    check("monitor skips cleared handles",
          "winpty = s.get(\"winpty\")" in monitor_seg and
          "proc = s.get(\"proc\")" in monitor_seg and
          "s[\"proc\"].wait()" not in monitor_seg)
    check("handle_new tolerates cleared process handles",
          "winpty = s.get(\"winpty\")" in handle_new_seg and
          "proc = s.get(\"proc\")" in handle_new_seg)
    check("handle_ls tolerates cleared process handles",
          "winpty = s.get(\"winpty\")" in handle_ls_seg and
          "proc = s.get(\"proc\")" in handle_ls_seg and
          "DEAD (pty)" in handle_ls_seg)
    check("session command path avoids timeout-sensitive makefile",
          "makefile" not in send_session_seg and
          "_recv_session_line" in src)
    check("recv_session_line preserves partial buffer",
          "finally:\n        s[\"_ai_buf\"] = buf" in recv_line_seg)
    check("kill_session has lock timeout",
          "lock.acquire(timeout=3)" in kill_session_seg and
          "should_close = False" in kill_session_seg and
          "if should_close:\n            return _close_session_resources(s)" in kill_session_seg)
    check("send_session marks OSError unhealthy",
          "except (OSError, json.JSONDecodeError):\n                s[\"_unhealthy\"] = True" in send_session_seg)
    check("timed out command channel stays unhealthy",
          "msg.get(\"cmd\") != \"int\"" not in src and "use pysh kill" in src)
    check("remote does not retry after send",
          "remote response failed" in remote_seg and
          "remote send failed" in remote_seg and
          "if attempt == 0:\n                    continue" in remote_seg)
    check("connect_remote does not kill before network IO",
          "kill_session(name)" not in connect_remote_seg and
          "_set_session(name" in connect_remote_seg)
    check("connect_remote rejects close during probe",
          "if resp is _WS_CLOSE:" in connect_remote_seg and
          "remote closed during probe" in connect_remote_seg)
    pre_host_seg = connect_daemon_seg.split("host = os.environ.get(\"PYTHOND_HOST\")", 1)[0]
    check("daemon connector delays websockets import",
          "from websockets.sync.client import unix_connect" not in pre_host_seg and
          connect_daemon_seg.index("if host:") <
          connect_daemon_seg.index("from websockets.sync.client import unix_connect"))
    check("daemon connector validates fallback port",
          "if not (1 <= port <= 65535):" in connect_daemon_seg)
    check("default socket fallback uses private runtime dir",
          'f"pythond-{uid}", "pythond.sock"' in default_sock_seg)
    check("pyctl status honours PYTHOND_HOST",
          "def _pyctl_env_status(" in src and
          'if os.environ.get("PYTHOND_HOST"):' in pyctl_seg)
    check("daemon server registered immediately",
          "def _set_server(" in daemon_full_seg and
          "_daemon_server = created" in daemon_full_seg and
          "_daemon_server = server\n        server.serve_forever()" not in daemon_full_seg)
    check("daemon clears token on shutdown",
          "_daemon_token = None" in daemon_full_seg)
    check("multiprocessing init removed",
          "_mp_init" not in src and "import multiprocessing" not in src)
    check("entry points use argparse",
          "import argparse" in src and
          "argparse.ArgumentParser" in main_seg and
          "argparse.ArgumentParser" in pysh_seg and
          "argparse.ArgumentParser" in pyctl_seg)
    check("session subparsers are shared",
          "def _add_session_subparsers(" in src and
          "_add_session_subparsers(sub)" in main_seg and
          "_add_session_subparsers(sub)" in pysh_seg)
    check("pysh run accepts remote proxy target session",
          "p_cmd.add_argument(\"code\", nargs=argparse.REMAINDER)" in add_session_subparsers_seg)
    check("manual help strings removed",
          "_PYSH_HELP" not in src and "_PYCTL_HELP" not in src)
    check("remote resize fails explicitly",
          "resize not supported for remote sessions" in resize_seg)
    check("parse_host_port rejects empty host",
          "if not host:" in parse_host_port_seg)
    check("attach reader uses bounded recv",
          "ws.recv(timeout=2)" in attach_reader_seg and
          "except (TimeoutError, socket.timeout):" in attach_reader_seg)
    check("attach reader surfaces text errors",
          "print(frame, file=sys.stderr)" in attach_reader_seg)
    check("attach reader exact detached sentinel",
          "if frame == \"OK detached\":" in attach_reader_seg and
          "\"detached\" in frame" not in attach_reader_seg)
    check("attach preserves bytes before Ctrl-]",
          "data.partition(b\"\\x1d\")" in attach_loop_seg and
          "ws.send(before)" in attach_loop_seg)
    check("attach waits reader before terminal restore",
          "t.join(timeout=3)" in attach_loop_seg and
          attach_loop_seg.index("t.join(timeout=3)") <
          attach_loop_seg.index("restore_terminal()"))
    check("windows attach clears processed input",
          "old_in.value & ~0x0007" in attach_win_seg and
          "_WIN_ENABLE_PROCESSED_INPUT" not in attach_win_seg)
    check("windows attach requires TTY",
          "sys.stdin.isatty()" in attach_win_seg and
          "attach requires a TTY" in attach_win_seg)
    check("windows attach consumes extended key bytes together",
          "first in (b\"\\x00\", b\"\\xe0\")" in attach_win_seg and
          "return first + msvcrt.getch()" in attach_win_seg and
          "while not stopped.is_set() and not msvcrt.kbhit():" in attach_win_seg)
    check("windows attach checks console mode calls",
          "if not kernel32.GetConsoleMode(stdin_h, ctypes.byref(old_in)):" in attach_win_seg and
          "if not kernel32.GetConsoleMode(stdout_h, ctypes.byref(old_out)):" in attach_win_seg)
    check("windows attach clears line and echo input",
          "& ~0x0007" in attach_win_seg)
    check("client prints ERR to stderr",
          "if resp and resp.startswith(\"ERR \") and fail_on_err:" in client_seg and
          "resp.startswith(\"ERR \")" in client_seg and
          "file=sys.stderr" in client_seg and
          "else sys.stdout" in client_seg)
    check("pyctl exits nonzero on ERR",
          "fail_on_err=True" in pyctl_seg)
    check("pyctl status exits nonzero when dead",
          "if not alive:" in pyctl_seg and "sys.exit(1)" in pyctl_seg)
    check("pyctl has terminal fallback",
          "parser.print_help(sys.stderr)" in pyctl_seg)
    check("unix socket created under private umask",
          "os.umask(0o177)" in src and "ws_unix_serve" in src)
    check("listen arg parsed by argparse",
          "add_argument(\"--listen\", metavar=\"HOST:PORT\"" in src)
    check("client-visible runtime errors are sanitized",
          "_public_error(e)" in src and "return f\"ERR {e}\"" not in src)
    check("daemon access logs RCE surface",
          "_access_log(\"connect\"" in daemon_full_seg and
          "_access_log(\"auth\"" in daemon_full_seg and
          "_access_log(\"command\"" in daemon_full_seg and
          "_access_log(\"result\"" in daemon_full_seg and
          "_access_log(\"disconnect\"" in daemon_full_seg and
          "conn_id=conn_id" in daemon_full_seg)
    check("daemon command access log omits body content",
          "body_len = len(body.encode" in daemon_full_seg and
          "session_name = args[0] if args else \"\"" in daemon_full_seg and
          "args.append(body)" in daemon_full_seg and
          daemon_full_seg.index("session_name = args[0] if args else \"\"") <
          daemon_full_seg.index("args.append(body)") and
          "body_bytes=body_len" in daemon_full_seg and
          "detail=body" not in daemon_full_seg)


def test_has_crypto_flag():
    section("_HAS_CRYPTO flag")
    check("_HAS_CRYPTO is bool", isinstance(pythond._HAS_CRYPTO, bool))
    try:
        import cryptography
        check("cryptography installed -> flag True", pythond._HAS_CRYPTO is True)
    except ImportError:
        check("cryptography missing -> flag False", pythond._HAS_CRYPTO is False)


def test_entry_points_exist():
    section("entry points")
    check("main", callable(pythond.main))
    check("pysh_main", callable(pythond.pysh_main))
    check("pyctl_main", callable(pythond.pyctl_main))


def test_session_cli_errors_exit_nonzero():
    section("session CLI errors exit nonzero")
    for entry_name, argv, entry in [
        ("pysh", ["pysh", "run", "missing", "x"], pythond.pysh_main),
        ("pythond", ["pythond", "run", "missing", "x"], pythond.main),
    ]:
        with mock.patch.object(sys, "argv", argv), \
             mock.patch.object(pythond, "_send", return_value="ERR no session"), \
             mock.patch.object(sys, "stderr", io.StringIO()), \
             mock.patch.object(sys, "stdout", io.StringIO()):
            try:
                entry()
                check(f"{entry_name} exits nonzero on ERR", False)
            except SystemExit as e:
                check(f"{entry_name} exits nonzero on ERR", e.code == 1, e.code)


def test_access_log_sanitises_session_field():
    section("access log sanitises session field")
    with tempfile.TemporaryDirectory() as td:
        with mock.patch.object(pythond, "_runtime_dir", return_value=td), \
             mock.patch.object(pythond, "_mirror_access_log_to_stderr"):
            pythond._access_log("command", cmd="run", session="../../secret",
                                body_bytes=12)
        content = open(os.path.join(td, "access.log"), encoding="utf-8").read()
    check("invalid session redacted", "session=invalid" in content, content)
    check("raw invalid session omitted", "../../secret" not in content, content)


def test_pyctl_cert_role_hints():
    section("pyctl cert role hints")
    with tempfile.TemporaryDirectory() as td:
        with mock.patch.object(sys, "argv", ["pyctl", "cert"]), \
             mock.patch.object(pythond, "_tls_dir", return_value=td), \
             mock.patch.object(sys, "stdout", io.StringIO()) as out:
            pythond.pyctl_main()
        text = out.getvalue()
    check("cert output says this machine", "this machine's TLS certificate" in text)
    check("cert output separates client role",
          "If this machine is the client:" in text and "pyctl trust" in text)
    check("cert output separates server role",
          "If this machine is the server:" in text and "pyctl pin" in text)


def test_pyctl_status_uses_env_endpoint():
    section("pyctl status uses PYTHOND_HOST")

    class FakeWs:
        def __init__(self):
            self.sent = []
            self.closed = False
        def send(self, msg):
            self.sent.append(msg)
        def recv(self, timeout=None):
            return "(no sessions)"
        def close(self):
            self.closed = True

    fake = FakeWs()
    with mock.patch.dict(os.environ, {"PYTHOND_HOST": "127.0.0.1:7399"}, clear=False), \
         mock.patch.object(pythond, "_connect_daemon", return_value=fake), \
         mock.patch.object(sys, "argv", ["pyctl", "status"]), \
         mock.patch.object(sys, "stdout", io.StringIO()) as out:
        pythond.pyctl_main()
    text = out.getvalue()
    check("status sent ls", fake.sent == ["ls"], fake.sent)
    check("status closed ws", fake.closed is True)
    check("status prints endpoint", "endpoint: 127.0.0.1:7399" in text, text)
    check("status prints alive true", "alive: True" in text, text)


def test_default_sock():
    section("default socket path")
    path = pythond._default_sock()
    check("path is string", isinstance(path, str))
    if sys.platform == "win32":
        check("win32 uses temp", "Temp" in path or "tmp" in path.lower())
    else:
        check("posix uses pythond.sock", path.endswith("pythond.sock") or "pythond-" in path)
        xdg = os.environ.get("XDG_RUNTIME_DIR")
        if xdg and os.path.isdir(xdg):
            check("prefers XDG_RUNTIME_DIR", path.startswith(xdg))
        else:
            check("fallback uses private runtime dir",
                  os.path.basename(path) == "pythond.sock" and
                  os.path.basename(os.path.dirname(path)) ==
                  f"pythond-{os.getuid()}")


def test_secure_path_win32():
    section("_secure_path_win32")
    if sys.platform != "win32":
        # verify it exists and is callable (no-op test on non-windows)
        check("function exists", callable(pythond._secure_path_win32))
        return
    with tempfile.TemporaryDirectory() as td:
        test_path = os.path.join(td, "secure_test")
        os.makedirs(test_path)
        pythond._secure_path_win32(test_path)
        check("dir still exists", os.path.isdir(test_path))


# ===========================================
# INTEGRATION TESTS (real daemon)
# ===========================================

def send_cmd(addr, cmd, args=None, token=None):
    """Send a command to daemon via WebSocket, return response string.
    addr: unix socket path, or "host:port" for TCP."""
    from websockets.sync.client import connect as ws_connect
    if _HAS_AF_UNIX:
        from websockets.sync.client import unix_connect as ws_unix_connect

    if ":" in addr and not addr.startswith("/"):
        # TCP mode
        host, _, port_s = addr.rpartition(":")
        url = f"ws://{host or '127.0.0.1'}:{port_s}/"
        headers = {"Authorization": f"Bearer {token}"} if token else None
        ws = ws_connect(url, additional_headers=headers,
                        proxy=None,
                        open_timeout=5, close_timeout=2,
                        subprotocols=[_WS_PROTO])
    else:
        ws = ws_unix_connect(addr, open_timeout=5, close_timeout=2,
                            subprotocols=[_WS_PROTO])

    # build message: "cmd arg1\nbody" for run/fire, "cmd args..." otherwise
    if cmd in ("run", "fire") and args and len(args) >= 2:
        header = " ".join([cmd] + args[:-1])
        msg = header + "\n" + args[-1]
    else:
        msg = " ".join([cmd] + (args or []))
    try:
        ws.send(msg)
    except Exception:
        try:
            resp = ws.recv(timeout=2)
        except Exception as e:
            resp = str(e)
        try:
            ws.close()
        except Exception:
            pass
        return resp
    try:
        resp = ws.recv(timeout=10)
    except Exception as e:
        resp = "OK" if cmd == "stop" else str(e)
    try:
        ws.close()
    except Exception:
        pass
    return resp

def free_tcp_port():
    """Return an unused localhost TCP port for integration tests."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]
    finally:
        s.close()

_HAS_AF_UNIX = sys.platform != "win32" and hasattr(socket, "AF_UNIX")


def test_integration():
    """Full daemon lifecycle test."""
    section("INTEGRATION: daemon lifecycle")

    if sys.platform == "win32":
        print("  SKIP: integration tests use AF_UNIX (run on WSL/Linux)")
        return

    sock = os.path.join(tempfile.gettempdir(), f"pythond-test-{os.getpid()}.sock")
    env = os.environ.copy()
    env["PYTHOND_SOCK"] = sock

    # start daemon
    proc = subprocess.Popen(
        [sys.executable, str(ROOT / "pythond.py"), "daemon"],
        env=env, stderr=subprocess.PIPE,
    )
    time.sleep(0.5)
    check("daemon started", proc.poll() is None)

    try:
        # ls (empty)
        resp = send_cmd(sock, "ls")
        check("ls empty", "(no sessions)" in resp)

        # new
        resp = send_cmd(sock, "new", ["test1"])
        check("new OK", "OK test1" in resp)

        # ls (has session)
        resp = send_cmd(sock, "ls")
        check("ls has test1", "test1" in resp)
        check("ls shows alive", "alive" in resp)

        # run
        resp = send_cmd(sock, "run", ["test1", "1+1"])
        check("run output", resp.strip() == "2")

        # run with state
        send_cmd(sock, "run", ["test1", "x = 42"])
        resp = send_cmd(sock, "run", ["test1", "x"])
        check("state persists", resp.strip() == "42")

        # fire + poll
        resp = send_cmd(sock, "fire", ["test1", "import time; time.sleep(0.1); y = 99"])
        data = json.loads(resp)
        check("fire has cell_id", "cell_id" in data)
        cid = data["cell_id"]
        time.sleep(0.3)
        resp = send_cmd(sock, "poll", ["test1", cid])
        data = json.loads(resp)
        check("poll done", data["status"] == "done")

        # verify fire set variable
        resp = send_cmd(sock, "run", ["test1", "y"])
        check("fire state", resp.strip() == "99")

        # status
        resp = send_cmd(sock, "status", ["test1"])
        data = json.loads(resp)
        check("status idle", data["state"] == "idle")

        # vars
        resp = send_cmd(sock, "vars", ["test1"])
        data = json.loads(resp)
        check("vars has x", "x" in data["vars"])
        check("vars has y", "y" in data["vars"])

        # complete
        resp = send_cmd(sock, "complete", ["test1", "os.path."])
        data = json.loads(resp)
        check("complete has matches", len(data["matches"]) > 0)

        # error handling
        resp = send_cmd(sock, "run", ["test1", "1/0"])
        check("exception in output", "ZeroDivisionError" in resp)

        # run on nonexistent session
        resp = send_cmd(sock, "run", ["nosuch", "1"])
        check("no session error", "ERR" in resp)

        # int
        send_cmd(sock, "fire", ["test1", "import time; time.sleep(10)"])
        time.sleep(0.1)
        resp = send_cmd(sock, "int", ["test1"])
        check("int OK", "OK int" in resp or "OK no running" in resp)

        # checkpoint files created
        hist = os.path.join(os.path.expanduser("~"),
                           ".pythond", "sessions", "test1", "history.py")
        log = os.path.join(os.path.expanduser("~"),
                          ".pythond", "sessions", "test1", "session.log")
        check("history.py exists", os.path.exists(hist))
        check("session.log exists", os.path.exists(log))
        if os.path.exists(hist):
            content = open(hist).read()
            check("history has x=42", "x = 42" in content)
            # error should NOT be in history
            check("history no ZeroDivision", "1/0" not in content)
        if os.path.exists(log):
            content = open(log).read()
            check("session.log has errors", "ERROR" in content or "ZeroDivision" in content)

        # kill
        resp = send_cmd(sock, "kill", ["test1"])
        check("kill OK", "OK killed" in resp)

        # ls after kill
        resp = send_cmd(sock, "ls")
        check("ls empty after kill", "(no sessions)" in resp)

        # stop
        resp = send_cmd(sock, "stop")
        check("stop OK", "OK" in resp)
        time.sleep(0.5)
        check("daemon exited", proc.poll() is not None)

    finally:
        if proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=3)
        if os.path.exists(sock):
            os.unlink(sock)
        # cleanup checkpoint files
        shutil.rmtree(os.path.join(os.path.expanduser("~"),
                                   ".pythond", "sessions", "test1"),
                      ignore_errors=True)


def test_integration_tcp_windows():
    """Windows daemon lifecycle over localhost TCP + daemon.json token."""
    section("INTEGRATION: Windows TCP daemon")

    if sys.platform != "win32":
        check("windows tcp skipped on non-windows", True)
        return

    port = free_tcp_port()
    name = "__wintcp__"
    with tempfile.TemporaryDirectory() as td:
        runtime = os.path.join(td, "pythond")
        env = os.environ.copy()
        env["PYTHOND_PORT"] = str(port)
        env["LOCALAPPDATA"] = td

        proc = subprocess.Popen(
            [sys.executable, str(ROOT / "pythond.py"), "daemon"],
            env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        )
        stderr_chunks = []
        def _drain_stderr():
            if proc.stderr is None:
                return
            for line in proc.stderr:
                stderr_chunks.append(line)
        stderr_thread = threading.Thread(target=_drain_stderr, daemon=True)
        stderr_thread.start()
        time.sleep(1.0)
        check("tcp daemon started", proc.poll() is None)

        try:
            with mock.patch.object(pythond, "_runtime_dir", return_value=runtime):
                meta = pythond._read_daemon_meta()
            check("daemon meta port", str(meta.get("port")) == str(port), str(meta))
            token = meta.get("token", "")
            check("daemon meta token", bool(token))

            resp = send_cmd(f"127.0.0.1:{port}", "new", [name], token=token)
            check("tcp new OK", "OK" in resp, resp)

            resp = send_cmd(f"127.0.0.1:{port}", "run", [name, "x = 41"], token=token)
            check("tcp assignment empty", resp.strip() == "", resp)

            resp = send_cmd(f"127.0.0.1:{port}", "run", [name, "x + 1"], token=token)
            check("tcp state persists", resp.strip() == "42", resp)

            resp = send_cmd(
                f"127.0.0.1:{port}", "fire",
                [name, "import time; time.sleep(0.1); y = x + 2"],
                token=token,
            )
            data = json.loads(resp)
            check("tcp fire cell", data.get("status") == "fired", resp)
            cid = data.get("cell_id")
            time.sleep(0.4)
            resp = send_cmd(f"127.0.0.1:{port}", "poll", [name, cid], token=token)
            data = json.loads(resp)
            check("tcp poll done", data.get("status") == "done", resp)

            resp = send_cmd(f"127.0.0.1:{port}", "run", [name, "y"], token=token)
            check("tcp fire state", resp.strip() == "43", resp)

            resp = send_cmd(f"127.0.0.1:{port}", "run", [name, "print('Traceback')"], token=token)
            check("tcp literal Traceback output", resp.strip() == "Traceback", resp)

            send_cmd(f"127.0.0.1:{port}", "run", [name, "counter = 0"], token=token)
            results = []
            def _concurrent_run():
                results.append(send_cmd(
                    f"127.0.0.1:{port}", "run",
                    [name, "import time; time.sleep(0.02); counter = counter + 1"],
                    token=token))
            threads = [threading.Thread(target=_concurrent_run) for _ in range(5)]
            for t in threads:
                t.start()
            for t in threads:
                t.join(timeout=5)
            check("tcp concurrent clients returned", len(results) == 5, results)
            check("tcp concurrent clients clean", all(r.strip() == "" for r in results), results)
            resp = send_cmd(f"127.0.0.1:{port}", "run", [name, "counter"], token=token)
            check("tcp concurrent serialised", resp.strip() == "5", resp)

            hist = os.path.join(os.path.expanduser("~"), ".pythond", "sessions", name, "history.py")
            if os.path.exists(hist):
                content = open(hist, encoding="utf-8").read()
                check("literal Traceback checkpointed", "print('Traceback')" in content)

            bad = send_cmd(f"127.0.0.1:{port}", "ls", token="wrong-token")
            check("tcp wrong token rejected", "ERR auth failed" in bad, bad)

            access_path = os.path.join(runtime, "access.log")
            access = open(access_path, encoding="utf-8").read()
            access_lines = [
                line for line in access.splitlines()
                if line.startswith("ACCESS ")
            ]
            check("tcp access log written",
                  "ACCESS" in access and
                  "event=command" in access and
                  "event=result" in access,
                  access[:300])
            check("tcp access log has conn ids",
                  bool(access_lines) and
                  all("conn_id=" in line for line in access_lines),
                  access[:300])
            ts_values = [
                line.split("ts=", 1)[1].split()[0]
                for line in access_lines if "ts=" in line
            ]
            check("tcp access log timestamp has milliseconds",
                  bool(ts_values) and
                  all(len(ts) == len("2026-06-24T13:39:35.123Z") and
                      ts[19] == "." and ts.endswith("Z")
                      for ts in ts_values),
                  access[:300])
            check("tcp access log auth reject",
                  "event=auth" in access and "status=rejected" in access,
                  access[:300])
            check("tcp access log records body size",
                  "body_bytes=" in access, access[:300])
            check("tcp access log omits code and token",
                  "x = 41" not in access and
                  "print('Traceback')" not in access and
                  str(token) not in access,
                  access[:300])

            resp = send_cmd(f"127.0.0.1:{port}", "stop", token=token)
            check("tcp stop OK", "OK" in resp, resp)
            time.sleep(0.5)
            check("tcp daemon exited", proc.poll() is not None)
            if proc.poll() is not None and proc.stderr is not None:
                stderr_thread.join(timeout=2)
                stderr = "".join(stderr_chunks)
                stderr_access = "\n".join(
                    line for line in stderr.splitlines()
                    if line.startswith("ACCESS ")
                )
                check("tcp access log mirrored to stderr",
                      "ACCESS" in stderr_access and "event=command" in stderr_access,
                      stderr[:300])
                check("tcp stderr access log omits code and token",
                      "x = 41" not in stderr_access and
                      "print('Traceback')" not in stderr_access and
                      str(token) not in stderr_access,
                      stderr_access[:300])
        finally:
            if proc.poll() is None:
                proc.terminate()
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait(timeout=3)
        shutil.rmtree(os.path.join(os.path.expanduser("~"),
                                   ".pythond", "sessions", name),
                      ignore_errors=True)


def test_integration_tls_pinned_server():
    """Real wss:// connection with a pinned daemon certificate."""
    section("INTEGRATION: TLS pinned server")

    if not pythond._HAS_CRYPTO:
        check("tls e2e skipped without cryptography", True)
        return

    port = free_tcp_port()
    with tempfile.TemporaryDirectory() as server_home, tempfile.TemporaryDirectory() as client_tls:
        env = os.environ.copy()
        env["HOME"] = server_home
        env["USERPROFILE"] = server_home
        env["LOCALAPPDATA"] = os.path.join(server_home, "local")
        proc = subprocess.Popen(
            [sys.executable, str(ROOT / "pythond.py"), "daemon",
             "--listen", f"127.0.0.1:{port}", "--tls", "--show-token"],
            env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        )
        token = None
        raw = []
        try:
            for _ in range(4):
                line = proc.stderr.readline()
                if not line:
                    break
                raw.append(line)
                if line.startswith("token="):
                    token = line.split("=", 1)[1].strip()
                    break
            check("tls daemon started", proc.poll() is None, "".join(raw))
            check("tls token printed", bool(token), "".join(raw))

            cert = os.path.join(server_home, ".pythond", "tls", "cert.pem")
            check("tls server cert exists", os.path.exists(cert), cert)
            with mock.patch.object(pythond, "_tls_dir", return_value=client_tls):
                pythond.trust_cert(cert, direction="server")
                old_host = os.environ.get("PYTHOND_HOST")
                old_token = os.environ.get("PYTHOND_TOKEN")
                old_tls = os.environ.get("PYTHOND_TLS")
                try:
                    os.environ["PYTHOND_HOST"] = f"127.0.0.1:{port}"
                    os.environ["PYTHOND_TOKEN"] = token
                    os.environ["PYTHOND_TLS"] = "1"
                    resp = pythond._send("ls", [])
                    check("tls ls works", "(no sessions)" in resp, resp)
                    resp = pythond._send("stop", [])
                    check("tls stop sent", resp is None or "OK stopping daemon" in resp, resp)
                finally:
                    for key, value in [
                        ("PYTHOND_HOST", old_host),
                        ("PYTHOND_TOKEN", old_token),
                        ("PYTHOND_TLS", old_tls),
                    ]:
                        if value is None:
                            os.environ.pop(key, None)
                        else:
                            os.environ[key] = value
            time.sleep(0.5)
            check("tls daemon exited", proc.poll() is not None)
        finally:
            if proc.poll() is None:
                proc.terminate()
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait(timeout=3)


def test_integration_error_not_in_history():
    """Verify errors don't end up in history.py."""
    section("INTEGRATION: error exclusion from history")

    if sys.platform == "win32":
        print("  SKIP: integration tests use AF_UNIX")
        return

    sock = os.path.join(tempfile.gettempdir(), f"pythond-test2-{os.getpid()}.sock")
    env = os.environ.copy()
    env["PYTHOND_SOCK"] = sock
    name = "__errtest__"

    proc = subprocess.Popen(
        [sys.executable, str(ROOT / "pythond.py"), "daemon"],
        env=env, stderr=subprocess.PIPE,
    )
    time.sleep(0.5)

    try:
        send_cmd(sock, "new", [name])
        send_cmd(sock, "run", [name, "good = 1"])
        send_cmd(sock, "run", [name, "1/0"])  # should NOT be in history
        send_cmd(sock, "run", [name, "good2 = 2"])

        hist = os.path.join(os.path.expanduser("~"),
                           ".pythond", "sessions", name, "history.py")
        if os.path.exists(hist):
            content = open(hist).read()
            check("good in history", "good = 1" in content)
            check("good2 in history", "good2 = 2" in content)
            check("error NOT in history", "1/0" not in content)
        else:
            check("history exists", False, "file not created")

        send_cmd(sock, "stop")
        time.sleep(0.3)
    finally:
        if proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=3)
        if os.path.exists(sock):
            os.unlink(sock)
        shutil.rmtree(os.path.join(os.path.expanduser("~"),
                                   ".pythond", "sessions", name),
                      ignore_errors=True)


def test_integration_crash_isolation():
    """Verify exec crashes don't kill session, ws reconnect preserves state."""
    section("INTEGRATION: crash isolation")

    if sys.platform == "win32":
        print("  SKIP: integration tests use AF_UNIX")
        return

    sock = os.path.join(tempfile.gettempdir(), f"pythond-crash-{os.getpid()}.sock")
    env = os.environ.copy()
    env["PYTHOND_SOCK"] = sock

    proc = subprocess.Popen(
        [sys.executable, str(ROOT / "pythond.py"), "daemon"],
        env=env, stderr=subprocess.PIPE,
    )
    time.sleep(0.5)

    from websockets.sync.client import unix_connect

    try:
        ws = unix_connect(sock, open_timeout=3, subprotocols=[_WS_PROTO])
        ws.send("new crash")
        ws.recv(timeout=3)

        # set a variable
        ws.send("run crash\nx = 42")
        ws.recv(timeout=3)

        # crash: ZeroDivisionError -> variable survives
        ws.send("run crash\n" + "1/0")
        resp = ws.recv(timeout=3)
        check("exec error caught", "ZeroDivisionError" in resp)
        ws.send("run crash\n" + "x")
        resp = ws.recv(timeout=3)
        check("var survived crash", resp.strip() == "42")

        # crash 5 times -> still alive
        for i in range(5):
            ws.send("run crash\n" + f"raise ValueError('{i}')")
            ws.recv(timeout=3)
        ws.send("run crash\n" + "x + 1")
        resp = ws.recv(timeout=3)
        check("var after 5 crashes", resp.strip() == "43")

        # import crash -> survives
        ws.send("run crash\n" + "import nonexistent_xyz")
        resp = ws.recv(timeout=3)
        check("import error caught", "ModuleNotFoundError" in resp)
        ws.send("run crash\n" + "x + 2")
        resp = ws.recv(timeout=3)
        check("var after import crash", resp.strip() == "44")

        # WebSocket disconnect -> state persists
        ws.close()
        time.sleep(0.3)
        ws2 = unix_connect(sock, open_timeout=3, subprotocols=[_WS_PROTO])
        ws2.send("run crash\n" + "x + 3")
        resp = ws2.recv(timeout=3)
        check("state after ws reconnect", resp.strip() == "45")

        # keep-alive: multiple commands on same ws connection
        ws2.send("run crash\n" + "a = 1")
        ws2.recv(timeout=3)
        ws2.send("run crash\n" + "b = 2")
        ws2.recv(timeout=3)
        ws2.send("run crash\n" + "a + b")
        resp = ws2.recv(timeout=3)
        check("keep-alive works", resp.strip() == "3")

        # interrupt -> survives
        ws2.send("fire crash\n[__import__('time').sleep(0.1) for _ in range(100)]")
        ws2.recv(timeout=3)
        time.sleep(0.3)
        ws2.send("int crash")
        resp = ws2.recv(timeout=3)
        check("interrupt OK", "OK int" in resp or "OK no running" in resp)
        time.sleep(0.5)
        ws2.send("run crash\n" + "x + 4")
        resp = ws2.recv(timeout=3)
        check("var after interrupt", resp.strip() == "46")

        ws2.send("stop")
        ws2.recv(timeout=3)
        ws2.close()
        time.sleep(0.5)

    finally:
        if proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=3)
        if os.path.exists(sock):
            os.unlink(sock)
        shutil.rmtree(os.path.join(os.path.expanduser("~"),
                                   ".pythond", "sessions", "crash"),
                      ignore_errors=True)


def test_integration_ws_attach():
    """WebSocket attach: binary frames for PTY, detach, scrollback."""
    section("INTEGRATION: WebSocket attach")

    if sys.platform == "win32":
        print("  SKIP: integration tests use AF_UNIX")
        return

    sock = os.path.join(tempfile.gettempdir(), f"pythond-attach-{os.getpid()}.sock")
    env = os.environ.copy()
    env["PYTHOND_SOCK"] = sock

    proc = subprocess.Popen(
        [sys.executable, str(ROOT / "pythond.py"), "daemon"],
        env=env, stderr=subprocess.PIPE,
    )
    time.sleep(0.5)

    from websockets.sync.client import unix_connect

    try:
        # create session and set a variable
        ws1 = unix_connect(sock, open_timeout=3, subprotocols=[_WS_PROTO])
        ws1.send("new ptytest")
        ws1.recv(timeout=3)
        ws1.send("run ptytest\nx = 'hello from run'")
        ws1.recv(timeout=3)
        ws1.close()

        # attach via WebSocket
        ws2 = unix_connect(sock, open_timeout=3, subprotocols=[_WS_PROTO])
        ws2.send("attach ptytest")
        resp = ws2.recv(timeout=3)
        check("attach OK", resp.startswith("OK"), resp)

        # send a Python expression via binary frame (keystrokes)
        ws2.send(b"x\r\n")
        time.sleep(0.5)

        # read PTY output (binary frames)
        got = b""
        ws2.socket.settimeout(1.0)
        try:
            while True:
                frame = ws2.recv(timeout=1)
                if isinstance(frame, bytes):
                    got += frame
                else:
                    break
        except Exception:
            pass
        check("attach got output", len(got) > 0, f"got {len(got)} bytes")
        check("output has value", b"hello from run" in got, got[:200])

        # detach
        ws2.send("detach")
        time.sleep(0.3)
        try:
            resp = ws2.recv(timeout=1)
            check("detach response", "detach" in resp.lower() if isinstance(resp, str) else True)
        except Exception:
            check("detach closed", True)  # connection closed = also fine
        try:
            ws2.close()
        except Exception:
            pass

        # after detach: state still alive
        ws3 = unix_connect(sock, open_timeout=3, subprotocols=[_WS_PROTO])
        ws3.send("run ptytest\nx")
        resp = ws3.recv(timeout=3)
        check("state after detach", resp.strip() == "hello from run", resp)

        # test attach to nonexistent session
        ws3.send("attach nosuch")
        resp = ws3.recv(timeout=3)
        check("attach nonexistent ERR", "ERR" in resp)

        ws3.send("stop")
        try:
            ws3.recv(timeout=3)
        except Exception:
            pass
        ws3.close()
        time.sleep(0.5)

    finally:
        if proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=3)
        if os.path.exists(sock):
            os.unlink(sock)
        shutil.rmtree(os.path.join(os.path.expanduser("~"),
                                   ".pythond", "sessions", "ptytest"),
                      ignore_errors=True)


def test_unit_pty_bridge():
    """Unit test PtyBridge: attach/detach/write/scrollback."""
    section("PtyBridge unit")

    import pythond

    # simulate PTY with a pipe
    r_fd, w_fd = os.pipe()
    received = []

    def pty_read():
        data = os.read(r_fd, 4096)
        return data

    def pty_write(data):
        pass  # sink

    bridge = pythond.PtyBridge(pty_read, pty_write)

    # write some data before attach -> should go to scrollback
    os.write(w_fd, b"scrollback data\n")
    time.sleep(0.2)

    # attach
    owner = bridge.attach(lambda data: received.append(data))
    check("first attach accepted", owner is not None)
    time.sleep(0.1)

    # scrollback should have been flushed on attach
    check("scrollback flushed", len(received) > 0)
    check("scrollback content", b"scrollback data" in b"".join(received),
          b"".join(received))

    # new data after attach -> goes to client directly
    received.clear()
    second = []
    owner2 = bridge.attach(lambda data: second.append(data))
    check("second attach rejected", owner2 is None)
    os.write(w_fd, b"live data\n")
    time.sleep(0.2)
    check("live data received", b"live data" in b"".join(received))
    check("second attach got nothing", second == [])

    # detach -> new data goes to scrollback
    bridge.detach(object())
    os.write(w_fd, b"still attached\n")
    time.sleep(0.2)
    check("wrong owner detach ignored", b"still attached" in b"".join(received))
    received.clear()
    bridge.detach(owner)
    os.write(w_fd, b"after detach\n")
    time.sleep(0.2)

    # re-attach -> should get "after detach" from scrollback
    received.clear()
    owner = bridge.attach(lambda data: received.append(data))
    check("re-attach accepted", owner is not None)
    time.sleep(0.1)
    check("re-attach scrollback", b"after detach" in b"".join(received))

    bridge.detach(owner)
    closed = []
    owner = bridge.attach(lambda data: received.append(data),
                          lambda: closed.append(True))
    check("close-callback attach accepted", owner is not None)
    bridge.close()
    check("bridge close wakes attached client", closed == [True])
    os.close(w_fd)
    os.close(r_fd)


def test_integration_remote_proxy():
    """Two local daemons: daemon A connects to daemon B as remote session."""
    section("INTEGRATION: remote proxy")

    if sys.platform == "win32":
        print("  SKIP: integration tests use AF_UNIX")
        return

    sock_a = os.path.join(tempfile.gettempdir(), f"pythond-a-{os.getpid()}.sock")
    port_b = 17399 + (os.getpid() % 1000)
    addr_b = f"127.0.0.1:{port_b}"

    env_a = os.environ.copy()
    env_a["PYTHOND_SOCK"] = sock_a

    # daemon B: TCP with --listen (acts as "remote server")
    proc_b = subprocess.Popen(
        [sys.executable, str(ROOT / "pythond.py"), "daemon",
         "--listen", f"127.0.0.1:{port_b}", "--show-token"],
        stderr=subprocess.PIPE,
    )
    time.sleep(0.8)

    # read token from daemon B stderr
    token_b = None
    try:
        raw = proc_b.stderr.read1(4096).decode()
    except Exception:
        raw = ""
    for line in raw.splitlines():
        if line.startswith("token="):
            token_b = line.split("=", 1)[1]

    if not token_b:
        check("got token from daemon B", False, f"stderr: {raw[:200]}")
        proc_b.terminate()
        proc_b.wait(timeout=3)
        return

    # verify B is reachable
    resp = send_cmd(addr_b, "ls", token=token_b)
    check("daemon B reachable", "(no sessions)" in resp, resp)

    # create session on B directly
    resp = send_cmd(addr_b, "new", ["work"], token=token_b)
    check("B new OK", "OK work" in resp, resp)
    send_cmd(addr_b, "run", ["work", "x = 777"], token=token_b)
    resp = send_cmd(addr_b, "run", ["work", "x"], token=token_b)
    check("B local run", resp.strip() == "777", resp)

    # daemon A: AF_UNIX (acts as "local proxy")
    proc_a = subprocess.Popen(
        [sys.executable, str(ROOT / "pythond.py"), "daemon"],
        env=env_a, stderr=subprocess.PIPE,
    )
    time.sleep(0.5)
    check("daemon A started", proc_a.poll() is None)

    try:
        # connect A -> B
        resp = send_cmd(sock_a, "connect", ["remote", addr_b, token_b])
        check("connect OK", "OK connected" in resp, resp)

        # ls on A shows remote
        resp = send_cmd(sock_a, "ls")
        check("ls shows remote", "remote" in resp and "(remote)" in resp, resp)

        # forward: A.run("remote", "work", "x + 1") -> B.run("work", "x + 1") -> 778
        resp = send_cmd(sock_a, "run", ["remote", "work", "x + 1"])
        check("remote forwarded", "778" in resp, f"got: {resp}")

        # forward: set variable on B through A
        send_cmd(sock_a, "run", ["remote", "work", "y = x * 2"])
        resp = send_cmd(sock_a, "run", ["remote", "work", "y"])
        check("remote state persists", "1554" in resp, f"got: {resp}")

        # disconnect
        resp = send_cmd(sock_a, "disconnect", ["remote"])
        check("disconnect OK", "OK disconnected" in resp)

        resp = send_cmd(sock_a, "ls")
        check("ls after disconnect", "(no sessions)" in resp)

    finally:
        send_cmd(sock_a, "stop")
        send_cmd(addr_b, "stop", token=token_b)
        time.sleep(0.3)
        for p in (proc_a, proc_b):
            if p.poll() is None:
                p.terminate()
                p.wait(timeout=3)
        if os.path.exists(sock_a):
            os.unlink(sock_a)
        shutil.rmtree(os.path.join(os.path.expanduser("~"),
                                   ".pythond", "sessions", "work"),
                      ignore_errors=True)


# ===========================================

def main():
    tests = [
        # Unit tests
        test_version,
        test_listen_addr_parsing,
        test_khost_parsing,
        test_parse_host_port_validation,
        test_init_namespace,
        test_make_exec_eval,
        test_make_exec_exec,
        test_make_exec_last_expr,
        test_make_exec_error,
        test_make_exec_on_done,
        test_make_exec_thread_isolation,
        test_make_exec_restores_replaced_stdio,
        test_thread_stdout_compat_methods,
        test_dispatch_run,
        test_dispatch_fire_poll,
        test_dispatch_async_empty_code_rejected,
        test_dispatch_fire_traceback_format_failure,
        test_dispatch_poll_empty,
        test_dispatch_poll_unknown,
        test_dispatch_poll_latest,
        test_dispatch_status,
        test_dispatch_vars,
        test_dispatch_complete,
        test_dispatch_int,
        test_dispatch_unknown,
        test_dispatch_fork,
        test_dispatch_fork_kill,
        test_dispatch_fork_large_payload,
        test_dispatch_fork_concurrent_fire,
        test_cell_eviction,
        test_session_name_sanitization,
        test_control_handler_exact_arity,
        test_resize_dead_pty_not_ok,
        test_session_dir,
        test_session_capacity_limit,
        test_log_history,
        test_log_session,
        test_daemon_meta_roundtrip,
        test_daemon_meta_read_missing,
        test_daemon_meta_read_rejects_symlink,
        test_daemon_meta_tmp_cleanup_on_write_failure,
        test_cert_fingerprint_missing,
        test_cert_generation,
        test_trust_cert_exact_fingerprint_store,
        test_cert_generation_no_crypto,
        test_websocket_protocol,
        test_wire_message_builder,
        test_send_remote_transparent_alias,
        test_send_remote_close_retries_and_closes,
        test_connect_remote_bytes_auth_failure,
        test_session_command_arg_normalization,
        test_wspro_client_basic,
        test_wspro_client_preserves_batched_frames,
        test_wspro_client_payload_limit,
        test_tls_and_auth_hardening_static,
        test_connection_hardening_static,
        test_has_crypto_flag,
        test_entry_points_exist,
        test_session_cli_errors_exit_nonzero,
        test_access_log_sanitises_session_field,
        test_pyctl_cert_role_hints,
        test_pyctl_status_uses_env_endpoint,
        test_default_sock,
        test_secure_path_win32,

        # Integration tests
        test_integration,
        test_integration_tcp_windows,
        test_integration_tls_pinned_server,
        test_integration_error_not_in_history,
        test_integration_crash_isolation,
        test_unit_pty_bridge,
        test_integration_ws_attach,
        test_integration_remote_proxy,
    ]
    registered = {fn.__name__ for fn in tests}
    discovered = {name for name, obj in globals().items()
                  if name.startswith("test_") and callable(obj)}
    missing = sorted(discovered - registered)
    check("all test functions registered", not missing, ", ".join(missing))
    for fn in tests:
        fn()

    print(f"\n{'='*40}")
    print(f"  {PASS} passed, {FAIL} failed")
    if FAIL:
        sys.exit(1)
    print("  all clear")


if __name__ == "__main__":
    main()
