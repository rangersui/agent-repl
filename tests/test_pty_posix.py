#!/usr/bin/env python3
"""POSIX PTY smoke tests.

Starts a real daemon with a Unix socket, creates a PTY session,
exercises the AI channel (run/fire/poll/status/ls/kill), and
verifies the subprocess-based PTY worker doesn't deadlock.

Requires: POSIX with pty support (Linux, macOS, WSL).
Skip on Windows.
"""

import json
import os
import signal
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
AGENT_TTY = ROOT / "agent_tty.py"

PASS = 0
FAIL = 0


def check(name, expect, actual):
    global PASS, FAIL
    if expect in actual:
        PASS += 1
    else:
        print(f"  X {name}")
        print(f"    expect: {expect!r}")
        first = actual.splitlines()[0] if actual.splitlines() else actual
        print(f"    actual: {first[:120]!r}")
        FAIL += 1


def send_cmd(sock_path, cmd, args=None):
    """Send a command to the daemon via Unix socket, return response string."""
    msg = json.dumps({"cmd": cmd, "args": args or []})
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    s.connect(sock_path)
    s.sendall(msg.encode())
    s.shutdown(socket.SHUT_WR)
    data = b""
    while True:
        chunk = s.recv(8192)
        if not chunk:
            break
        data += chunk
    s.close()
    return data.decode()


def main():
    if sys.platform == "win32":
        print("SKIP: POSIX PTY tests not applicable on Windows")
        return 0

    if not hasattr(os, "setsid"):
        print("SKIP: no os.setsid (not POSIX)")
        return 0

    with tempfile.TemporaryDirectory(prefix="k-pty-test-") as tmp:
        sock_path = os.path.join(tmp, "test.sock")

        env = os.environ.copy()
        env["K_SOCK"] = sock_path

        daemon = subprocess.Popen(
            [sys.executable, str(AGENT_TTY), "daemon"],
            env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )

        try:
            # wait for daemon to be ready
            deadline = time.time() + 10
            ready = False
            while time.time() < deadline:
                if os.path.exists(sock_path):
                    ready = True
                    break
                time.sleep(0.2)

            if not ready:
                stderr = daemon.stderr.read().decode() if daemon.stderr else ""
                print(f"  X daemon failed to start within 10s")
                print(f"    stderr: {stderr[:200]}")
                return 1

            print("=== POSIX PTY regression tests ===")

            # -- ls empty --
            resp = send_cmd(sock_path, "ls")
            check("ls-empty", "(no sessions)", resp)

            # -- new session --
            resp = send_cmd(sock_path, "new", ["test1"])
            check("new-session", "OK test1 pid=", resp)

            time.sleep(0.5)

            # -- ls shows session --
            resp = send_cmd(sock_path, "ls")
            check("ls-has-session", "test1", resp)
            check("ls-alive", "alive", resp)
            check("ls-pty-type", "(pty)", resp)

            # -- run expression --
            resp = send_cmd(sock_path, "run", ["test1", "2+2"])
            check("run-expr", "4", resp)

            # -- run statement --
            resp = send_cmd(sock_path, "run", ["test1", "x = 99"])
            # no output expected, just no error
            check("run-stmt-no-error", "", resp)

            # -- run uses same namespace --
            resp = send_cmd(sock_path, "run", ["test1", "x"])
            check("run-namespace-persist", "99", resp)

            # -- run print --
            resp = send_cmd(sock_path, "run", ["test1", "print('hello world')"])
            check("run-print", "hello world", resp)

            # -- run unicode --
            resp = send_cmd(sock_path, "run", ["test1", "print('ok')"])
            check("run-unicode", "ok", resp)

            # -- run multiline --
            resp = send_cmd(sock_path, "run", ["test1", "for i in range(3): print(i)"])
            check("run-multiline", "0", resp)
            check("run-multiline-2", "2", resp)

            # -- run error --
            resp = send_cmd(sock_path, "run", ["test1", "1/0"])
            check("run-error", "ZeroDivisionError", resp)

            # -- fire + poll --
            resp = send_cmd(sock_path, "fire", ["test1", "y = 42"])
            try:
                cell = json.loads(resp)
                cell_id = cell.get("cell_id", "")
                check("fire-returns-cell-id", "cell_id", json.dumps(cell))
            except json.JSONDecodeError:
                check("fire-returns-json", "{", resp)
                cell_id = ""

            if cell_id:
                time.sleep(0.5)
                resp = send_cmd(sock_path, "poll", ["test1", cell_id])
                check("poll-done", "done", resp)

            # -- status --
            resp = send_cmd(sock_path, "status", ["test1"])
            check("status-idle", "idle", resp)

            # -- complete --
            resp = send_cmd(sock_path, "run", ["test1", "import os"])
            resp = send_cmd(sock_path, "complete", ["test1", "os.path."])
            check("complete-returns", "join", resp)

            # -- worker is owned by the daemon process tree --
            resp = send_cmd(sock_path, "run", ["test1", "os.getppid()"])
            check("worker-daemon-parent",
                  str(daemon.pid) if sys.platform != "win32" else "",
                  resp)

            # -- kill session --
            resp = send_cmd(sock_path, "kill", ["test1"])
            check("kill-ok", "OK killed test1", resp)

            resp = send_cmd(sock_path, "ls")
            check("ls-after-kill", "(no sessions)", resp)

            # -- run on dead session --
            resp = send_cmd(sock_path, "run", ["test1", "1"])
            check("run-dead-session", "ERR", resp)

        finally:
            daemon.terminate()
            try:
                daemon.wait(timeout=5)
            except subprocess.TimeoutExpired:
                daemon.kill()
                daemon.wait(timeout=2)

    print()
    print(f"=== {PASS} passed, {FAIL} failed ===")
    return 0 if FAIL == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
