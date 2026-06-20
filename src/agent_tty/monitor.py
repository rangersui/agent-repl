#!/usr/bin/env python3
r"""
km -- interrupt-driven monitor for k sessions.

Watches a tmux session via pipe-pane (not polling).
Outputs structured JSON events to stdout.
Each stdout line -> one Monitor notification -> agent interrupt.

Usage:
  km <session> [cell_id] [-1]
  km --version

  session    tmux session to watch
  cell_id    only match this cell (optional, matches any cell if omitted)
  -1         exit after first completion (one-shot / .then())

Examples:
  km work abc123 -1          <- await one cell
  km work -1                 <- await any cell completion
  km work                    <- continuous, all completions

Architecture:
  tmux pipe-pane -> log file -> tail -f -> parse -> JSON event -> stdout
  No polling. Interrupt-driven end to end.
  This is the .then() callback mechanism.
"""

from __future__ import annotations

import sys
import os
import json
import shlex
import signal
import subprocess

from datetime import datetime, timezone
from typing import Any

_src = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _src not in sys.path:
    sys.path.insert(0, _src)

from agent_tty import __version__  # noqa: E402

_VERSION_ARGS = ("--version", "-V", "version")
if len(sys.argv) >= 2 and sys.argv[1] in _VERSION_ARGS:
    print(f"agent-tty {__version__}")
    sys.exit(0)

if len(sys.argv) < 2 or sys.argv[1] in ("-h", "--help"):
    print(__doc__.strip())
    sys.exit(0)

from agent_tty._shared import (
    TMUX, TAIL, ANSI_RE, CELL_DIR,
    FIRED, DONE, CLOSED, ERROR, NOTIFY,
    CELL_START_RE, CELL_END_RE, NOTIFY_EVENT_RE,
    CELL_ID_RE,
    ensure_private_dir, open_private, validate_cell_id, validate_name,
)


JsonMap = dict[str, Any]


def _emit(d: JsonMap) -> None:
    """One JSON line to stdout = one agent interrupt."""
    d["ts"] = datetime.now(timezone.utc).isoformat()
    sys.stdout.write(json.dumps(d, ensure_ascii=False) + "\n")
    sys.stdout.flush()


class E:
    """km event factory."""

    @staticmethod
    def started(cell_id: str, session: str) -> None:
        _emit({"cell_id": cell_id, "session": session, "status": FIRED})

    @staticmethod
    def completed(cell_id: str, session: str, status: str = DONE) -> None:
        _emit({"cell_id": cell_id, "session": session, "status": status})

    @staticmethod
    def notify(session: str, who: str, message: str) -> None:
        _emit({"session": session, "status": NOTIFY, "from": who, "message": message})

    @staticmethod
    def closed(session: str) -> None:
        _emit({"session": session, "status": CLOSED})

    @staticmethod
    def error(session: str, message: str) -> None:
        _emit({"session": session, "status": ERROR, "message": message})


def session_log_path(session: str) -> str:
    validate_name(session, prefix="km:")
    return os.path.join(ensure_private_dir(os.path.join(CELL_DIR, session)), "_output.log")


def start_pipe(session: str) -> str:
    """(Re)start pipe-pane. Idempotent — replaces dead/existing pipe."""
    logfile = session_log_path(session)
    with open_private(logfile, os.O_WRONLY | os.O_CREAT | os.O_APPEND, "a"):
        pass
    subprocess.run(
        [TMUX, "pipe-pane", "-t", session, f"cat >> {shlex.quote(logfile)}"],
        check=True,
    )
    return logfile


def stop_pipe(session: str, logfile: str, tail_proc: subprocess.Popen[str] | None = None) -> None:
    """Cleanup: kill tail. Don't stop pipe-pane or remove log — k owns those."""
    if tail_proc and tail_proc.poll() is None:
        tail_proc.kill()
        tail_proc.wait()


def monitor(session: str, cell_id: str | None = None, oneshot: bool = False) -> int:
    # verify session
    r = subprocess.run([TMUX, "has-session", "-t", session], capture_output=True)
    if r.returncode != 0:
        E.error(session, f"no session '{session}'; use k new {session} bash")
        return 1

    logfile = start_pipe(session)
    tail_proc: subprocess.Popen[str] | None = None

    def cleanup(*_: object) -> None:
        stop_pipe(session, logfile, tail_proc)

    def request_stop(*_: object) -> None:
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)

    try:
        # Pre-scan: one-shot must notice completions that happened before km
        # started. With no cell_id, return the latest fired cell that completed.
        scan_offset = None
        if oneshot:
            seen_fired: set[str] = set()
            last_completion: tuple[str, str] | None = None
            try:
                with open_private(logfile, os.O_RDONLY, "rb") as f:
                    for raw_line in f:
                        line = ANSI_RE.sub("", raw_line.decode("utf-8", errors="replace")).strip()
                        if not line:
                            continue
                        m = CELL_START_RE.match(line)
                        if m:
                            cid = m.group(1)
                            if cell_id is None or cid == cell_id:
                                seen_fired.add(cid)
                            continue
                        m = CELL_END_RE.match(line)
                        if m:
                            cid, status = m.group(1), m.group(2)
                            if cell_id is not None and cid == cell_id:
                                E.completed(cell_id, session, status)
                                return 0
                            if cell_id is None and cid in seen_fired:
                                last_completion = (cid, status)
                                seen_fired.discard(cid)
                    scan_offset = f.tell()
                if cell_id is None and last_completion:
                    cid, status = last_completion
                    E.completed(cid, session, status)
                    return 0
            except OSError:
                scan_offset = 0

        # tail -f: interrupt-driven (inotify on linux, kqueue on mac)
        # If pre-scanned, start from scan position to cover the race window;
        # otherwise start from EOF (only new events).
        if scan_offset is not None:
            tail_cmd = [TAIL, "-c", f"+{scan_offset + 1}", "-f", logfile]
        else:
            tail_cmd = [TAIL, "-n", "0", "-f", logfile]

        tail_proc = subprocess.Popen(
            tail_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            errors="replace",
            bufsize=1,
        )

        assert tail_proc.stdout is not None
        for raw_line in tail_proc.stdout:
            line = ANSI_RE.sub("", raw_line).strip()
            if not line:
                continue

            # check start
            m = CELL_START_RE.match(line)
            if m:
                cid = m.group(1)
                if cell_id is None or cid == cell_id:
                    E.started(cid, session)
                continue

            # check done/timeout/interrupted
            m = CELL_END_RE.match(line)
            if m:
                cid, status = m.group(1), m.group(2)
                if cell_id is None or cid == cell_id:
                    E.completed(cid, session, status)
                    if oneshot:
                        return 0
                continue

            # check notify
            m = NOTIFY_EVENT_RE.match(line)
            if m:
                who, message = m.group(1), m.group(2)
                E.notify(session, who, message)
                continue

        # tail ended (session died?)
        E.closed(session)
        return 1

    except KeyboardInterrupt:
        return 0

    finally:
        cleanup()


def main() -> int:
    args = sys.argv[1:]
    if not args or args[0] in ("-h", "--help"):
        print(__doc__.strip())
        return 0

    session = args[0]
    validate_name(session, prefix="km:")
    cell_id: str | None = None
    oneshot = False

    for arg in args[1:]:
        if arg == "-1":
            oneshot = True
        elif CELL_ID_RE.match(arg):
            cell_id = validate_cell_id(arg)
        else:
            print(f"unknown arg: {arg}", file=sys.stderr)
            return 1

    return monitor(session, cell_id, oneshot)


if __name__ == "__main__":
    sys.exit(main())
