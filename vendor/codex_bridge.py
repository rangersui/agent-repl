#!/usr/bin/env python3
"""Bridge agent-tty km events into a Codex app-server thread.

This is an experimental local bridge. It does not depend on the Codex SDK:
it speaks newline-delimited JSON-RPC to `codex app-server` over stdin/stdout.

Typical use:

    python vendor/codex_bridge.py --list-threads --thread-search "agent tty"
    python vendor/codex_bridge.py --list-loaded
    python vendor/codex_bridge.py --session work --repo . --thread-id 019...
    python vendor/codex_bridge.py --session work --repo . --thread-search "agent tty"
    python vendor/codex_bridge.py --session work --repo . --new-thread

Type-seal shape:

    _CodexRpc           raw JSON-RPC pipe; private implementation detail
    InitializedCodex    proof that initialize + initialized completed
    ThreadHandle        proof that app-server accepted/resumed/started a thread
    ThreadRuntimeStatus validated active/idle status for visible delivery
    IdleThread          proof that app-server reported the thread idle
    StartedTurn         proof that app-server accepted a visible turn
    KmEvent             validated km JSON line
    PollResult          validated best-effort k poll result for a completed cell
    EventPrompt         visible turn text derived from a KmEvent

Protected operations require proof objects. `turn/start` does not accept a raw
thread id, a merely resumed thread, or arbitrary bridge text: it requires an
`IdleThread` proof and an `EventPrompt`. km lines are not forwarded until they
become KmEvent objects.

Notes:
    * `thread/loaded/list` only lists threads loaded in the app-server process
      this bridge is connected to. It is a candidate source, not a guaranteed
      "current Desktop UI thread" oracle.
    * Codex App Server has lower-level primitives, not a single Monitor-like
      notification primitive. `thread/inject_items` persists data for later
      model requests but does not wake the agent; `turn/steer` needs an active
      turn and expected turn id; `turn/interrupt` cannot carry the event
      payload; and `turn/start` can create a parallel side turn if misused while
      another turn is active.
    * This bridge intentionally owns that state machine: it queues km events
      while the target thread is active, then starts one visible turn when the
      thread becomes idle.
    * Codex Desktop may not live-refresh turns started by another app-server
      client. The event can be persisted and still be invisible until Desktop
      reloads. For interactive monitoring, prefer an external sink such as tmux,
      a web UI, email, or another explicit notification channel.
"""

from __future__ import annotations

import argparse
import json
import os
import queue
import re
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from typing import Any


JsonMap = dict[str, Any]
TERMINAL_STATUSES = frozenset({"done", "timeout", "interrupted", "notify", "error", "closed"})
FORWARDABLE_STATUSES = TERMINAL_STATUSES | {"fired"}
POLLABLE_STATUSES = frozenset({"done", "timeout", "interrupted", "error"})
SAFE_SESSION_RE = re.compile(r"^[A-Za-z0-9_.-]+$")
CELL_ID_RE = re.compile(r"^[0-9a-f]{12}$")


class BridgeError(RuntimeError):
    pass


class BridgeTransportError(BridgeError):
    pass


class BridgeRpcError(BridgeError):
    pass


class BridgeProtocolError(BridgeError):
    pass


class BridgeThreadError(BridgeError):
    pass


class BridgeThreadStateError(BridgeThreadError):
    pass


class BridgeTurnError(BridgeError):
    pass


@dataclass(frozen=True)
class PendingRequest:
    method: str
    response: "queue.Queue[JsonMap]"


@dataclass(frozen=True)
class ThreadHandle:
    """Proof that app-server accepted this thread id for this bridge."""

    _id: str

    def __post_init__(self) -> None:
        _validate_wire_id(self._id, "thread id")

    @property
    def id(self) -> str:
        return self._id


@dataclass(frozen=True)
class StartedTurn:
    """Proof that app-server accepted a visible turn for this bridge."""

    _id: str

    def __post_init__(self) -> None:
        _validate_wire_id(self._id, "turn id")

    @property
    def id(self) -> str:
        return self._id


@dataclass(frozen=True)
class ThreadRuntimeStatus:
    """Validated runtime status for a loaded Codex thread."""

    kind: str
    active_flags: tuple[str, ...] = ()

    @classmethod
    def parse(cls: type["ThreadRuntimeStatus"], raw: Any) -> "ThreadRuntimeStatus":
        if not isinstance(raw, dict):
            raise BridgeError(f"invalid thread status: {raw!r}")
        kind = raw.get("type")
        if not isinstance(kind, str):
            raise BridgeError(f"invalid thread status type: {raw!r}")
        flags = raw.get("activeFlags", [])
        if flags is None:
            flags = []
        if not isinstance(flags, list) or not all(isinstance(item, str) for item in flags):
            raise BridgeError(f"invalid thread activeFlags: {raw!r}")
        return cls(kind=kind, active_flags=tuple(flags))

    @property
    def is_idle(self) -> bool:
        return self.kind == "idle"

    def require_idle(self, thread: ThreadHandle) -> "IdleThread":
        return IdleThread(thread, self, _IDLE_THREAD_TOKEN)


_IDLE_THREAD_TOKEN = object()


class IdleThread:
    """Proof that app-server reported this thread idle for visible delivery."""

    def __init__(self, thread: ThreadHandle, status: ThreadRuntimeStatus, token: object) -> None:
        if token is not _IDLE_THREAD_TOKEN:
            raise BridgeThreadStateError("IdleThread must be created from ThreadRuntimeStatus.require_idle")
        if not status.is_idle:
            raise BridgeThreadStateError(f"thread {thread.id} is not idle: {status.kind}")
        self._thread = thread
        self._status = status

    @property
    def id(self) -> str:
        return self._thread.id

    @property
    def thread(self) -> ThreadHandle:
        return self._thread

    @property
    def status(self) -> ThreadRuntimeStatus:
        return self._status


@dataclass(frozen=True)
class PollResult:
    """Best-effort k poll result attached to a km event."""

    raw: JsonMap | None
    output: str | None
    error: str | None = None

    @classmethod
    def from_completed_process(cls: type["PollResult"], proc: subprocess.CompletedProcess[str]) -> "PollResult":
        stdout = proc.stdout.strip()
        stderr = proc.stderr.strip()
        if proc.returncode != 0:
            detail = stderr or stdout or f"k poll exited {proc.returncode}"
            return cls(raw=None, output=None, error=detail)
        try:
            raw = json.loads(stdout)
        except json.JSONDecodeError:
            return cls(raw=None, output=None, error=f"k poll returned non-JSON output: {stdout!r}")
        if not isinstance(raw, dict):
            return cls(raw=None, output=None, error=f"k poll returned non-object JSON: {raw!r}")
        output = raw.get("output")
        if output is not None and not isinstance(output, str):
            return cls(raw=raw, output=None, error=f"k poll returned non-string output: {output!r}")
        return cls(raw=raw, output=output)


@dataclass(frozen=True)
class KmEvent:
    """Validated km event line."""

    status: str
    session: str
    raw: JsonMap
    cell_id: str | None = None

    @classmethod
    def parse(cls: type["KmEvent"], line: str) -> "KmEvent | None":
        try:
            raw = json.loads(line)
        except json.JSONDecodeError:
            return None
        if not isinstance(raw, dict):
            return None
        status = raw.get("status")
        session = raw.get("session")
        if not isinstance(status, str) or status not in FORWARDABLE_STATUSES:
            return None
        if not isinstance(session, str) or not SAFE_SESSION_RE.match(session):
            return None
        cell_id = raw.get("cell_id")
        if cell_id is not None and (not isinstance(cell_id, str) or not CELL_ID_RE.match(cell_id)):
            return None
        return cls(status=status, session=session, cell_id=cell_id, raw=raw)

    @property
    def should_poll(self) -> bool:
        return self.cell_id is not None and self.status in POLLABLE_STATUSES


@dataclass(frozen=True)
class EventPrompt:
    """Visible turn text derived from a KmEvent, not arbitrary bridge text."""

    text: str

    @classmethod
    def from_event(cls: type["EventPrompt"], event: KmEvent, poll: PollResult | None, max_output_chars: int) -> "EventPrompt":
        cell_hint = f" cell={event.cell_id}" if event.cell_id else ""
        lines = [
            "agent-tty km event arrived.",
            "",
            f"Session: {event.session}",
            f"Status: {event.status}{cell_hint}",
            "",
            _code_block("Event JSON:", "json", json.dumps(event.raw, ensure_ascii=False, sort_keys=True)),
        ]
        if poll is not None:
            lines.extend(_format_poll_result(poll, max_output_chars))
        lines.extend(
            [
                "",
                "React to this visible terminal event. If cell output is present, continue the task from that output.",
            ]
        )
        return cls("\n".join(lines))

    @classmethod
    def batch(cls: type["EventPrompt"], prompts: list["EventPrompt"]) -> "EventPrompt":
        if not prompts:
            raise BridgeError("cannot start a turn without an event prompt")
        if len(prompts) == 1:
            return prompts[0]
        body = "\n\n---\n\n".join(prompt.text for prompt in prompts)
        return cls(f"Multiple agent-tty events arrived while this Codex thread was busy.\n\n{body}")

    def to_turn_input(self) -> list[JsonMap]:
        return [{"type": "text", "text": self.text}]


class ThreadSelector:
    """Sealed selector base; parse_args creates one concrete selector."""


@dataclass(frozen=True)
class ExplicitThread(ThreadSelector):
    thread_id: str


@dataclass(frozen=True)
class LoadedThread(ThreadSelector):
    pass


@dataclass(frozen=True)
class SearchThread(ThreadSelector):
    search_term: str
    limit: int
    cwd: str | None


@dataclass(frozen=True)
class NewThread(ThreadSelector):
    model: str | None


class _CodexRpc:
    """Raw JSON-RPC transport. Keep app-server protocol methods out of callers."""

    def __init__(self, codex_bin: str) -> None:
        try:
            self._proc = subprocess.Popen(
                [codex_bin, "app-server"],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=None,
                text=True,
                bufsize=1,
            )
        except OSError as exc:
            raise BridgeTransportError(f"failed to start codex app-server: {exc}") from exc
        if self._proc.stdin is None or self._proc.stdout is None:
            raise BridgeTransportError("failed to open codex app-server pipes")
        self._stdin = self._proc.stdin
        self._stdout = self._proc.stdout
        self._next_id = 1
        self._pending: dict[int, PendingRequest] = {}
        self.notifications: "queue.Queue[JsonMap]" = queue.Queue()
        self._reader = threading.Thread(target=self._read_loop, daemon=True)
        self._reader.start()

    def close(self) -> None:
        if self._proc.poll() is None:
            self._proc.terminate()
            try:
                self._proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self._proc.kill()

    def notify(self, method: str, params: JsonMap | None = None) -> None:
        self._send({"method": method, "params": params or {}})

    def request(self, method: str, params: JsonMap | None = None, *, timeout: float = 60.0) -> JsonMap:
        request_id = self._next_id
        self._next_id += 1
        response_queue: "queue.Queue[JsonMap]" = queue.Queue(maxsize=1)
        self._pending[request_id] = PendingRequest(method, response_queue)
        try:
            self._send({"method": method, "id": request_id, "params": params or {}})
        except OSError as exc:
            self._pending.pop(request_id, None)
            raise BridgeTransportError(f"failed to send {method}: {exc}") from exc
        try:
            msg = response_queue.get(timeout=timeout)
        except queue.Empty as exc:
            self._pending.pop(request_id, None)
            raise BridgeRpcError(f"timeout waiting for {method}") from exc
        if "error" in msg:
            raise BridgeRpcError(f"{method} failed: {msg['error']}")
        result = msg.get("result", {})
        if not isinstance(result, dict):
            raise BridgeProtocolError(f"{method} returned non-object result: {result!r}")
        return result

    def _send(self, msg: JsonMap) -> None:
        self._stdin.write(json.dumps(msg, separators=(",", ":")) + "\n")
        self._stdin.flush()

    def _read_loop(self) -> None:
        for line in self._stdout:
            try:
                msg = json.loads(line)
            except json.JSONDecodeError:
                print(f"[codex raw] {line.rstrip()}", file=sys.stderr)
                continue
            if not isinstance(msg, dict):
                continue
            request_id = msg.get("id")
            if isinstance(request_id, int):
                pending = self._pending.pop(request_id, None)
                if pending:
                    pending.response.put(msg)
                else:
                    self.notifications.put(msg)
            else:
                self.notifications.put(msg)
        self._fail_pending("codex app-server stdout closed")

    def _fail_pending(self, reason: str) -> None:
        pending = list(self._pending.values())
        self._pending.clear()
        for request in pending:
            request.response.put({"error": reason})


class InitializedCodex:
    """Proof object for an initialized app-server connection."""

    def __init__(self, rpc: _CodexRpc) -> None:
        self._rpc = rpc

    @classmethod
    def connect(cls: type["InitializedCodex"], codex_bin: str) -> "InitializedCodex":
        rpc = _CodexRpc(codex_bin)
        try:
            rpc.request(
                "initialize",
                {
                    "clientInfo": {
                        "name": "agent_tty_bridge",
                        "title": "agent-tty bridge",
                        "version": "0.1.0",
                    },
                    "capabilities": {"experimentalApi": True},
                },
            )
            rpc.notify("initialized")
            return cls(rpc)
        except Exception:
            rpc.close()
            raise

    def close(self) -> None:
        self._rpc.close()

    def list_loaded_thread_ids(self) -> list[str]:
        result = self._rpc.request("thread/loaded/list")
        data = result.get("data", [])
        if not isinstance(data, list):
            raise BridgeError(f"thread/loaded/list returned unexpected result: {result}")
        out: list[str] = []
        for item in data:
            if not isinstance(item, str):
                raise BridgeError(f"thread/loaded/list returned non-string thread id: {item!r}")
            _validate_wire_id(item, "loaded thread id")
            out.append(item)
        return out

    def list_threads(self, search_term: str | None, limit: int, cwd: str | None = None) -> list[JsonMap]:
        params: JsonMap = {"limit": limit, "sortKey": "recency_at"}
        if search_term:
            params["searchTerm"] = search_term
        if cwd:
            params["cwd"] = cwd
        result = self._rpc.request("thread/list", params)
        data = result.get("data", [])
        if not isinstance(data, list):
            raise BridgeError(f"thread/list returned unexpected result: {result}")
        return [item for item in data if isinstance(item, dict)]

    def resume_thread(self, thread_id: str) -> ThreadHandle:
        _validate_wire_id(thread_id, "thread id")
        try:
            result = self._rpc.request("thread/resume", {"threadId": thread_id, "excludeTurns": True})
        except (BridgeRpcError, BridgeProtocolError) as exc:
            raise BridgeThreadError(str(exc)) from exc
        return _thread_from_result("thread/resume", result)

    def start_thread(self, model: str | None) -> ThreadHandle:
        params: JsonMap = {}
        if model:
            params["model"] = model
        try:
            result = self._rpc.request("thread/start", params)
        except (BridgeRpcError, BridgeProtocolError) as exc:
            raise BridgeThreadError(str(exc)) from exc
        return _thread_from_result("thread/start", result)

    def read_thread_status(self, thread: ThreadHandle) -> ThreadRuntimeStatus:
        try:
            result = self._rpc.request("thread/read", {"threadId": thread.id, "includeTurns": False})
        except (BridgeRpcError, BridgeProtocolError) as exc:
            raise BridgeThreadError(str(exc)) from exc
        thread_obj = result.get("thread")
        if not isinstance(thread_obj, dict):
            raise BridgeThreadError(f"thread/read returned unexpected result: {result}")
        return ThreadRuntimeStatus.parse(thread_obj.get("status"))

    def drain_thread_status(self, thread: ThreadHandle, current: ThreadRuntimeStatus) -> ThreadRuntimeStatus:
        status = current
        while True:
            try:
                msg = self._rpc.notifications.get_nowait()
            except queue.Empty:
                return status
            updated = _status_from_notification(thread, msg)
            if updated is not None:
                status = updated

    def start_visible_turn(self, thread: IdleThread, prompt: EventPrompt) -> StartedTurn:
        try:
            result = self._rpc.request(
                "turn/start",
                {
                    "threadId": thread.id,
                    "input": prompt.to_turn_input(),
                },
            )
        except (BridgeRpcError, BridgeProtocolError) as exc:
            raise BridgeTurnError(str(exc)) from exc
        turn = result.get("turn")
        if not isinstance(turn, dict) or not isinstance(turn.get("id"), str):
            raise BridgeTurnError(f"turn/start returned unexpected result: {result}")
        return StartedTurn(str(turn["id"]))


def _thread_from_result(method: str, result: JsonMap) -> ThreadHandle:
    thread = result.get("thread")
    if not isinstance(thread, dict) or not isinstance(thread.get("id"), str):
        raise BridgeThreadError(f"{method} returned unexpected result: {result}")
    return ThreadHandle(str(thread["id"]))


def _status_from_notification(thread: ThreadHandle, msg: JsonMap) -> ThreadRuntimeStatus | None:
    if msg.get("method") != "thread/status/changed":
        return None
    params = msg.get("params")
    if not isinstance(params, dict) or params.get("threadId") != thread.id:
        return None
    return ThreadRuntimeStatus.parse(params.get("status"))


def _validate_wire_id(value: str, label: str) -> None:
    if not value or any(ch.isspace() or ord(ch) < 32 for ch in value):
        raise BridgeError(f"invalid {label}: {value!r}")


def choose_thread(codex: InitializedCodex, selector: ThreadSelector) -> ThreadHandle:
    if isinstance(selector, ExplicitThread):
        return codex.resume_thread(selector.thread_id)

    if isinstance(selector, LoadedThread):
        loaded = codex.list_loaded_thread_ids()
        if len(loaded) == 1:
            return codex.resume_thread(loaded[0])
        if len(loaded) > 1:
            raise BridgeError("multiple loaded threads; pass --thread-id explicitly: " + ", ".join(loaded))
        raise BridgeError("no loaded thread visible to this app-server process")

    if isinstance(selector, SearchThread):
        threads = codex.list_threads(selector.search_term, selector.limit, selector.cwd)
        if len(threads) == 1:
            thread_id = threads[0].get("id")
            if not isinstance(thread_id, str):
                raise BridgeError(f"thread/list match has no id: {threads[0]}")
            return codex.resume_thread(thread_id)
        if threads:
            preview = "\n".join(_format_thread_summary(t) for t in threads)
            raise BridgeError("multiple thread matches; pass --thread-id:\n" + preview)
        raise BridgeError(f"no thread matched searchTerm={selector.search_term!r}")

    if isinstance(selector, NewThread):
        return codex.start_thread(selector.model)

    raise BridgeError(f"unsupported thread selector: {selector!r}")


def _format_thread_summary(thread: JsonMap) -> str:
    return (
        f"{thread.get('id')}  recency={thread.get('recencyAt', '?')}  updated={thread.get('updatedAt', '?')}  "
        f"status={thread.get('status', {})}  {thread.get('preview', '')}"
    )


def km_command(args: argparse.Namespace) -> list[str]:
    command = [args.km_bin, args.session]
    if args.once:
        command.append("-1")
    return command


def start_km(args: argparse.Namespace) -> subprocess.Popen[str]:
    proc = subprocess.Popen(
        km_command(args),
        cwd=args.repo,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=None,
        text=True,
        bufsize=1,
    )
    if proc.stdout is None:
        raise BridgeError("failed to open km stdout")
    return proc


def _read_km_lines(proc: subprocess.Popen[str], lines: "queue.Queue[str | None]") -> None:
    if proc.stdout is None:
        lines.put(None)
        return
    for line in proc.stdout:
        lines.put(line.rstrip("\n"))
    lines.put(None)


def poll_cell_output(args: argparse.Namespace, event: KmEvent) -> PollResult | None:
    if not args.poll_output or not event.should_poll:
        return None
    assert event.cell_id is not None
    cmd = [args.k_bin, "poll", event.session, event.cell_id]
    try:
        proc = subprocess.run(
            cmd,
            cwd=args.repo,
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            timeout=args.poll_timeout,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        return PollResult(raw=None, output=None, error=str(exc))
    return PollResult.from_completed_process(proc)


def run_bridge(args: argparse.Namespace) -> int:
    codex = InitializedCodex.connect(args.codex_bin)
    km_proc: subprocess.Popen[str] | None = None
    try:
        if args.list_loaded:
            print(json.dumps({"data": codex.list_loaded_thread_ids()}, ensure_ascii=False, indent=2))
            return 0
        if args.list_threads:
            for summary in codex.list_threads(args.thread_search, args.thread_search_limit, args.thread_cwd):
                print(_format_thread_summary(summary))
            return 0

        thread = choose_thread(codex, args.thread_selector)
        status = codex.read_thread_status(thread)
        print(f"[bridge] thread={thread.id} status={status.kind}", file=sys.stderr)

        km_proc = start_km(args)
        km_lines: "queue.Queue[str | None]" = queue.Queue()
        km_reader = threading.Thread(target=_read_km_lines, args=(km_proc, km_lines), daemon=True)
        km_reader.start()

        statuses = set(args.status)
        pending: list[EventPrompt] = []
        km_closed = False
        last_status_check = 0.0
        while True:
            status = codex.drain_thread_status(thread, status)
            try:
                line = km_lines.get(timeout=args.status_check_interval)
            except queue.Empty:
                line = ""

            if line is None:
                km_closed = True
            elif line:
                print(f"[km] {line}", file=sys.stderr)
                event = KmEvent.parse(line)
                if event is not None and event.status in statuses:
                    poll = poll_cell_output(args, event)
                    pending.append(EventPrompt.from_event(event, poll, args.max_output_chars))
                    print(f"[bridge] queued status={event.status} session={event.session}", file=sys.stderr)

            now = time.monotonic()
            if pending and (status.is_idle or now - last_status_check >= args.status_check_interval):
                status = codex.read_thread_status(thread)
                last_status_check = now

            if pending and status.is_idle:
                idle_thread = status.require_idle(thread)
                prompt = EventPrompt.batch(pending)
                turn = codex.start_visible_turn(idle_thread, prompt)
                print(f"[bridge] started visible turn={turn.id} events={len(pending)}", file=sys.stderr)
                pending.clear()
                if args.once:
                    return 0
                status = codex.read_thread_status(thread)

            if km_closed and not pending:
                return km_proc.wait()
    finally:
        if km_proc is not None and km_proc.poll() is None:
            km_proc.terminate()
        codex.close()


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--session", default="work", help="agent-tty session for km")
    parser.add_argument("--repo", default=os.getcwd(), help="directory where scripts/k and scripts/km run")
    parser.add_argument("--k-bin", default="./scripts/k", help="k executable path for polling completed cells")
    parser.add_argument("--km-bin", default="./scripts/km", help="km executable path")
    parser.add_argument("--codex-bin", default="codex", help="codex executable path")
    parser.add_argument("--thread-id", help="existing Codex thread id to resume")
    parser.add_argument("--thread-search", help="searchTerm for thread/list, then resume exact match")
    parser.add_argument("--thread-search-limit", type=int, default=10)
    parser.add_argument("--thread-cwd", help="filter thread/list by exact cwd; relative paths become absolute")
    parser.add_argument("--list-threads", action="store_true", help="print thread/list candidates and exit")
    parser.add_argument("--list-loaded", action="store_true", help="print thread/loaded/list data and exit")
    parser.add_argument("--use-loaded", action="store_true", help="resume the single thread loaded in this app-server")
    parser.add_argument("--new-thread", action="store_true", help="create a new Codex thread")
    parser.add_argument("--model", help="model for --new-thread")

    parser.add_argument(
        "--once",
        action="store_true",
        help="pass -1 to km and exit after the first visible event turn is started",
    )
    parser.add_argument(
        "--no-poll-output",
        action="store_false",
        dest="poll_output",
        help="forward km events without running k poll for completed cells",
    )
    parser.add_argument("--poll-timeout", type=float, default=10.0, help="seconds to wait for k poll")
    parser.add_argument(
        "--max-output-chars",
        type=int,
        default=24000,
        help="maximum cell output characters to include in the visible turn",
    )
    parser.add_argument(
        "--status-check-interval",
        type=float,
        default=0.5,
        help="seconds between idle checks while events are queued",
    )
    parser.add_argument(
        "--status",
        action="append",
        choices=sorted(FORWARDABLE_STATUSES),
        default=[],
        help="km status to forward; repeatable. Default: terminal/notify/error statuses",
    )
    args = parser.parse_args(argv)
    _validate_args(parser, args)
    args.thread_selector = _thread_selector_from_args(args)
    if not args.status:
        args.status = sorted(TERMINAL_STATUSES)
    return args


def _validate_args(parser: argparse.ArgumentParser, args: argparse.Namespace) -> None:
    if not SAFE_SESSION_RE.match(args.session):
        parser.error("--session must match [A-Za-z0-9_.-]+")
    if args.thread_search_limit <= 0:
        parser.error("--thread-search-limit must be positive")
    if args.poll_timeout <= 0:
        parser.error("--poll-timeout must be positive")
    if args.max_output_chars <= 0:
        parser.error("--max-output-chars must be positive")
    if args.status_check_interval <= 0:
        parser.error("--status-check-interval must be positive")
    if args.model and not args.new_thread:
        parser.error("--model is only valid with --new-thread")
    if args.thread_cwd:
        args.thread_cwd = os.path.abspath(args.thread_cwd)
    if not os.path.isdir(args.repo):
        parser.error(f"--repo is not a directory: {args.repo}")
    list_modes = [args.list_threads, args.list_loaded]
    if sum(1 for item in list_modes if item) > 1:
        parser.error("choose only one of --list-threads or --list-loaded")
    if args.list_loaded and args.thread_search:
        parser.error("--thread-search only applies to --list-threads or thread selection")
    if args.thread_cwd and not (args.list_threads or args.thread_search):
        parser.error("--thread-cwd only applies to --list-threads or --thread-search")
    if any(list_modes):
        if args.thread_id or args.use_loaded or args.new_thread:
            parser.error("list modes cannot be combined with a thread target")
        return
    target_modes = [bool(args.thread_id), bool(args.thread_search), args.use_loaded, args.new_thread]
    if sum(1 for item in target_modes if item) != 1:
        parser.error("choose exactly one of --thread-id, --thread-search, --use-loaded, or --new-thread")


def _thread_selector_from_args(args: argparse.Namespace) -> ThreadSelector | None:
    if args.list_loaded or args.list_threads:
        return None
    if args.thread_id:
        return ExplicitThread(args.thread_id)
    if args.thread_search:
        return SearchThread(args.thread_search, args.thread_search_limit, args.thread_cwd)
    if args.use_loaded:
        return LoadedThread()
    if args.new_thread:
        return NewThread(args.model)
    return None


def _format_poll_result(poll: PollResult, max_output_chars: int) -> list[str]:
    lines: list[str] = [""]
    if poll.raw is not None:
        display = dict(poll.raw)
        if isinstance(display.get("output"), str):
            display["output"] = "<shown below>"
        lines.append(_code_block("k poll result:", "json", json.dumps(display, ensure_ascii=False, sort_keys=True)))
    if poll.output is not None:
        output, truncated = _truncate_text(poll.output, max_output_chars)
        label = "Cell output"
        if truncated:
            label += f" (truncated to {max_output_chars} chars)"
        lines.append(_code_block(f"{label}:", "text", output))
    if poll.error:
        lines.append(f"k poll warning: {poll.error}")
    return lines


def _truncate_text(text: str, max_chars: int) -> tuple[str, bool]:
    if len(text) <= max_chars:
        return text, False
    omitted = len(text) - max_chars
    return text[:max_chars] + f"\n...[truncated {omitted} chars]", True


def _code_block(label: str, info: str, body: str) -> str:
    fence = "```"
    while fence in body:
        fence += "`"
    return f"{label}\n{fence}{info}\n{body}\n{fence}"


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(sys.argv[1:] if argv is None else argv)
        return run_bridge(args)
    except (BridgeError, OSError, KeyboardInterrupt) as exc:
        print(f"ERR {exc}", file=sys.stderr)
        return 130 if isinstance(exc, KeyboardInterrupt) else 1


if __name__ == "__main__":
    raise SystemExit(main())


