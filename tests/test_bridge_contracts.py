#!/usr/bin/env python3
"""Static contracts for vendor/codex_bridge.py. No app-server or km process is started."""

import ast
from pathlib import Path
from typing import Any, cast


ROOT = Path(__file__).resolve().parents[1]
BRIDGE_PATH = ROOT / "vendor" / "codex_bridge.py"
BRIDGE_SRC = BRIDGE_PATH.read_text(encoding="utf-8")
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


def klass(tree: ast.Module, name: str) -> ast.ClassDef | None:
    for node in tree.body:
        if isinstance(node, ast.ClassDef) and node.name == name:
            return node
    FAILURES.append(f"missing class {name}")
    return None


def absent_class(tree: ast.Module, name: str) -> None:
    for node in tree.body:
        if isinstance(node, ast.ClassDef) and node.name == name:
            FAILURES.append(f"forbidden class {name}")


def function(tree: ast.Module, name: str) -> ast.FunctionDef | None:
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name == name:
            return node
    FAILURES.append(f"missing function {name}")
    return None


def method(cls: ast.ClassDef | None, name: str, *, required: bool = True) -> ast.FunctionDef | None:
    if cls is None:
        return None
    for node in cls.body:
        if isinstance(node, ast.FunctionDef) and node.name == name:
            return node
    if required:
        FAILURES.append(f"missing method {cls.name}.{name}")
    return None


def segment(src: str, node: ast.AST | None) -> str:
    if node is None:
        return ""
    lines = src.splitlines()
    positioned = cast(Any, node)
    return "\n".join(lines[positioned.lineno - 1 : positioned.end_lineno])


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


TREE = parse(BRIDGE_PATH, BRIDGE_SRC)
check_no_except_pass("vendor/codex_bridge.py", TREE)
check_function_annotations("vendor/codex_bridge.py", TREE)

raw_rpc_cls = klass(TREE, "_CodexRpc")
codex_cls = klass(TREE, "InitializedCodex")
thread_handle_cls = klass(TREE, "ThreadHandle")
idle_thread_cls = klass(TREE, "IdleThread")
started_turn_cls = klass(TREE, "StartedTurn")
thread_status_cls = klass(TREE, "ThreadRuntimeStatus")
poll_result_cls = klass(TREE, "PollResult")
absent_class(TREE, "TurnHandle")
km_event_cls = klass(TREE, "KmEvent")
event_prompt_cls = klass(TREE, "EventPrompt")
selector_cls = klass(TREE, "ThreadSelector")
for cls_name in ("ExplicitThread", "LoadedThread", "SearchThread", "NewThread"):
    klass(TREE, cls_name)

choose_thread_fn = function(TREE, "choose_thread")
run_bridge_fn = function(TREE, "run_bridge")
parse_args_fn = function(TREE, "parse_args")
validate_args_fn = function(TREE, "_validate_args")
thread_selector_fn = function(TREE, "_thread_selector_from_args")
start_km_fn = function(TREE, "start_km")
read_km_lines_fn = function(TREE, "_read_km_lines")
poll_cell_fn = function(TREE, "poll_cell_output")
format_poll_fn = function(TREE, "_format_poll_result")
code_block_fn = function(TREE, "_code_block")

raw_rpc_seg = segment(BRIDGE_SRC, raw_rpc_cls)
codex_seg = segment(BRIDGE_SRC, codex_cls)
thread_seg = segment(BRIDGE_SRC, thread_handle_cls)
idle_thread_seg = segment(BRIDGE_SRC, idle_thread_cls)
started_turn_seg = segment(BRIDGE_SRC, started_turn_cls)
thread_status_seg = segment(BRIDGE_SRC, thread_status_cls)
poll_result_seg = segment(BRIDGE_SRC, poll_result_cls)
km_event_seg = segment(BRIDGE_SRC, km_event_cls)
event_prompt_seg = segment(BRIDGE_SRC, event_prompt_cls)
choose_seg = segment(BRIDGE_SRC, choose_thread_fn)
run_seg = segment(BRIDGE_SRC, run_bridge_fn)
parse_seg = segment(BRIDGE_SRC, parse_args_fn)
validate_seg = segment(BRIDGE_SRC, validate_args_fn)
thread_selector_seg = segment(BRIDGE_SRC, thread_selector_fn)
start_km_seg = segment(BRIDGE_SRC, start_km_fn)
read_km_lines_seg = segment(BRIDGE_SRC, read_km_lines_fn)
poll_cell_seg = segment(BRIDGE_SRC, poll_cell_fn)
format_poll_seg = segment(BRIDGE_SRC, format_poll_fn)
code_block_seg = segment(BRIDGE_SRC, code_block_fn)

check("bridge: raw JSON-RPC class is private", "class _CodexRpc" in BRIDGE_SRC)
check("bridge: initialized proof class exists", "class InitializedCodex" in BRIDGE_SRC)
check("bridge: thread proof class exists", "class ThreadHandle" in BRIDGE_SRC and "_id: str" in thread_seg)
check("bridge: idle thread proof class exists", "class IdleThread" in BRIDGE_SRC and "_IDLE_THREAD_TOKEN" in idle_thread_seg)
check("bridge: visible turn proof exists", "class StartedTurn" in BRIDGE_SRC and "_id: str" in started_turn_seg)
check("bridge: runtime status proof exists", "class ThreadRuntimeStatus" in BRIDGE_SRC and "is_idle" in thread_status_seg and "require_idle" in thread_status_seg)
check("bridge: poll proof class exists", "class PollResult" in BRIDGE_SRC and "from_completed_process" in poll_result_seg)
check("bridge: event proof class exists", "class KmEvent" in BRIDGE_SRC and "def parse(" in km_event_seg)
check("bridge: prompt proof class exists", "class EventPrompt" in BRIDGE_SRC and "from_event" in event_prompt_seg)
check("bridge: selector seal exists", selector_cls is not None and "ThreadSelector" in choose_seg)

connect_seg = segment(BRIDGE_SRC, method(codex_cls, "connect"))
check("InitializedCodex.connect: performs initialize", '"initialize"' in connect_seg and '"initialized"' in connect_seg)
check("InitializedCodex.connect: closes failed rpc", "rpc.close()" in connect_seg and "raise" in connect_seg)
check("InitializedCodex: owns raw request calls", ".request(" in codex_seg)
check("run_bridge: does not call raw app-server request", ".request(" not in run_seg)

read_status_seg = segment(BRIDGE_SRC, method(codex_cls, "read_thread_status"))
drain_status_seg = segment(BRIDGE_SRC, method(codex_cls, "drain_thread_status"))
start_visible_seg = segment(BRIDGE_SRC, method(codex_cls, "start_visible_turn"))
check("read_thread_status: uses thread/read", '"thread/read"' in read_status_seg and "ThreadRuntimeStatus.parse" in read_status_seg)
check("read_thread_status: requires ThreadHandle", "thread: ThreadHandle" in read_status_seg)
check("drain_thread_status: consumes notifications", "notifications.get_nowait" in drain_status_seg and "_status_from_notification" in drain_status_seg)
check("start_visible_turn: requires IdleThread", "thread: IdleThread" in start_visible_seg and "thread: ThreadHandle" not in start_visible_seg)
check("start_visible_turn: requires EventPrompt", "prompt: EventPrompt" in start_visible_seg)
check("start_visible_turn: uses sealed thread id", '"threadId": thread.id' in start_visible_seg)
check("start_visible_turn: uses visible turn/start", '"turn/start"' in start_visible_seg)
check("start_visible_turn: uses prompt turn input", "prompt.to_turn_input()" in start_visible_seg)
check("start_visible_turn: returns StartedTurn", "-> StartedTurn" in start_visible_seg and "StartedTurn(" in start_visible_seg)
check("bridge: does not use inject_items as notification", "def inject_items" not in BRIDGE_SRC and '"thread/inject_items"' not in codex_seg)
check("bridge: queues active turns instead of invisible steer", '"turn/steer"' not in BRIDGE_SRC)
check("bridge: no wait-turn lifecycle coupling", "wait_for_turn_completion" not in BRIDGE_SRC and "--wait-turn" not in BRIDGE_SRC)
check("bridge: no turn-cwd argument", "--turn-cwd" not in BRIDGE_SRC)

resume_seg = segment(BRIDGE_SRC, method(codex_cls, "resume_thread"))
start_thread_seg = segment(BRIDGE_SRC, method(codex_cls, "start_thread"))
check("resume_thread: returns ThreadHandle", "-> ThreadHandle" in resume_seg and "_thread_from_result" in resume_seg)
check("resume_thread: fail-loud on malformed server result", "return ThreadHandle(thread_id)" not in resume_seg and '"thread/resume"' in resume_seg)
check("start_thread: returns ThreadHandle", "-> ThreadHandle" in start_thread_seg and "_thread_from_result" in start_thread_seg)
check("choose_thread: returns ThreadHandle", "-> ThreadHandle" in choose_seg)
check("choose_thread: all modes go through InitializedCodex gates", all(
    needle in choose_seg for needle in ("resume_thread", "list_loaded_thread_ids", "list_threads", "start_thread")
))

check("ThreadRuntimeStatus: validates status dict", "invalid thread status" in thread_status_seg and 'raw.get("type")' in thread_status_seg)
check("ThreadRuntimeStatus: validates active flags", "activeFlags" in thread_status_seg and "tuple(flags)" in thread_status_seg)
check("IdleThread: rejects direct minting and non-idle status", "_IDLE_THREAD_TOKEN" in idle_thread_seg and "not status.is_idle" in idle_thread_seg)
check("PollResult: parses k poll JSON", "json.loads(stdout)" in poll_result_seg and "non-object JSON" in poll_result_seg)
check("PollResult: validates output string", 'raw.get("output")' in poll_result_seg and "non-string output" in poll_result_seg)
check("KmEvent.parse: validates JSON dict", "json.loads(line)" in km_event_seg and "isinstance(raw, dict)" in km_event_seg)
check("KmEvent.parse: validates status", "FORWARDABLE_STATUSES" in km_event_seg)
check("KmEvent.parse: validates session", "SAFE_SESSION_RE.match(session)" in km_event_seg)
check("KmEvent.parse: validates cell id", "CELL_ID_RE.match(cell_id)" in km_event_seg)
check("KmEvent: pollable status gate", "POLLABLE_STATUSES" in km_event_seg and "should_poll" in km_event_seg)
check("EventPrompt: only built from KmEvent", "event: KmEvent" in event_prompt_seg)
check("EventPrompt: includes poll output", "_format_poll_result" in event_prompt_seg and "Cell output" in format_poll_seg)
check("EventPrompt: emits visible turn input", "def to_turn_input" in event_prompt_seg and '"type": "text"' in event_prompt_seg)
check("EventPrompt: batches queued events", "def batch" in event_prompt_seg and "Multiple agent-tty events" in event_prompt_seg)
check("code block: lengthens fence around body", "while fence in body" in code_block_seg)

check("poll_cell_output: runs k poll", '"poll"' in poll_cell_seg and "subprocess.run" in poll_cell_seg)
check("poll_cell_output: gated by --no-poll-output", "args.poll_output" in poll_cell_seg)
check("poll_cell_output: captures failures as PollResult", "PollResult(raw=None" in poll_cell_seg and "TimeoutExpired" in poll_cell_seg)
check("run_bridge: parses km line into KmEvent", "event = KmEvent.parse(line)" in run_seg)
check("run_bridge: polls completed cell output", "poll = poll_cell_output(args, event)" in run_seg)
check("run_bridge: queues EventPrompt from event", "pending.append(EventPrompt.from_event(event, poll, args.max_output_chars))" in run_seg)
check("run_bridge: checks idle before visible start", "status.is_idle" in run_seg and "codex.read_thread_status(thread)" in run_seg)
check("run_bridge: starts visible turn only with idle proof", "idle_thread = status.require_idle(thread)" in run_seg and "codex.start_visible_turn(idle_thread, prompt)" in run_seg)
check("run_bridge: batches pending prompts", "EventPrompt.batch(pending)" in run_seg)
check("run_bridge: refreshes server status after start", "status = codex.read_thread_status(thread)" in run_seg and 'ThreadRuntimeStatus(kind="active")' not in run_seg)
check("run_bridge: once exits after visible delivery", "if args.once" in run_seg and "return 0" in run_seg)

check("argparse: raw formatter for readable examples", "RawDescriptionHelpFormatter" in parse_seg)
check("argparse: list thread ergonomics", "--list-threads" in parse_seg and "--list-loaded" in parse_seg)
check("argparse: exact one target", "choose exactly one" in validate_seg)
check("argparse: positive search limit", "--thread-search-limit must be positive" in validate_seg)
check("argparse: positive poll controls", "--poll-timeout must be positive" in validate_seg and "--max-output-chars must be positive" in validate_seg)
check("argparse: positive status interval", "--status-check-interval must be positive" in validate_seg)
check("argparse: safe session name", "SAFE_SESSION_RE.match" in validate_seg)
check("argparse: model only new thread", "--model is only valid with --new-thread" in validate_seg)
check("argparse: thread cwd is scoped", "--thread-cwd only applies" in validate_seg and "os.path.abspath(args.thread_cwd)" in validate_seg)
check("argparse: repo preflight", "os.path.isdir(args.repo)" in validate_seg)
check("argparse: selector sealed after validation", "args.thread_selector = _thread_selector_from_args(args)" in parse_seg)
check("argparse: k poll controls", "--k-bin" in parse_seg and "--no-poll-output" in parse_seg)
check("thread selector: bridge target uses sealed selector", "choose_thread(codex, args.thread_selector)" in run_seg)
check("thread selector: produces explicit selector types", all(
    needle in thread_selector_seg for needle in ("ExplicitThread", "SearchThread", "LoadedThread", "NewThread")
))

list_loaded_seg = segment(BRIDGE_SRC, method(codex_cls, "list_loaded_thread_ids"))
list_threads_seg = segment(BRIDGE_SRC, method(codex_cls, "list_threads"))
check("loaded/list: rejects non-string ids", "non-string thread id" in list_loaded_seg and "_validate_wire_id" in list_loaded_seg)
check("thread/list: uses recency ordering", '"sortKey": "recency_at"' in list_threads_seg)
check("thread/list: supports cwd filter", '"cwd"] = cwd' in list_threads_seg)
check("resume_thread: excludes large turn history", '"excludeTurns": True' in resume_seg)

check("km process: runs via start_km helper", "km_proc = start_km(args)" in run_seg)
check("km process: stdout checked", "proc.stdout is None" in start_km_seg)
check("km process: read in background", "threading.Thread" in run_seg and "_read_km_lines" in run_seg)
check("km process: EOF sentinel", "lines.put(None)" in read_km_lines_seg)
check("km process: terminated on exit", "km_proc.terminate()" in run_seg)
check("codex process: closed on exit", "codex.close()" in run_seg)

check("bridge: no SDK dependency", "openai_codex" not in BRIDGE_SRC and "@openai/codex-sdk" not in BRIDGE_SRC)
check("bridge: no shell=True", "shell=True" not in BRIDGE_SRC)
check("bridge: no eval/exec", "eval(" not in BRIDGE_SRC and "exec(" not in BRIDGE_SRC)

if FAILURES:
    print("bridge contract failures:")
    for failure in FAILURES:
        print("  -", failure)
    raise SystemExit(1)

print("bridge contract tests passed")
