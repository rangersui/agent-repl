#!/usr/bin/env python3
"""agent-tty.py -- persistent Python runtime for AI agents.

Each session owns a long-lived Python namespace in a child process.  AI commands
enter that namespace as eval/exec cells; shell commands, browsers, databases,
packet capture, and other external tools are reached from that live process
through Python libraries or subprocess.

Execution model:
  - run executes one cell synchronously and returns captured stdout/stderr.
  - fire starts a background cell and returns a cell_id immediately.
  - poll reads a fired cell by cell_id, or the most recent cell if omitted.
  - fire is queued inside one session: cells execute serially under one lock.
  - For real parallel execution, create multiple sessions.

Human interaction:
  - POSIX sessions use a real PTY: readline, tab completion, arrows, Ctrl-C.
  - Windows sessions expose InteractiveConsole through a local TCP socket.
  - attach connects a human to the same live namespace the AI is using.

Transport:
  - AF_UNIX mode uses filesystem-local K_SOCK, default /tmp/k.sock.
  - TCP fallback uses 127.0.0.1:K_PORT and requires K_TOKEN.
  - The daemon prints the token; k.py.template is a local wrapper template that
    stores K_TOKEN/K_PORT so agent calls stay short.  The real k.py is ignored.

Commands:
    k daemon                  start daemon in foreground
    k new <name>              create a Python session
    k kill <name>             terminate session process and forget it
    k run <name> "code"       sync Python eval/exec, print raw output
    k fire <name> "code"      async queued eval/exec, print JSON cell_id
    k poll <name> [cell_id]   print JSON cell result
    k status <name>           print JSON session state
    k vars <name>             print JSON list of public namespace names
    k complete <name> "text"  print JSON Python completion candidates
    k ls                      list sessions
    k attach <name>           attach human REPL to the session

Output formats:
    new, kill, ls             text
    run                       raw captured output
    fire, poll, status,
    vars, complete            JSON
    attach                    interactive stream
"""
import sys, os, socket, json, threading, uuid, io, traceback, time, tempfile, code
import signal, subprocess
import multiprocessing as mp
import secrets

_HAS_AF_UNIX = hasattr(socket, "AF_UNIX")
_HAS_PTY = False
if sys.platform != "win32":
    try:
        import pty, tty, termios, fcntl
        import select as _sel
        _HAS_PTY = True
    except ImportError:
        pass

def _default_sock():
    if sys.platform == "win32":
        return os.path.join(tempfile.gettempdir(), "k.sock")
    return "/tmp/k.sock"

SOCK = os.environ.get("K_SOCK", _default_sock())

# -----------------------------------------------
# SOCKET helpers
# -----------------------------------------------

def _server_socket():
    """Create the daemon control socket.

    AF_UNIX mode is path-based and intended for POSIX local use.  TCP mode is
    loopback-only and must be authenticated with K_TOKEN because any local
    process can attempt to connect to the port.
    """
    if _HAS_AF_UNIX:
        if os.path.exists(SOCK):
            os.unlink(SOCK)
        srv = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        srv.bind(SOCK)
    else:
        port = int(os.environ.get("K_PORT", "7399"))
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        if sys.platform == "win32":
            srv.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)  # type: ignore[attr-defined]
        else:
            srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind(("127.0.0.1", port))
    return srv

def _client_socket():
    """Connect to the daemon control socket selected by K_SOCK or K_PORT."""
    if _HAS_AF_UNIX:
        s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        s.connect(SOCK)
    else:
        port = int(os.environ.get("K_PORT", "7399"))
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect(("127.0.0.1", port))
    return s

# =============================================
# SHARED WORKER LOGIC
# =============================================

def _init_namespace():
    """Create the persistent Python namespace for one session.

    The imports here are convenience imports for agent cells.  Code running in
    a session has normal Python process permissions.
    """
    ns = {"__builtins__": __builtins__}
    exec("import os,sys,json,subprocess,shutil,hashlib,time,re,glob,sqlite3,socket", ns)
    return ns

def _make_exec(ns, lock, on_done=None):
    """Build _exec(src): eval/exec in ns and return captured output.

    Strings are printed as raw text; non-strings are repr()'d like a REPL.
    stdout/stderr redirection is protected by lock so queued run/fire cells keep
    output capture coherent.

    on_done(src, output), when provided, broadcasts completed AI cells to an
    attached human REPL.
    """
    def _exec(src):
        with lock:
            buf = io.StringIO()
            old_out, old_err = sys.stdout, sys.stderr
            sys.stdout = sys.stderr = buf
            try:
                try:
                    r = eval(compile(src, "<k>", "eval"), ns)
                    if r is not None:
                        if isinstance(r, str):
                            print(r)
                        else:
                            print(repr(r))
                except SyntaxError:
                    exec(compile(src, "<k>", "exec"), ns)
            except:
                traceback.print_exc()
            finally:
                sys.stdout, sys.stderr = old_out, old_err
            output = buf.getvalue().rstrip()
            if on_done:
                on_done(src, output)
            return output
    return _exec

def _dispatch(cmd, args, _exec, cells, ns):
    """Handle one AI protocol command inside a session.

    This function returns dictionaries only.  The daemon decides which commands
    are rendered as raw text versus JSON at the client boundary.
    """
    if cmd == "run":
        return {"output": _exec(args[0])}
    elif cmd == "fire":
        cid = uuid.uuid4().hex[:12]
        res = {"output": "", "status": "running"}
        def _bg(c=args[0], r=res):
            r["output"] = _exec(c)
            r["status"] = "done"
        threading.Thread(target=_bg, daemon=True).start()
        cells[cid] = res
        return {"cell_id": cid, "status": "fired"}
    elif cmd == "poll":
        target = args[0] if args else None
        if target:
            if target not in cells:
                return {"cell_id": target, "status": "error",
                         "output": "unknown cell"}
            r = cells[target]
            return {"cell_id": target, "status": r["status"],
                     "output": r["output"]}
        if not cells:
            return {"status": "idle"}
        last_id = list(cells)[-1]
        r = cells[last_id]
        return {"cell_id": last_id, "status": r["status"],
                 "output": r["output"]}
    elif cmd == "status":
        vs = len([v for v in ns if not v.startswith("_")])
        running = [cid for cid, r in cells.items()
                   if r["status"] == "running"]
        return {"state": "running" if running else "idle",
                "running": running, "vars": vs, "cells": len(cells)}
    elif cmd == "vars":
        return {"vars": [v for v in ns if not v.startswith("_")]}
    elif cmd == "complete":
        import rlcompleter
        text = args[0] if args else ""
        c = rlcompleter.Completer(ns)
        matches = []
        for i in range(200):
            m = c.complete(text, i)
            if m is None:
                break
            matches.append(m)
        return {"matches": matches}
    return {"error": f"unknown cmd: {cmd}"}

# =============================================
# POSIX: real PTY worker (readline, tab, arrows)
# =============================================

def session_worker_pty(ai_sock):
    """Runs in subprocess with PTY slave as stdin/stdout/stderr.

    POSIX human attach goes through the PTY and therefore gets real readline,
    tab completion, terminal signals, and normal Python REPL behaviour.  AI
    commands use ai_sock, a private socketpair using one JSON object per line.
    Both paths share the same namespace and lock.
    """
    ns = _init_namespace()
    cells = {}
    lock = threading.Lock()

    def _broadcast(src, output):
        lines = src.strip().splitlines()
        sys.stdout.write("\n")
        for i, ln in enumerate(lines):
            sys.stdout.write(f"{'[ai] >>> ' if i == 0 else '[ai] ... '}{ln}\n")
        if output:
            sys.stdout.write(output + "\n")
        sys.stdout.flush()

    _exec = _make_exec(ns, lock, _broadcast)

    try:
        import readline, rlcompleter
        _completer = rlcompleter.Completer(ns)
        readline.set_completer(_completer.complete)
        readline.parse_and_bind("tab: complete")
    except ImportError:
        pass

    def _ai_loop():
        rf = ai_sock.makefile("r")
        wf = ai_sock.makefile("w")
        while True:
            try:
                line = rf.readline()
                if not line:
                    break
                msg = json.loads(line)
                resp = _dispatch(msg["cmd"], msg.get("args", []),
                                 _exec, cells, ns)
                wf.write(json.dumps(resp) + "\n")
                wf.flush()
            except Exception as e:
                try:
                    wf.write(json.dumps({"error": str(e)}) + "\n")
                    wf.flush()
                except:
                    break

    threading.Thread(target=_ai_loop, daemon=True).start()

    class LockedConsole(code.InteractiveConsole):
        def runsource(self, source, filename="<input>", symbol="single"):
            with lock:
                return super().runsource(source, filename, symbol)

    # Ctrl-] is handled by the attach client and detaches the human.  If EOF
    # reaches the Python console anyway, restart the prompt so the session
    # stays alive.  exit() raises SystemExit and intentionally kills it.
    while True:
        try:
            LockedConsole(locals=ns).interact(
                banner="shared with AI. Ctrl-] detaches. exit() kills session.",
                exitmsg="")
        except SystemExit:
            break

# =============================================
# WINDOWS: InteractiveConsole over TCP socket
# =============================================

def session_worker(rx, tx):
    """Runs in mp.Process. InteractiveConsole over TCP socket for human.

    This path serves platforms that use the socket console.  A local TCP REPL
    server gives the human an InteractiveConsole; the AI channel uses
    multiprocessing Pipe.  Both share the same namespace and execution lock.
    """
    ns = _init_namespace()
    cells = {}
    _lock = threading.Lock()
    _watchers = []
    _watchers_lock = threading.Lock()

    def _broadcast(src, output):
        """Show completed AI cells to currently attached Windows clients."""
        with _watchers_lock:
            lines = src.strip().splitlines()
            for wf in _watchers[:]:
                try:
                    wf.write("\n")
                    for i, ln in enumerate(lines):
                        wf.write(f"{'[ai] >>> ' if i == 0 else '[ai] ... '}{ln}\n")
                    if output:
                        wf.write(output + "\n")
                    wf.flush()
                except (OSError, ValueError):
                    _watchers.remove(wf)

    _exec = _make_exec(ns, _lock, _broadcast)

    class SharedConsole(code.InteractiveConsole):
        def __init__(self, ns, lock, rfile, wfile):
            super().__init__(locals=ns)
            self._lock = lock
            self._rf = rfile
            self._wf = wfile

        def runsource(self, source, filename="<input>", symbol="single"):
            with self._lock:
                old_out, old_err = sys.stdout, sys.stderr
                sys.stdout = sys.stderr = self._wf
                try:
                    return super().runsource(source, filename, symbol)
                finally:
                    sys.stdout, sys.stderr = old_out, old_err

        def write(self, data):
            self._wf.write(data)
            self._wf.flush()

        def raw_input(self, prompt=""):
            self._wf.write(prompt)
            self._wf.flush()
            line = self._rf.readline()
            if not line:
                raise EOFError
            return line.rstrip("\n")

    repl_srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    repl_srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    repl_srv.bind(("127.0.0.1", 0))
    repl_port = repl_srv.getsockname()[1]
    repl_srv.listen(1)

    def _repl_server():
        while True:
            try:
                conn, _ = repl_srv.accept()
            except OSError:
                break
            # Windows defaults conn.makefile() to the system locale
            # (e.g. GBK on Chinese Windows).  Force UTF-8.
            rf = conn.makefile("r", encoding="utf-8")
            wf = conn.makefile("w", encoding="utf-8")
            with _watchers_lock:
                _watchers.append(wf)
            try:
                c = SharedConsole(ns, _lock, rf, wf)
                c.interact(banner="shared with AI. EOF detaches.",
                           exitmsg="detached")
            except (OSError, EOFError):
                pass
            finally:
                with _watchers_lock:
                    if wf in _watchers:
                        _watchers.remove(wf)
                conn.close()

    threading.Thread(target=_repl_server, daemon=True).start()
    tx.send({"_repl_port": repl_port})

    while True:
        try:
            msg = rx.recv()
        except (EOFError, KeyboardInterrupt):
            break
        try:
            resp = _dispatch(msg["cmd"], msg.get("args", []),
                             _exec, cells, ns)
            tx.send(resp)
        except Exception as e:
            tx.send({"error": str(e)})

# =============================================
# DAEMON -- socket + process manager
# =============================================

sessions = {}
_daemon_token = None

def _start_pty_bridge(master_fd):
    """Start the human attach bridge for a POSIX PTY session.

    The bridge continuously drains the PTY master fd so detached sessions keep
    making progress.  Recent output is kept as small scrollback so the next
    attach sees the current prompt/context.

    Returns (port, server_socket).  The daemon stores server_socket and closes
    it during kill_session() for explicit cleanup.
    """
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("127.0.0.1", 0))
    port = srv.getsockname()[1]
    srv.listen(1)

    _client_conn = [None]
    _lock = threading.Lock()
    _scrollback = bytearray()
    _MAX_SCROLL = 8192

    def _pty_reader():
        while True:
            try:
                data = os.read(master_fd, 4096)
            except OSError:
                break
            if not data:
                break
            with _lock:
                if _client_conn[0]:
                    try:
                        _client_conn[0].sendall(data)
                    except OSError:
                        _client_conn[0] = None
                else:
                    _scrollback.extend(data)
                    if len(_scrollback) > _MAX_SCROLL:
                        del _scrollback[:-_MAX_SCROLL]

    def _acceptor():
        while True:
            try:
                conn, _ = srv.accept()
            except OSError:
                break
            with _lock:
                old = _client_conn[0]
                _client_conn[0] = conn
                if _scrollback:
                    try:
                        conn.sendall(bytes(_scrollback))
                    except OSError:
                        pass
                    _scrollback.clear()
            if old:
                try:
                    old.close()
                except OSError:
                    pass

            def _client_reader(c=conn):
                try:
                    while True:
                        data = c.recv(4096)
                        if not data:
                            break
                        os.write(master_fd, data)
                except OSError:
                    pass
                with _lock:
                    if _client_conn[0] is c:
                        _client_conn[0] = None

            threading.Thread(target=_client_reader, daemon=True).start()

    threading.Thread(target=_pty_reader, daemon=True).start()
    threading.Thread(target=_acceptor, daemon=True).start()
    return port, srv

def new_session(name):
    """Create or replace one named Python session."""
    if name in sessions:
        kill_session(name)
    if _HAS_PTY:
        master_fd, slave_fd = pty.openpty()
        ai_parent, ai_child = socket.socketpair()
        p = subprocess.Popen(
            [sys.executable, os.path.abspath(__file__),
             "_worker_pty", str(slave_fd), str(ai_child.fileno())],
            close_fds=True,
            pass_fds=(slave_fd, ai_child.fileno()),
        )
        os.close(slave_fd)
        ai_child.close()
        bridge_port, bridge_srv = _start_pty_bridge(master_fd)
        sessions[name] = {
            "type": "pty", "proc": p, "master_fd": master_fd,
            "ai": ai_parent, "repl_port": bridge_port,
            "bridge_srv": bridge_srv,
        }
    else:
        parent_rx, child_tx = mp.Pipe(duplex=False)
        child_rx, parent_tx = mp.Pipe(duplex=False)
        p = mp.Process(target=session_worker, args=(child_rx, child_tx),
                       daemon=True)
        p.start()
        init = parent_rx.recv()
        repl_port = init.get("_repl_port", 0)
        sessions[name] = {
            "type": "socket", "proc": p, "tx": parent_tx, "rx": parent_rx,
            "repl_port": repl_port,
        }

def kill_session(name):
    """Terminate one named session and close all daemon-owned resources."""
    if name not in sessions:
        return False
    s = sessions[name]
    if s["type"] == "pty":
        s["proc"].terminate()
        try:
            s["proc"].wait(timeout=3)
        except subprocess.TimeoutExpired:
            s["proc"].kill()
            s["proc"].wait(timeout=1)
        for resource in ("master_fd",):
            try:
                os.close(s[resource])
            except OSError:
                pass
        for resource in ("ai", "bridge_srv"):
            try:
                s[resource].close()
            except OSError:
                pass
    else:
        if s["proc"].is_alive():
            s["proc"].terminate()
            s["proc"].join(timeout=3)
            if s["proc"].is_alive():
                s["proc"].kill()
                s["proc"].join(timeout=1)
    del sessions[name]
    return True

def send_session(name, msg, timeout=30):
    """Send one AI command to a session and wait for its response."""
    s = sessions[name]
    if s["type"] == "pty":
        try:
            if "ai_wf" not in s:
                s["ai_rf"] = s["ai"].makefile("r")
                s["ai_wf"] = s["ai"].makefile("w")
            s["ai_wf"].write(json.dumps(msg) + "\n")
            s["ai_wf"].flush()
            line = s["ai_rf"].readline()
            if not line:
                return {"error": f"session '{name}' dead -- k new {name} to restart"}
            return json.loads(line)
        except (OSError, json.JSONDecodeError) as e:
            return {"error": str(e)}
    else:
        if not s["proc"].is_alive():
            return {"error": f"session '{name}' dead -- k new {name} to restart"}
        s["tx"].send(msg)
        if s["rx"].poll(timeout):
            return s["rx"].recv()
        return {"error": "timeout"}

def handle_client(cmd, args):
    """Handle one daemon control command from a client process."""
    if cmd == "new":
        if not args:
            return "ERR usage: k new <name>"
        name = args[0]
        if len(args) > 1:
            return (f"ERR k new takes a name only"
                    f" (got extra: {' '.join(args[1:])})."
                    f" sessions are always Python")
        new_session(name)
        s = sessions[name]
        return f"OK {name} pid={s['proc'].pid}"

    elif cmd == "kill":
        if not args:
            return "ERR usage: k kill <name>"
        name = args[0]
        if kill_session(name):
            return f"OK killed {name}"
        return f"ERR no session '{name}'"

    elif cmd == "repl_port":
        if not args:
            return "ERR usage: k attach <name>"
        name = args[0]
        if name not in sessions:
            return f"ERR no session '{name}' -- k new {name}"
        return str(sessions[name]["repl_port"])

    elif cmd == "ls":
        lines = []
        for n, s in sessions.items():
            if s["type"] == "pty":
                alive = "DEAD" if s["proc"].poll() is not None else "alive"
                lines.append(f"  {n}: {alive} pid={s['proc'].pid} (pty)")
            else:
                alive = "alive" if s["proc"].is_alive() else "DEAD"
                lines.append(f"  {n}: {alive} pid={s['proc'].pid}")
        return "\n".join(lines) or "(no sessions)"

    elif cmd in ("run", "fire", "poll", "status", "vars", "complete"):
        if not args:
            return "ERR need session name"
        name = args[0]
        if name not in sessions:
            return f"ERR no session '{name}' -- k new {name}"
        inner_args = args[1:]
        if cmd in ("run", "fire") and inner_args:
            code_str = inner_args[0]
            lines = code_str.strip().splitlines()
            pfx = f"{name}>>> " if len(sessions) > 1 else ">>> "
            cont = "." * len(pfx.rstrip()) + " "
            for i, ln in enumerate(lines):
                print(f"{pfx if i == 0 else cont}{ln}", file=sys.stderr)
        resp = send_session(name, {"cmd": cmd, "args": inner_args})
        if isinstance(resp, dict):
            if list(resp.keys()) == ["output"]:
                result = resp["output"]
                if cmd == "run" and result:
                    print(result, file=sys.stderr)
                return result
            return json.dumps(resp)
        return str(resp)

    return f"ERR unknown: {cmd}"

def daemon():
    """Run the daemon event loop until interrupted.

    The daemon owns session processes and control sockets.  It is intentionally
    foreground-friendly: stderr shows token/mode information and echoes AI-run
    cells so humans can observe what the agent is doing.
    """
    global _daemon_token
    srv = _server_socket()
    srv.listen(8)
    addr = SOCK if _HAS_AF_UNIX else f"127.0.0.1:{os.environ.get('K_PORT', '7399')}"
    mode = "pty" if _HAS_PTY else "socket"
    if not _HAS_AF_UNIX:
        _daemon_token = secrets.token_hex(16)
        print(f"k daemon pid={os.getpid()} {addr} mode={mode} token={_daemon_token}",
              file=sys.stderr)
        print(f"export K_TOKEN={_daemon_token}", file=sys.stderr)
    else:
        print(f"k daemon pid={os.getpid()} {addr} mode={mode}", file=sys.stderr)

    try:
        while True:
            conn, _ = srv.accept()
            data = b""
            while True:
                chunk = conn.recv(8192)
                if not chunk:
                    break
                data += chunk
            try:
                msg = json.loads(data.decode())
                if not _HAS_AF_UNIX:
                    if msg.get("token") != _daemon_token:
                        conn.sendall(b"ERR auth failed")
                        conn.close()
                        continue
                resp = handle_client(msg["cmd"], msg.get("args", []))
            except Exception as e:
                resp = f"ERR {e}"
            conn.sendall((resp or "").encode())
            conn.close()
    except KeyboardInterrupt:
        print("\nk stopped", file=sys.stderr)
    finally:
        for name in list(sessions):
            kill_session(name)
        srv.close()
        if _HAS_AF_UNIX and os.path.exists(SOCK):
            os.unlink(SOCK)

# =============================================
# CLIENT
# =============================================

def _send(cmd, args):
    """Send one command to daemon, return response string."""
    try:
        s = _client_socket()
    except (ConnectionRefusedError, FileNotFoundError, OSError):
        return None
    msg = {"cmd": cmd, "args": args}
    if not _HAS_AF_UNIX:
        token = os.environ.get("K_TOKEN")
        if token:
            msg["token"] = token
    s.sendall(json.dumps(msg).encode())
    s.shutdown(socket.SHUT_WR)
    resp = b""
    while True:
        chunk = s.recv(8192)
        if not chunk:
            break
        resp += chunk
    s.close()
    return resp.decode()

def client(cmd, args):
    """CLI client for non-interactive commands."""
    resp = _send(cmd, args)
    if resp is None:
        print("ERR daemon not running -- start: k daemon", file=sys.stderr)
        sys.exit(1)
    if resp:
        print(resp)

def attach(name):
    """Connect a human terminal to a session REPL.

    POSIX attach is raw; Ctrl-] returns to the shell while the session stays
    alive.  Windows attach is line-based; ending stdin detaches.
    """
    resp = _send("repl_port", [name])
    if resp is None:
        print("ERR daemon not running", file=sys.stderr)
        return
    if resp.startswith("ERR"):
        print(resp, file=sys.stderr)
        return
    port = int(resp)
    if _HAS_PTY:
        _attach_pty(port)
    else:
        _attach_socket(port)

def _attach_pty(port):
    """Raw terminal: forward keystrokes to PTY, display output.
    Ctrl-] returns to the shell; the session stays alive."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(("127.0.0.1", port))
    old = termios.tcgetattr(sys.stdin)
    try:
        tty.setraw(sys.stdin)
        while True:
            r, _, _ = _sel.select([sys.stdin, s], [], [])
            if sys.stdin in r:
                data = os.read(sys.stdin.fileno(), 1024)
                if not data:
                    break
                if b'\x1d' in data:  # Ctrl-]
                    break
                s.sendall(data)
            if s in r:
                data = s.recv(4096)
                if not data:
                    break
                os.write(sys.stdout.fileno(), data)
    except (KeyboardInterrupt, OSError):
        pass
    finally:
        termios.tcsetattr(sys.stdin, termios.TCSADRAIN, old)
        print()
        s.close()

def _attach_socket(port):
    """Line-based attach for Windows InteractiveConsole over socket."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(("127.0.0.1", port))
    done = threading.Event()

    def _reader():
        try:
            while not done.is_set():
                data = s.recv(4096)
                if not data:
                    break
                sys.stdout.write(data.decode(errors="replace"))
                sys.stdout.flush()
        except OSError:
            pass
        done.set()

    threading.Thread(target=_reader, daemon=True).start()
    try:
        while not done.is_set():
            line = sys.stdin.readline()
            if not line:
                try:
                    s.shutdown(socket.SHUT_WR)
                except OSError:
                    pass
                done.wait(timeout=10)
                break
            s.sendall(line.encode())
    except (KeyboardInterrupt, OSError):
        pass
    finally:
        done.set()
        try:
            s.close()
        except OSError:
            pass

# =============================================

if __name__ == "__main__":
    try:
        mp.set_start_method("fork", force=True)
    except ValueError:
        pass  # Windows: spawn is default
    argv = sys.argv[1:]
    if not argv or argv[0] in ("-h", "--help"):
        print(__doc__)
        sys.exit(0)
    if argv[0] == "_worker_pty":
        slave_fd = int(argv[1])
        ai_fd = int(argv[2])
        os.setsid()
        try:
            TIOCSCTTY = getattr(termios, 'TIOCSCTTY', 0x540E)
            fcntl.ioctl(slave_fd, TIOCSCTTY, 0)
        except (OSError, NameError):
            pass
        os.dup2(slave_fd, 0)
        os.dup2(slave_fd, 1)
        os.dup2(slave_fd, 2)
        if slave_fd > 2:
            os.close(slave_fd)
        ai_sock = socket.socket(fileno=ai_fd)
        session_worker_pty(ai_sock)
        sys.exit(0)
    if argv[0] == "daemon":
        daemon()
    elif argv[0] == "attach":
        name = argv[1] if len(argv) > 1 else "default"
        attach(name)
    else:
        client(argv[0], argv[1:])
