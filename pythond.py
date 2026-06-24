#!/usr/bin/env python3
"""pythond -- sshd gives you a shell. pythond gives you Python.

Persistent Python runtime daemon for agents and humans.
Code in, result out. No terminal. No ANSI. No parsing.

    pysh run work "x = 42"   ->  (sets x)
    pysh run work "x + 1"    ->  43

Variables, connections, threads survive between calls.  Connection != state.
Disconnect and reconnect -- namespace still alive.

AI agents use pysh as their Python runtime: one-shot bash_tool calls feed
code into a persistent namespace.  Humans use pysh attach for an interactive
REPL into the same namespace.  Both see the same objects.

The two-daemon proxy (pyctl connect) lets the local daemon reverse-proxy
to a remote pythond -- the agent sends code locally, it executes remotely.

Three entry points (pip install pythond):
  pythond    daemon lifecycle and all commands
  pysh       send code to sessions (local or remote, transparent)
  pyctl      manage the daemon (start, stop, proxy, certs)

Session commands (pysh):
    pysh new <name>              create a Python session
    pysh run <name> "code"       sync eval/exec, raw output
    pysh fire <name> "code"      async thread -- shares namespace, can't kill C
    pysh fork <name> "code"      async process (POSIX only) -- killable, pickles vars back
    pysh poll <name> [cell_id]   check async result
    pysh attach <name>           human REPL (Ctrl-] to detach)
    pysh int <name>              best-effort interrupt:
                                 fork cells are killed;
                                 fire cells get KeyboardInterrupt (Python only).
                                 Cannot stop run or C-stuck threads; use kill.
    pysh kill <name>             terminate session
    pysh ls                      list sessions
    pysh status <name>           session health (JSON)
    pysh vars <name>             namespace names (JSON)
    pysh complete <name> "text"  tab completion (JSON)

Daemon commands (pyctl / pythond):
    pythond daemon [--listen HOST:PORT] [--tls] [--show-token]
    pyctl start [--listen HOST:PORT] [--tls] [--show-token]
    pyctl stop
    pyctl status
    pyctl connect <name> <host:port> <token> [--tls]
                                 tell daemon to proxy to remote pythond
    pyctl disconnect <name>      drop remote proxy connection
    pyctl cert                   generate/show this machine's TLS cert
    pyctl trust <cert.pem>       let this client connect (server-side)
    pyctl pin <cert.pem>         verify this server is real (client-side)

Protocol:
  WebSocket text frames.  First line = command + args (space-separated).
  After first newline = code body (Python source, never escaped).
  Example: "run work\\nprint('hello')" -> "hello"
  Keep-alive: multiple commands per WebSocket connection.

Transport:
  Local POSIX:   ws:// over AF_UNIX ($XDG_RUNTIME_DIR/pythond.sock) -- socket perms, no token.
  Local Windows: ws://127.0.0.1:PORT -- token auth via daemon.json.
  Remote:        wss://HOST:PORT -- token auth, optional mTLS (mutual TLS).

Security (same model as SSH):
  Not a sandbox: code runs with the daemon user's OS permissions.
  Once authenticated, full access to all sessions -- no per-session isolation.
  Local POSIX:   AF_UNIX socket mode 0o600.
  Local Windows: OWNER RIGHTS DACL via icacls (owner-level isolation, comparable to Unix chmod 700).
  Remote token:  wss:// + shared token (symmetric, password-like).
  Remote mTLS:   wss:// + mutual cert verification, plus token.
    pyctl trust  = authorized_keys (server lets client in).
    pyctl pin    = known_hosts (client verifies server).
  Access logs:   ACCESS lines go to access.log and stderr. Fields are
                 event-specific; command/result lines include conn_id, peer,
                 cmd, session, status/body_bytes as applicable, never code.
  Crash containment: per-session worker processes; daemon tries to reap failed sessions.

Auto-checkpoint:
  ~/.pythond/sessions/<name>/history.py -- successful sync execs, plus async
    execs when poll observes completion; replayable.
  ~/.pythond/sessions/<name>/session.log -- all activity including errors.
  $runtime/pythond/access.log -- daemon access log, no tokens or code bodies.

Output formats:
    new, kill, stop, ls        text
    run                        raw captured output
    fire, fork, poll, status,
    vars, complete             JSON
    int                        JSON (worker) -> text (pysh)
    attach                     interactive stream

JSON responses:
    fire  -> {"cell_id": "abc123", "status": "fired"}
    fork  -> {"cell_id": "abc123", "status": "forked"}
    poll (running)  -> {"cell_id": "abc123", "status": "running", "output": ""}
    poll (fire done) -> {"cell_id": "abc123", "status": "done", "output": "42"}
    poll (fork done) -> {"cell_id": "abc123", "status": "done", "output": "42",
                        "merged": ["model", "df"], "skipped": ["db_conn"]}
    int   -> text ("OK int ..."), internally:
            {"threads": 1, "processes": 1,
            "note": "thread interrupts are best-effort; fork processes are hard-killed"}

fire vs fork:
    fire = threading.Thread.  Shares namespace -- fire'd code can set variables
    that later calls read.  Cannot be killed when stuck in C code.
    Exec is serialized (one cell at a time) -- async to the client, not parallel.
    fork = os.fork() child process (POSIX only).  Gets a COW copy of namespace.
    Killable (SIGKILL).  New/changed vars are pickled back and merged.
    Unpicklable objects (sockets, locks, file handles) are skipped.
    In-place mutations (list.append, dict[k]=v) won't merge -- use assignment.
    Merge is last-writer-wins: a completed fork may overwrite variables changed
    in the parent while the fork was running.
    Forking after native thread runtimes are initialized (CUDA, OpenMP, BLAS)
    is risky; use fork early or kill/recreate the session if it wedges.
"""
from __future__ import annotations

import sys, os, socket, json, threading, uuid, io, traceback, time, tempfile, code
import argparse
import collections
import select
import signal, subprocess
import pickle
import secrets
import hmac
import re
import stat
import ctypes
import itertools
import contextlib
import typing
import queue
import wsproto
from wsproto import ConnectionType
from wsproto import events as ws_events
from wsproto.utilities import LocalProtocolError

__version__ = "0.3.0"
JsonDict = dict[str, typing.Any]
WebSocketLike = typing.Any
SocketLike = typing.Any

def _protocol_version(version: str) -> str:
    """Return the WebSocket protocol version as major.minor."""
    match = re.match(r"^(\d+)\.(\d+)(?:\.|$)", version)
    if not match:
        raise RuntimeError(f"invalid version: {version}")
    return f"{match.group(1)}.{match.group(2)}"

_WS_PROTO: typing.Any = f"pythond.{_protocol_version(__version__)}"
_WS_HELLO = "tis but a scratch"
_MAX_SESSIONS = int(os.environ.get("PYTHOND_MAX_SESSIONS", "128"))
_MAX_WS_PAYLOAD = int(os.environ.get("PYTHOND_MAX_WS_PAYLOAD", str(16 * 1024 * 1024)))
_MAX_WORKER_RESPONSE = int(os.environ.get("PYTHOND_MAX_WORKER_RESPONSE", str(16 * 1024 * 1024)))
_MAX_TLS_BRIDGE_THREADS = int(os.environ.get("PYTHOND_MAX_TLS_BRIDGE_THREADS", "256"))
_TLS_BRIDGE_IO_TIMEOUT = float(os.environ.get("PYTHOND_TLS_BRIDGE_IO_TIMEOUT", "30"))
_SESSION_NAME_RE = re.compile(r"^[A-Za-z0-9_.-]{1,80}$")
_BUFFER_CHUNK = 64 * 1024
_WIN_FILE_ATTRIBUTE_REPARSE_POINT = 0x0400
_ASYNC_CELL_TTL = 300
_ATTACH_READ_SIZE = 1024
_WIN_ENABLE_VIRTUAL_TERMINAL_INPUT = 0x0200
_WIN_ENABLE_PROCESSED_OUTPUT = 0x0001
_WIN_ENABLE_WRAP_AT_EOL_OUTPUT = 0x0002
_WIN_ENABLE_VIRTUAL_TERMINAL_PROCESSING = 0x0004
_WS_CLOSE = object()
_ACCESS_STDERR_QUEUE: queue.Queue[str] = queue.Queue(maxsize=1024)
_ACCESS_STDERR_STARTED = False
_ACCESS_STDERR_LOCK = threading.Lock()
_ACCESS_CONN_SEQ = itertools.count(1)
_CELL_SEQ = itertools.count()
_INTERRUPT_LOCK = threading.Lock()
_WORKER_SPAWN_LOCK = threading.Lock()
_WORKER_ENV = "PYTHOND_INTERNAL_WORKER"
_SET_ASYNC_EXC: typing.Any = ctypes.pythonapi.PyThreadState_SetAsyncExc
_SET_ASYNC_EXC.restype = ctypes.c_int

_HAS_AF_UNIX = sys.platform != "win32" and hasattr(socket, "AF_UNIX")
_HAS_PTY = False
_WinPty = None  # reassigned by conditional import below
if sys.platform != "win32":
    try:
        import pty, tty, termios, fcntl
        import select as _sel
        _HAS_PTY = True
    except ImportError:
        pass  # optional module -- feature disabled without it
else:
    try:
        from winpty import PtyProcess as _WinPty  # type: ignore[no-redef]
        _HAS_PTY = True
    except ImportError:
        pass  # optional module -- feature disabled without it

def _default_sock() -> str:
    """Default AF_UNIX socket path.

    Prefers $XDG_RUNTIME_DIR/pythond.sock (/run/user/$UID/, mode 0o700).
    Falls back to private $TMPDIR/pythond-$UID/pythond.sock.
    """
    xdg = os.environ.get("XDG_RUNTIME_DIR")
    if xdg and os.path.isdir(xdg):
        return os.path.join(xdg, "pythond.sock")
    uid = os.getuid() if hasattr(os, "getuid") else os.getpid()
    return os.path.join(tempfile.gettempdir(), f"pythond-{uid}", "pythond.sock")

SOCK = os.environ.get("PYTHOND_SOCK", _default_sock())

# -----------------------------------------------
# SESSION HISTORY (auto-checkpoint)
# -----------------------------------------------

def _validate_session_name(name: str) -> str:
    """Validate a session/proxy name before it becomes a filesystem path."""
    if (not isinstance(name, str) or not _SESSION_NAME_RE.fullmatch(name)
            or ".." in name
            or name in (".", "..")
            or name.rstrip(" .") != name):
        raise ValueError("invalid session name")
    return name

def _public_error(e: BaseException) -> str:
    """Return a short client-facing error that does not expose host paths."""
    msg = str(e)
    if isinstance(e, ValueError):
        return msg
    if isinstance(e, RuntimeError) and "/" not in msg and "\\" not in msg:
        return msg
    return e.__class__.__name__

def _format_peer(peer: typing.Any) -> str:
    """Format a WebSocket peer for logs without raising."""
    if peer is None:
        return "-"
    if isinstance(peer, tuple):
        return ":".join(str(x) for x in peer)
    return str(peer)

def _utc_timestamp_ms() -> str:
    """Return an RFC3339-like UTC timestamp with millisecond precision."""
    ns = time.time_ns()
    sec = ns // 1_000_000_000
    ms = (ns // 1_000_000) % 1000
    return f"{time.strftime('%Y-%m-%dT%H:%M:%S', time.gmtime(sec))}.{ms:03d}Z"

def _access_stderr_worker() -> None:
    """Drain access log lines to stderr without blocking request threads."""
    try:
        fd = sys.stderr.fileno()
    except (AttributeError, OSError, ValueError):
        fd = None
    while True:
        line = _ACCESS_STDERR_QUEUE.get()
        data = line.encode("utf-8", "replace")
        try:
            if fd is None:
                sys.stderr.write(line)
                sys.stderr.flush()
            else:
                os.write(fd, data)
        except Exception:
            # stderr itself is broken or blocked; access.log remains durable.
            pass

def _ensure_access_stderr_worker() -> None:
    """Start the bounded stderr mirror worker once."""
    global _ACCESS_STDERR_STARTED
    if _ACCESS_STDERR_STARTED:
        return
    with _ACCESS_STDERR_LOCK:
        if _ACCESS_STDERR_STARTED:
            return
        t = threading.Thread(target=_access_stderr_worker, daemon=True)
        t.start()
        _ACCESS_STDERR_STARTED = True

def _mirror_access_log_to_stderr(line: str) -> None:
    """Best-effort stderr mirror without letting stderr block the daemon."""
    _ensure_access_stderr_worker()
    try:
        _ACCESS_STDERR_QUEUE.put_nowait(line)
    except queue.Full:
        pass

def _access_log(
    event: str,
    *,
    conn_id: int | None = None,
    peer: typing.Any = None,
    cmd: str | None = None,
    session: str | None = None,
    status: str | None = None,
    body_bytes: int | None = None,
    detail: str | None = None,
) -> None:
    """Emit one access log line for daemon operations.

    This daemon executes arbitrary Python.  Access logs must prove what surface
    was used without leaking tokens or code bodies.  The file is durable local
    evidence; stderr gives supervisors and service managers the live stream.
    """
    def _field(value: str) -> str:
        return value if _SESSION_NAME_RE.fullmatch(value) else "invalid"

    fields = [
        "ACCESS",
        f"ts={_utc_timestamp_ms()}",
        f"event={event}",
    ]
    if conn_id is not None:
        fields.append(f"conn_id={conn_id}")
    fields.append(f"peer={_format_peer(peer)}")
    if cmd is not None:
        fields.append(f"cmd={cmd or '-'}")
    if session is not None:
        fields.append(f"session={_field(session) if session else '-'}")
    if status is not None:
        fields.append(f"status={status}")
    if body_bytes is not None:
        fields.append(f"body_bytes={body_bytes}")
    if detail is not None:
        fields.append(f"detail={detail}")
    line = " ".join(fields) + "\n"
    try:
        path = os.path.join(_runtime_dir(), "access.log")
        flags = os.O_WRONLY | os.O_CREAT | os.O_APPEND
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        fd = os.open(path, flags, 0o600) if sys.platform != "win32" else os.open(path, flags)
        try:
            os.write(fd, line.encode("utf-8", "replace"))
            if sys.platform != "win32":
                os.fchmod(fd, 0o600)
        finally:
            os.close(fd)
    except OSError as e:
        print(f"WARN: access log failed: {e}", file=sys.stderr)
    _mirror_access_log_to_stderr(line)

def _ensure_private_dir(path: str) -> str:
    """Create a daemon data directory and restrict it to the current user."""
    created = not os.path.isdir(path)
    os.makedirs(path, exist_ok=True)
    if sys.platform == "win32":
        attrs = ctypes.windll.kernel32.GetFileAttributesW(str(path))
        if attrs != 0xFFFFFFFF and attrs & _WIN_FILE_ATTRIBUTE_REPARSE_POINT:
            raise RuntimeError(f"insecure directory: reparse point: {path}")
        _secure_path_win32(path)
    else:
        st = os.lstat(path)
        if not stat.S_ISDIR(st.st_mode) or st.st_uid != os.getuid():
            raise RuntimeError(f"insecure directory: {path}")
        try:
            os.chmod(path, 0o700)
        except OSError:
            raise RuntimeError(f"cannot secure directory: {path}")
    return path

def _session_dir(name: str) -> str:
    """Return ~/.pythond/sessions/<name>/, creating if needed."""
    _validate_session_name(name)
    home = os.path.expanduser("~")
    pythond_home = os.path.join(home, ".pythond")
    _ensure_private_dir(pythond_home)
    sessions_home = _ensure_private_dir(os.path.join(pythond_home, "sessions"))
    return _ensure_private_dir(os.path.join(sessions_home, name))

def _log_history(name: str, src: str) -> None:
    """Append successful exec source to history.py (replayable)."""
    try:
        path = os.path.join(_session_dir(name), "history.py")
        flags = os.O_WRONLY | os.O_APPEND | os.O_CREAT
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        fd = os.open(path, flags, 0o600)
        with os.fdopen(fd, "a", encoding="utf-8") as f:
            f.write(f"\n# [{time.strftime('%Y-%m-%d %H:%M:%S')}]\n{src}\n")
    except OSError as e:
        print(f"WARN: history log failed for {name}: {e}", file=sys.stderr)

def _log_session(name: str, src: str, output: str = "", error: bool = False) -> None:
    """Append all exec activity to session.log (human readable)."""
    try:
        path = os.path.join(_session_dir(name), "session.log")
        flags = os.O_WRONLY | os.O_APPEND | os.O_CREAT
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        fd = os.open(path, flags, 0o600)
        with os.fdopen(fd, "a", encoding="utf-8") as f:
            tag = "ERROR" if error else "OK"
            f.write(f"\n# [{time.strftime('%Y-%m-%d %H:%M:%S')}] {tag}\n")
            f.write(f"{src}\n")
            if output:
                for line in output.splitlines():
                    f.write(f"# > {line}\n")
    except OSError as e:
        print(f"WARN: session log failed for {name}: {e}", file=sys.stderr)

# -----------------------------------------------
# SOCKET helpers
# -----------------------------------------------

def _secure_path_win32(path: str) -> None:
    """Restrict a Windows path to owner, SYSTEM, and Administrators.

    Strips inherited ACLs and grants full control only to OWNER RIGHTS,
    SYSTEM, and BUILTIN\\Administrators.  This is owner-level isolation
    (comparable to Unix chmod 700), NOT process-tree isolation -- any
    process running as the same user can still access the path.

    We do NOT rely on CPython's mode=0o700 DACL side effect (CVE-2024-4030);
    we set it explicitly via icacls so it works on any Python version.
    """
    try:
        subprocess.run([
            "icacls", path,
            "/inheritance:r",                         # remove inherited ACLs
            "/grant:r", "OWNER RIGHTS:(OI)(CI)(F)",   # owner = full
            "/grant:r", "SYSTEM:(OI)(CI)(F)",         # SYSTEM = full
            "/grant:r", "BUILTIN\\Administrators:(OI)(CI)(F)",
        ], check=True, capture_output=True, timeout=10)
    except (FileNotFoundError, subprocess.CalledProcessError,
            subprocess.TimeoutExpired) as e:
        print(f"WARN: cannot set DACL on {path}: {e.__class__.__name__}",
              file=sys.stderr)
        raise RuntimeError(f"cannot secure directory: {path}") from e

def _runtime_dir() -> str:
    """Return the private runtime directory for daemon metadata."""
    if sys.platform == "win32":
        base = os.environ.get("LOCALAPPDATA") or tempfile.gettempdir()
        path = _ensure_private_dir(os.path.join(base, "pythond"))
    else:
        base = os.environ.get("XDG_RUNTIME_DIR")
        if base:
            path = os.path.join(base, "pythond")
        else:
            path = os.path.join(tempfile.gettempdir(),
                                f"pythond-{os.getuid()}")
        path = _ensure_private_dir(path)
    return path

def _daemon_meta_path() -> str:
    return os.path.join(_runtime_dir(), "daemon.json")

def _tcp_daemon_alive(meta: JsonDict) -> bool:
    """Return True when daemon metadata points to a reachable pythond daemon."""
    try:
        port_raw = meta.get("port")
        if port_raw is None:
            return False
        port = int(port_raw)
        token = str(meta.get("token", ""))
    except (TypeError, ValueError):
        return False
    ws = None
    try:
        from websockets.sync.client import connect as ws_connect
        ws = ws_connect(f"ws://127.0.0.1:{port}/",
                        additional_headers=_auth_headers(token or None),
                        proxy=None,
                        open_timeout=2, close_timeout=1,
                        subprotocols=[_WS_PROTO])
        try:
            ws.send("ls")
            ws.recv(timeout=2)
        except Exception:
            pass
        return True
    except Exception:
        return False
    finally:
        if ws is not None:
            try:
                ws.close()
            except Exception:
                pass

def _unix_daemon_alive() -> bool:
    """Return True when the AF_UNIX daemon socket accepts a command."""
    if not os.path.exists(SOCK):
        return False
    try:
        from websockets.sync.client import unix_connect as ws_unix_connect
        ws = ws_unix_connect(SOCK, open_timeout=2, close_timeout=1,
                             subprotocols=[_WS_PROTO])
        try:
            ws.send("ls")
            ws.recv(timeout=2)
            return True
        finally:
            ws.close()
    except Exception:
        return False

def _auth_headers(token: str | None) -> dict[str, str] | None:
    """Return WebSocket auth headers without putting credentials in URLs."""
    if not token:
        return None
    return {"Authorization": f"Bearer {token}"}

def _write_daemon_meta(port: int, token: str) -> None:
    """Persist daemon connection metadata for local client discovery."""
    path = _daemon_meta_path()
    existing = _read_daemon_meta()
    if existing and _tcp_daemon_alive(existing):
        pid = existing.get("pid", "?")
        old_port = existing.get("port", "?")
        raise RuntimeError(
            "daemon metadata already points to live daemon "
            f"pid={pid} port={old_port}; stop it before starting another "
            "auto-discoverable TCP daemon")
    tmp = path + ".tmp"
    data = {"port": int(port), "token": token, "pid": os.getpid()}
    flags = os.O_WRONLY | os.O_CREAT | os.O_TRUNC
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    # On Unix, set file mode 0o600.  On Windows, skip -- parent dir DACL
    # (set by _secure_path_win32) protects the file via inheritance.
    try:
        fd = os.open(tmp, flags, 0o600) if sys.platform != "win32" else os.open(tmp, flags)
        try:
            os.write(fd, json.dumps(data).encode())
            os.fsync(fd)
        finally:
            os.close(fd)
        if sys.platform != "win32":
            os.chmod(tmp, 0o600)
        os.replace(tmp, path)
    except Exception:
        if os.path.exists(tmp):
            try:
                os.remove(tmp)
            except OSError as e:
                print(f"WARN: cannot remove temp daemon metadata {tmp}: {e}",
                      file=sys.stderr)
        raise

def _read_daemon_meta() -> JsonDict:
    """Read daemon metadata, returning {} when absent or invalid."""
    try:
        flags = os.O_RDONLY
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        fd = os.open(_daemon_meta_path(), flags)
        with os.fdopen(fd, encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return {}
    if not isinstance(data, dict):
        return {}
    return data

def _remove_daemon_meta() -> None:
    meta = _read_daemon_meta()
    if meta.get("pid") != os.getpid():
        return
    try:
        os.remove(_daemon_meta_path())
    except FileNotFoundError:
        pass  # already gone -- nothing to remove
    except OSError as e:
        print(f"WARN: cannot remove {_daemon_meta_path()}: {e}",
              file=sys.stderr)

# -----------------------------------------------
# TLS (optional, for --listen remote mode)
#   pip install pythond  ->  adds cryptography
# -----------------------------------------------

import ssl as _ssl
import hashlib as _hashlib
import ipaddress as _ipaddress

try:
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.hazmat.primitives import serialization, hashes
    from cryptography import x509
    from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID
    import datetime as _dt
    _HAS_CRYPTO = True
except ImportError:
    _HAS_CRYPTO = False

def _tls_dir() -> str:
    """Return ~/.pythond/tls/, creating if needed."""
    home = _ensure_private_dir(os.path.join(os.path.expanduser("~"), ".pythond"))
    return _ensure_private_dir(os.path.join(home, "tls"))

def _generate_cert() -> tuple[str, str]:
    """Auto-generate self-signed RSA cert+key. Returns (cert_path, key_path).

    Requires `pip install pythond` (cryptography package).
    """
    if not _HAS_CRYPTO:
        raise RuntimeError(
            "TLS requires the cryptography package: pip install pythond")
    d = _tls_dir()
    cert_path = os.path.join(d, "cert.pem")
    key_path = os.path.join(d, "key.pem")
    if (os.path.exists(cert_path) and os.path.exists(key_path)
            and os.path.getsize(cert_path) > 0
            and os.path.getsize(key_path) > 0):
        if _cert_key_pair_valid(cert_path, key_path):
            return cert_path, key_path
        print("WARN: TLS cert/key mismatch; regenerating", file=sys.stderr)
        for fpath in (cert_path, key_path):
            with contextlib.suppress(OSError):
                os.unlink(fpath)

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    subject = issuer = x509.Name([
        x509.NameAttribute(NameOID.COMMON_NAME, "pythond"),
    ])
    cert = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(issuer)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(_dt.datetime.now(_dt.timezone.utc))
        .not_valid_after(_dt.datetime.now(_dt.timezone.utc) + _dt.timedelta(days=3650))
        .add_extension(
            x509.SubjectAlternativeName([
                x509.DNSName("localhost"),
                x509.DNSName("pythond"),
                x509.IPAddress(_ipaddress.ip_address("127.0.0.1")),
                x509.IPAddress(_ipaddress.ip_address("::1")),
            ]),
            critical=False,
        )
        .add_extension(
            x509.BasicConstraints(ca=False, path_length=None),
            critical=True,
        )
        .add_extension(
            x509.KeyUsage(
                digital_signature=True,
                content_commitment=False,
                key_encipherment=True,
                data_encipherment=False,
                key_agreement=False,
                key_cert_sign=False,
                crl_sign=False,
                encipher_only=False,
                decipher_only=False,
            ),
            critical=True,
        )
        .add_extension(
            x509.ExtendedKeyUsage([
                ExtendedKeyUsageOID.SERVER_AUTH,
                ExtendedKeyUsageOID.CLIENT_AUTH,
            ]),
            critical=False,
        )
        .sign(key, hashes.SHA256())
    )

    key_pem = key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    )
    cert_pem = cert.public_bytes(serialization.Encoding.PEM)

    tmp_key = key_path + ".tmp"
    tmp_cert = cert_path + ".tmp"
    try:
        for fpath, data, mode in [
            (tmp_key, key_pem, 0o600),
            (tmp_cert, cert_pem, 0o644),
        ]:
            fd = os.open(fpath,
                         os.O_WRONLY | os.O_CREAT | os.O_TRUNC,
                         mode)
            try:
                os.write(fd, data)
                os.fsync(fd)
            finally:
                os.close(fd)
            if sys.platform != "win32":
                os.chmod(fpath, mode)
        os.replace(tmp_key, key_path)
        os.replace(tmp_cert, cert_path)
    except Exception:
        for fpath in (tmp_key, tmp_cert, key_path, cert_path):
            with contextlib.suppress(OSError):
                os.unlink(fpath)
        raise

    return cert_path, key_path

def _cert_key_pair_valid(cert_path: str, key_path: str) -> bool:
    """Return whether an existing cert.pem matches key.pem."""
    try:
        with open(cert_path, "rb") as f:
            cert = x509.load_pem_x509_certificate(f.read())
        with open(key_path, "rb") as f:
            key = serialization.load_pem_private_key(f.read(), password=None)
        cert_public = typing.cast(typing.Any, cert.public_key())
        key_public = typing.cast(typing.Any, key.public_key())
        return cert_public.public_numbers() == key_public.public_numbers()
    except (AttributeError, NameError, OSError, TypeError, ValueError):
        return False

def _cert_fingerprint(cert_path: str) -> str:
    """Return SHA-256 fingerprint of cert for pinning."""
    try:
        with open(cert_path, "rb") as f:
            der = _ssl.PEM_cert_to_DER_cert(f.read().decode())
        digest = _hashlib.sha256(der).hexdigest().upper()
        return ":".join(digest[i:i+2] for i in range(0, len(digest), 2))
    except (OSError, ValueError):
        return "unknown"

def _cert_fingerprint_der(der: bytes) -> str:
    """Return colon-separated SHA-256 fingerprint for DER certificate bytes."""
    digest = _hashlib.sha256(der).hexdigest().upper()
    return ":".join(digest[i:i+2] for i in range(0, len(digest), 2))

def _normalise_fingerprint(value: str) -> str:
    """Normalise a stored fingerprint to colon-separated uppercase hex."""
    raw = re.sub(r"[^0-9A-Fa-f]", "", value)
    if len(raw) != 64:
        return ""
    raw = raw.upper()
    return ":".join(raw[i:i+2] for i in range(0, len(raw), 2))

def _cert_ca_capable(cert_path: str) -> bool:
    """Return True when a cert can act as a CA or cannot be inspected."""
    if not _HAS_CRYPTO:
        return True
    try:
        with open(cert_path, "rb") as f:
            cert = x509.load_pem_x509_certificate(f.read())
        try:
            basic = cert.extensions.get_extension_for_class(
                x509.BasicConstraints
            ).value
            if basic.ca:
                return True
        except x509.ExtensionNotFound:
            pass
        try:
            usage = cert.extensions.get_extension_for_class(
                x509.KeyUsage
            ).value
            if usage.key_cert_sign or usage.crl_sign:
                return True
        except x509.ExtensionNotFound:
            pass
        return False
    except (OSError, ValueError):
        return True

def _trusted_fingerprints(directory: str) -> set[str]:
    """Load trusted exact peer certificate fingerprints from a directory."""
    trusted: set[str] = set()
    for filename in os.listdir(directory):
        path = os.path.join(directory, filename)
        if filename.endswith(".pem"):
            if _cert_ca_capable(path):
                print(f"warn: skipping CA-capable cert {filename}", file=sys.stderr)
                continue
            fp = _cert_fingerprint(path)
        else:
            try:
                with open(path, encoding="ascii") as f:
                    fp = _normalise_fingerprint(f.read().strip().split()[0])
            except (IndexError, OSError, UnicodeDecodeError):
                fp = ""
        if fp and fp != "unknown":
            trusted.add(fp)
    return trusted

def _verify_peer_fingerprint(
    sock: SocketLike,
    directory: str,
    role: str,
) -> str:
    """Verify peer cert fingerprint exactly; return fingerprint or raise."""
    trusted = _trusted_fingerprints(directory)
    if not trusted:
        raise RuntimeError(f"no trusted {role} fingerprints")
    der = sock.getpeercert(binary_form=True)
    if not der:
        raise RuntimeError(f"missing {role} certificate")
    fp = _cert_fingerprint_der(der)
    for expected in trusted:
        if hmac.compare_digest(fp, expected):
            return fp
    raise RuntimeError(f"untrusted {role} certificate fingerprint")

def _trusted_clients_dir() -> str:
    """Return ~/.pythond/tls/trusted_clients/ -- server trusts these clients."""
    return _ensure_private_dir(os.path.join(_tls_dir(), "trusted_clients"))

def _trusted_servers_dir() -> str:
    """Return ~/.pythond/tls/trusted_servers/ -- client trusts these servers."""
    return _ensure_private_dir(os.path.join(_tls_dir(), "trusted_servers"))

def _load_trusted_certs(ssl_ctx: _ssl.SSLContext, directory: str) -> int:
    """Load non-CA PEMs for TLS client-cert handshakes. Returns count."""
    count = 0
    for f in os.listdir(directory):
        if f.endswith(".pem"):
            cert_path = os.path.join(directory, f)
            if _cert_ca_capable(cert_path):
                print(f"warn: skipping CA-capable cert {f}", file=sys.stderr)
                continue
            try:
                ssl_ctx.load_verify_locations(cert_path)
                count += 1
            except (_ssl.SSLError, OSError):
                print(f"warn: skipping malformed cert {f}", file=sys.stderr)
    return count

def trust_cert(cert_path: str, direction: str = "client") -> tuple[str, str]:
    """Copy a cert into the appropriate trusted dir.

    direction="client" -> server trusts this client (pyctl trust)
    direction="server" -> client trusts this server (pyctl pin)
    """
    if direction not in ("client", "server"):
        raise ValueError("direction must be 'client' or 'server'")
    td = _trusted_clients_dir() if direction == "client" else _trusted_servers_dir()
    fp = _cert_fingerprint(cert_path)
    if fp == "unknown":
        raise RuntimeError("invalid certificate")
    if _cert_ca_capable(cert_path):
        raise RuntimeError("refusing CA-capable certificate")
    name = fp.replace(":", "")[:16] + ".pem"
    dest = os.path.join(td, name)
    fp_dest = os.path.join(td, name + ".fingerprint")
    import shutil
    shutil.copy2(cert_path, dest)
    fd = os.open(fp_dest, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    try:
        os.write(fd, (fp + "\n").encode("ascii"))
        os.fsync(fd)
    finally:
        os.close(fd)
    if sys.platform != "win32":
        os.chmod(fp_dest, 0o600)
    return dest, fp


class _Servable(typing.Protocol):
    """Duck-type protocol for daemon server objects.

    Both websockets Server and _TlsTerminatedServer expose serve_forever()
    and shutdown().  This protocol lets mypy verify structural compatibility
    without inheritance.
    """
    def serve_forever(self) -> None: ...
    def shutdown(self) -> None: ...


class _TlsTerminatedServer:
    """TLS front-end that forwards plaintext WebSocket bytes internally.

    websockets.sync.server.serve(..., ssl=ctx) can fail the opening handshake on
    some Windows/Python/OpenSSL stacks. This keeps the proven plaintext
    WebSocket server as the protocol engine and uses Python ssl only as a
    byte-stream terminator.
    """

    def __init__(
        self,
        ws_serve: typing.Callable[..., typing.Any],
        handler: typing.Callable[..., typing.Any],
        host: str,
        port: int,
        ssl_ctx: _ssl.SSLContext,
        subprotocols: list[str],
        trusted_client_dir: str | None = None,
    ) -> None:
        self._stopped = threading.Event()
        self._ssl_ctx = ssl_ctx
        self._trusted_client_dir = trusted_client_dir
        self._inner = ws_serve(handler, "127.0.0.1", 0,
                               subprotocols=subprotocols)
        try:
            inner_addr = self._inner.socket.getsockname()
            self._inner_port = inner_addr[1]
            self._sock = socket.create_server((host, port))
            self._sock.settimeout(1.0)
            self._bridge_threads: list[threading.Thread] = []
            self._inner_thread: threading.Thread | None = None
        except Exception:
            self._inner.shutdown()
            raise

    def serve_forever(self) -> None:
        self._inner_thread = threading.Thread(target=self._inner.serve_forever,
                                              daemon=True)
        self._inner_thread.start()
        while not self._stopped.is_set():
            self._reap_threads()
            try:
                raw, _addr = self._sock.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            if len(self._bridge_threads) >= _MAX_TLS_BRIDGE_THREADS:
                raw.close()
                continue
            worker = threading.Thread(target=self._handle, args=(raw,),
                                      daemon=True)
            worker.start()
            self._bridge_threads.append(worker)

    def _reap_threads(self) -> None:
        """Drop finished bridge threads so accepted connections do not leak."""
        self._bridge_threads = [
            t for t in self._bridge_threads if t.is_alive()
        ]

    def shutdown(self) -> None:
        self._stopped.set()
        try:
            self._sock.close()
        except OSError:
            pass
        try:
            self._inner.shutdown()
        except Exception as e:
            print(f"warn: TLS bridge failed: {type(e).__name__}", file=sys.stderr)

    def _handle(self, raw: socket.socket) -> None:
        tls_sock = None
        inner_sock = None
        try:
            tls_sock = self._ssl_ctx.wrap_socket(raw, server_side=True)
            if self._trusted_client_dir is not None:
                _verify_peer_fingerprint(tls_sock, self._trusted_client_dir,
                                         "client")
            inner_sock = socket.create_connection(("127.0.0.1",
                                                   self._inner_port))
            self._bridge(tls_sock, inner_sock)
        except Exception as e:
            print(f"WARN: TLS connection failed: {e.__class__.__name__}",
                  file=sys.stderr)
        finally:
            sockets: tuple[SocketLike | None, ...] = (
                tls_sock,
                inner_sock,
                None if tls_sock is not None else raw,
            )
            for sock in sockets:
                if sock is None:
                    continue
                try:
                    sock.close()
                except OSError:
                    pass

    def _bridge(self, tls_sock: SocketLike, inner_sock: socket.socket) -> None:
        tls_sock.setblocking(False)
        inner_sock.setblocking(False)
        peers = {tls_sock: inner_sock, inner_sock: tls_sock}
        while not self._stopped.is_set():
            try:
                readable = [tls_sock] if tls_sock.pending() else []
                if readable:
                    inner_readable, _, _ = select.select([inner_sock], [], [], 0)
                    readable.extend(inner_readable)
                else:
                    readable, _, _ = select.select(list(peers), [], [], 1.0)
            except (OSError, ValueError):
                return
            for src in readable:
                try:
                    data = src.recv(_BUFFER_CHUNK)
                except _ssl.SSLWantReadError:
                    readable_again, _, _ = select.select([src], [], [],
                                                         _TLS_BRIDGE_IO_TIMEOUT)
                    if not readable_again:
                        return
                    continue
                except (_ssl.SSLWantWriteError, BlockingIOError):
                    _, writable_again, _ = select.select([], [src], [],
                                                        _TLS_BRIDGE_IO_TIMEOUT)
                    if not writable_again:
                        return
                    continue
                except (OSError, _ssl.SSLError):
                    return
                if not data:
                    return
                if not self._send_all(peers[src], data):
                    return

    @staticmethod
    def _send_all(sock: SocketLike, data: bytes) -> bool:
        view = memoryview(data)
        deadline = time.monotonic() + _TLS_BRIDGE_IO_TIMEOUT
        try:
            while view:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                try:
                    sent = sock.send(view)
                    if sent == 0:
                        return False
                    view = view[sent:]
                except _ssl.SSLWantReadError:
                    readable, _, _ = select.select([sock], [], [],
                                                   min(1.0, remaining))
                    if not readable:
                        return False
                except (_ssl.SSLWantWriteError, BlockingIOError):
                    _, writable, _ = select.select([], [sock], [],
                                                   min(1.0, remaining))
                    if not writable:
                        return False
            return True
        except (OSError, _ssl.SSLError):
            return False

# =============================================
# SHARED WORKER LOGIC
# =============================================

def _init_namespace() -> JsonDict:
    """Create the persistent Python namespace for one session.

    The imports here are convenience imports for agent cells.  Code running in
    a session has normal Python process permissions.
    """
    ns = {"__builtins__": __builtins__}
    exec("import os,sys,json,subprocess,shutil,hashlib,time,re,glob,sqlite3,socket", ns)
    return ns

class _ThreadStdout:
    """Thread-local stdout wrapper.  Each thread captures to its own buffer.

    Main thread (exec cells): set _local.buf -> print captured to cell output.
    Sub-threads (user code): _local.buf is None -> print goes to real stdout.
    Prevents child thread output from bleeding into another cell's capture.
    """
    def __init__(self, real: typing.TextIO) -> None:
        self._real = real
        self._local = threading.local()
    def write(self, s: str) -> typing.Any:
        buf = getattr(self._local, "buf", None)
        return (buf or self._real).write(s)
    def writelines(self, lines: typing.Iterable[str]) -> None:
        buf = getattr(self._local, "buf", None)
        (buf or self._real).writelines(lines)
    def flush(self) -> None:
        buf = getattr(self._local, "buf", None)
        (buf or self._real).flush()
    def fileno(self) -> int:
        return self._real.fileno()
    def isatty(self) -> bool:
        return self._real.isatty()
    @property
    def encoding(self) -> str:
        return self._real.encoding

import ast as _ast

class _ExecOutput(str):
    """String output with an internal execution-error flag."""
    error: bool
    def __new__(cls, value: str, error: bool = False) -> "_ExecOutput":
        obj = str.__new__(cls, value)
        obj.error = bool(error)
        return obj

def _eval_exec_cell(src: str, ns: JsonDict) -> None:
    """Run src in ns with REPL-like semantics.

    Single expression -> eval -> print result (str raw, else repr).
    Multi-line with last expression -> exec stmts, eval last, print.
    Multi-line ending in statement -> exec all, no auto-print.
    Exceptions are NOT caught here -- caller decides how to handle.
    """
    try:
        r = eval(compile(src, "<cell>", "eval"), ns)
        if r is not None:
            print(r if isinstance(r, str) else repr(r))
        return
    except SyntaxError:
        pass
    tree = _ast.parse(src, "<cell>")
    last = tree.body[-1] if tree.body else None
    if isinstance(last, _ast.Expr):
        stmts = _ast.Module(body=tree.body[:-1], type_ignores=[])
        _ast.fix_missing_locations(stmts)
        exec(compile(stmts, "<cell>", "exec"), ns)
        expr = _ast.Expression(body=last.value)
        _ast.fix_missing_locations(expr)
        r = eval(compile(expr, "<cell>", "eval"), ns)
        if r is not None:
            print(r if isinstance(r, str) else repr(r))
    else:
        _ast.fix_missing_locations(tree)
        exec(compile(tree, "<cell>", "exec"), ns)

def _make_exec(
    ns: JsonDict,
    lock: threading.Lock,
    on_done: typing.Callable[[str, str], None] | None = None,
) -> typing.Callable[[str], _ExecOutput]:
    """Build _exec(src): eval/exec in ns and return captured output.

    Uses _ThreadStdout for thread-safe capture: the exec thread's output
    goes to the cell buffer; child threads spawned by user code write to
    the real stdout instead of bleeding into another cell's buffer.

    on_done(src, output), when provided, broadcasts completed AI cells to an
    attached human REPL.
    """
    # Keep stable wrappers; user code can assign sys.stdout/sys.stderr.
    stdout_wrapper = (
        sys.stdout if isinstance(sys.stdout, _ThreadStdout)
        else _ThreadStdout(sys.stdout)
    )
    stderr_wrapper = (
        sys.stderr if isinstance(sys.stderr, _ThreadStdout)
        else _ThreadStdout(sys.stderr)
    )
    sys.stdout = stdout_wrapper
    sys.stderr = stderr_wrapper

    def _exec(src: str) -> _ExecOutput:
        with lock:
            buf = io.StringIO()
            had_error = False
            sys.stdout = stdout_wrapper
            sys.stderr = stderr_wrapper
            stdout = stdout_wrapper
            stderr = stderr_wrapper
            stdout._local.buf = buf
            stderr._local.buf = buf
            try:
                _eval_exec_cell(src, ns)
            except KeyboardInterrupt:
                had_error = True
                traceback.print_exc()
            except SystemExit as e:
                had_error = True
                code_val = e.code if e.code is not None else 0
                print(f"exit({code_val})")
            except Exception:
                had_error = True
                traceback.print_exc()
            finally:
                stdout._local.buf = None
                stderr._local.buf = None
                sys.stdout = stdout_wrapper
                sys.stderr = stderr_wrapper
            output = buf.getvalue().rstrip("\n")
            result = _ExecOutput(output, had_error)
        if on_done:
            try:
                on_done(src, output)
            except Exception:
                traceback.print_exc(file=sys.stderr)
        return result
    return _exec

# Per-session in practice: each session runs in its own subprocess,
# so each process gets its own copy of this lock and cells dict.
_cells_lock = threading.Lock()

def _evict_stale_cells(cells: dict[str, JsonDict]) -> None:
    """Remove cells done > 5 minutes ago.  Caller must hold _cells_lock."""
    now = time.time()
    stale = [k for k, v in cells.items()
             if v["status"] == "done"
             and now - v.get("_done_at", now) > _ASYNC_CELL_TTL]
    for k in stale:
        del cells[k]

def _write_all(fd: int, data: bytes) -> None:
    """Write all bytes to fd.  os.write() may do partial writes on large data."""
    view = memoryview(data)
    while view:
        n = os.write(fd, view)
        if n == 0:
            raise OSError("pipe write returned 0")
        view = view[n:]

@contextlib.contextmanager
def _locked(lock: threading.Lock | None) -> typing.Iterator[None]:
    if lock is None:
        yield
    else:
        with lock:
            yield

def _public_names(ns: JsonDict, lock: threading.Lock | None) -> list[str]:
    with _locked(lock):
        return [v for v in ns if not v.startswith("_")]

def _dispatch(
    cmd: str,
    args: list[str],
    _exec: typing.Callable[[str], _ExecOutput],
    cells: dict[str, JsonDict],
    ns: JsonDict,
    lock: threading.Lock | None = None,
) -> JsonDict:
    """Handle one AI protocol command inside a session.

    This function returns dictionaries only.  The daemon decides which commands
    are rendered as raw text versus JSON at the client boundary.
    lock, when provided, serializes fork merge with exec to prevent namespace races.
    """
    if cmd in ("run", "fire", "fork") and not args:
        return {"error": f"{cmd} requires code"}
    if cmd == "run":
        out = _exec(args[0])
        return {"output": str(out), "_error": bool(getattr(out, "error", False))}
    elif cmd == "fire":
        # threading.Thread, not os.fork() child process.
        # Thread shares the session namespace: fire'd code can set variables
        # (model = train(data)) that later run/fire calls can read.
        # Process would fork a copy -- writes to the copy don't propagate back.
        # Tradeoff: threads can't be force-killed when stuck in C code
        # (requests.get, time.sleep).  pysh kill (whole session) is the escape.
        if not args or not str(args[0]).strip():
            return {"error": "fire requires code"}
        cid = uuid.uuid4().hex[:12]
        res = {"output": "", "status": "running", "tid": None,
               "_seq": next(_CELL_SEQ)}
        def _bg(c: str = args[0], r: JsonDict = res) -> None:
            output = "(fire result failed)"
            error = True
            try:
                out = _exec(c)
                output = str(out)
                error = bool(getattr(out, "error", False))
            except BaseException:
                try:
                    output = traceback.format_exc().rstrip()
                except Exception:
                    output = "(traceback formatting failed)"
                error = True
            finally:
                with _cells_lock:
                    r["output"] = output
                    r["_error"] = error
                    r["status"] = "done"
                    r["_done_at"] = time.time()
                    r["tid"] = None
        t = threading.Thread(target=_bg, daemon=True)
        with _cells_lock:
            t.start()
            if res["status"] == "running":
                res["tid"] = t.ident
            cells[cid] = res
            _evict_stale_cells(cells)
        return {"cell_id": cid, "status": "fired"}
    elif cmd == "fork":
        # os.fork() child process, not threading.Thread.
        # Child gets a copy of namespace (fork COW).  Runs code, diffs the
        # namespace, pickles new/changed vars back through a pipe.
        # Parent merges the diff.  Unpicklable objects (sockets, locks) are
        # skipped -- the merge report tells you what didn't come back.
        # Tradeoff vs fire: can be killed (os.kill) but pickle overhead +
        # unpicklable objects won't propagate.
        # Limitation: diff uses id() -- in-place mutations (list.append,
        # dict update, obj.attr = x) don't change id() so they won't be
        # detected.  Use assignment (x = new_value) to ensure merge.
        # Assumes CPython: id() = memory address, no GC compaction post-fork.
        # If CPython ever adds compaction, this diff breaks.
        # Warning: POSIX fork in a multithreaded process (this worker has
        # REPL, ai_loop, and possibly fire'd threads).  Child inherits only
        # the calling thread; locks held by other threads stay locked forever.
        # Usually fine for pure-Python data.  Risky after loading libraries
        # that own native threads or process-global state (OpenMP, BLAS,
        # CUDA, sqlite, logging handlers).  pysh kill to escape.
        if sys.platform == "win32":
            return {"error": "fork not supported on Windows (no COW fork)"}
        if not args or not str(args[0]).strip():
            return {"error": "fork requires code"}
        cid = uuid.uuid4().hex[:12]
        # Use os.fork() + os._exit() instead of mp.Process.
        # mp.Process does Python cleanup after fork (join threads, atexit,
        # flush buffers) which deadlocks on locks held by threads that
        # don't exist in the child.  os._exit() skips all of that.
        r_fd, w_fd = os.pipe()
        child_pid = -1
        fork_locked = False
        try:
            # Prevent fork child's subprocess from inheriting pipe fds.
            # Without this, a grandchild process holds w_fd open -> parent's
            # read never sees EOF -> monitor hangs.
            try:
                os.set_inheritable(r_fd, False)
                os.set_inheritable(w_fd, False)
            except OSError as e:
                print(f"WARN: set_inheritable failed: {e}", file=sys.stderr)
            # Snapshot and fork under the same lock so the diff base and child
            # image match.  The child releases its inherited copy immediately
            # before evaluating user code.
            if lock:
                lock.acquire()
                fork_locked = True
            ns_snap = {k: id(v) for k, v in ns.items()}
            child_pid = os.fork()
        except BaseException:
            if fork_locked and lock:
                lock.release()
            for fd in (r_fd, w_fd):
                try:
                    os.close(fd)
                except OSError:
                    pass
            raise
        if child_pid != 0 and fork_locked and lock:
            lock.release()
        if child_pid == 0:
            # --- child process (exits via os._exit, no Python cleanup) ---
            if fork_locked and lock:
                lock.release()
            os.close(r_fd)
            try:
                os.setsid()  # type: ignore[attr-defined]
            except OSError:
                try:
                    payload = pickle.dumps({
                        "output": "fork child setsid failed",
                        "_error": True,
                        "diff": {},
                        "skipped": [],
                    })
                    _write_all(w_fd, payload)
                except Exception:
                    pass  # child is dying anyway
                finally:
                    try:
                        os.close(w_fd)
                    except OSError:
                        pass
                    os._exit(1)
            try:
                buf = io.StringIO()
                sys.stdout = sys.stderr = buf  # capture all output
                had_error = False
                try:
                    _eval_exec_cell(args[0], ns)
                except SystemExit as e:
                    had_error = True
                    code_val = e.code if e.code is not None else 0
                    print(f"exit({code_val})")
                except BaseException:
                    had_error = True
                    traceback.print_exc()
                output = buf.getvalue().rstrip("\n")
                # diff: new or changed vars (by identity)
                diff = {}
                skipped = []
                for k, v in ns.items():
                    if k.startswith("_"):
                        continue
                    if k not in ns_snap or id(v) != ns_snap[k]:
                        try:
                            pickle.dumps(v)
                            diff[k] = v
                        except Exception:
                            skipped.append(k)
                payload = pickle.dumps({"output": output, "_error": had_error,
                                        "diff": diff,
                                        "skipped": skipped})
                _write_all(w_fd, payload)
            except BaseException:
                try:
                    payload = pickle.dumps({"output": traceback.format_exc(),
                                            "_error": True,
                                            "diff": {}, "skipped": []})
                    _write_all(w_fd, payload)
                except Exception:
                    pass  # child is dying anyway
            finally:
                try:
                    os.close(w_fd)
                except OSError:
                    pass
                os._exit(0)  # skip all Python cleanup -- no deadlocks
        # --- parent process ---
        os.close(w_fd)
        res = {"output": "", "status": "running", "pid": child_pid,
               "pgid": child_pid,
               "_seq": next(_CELL_SEQ)}
        def _fork_monitor(r: JsonDict = res, fd: int = r_fd, pid: int = child_pid) -> None:
            """Read pipe first (unblocks child write), then reap child."""
            # Must read before waitpid: if child writes a large payload
            # (> pipe buffer ~64KB), child blocks on write until parent reads.
            # waitpid first -> parent waits for child -> child waits for read -> deadlock.
            chunks = []
            try:
                while True:
                    chunk = os.read(fd, _BUFFER_CHUNK)
                    if not chunk:
                        break
                    chunks.append(chunk)
            except OSError as e:
                print(f"WARN: fork pipe broken: {e}", file=sys.stderr)
            finally:
                try:
                    os.close(fd)
                except OSError:
                    pass
            try:
                os.waitpid(pid, 0)
            except ChildProcessError:
                pass  # already reaped
            output = ""
            had_error = False
            merged_keys: list[str] = []
            skipped: list[str] = []
            try:
                if chunks:
                    data = pickle.loads(b"".join(chunks))
                    output = data.get("output", "")
                    had_error = bool(data.get("_error", False))
                    merged = data.get("diff", {})
                    if had_error:
                        merged = {}
                    else:
                        with _locked(lock):
                            ns.update(merged)
                    merged_keys = list(merged.keys())
                    skipped = data.get("skipped", [])
                else:
                    output = "(killed)"
                    had_error = True
            except (EOFError, OSError, pickle.UnpicklingError):
                output = r.get("output", "") or "(killed)"
                had_error = True
            except Exception:
                output = r.get("output", "") or "(fork result read failed)"
                had_error = True
            finally:
                with _cells_lock:
                    r["pid"] = None
                    r["output"] = output
                    r["_error"] = had_error
                    r["_merged"] = merged_keys
                    r["_skipped"] = skipped
                    r["status"] = "done"
                    r["_done_at"] = time.time()
        with _cells_lock:
            cells[cid] = res
            _evict_stale_cells(cells)
        threading.Thread(target=_fork_monitor, daemon=True).start()
        return {"cell_id": cid, "status": "forked"}
    elif cmd == "int":
        # Two modes:
        #   fire'd cells (threads): SetAsyncExc -- best-effort, Python bytecode only.
        #     C code won't see it until it returns to Python.
        #   fork'd cells (processes): SIGKILL -- hard kill, stops anything.
        # Note: run blocks the AI loop, so int can't reach the worker while
        # run is executing.  Use fork for code that might hang.
        threads = 0
        processes = 0
        with _INTERRUPT_LOCK:
            with _cells_lock:
                snapshot = list(cells.items())
            for cid, r in snapshot:
                if r["status"] != "running":
                    continue
                tid = r.get("tid")
                pid = r.get("pid")
                pgid = r.get("pgid")
                if tid:
                    rc = _SET_ASYNC_EXC(
                        ctypes.c_ulong(tid),
                        ctypes.py_object(KeyboardInterrupt),
                    )
                    if rc > 1:
                        _SET_ASYNC_EXC(ctypes.c_ulong(tid), None)
                    if rc >= 1:
                        threads += 1
                elif pid:
                    try:
                        if pgid:
                            os.killpg(int(pgid), signal.SIGKILL)  # type: ignore[attr-defined]
                        else:
                            os.kill(pid, signal.SIGKILL)  # type: ignore[attr-defined]
                        processes += 1
                    except ProcessLookupError:
                        try:
                            os.kill(pid, signal.SIGKILL)  # type: ignore[attr-defined]
                            processes += 1
                        except (OSError, ProcessLookupError):
                            pass  # already dead
                    except (OSError, ProcessLookupError):
                        pass  # already dead
        return {"threads": threads, "processes": processes,
                "note": "thread interrupts are best-effort; "
                        "fork processes are hard-killed"}
    elif cmd == "poll":
        target = args[0] if args else None
        if target:
            with _cells_lock:
                cell = cells.get(target)
                _evict_stale_cells(cells)
                if cell is not None:
                    cell = dict(cell)
            if cell is None:
                return {"cell_id": target, "status": "error",
                         "output": "unknown cell"}
            resp = {"cell_id": target, "status": cell["status"],
                     "output": cell["output"]}
            if cell.get("_error"):
                resp["_error"] = True
            # fork'd cells: include merge report when done
            if "_merged" in cell:
                resp["merged"] = cell["_merged"]
                resp["skipped"] = cell["_skipped"]
            return resp
        with _cells_lock:
            _evict_stale_cells(cells)
            if not cells:
                return {"status": "idle"}
            last_id, r = max(
                cells.items(),
                key=lambda item: typing.cast(int, item[1].get("_seq", -1)),
            )
            r = dict(r)
        resp = {"cell_id": last_id, "status": r["status"],
                 "output": r["output"]}
        if r.get("_error"):
            resp["_error"] = True
        if "_merged" in r:
            resp["merged"] = r["_merged"]
            resp["skipped"] = r["_skipped"]
        return resp
    elif cmd == "status":
        vs = len(_public_names(ns, lock))
        with _cells_lock:
            _evict_stale_cells(cells)
            running = [cid for cid, r in cells.items()
                       if r["status"] == "running"]
            ncells = len(cells)
        return {"state": "running" if running else "idle",
                "running": running, "vars": vs, "cells": ncells}
    elif cmd == "vars":
        return {"vars": _public_names(ns, lock)}
    elif cmd == "complete":
        import rlcompleter
        text = args[0] if args else ""
        with _locked(lock):
            ns_snapshot = dict(ns)
        c = rlcompleter.Completer(ns_snapshot)
        matches: list[str] = []
        for i in range(200):
            try:
                m = c.complete(text, i)
            except Exception:
                return {"matches": matches, "_error": True}
            if m is None:
                break
            matches.append(m)
        return {"matches": matches}
    return {"error": f"unknown cmd: {cmd}"}

# =============================================
# POSIX: real PTY worker (readline, tab, arrows)
# =============================================

def session_worker_pty(ai_sock: socket.socket) -> None:
    """Runs in subprocess with PTY slave as stdin/stdout/stderr.

    Human attach goes through the PTY and therefore gets real readline, tab
    completion, terminal signals, and normal Python REPL behaviour.  AI commands
    use ai_sock, a private socketpair using one JSON object per line.  Both
    paths share the same namespace and lock.
    """
    ns = _init_namespace()
    cells: dict[str, JsonDict] = {}
    lock = threading.Lock()

    def _broadcast(src: str, output: str) -> None:
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
        readline_mod = typing.cast(typing.Any, readline)
        readline_mod.set_completer(_completer.complete)
        readline_mod.parse_and_bind("tab: complete")
    except ImportError:
        pass  # optional module -- feature disabled without it

    def _ai_loop() -> None:
        rf = ai_sock.makefile("r")
        wf = ai_sock.makefile("w")
        try:
            while True:
                try:
                    line = rf.readline()
                    if not line:
                        break
                    msg = json.loads(line)
                    resp = _dispatch(msg["cmd"], msg.get("args", []),
                                     _exec, cells, ns, lock)
                    wf.write(json.dumps(resp) + "\n")
                    wf.flush()
                except (json.JSONDecodeError, KeyError, TypeError):
                    try:
                        wf.write(json.dumps({"error": "worker protocol error"}) + "\n")
                        wf.flush()
                    except BaseException:
                        break
                    continue
                except Exception:
                    try:
                        wf.write(json.dumps({"error": "worker protocol error"}) + "\n")
                        wf.flush()
                    except BaseException:
                        break
                    continue
        finally:
            with contextlib.suppress(OSError):
                rf.close()
            with contextlib.suppress(OSError):
                wf.close()
            with contextlib.suppress(OSError):
                ai_sock.close()

    threading.Thread(target=_ai_loop, daemon=True).start()

    class LockedConsole(code.InteractiveConsole):
        """InteractiveConsole that holds the session lock during eval."""
        def runsource(
            self,
            source: str,
            filename: str = "<input>",
            symbol: str = "single",
        ) -> bool:
            """Execute under lock so AI and human cells don't interleave."""
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
# DAEMON -- socket + process manager
# =============================================

sessions: dict[str, JsonDict] = {}
_sessions_lock = threading.Lock()
_session_lock_guard = threading.Lock()
_daemon_token = None
_daemon_server: _Servable | None = None

def _session_lock(session: JsonDict) -> threading.Lock:
    """Return the per-session command lock, creating it atomically."""
    with _session_lock_guard:
        lock = session.get("_lock")
        if lock is None:
            lock = threading.Lock()
            session["_lock"] = lock
        return lock

def _session_close_lock(session: JsonDict) -> threading.Lock:
    """Return the per-session close-once lock, creating it atomically."""
    with _session_lock_guard:
        lock = session.get("_close_lock")
        if lock is None:
            lock = threading.Lock()
            session["_close_lock"] = lock
        return lock

def _get_session(name: str) -> JsonDict | None:
    """Return a session object by name, or None if absent."""
    with _sessions_lock:
        return sessions.get(name)

def _set_session(name: str, session: JsonDict) -> None:
    """Publish a newly created session atomically."""
    _validate_session_name(name)
    old_session = None
    with _sessions_lock:
        _check_session_capacity_locked(name)
        old_session = sessions.get(name)
        sessions[name] = session
    if old_session is not None and old_session is not session:
        _close_session_resources(old_session)

def _ensure_session_capacity(name: str) -> None:
    """Fail before allocating worker resources when no new session slot exists."""
    _validate_session_name(name)
    with _sessions_lock:
        _check_session_capacity_locked(name)

def _check_session_capacity_locked(name: str) -> None:
    """Validate session capacity. Caller must hold _sessions_lock."""
    if name not in sessions and len(sessions) >= _MAX_SESSIONS:
        raise RuntimeError(f"too many sessions (max {_MAX_SESSIONS})")

def _session_snapshot() -> list[tuple[str, JsonDict]]:
    """Return a stable list of (name, session) pairs."""
    with _sessions_lock:
        return list(sessions.items())

class PtyBridge:
    """Bridge: PTY <-> WebSocket binary frames.

    Continuously drains PTY output. If a client is attached, forwards bytes
    as WebSocket binary frames.  Otherwise buffers as scrollback.
    """
    def __init__(
        self,
        pty_read: typing.Callable[[], bytes],
        pty_write: typing.Callable[[bytes], typing.Any],
    ) -> None:
        """Start bridge with PTY read/write callbacks."""
        self._read = pty_read
        self._write = pty_write
        self._send_fn: typing.Callable[[bytes], typing.Any] | None = None
        self._pending_send_fn: typing.Callable[[bytes], typing.Any] | None = None
        self._close_fn: typing.Callable[[], typing.Any] | None = None
        self._owner: object | None = None
        self._lock = threading.Lock()
        self._scrollback = bytearray()
        self._MAX = _BUFFER_CHUNK
        threading.Thread(target=self._reader, daemon=True).start()

    def attach(
        self,
        send_fn: typing.Callable[[bytes], typing.Any],
        close_fn: typing.Callable[[], typing.Any] | None = None,
    ) -> object | None:
        """Reserve one attached client. Call flush_scrollback() after ack."""
        with self._lock:
            if self._owner is not None:
                return None
            owner = object()
            self._pending_send_fn = send_fn
            self._close_fn = close_fn
            self._owner = owner
        return owner

    def flush_scrollback(self, owner: object) -> bool:
        """Enable an attached client and flush buffered output after attach ack."""
        scrollback = b""
        send_fn = None
        with self._lock:
            if self._owner is not owner or self._pending_send_fn is None:
                return False
            send_fn = self._pending_send_fn
            self._send_fn = send_fn
            self._pending_send_fn = None
            if self._scrollback:
                scrollback = bytes(self._scrollback)
                self._scrollback.clear()
        if scrollback:
            try:
                send_fn(scrollback)
            except Exception:
                close_fn = None
                with self._lock:
                    if self._owner is owner:
                        self._buffer_scrollback_front_locked(scrollback)
                        close_fn = self._take_close_fn_locked(owner)
                self._call_close_fn(close_fn)
                return False
        return True

    def _take_close_fn_locked(
        self,
        owner: object | None = None,
    ) -> typing.Callable[[], typing.Any] | None:
        """Clear current attachment and return its close callback."""
        if owner is not None and self._owner is not owner:
            return None
        close_fn = self._close_fn
        self._send_fn = None
        self._pending_send_fn = None
        self._close_fn = None
        self._owner = None
        return close_fn

    def _call_close_fn(self, close_fn: typing.Callable[[], typing.Any] | None) -> None:
        """Best-effort close notification for attached WebSocket clients."""
        if close_fn is None:
            return
        try:
            close_fn()
        except Exception as e:
            print(f"WARN: PTY attach close callback failed: {e.__class__.__name__}",
                  file=sys.stderr)

    def _close_attached_client(self) -> None:
        """Detach and actively wake the currently attached client."""
        with self._lock:
            close_fn = self._take_close_fn_locked()
        self._call_close_fn(close_fn)

    def _buffer_scrollback_locked(self, data: bytes) -> None:
        """Append to bounded scrollback. Caller holds _lock."""
        self._scrollback.extend(data)
        if len(self._scrollback) > self._MAX:
            del self._scrollback[:-self._MAX]

    def _buffer_scrollback_front_locked(self, data: bytes) -> None:
        """Prepend failed attach scrollback. Caller holds _lock."""
        self._scrollback[:0] = data[-self._MAX:]
        if len(self._scrollback) > self._MAX:
            del self._scrollback[:-self._MAX]

    def detach(self, owner: object | None = None) -> None:
        """Detach current client. PTY output goes to scrollback buffer."""
        with self._lock:
            self._take_close_fn_locked(owner)

    def close(self) -> None:
        """Drop any attached client and buffered PTY output during session kill."""
        with self._lock:
            close_fn = self._take_close_fn_locked()
            self._scrollback.clear()
        self._call_close_fn(close_fn)

    def write(self, data: bytes) -> None:
        """Client -> PTY input."""
        self._write(data)

    def _reader(self) -> None:
        """Drain PTY output forever."""
        while True:
            try:
                data = self._read()
            except (OSError, EOFError):
                break
            if not data:
                break
            send_fn = None
            with self._lock:
                if self._send_fn:
                    send_fn = self._send_fn
                else:
                    self._buffer_scrollback_locked(data)
            if send_fn is not None:
                try:
                    send_fn(data)
                except Exception:
                    close_fn = None
                    with self._lock:
                        self._buffer_scrollback_locked(data)
                        if self._send_fn is send_fn:
                            close_fn = self._take_close_fn_locked()
                    self._call_close_fn(close_fn)
        self._close_attached_client()

def new_session(name: str) -> None:
    """Create or replace one named Python session."""
    _ensure_session_capacity(name)
    if _get_session(name) is not None:
        kill_session(name)
    if _HAS_PTY and _WinPty is not None:
        ai_srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        proc = None
        ai_conn: socket.socket | None = None
        try:
            if hasattr(socket, "SO_EXCLUSIVEADDRUSE"):
                ai_srv.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
            ai_srv.bind(("127.0.0.1", 0))
            ai_port = ai_srv.getsockname()[1]
            ai_srv.listen(1)
            ai_srv.settimeout(10)
            with _WORKER_SPAWN_LOCK:
                old_worker_env = os.environ.get(_WORKER_ENV)
                os.environ[_WORKER_ENV] = "1"
                try:
                    proc = _WinPty.spawn(
                        [sys.executable, os.path.abspath(__file__),
                         "_worker_winpty", str(ai_port)]
                    )
                finally:
                    if old_worker_env is None:
                        os.environ.pop(_WORKER_ENV, None)
                    else:
                        os.environ[_WORKER_ENV] = old_worker_env
            ai_conn, _ = ai_srv.accept()
        except socket.timeout:
            if proc is not None:
                proc.terminate(force=True)
            raise RuntimeError("winpty worker failed to connect")
        except BaseException:
            if proc is not None:
                with contextlib.suppress(Exception):
                    proc.terminate(force=True)
            raise
        finally:
            ai_srv.close()
        assert proc is not None
        assert ai_conn is not None
        def _read() -> bytes:
            try:
                return proc.read().encode()
            except EOFError:
                return b""
        def _write(data: bytes) -> None:
            proc.write(data.decode(errors="replace"))
        try:
            bridge = PtyBridge(_read, _write)
        except Exception:
            ai_conn.close()
            with contextlib.suppress(Exception):
                proc.terminate(force=True)
            raise
        session = {
            "type": "pty", "winpty": proc,
            "ai": ai_conn, "bridge": bridge,
        }
        try:
            _set_session(name, session)
        except Exception:
            _close_session_resources(session)
            raise
        threading.Thread(target=_monitor_session, args=(name,),
                         daemon=True).start()
    elif _HAS_PTY:
        master_fd = slave_fd = -1
        ai_parent = ai_child = None
        try:
            master_fd, slave_fd = pty.openpty()  # type: ignore[name-defined]
            ai_parent, ai_child = socket.socketpair()
            p = subprocess.Popen(
                [sys.executable, os.path.abspath(__file__),
                 "_worker_pty", str(slave_fd), str(ai_child.fileno())],
                close_fds=True,
                pass_fds=(slave_fd, ai_child.fileno()),
                env={**os.environ, _WORKER_ENV: "1"},
                start_new_session=True,
            )
        except Exception:
            for fd in (master_fd, slave_fd):
                if fd >= 0:
                    try:
                        os.close(fd)
                    except OSError:
                        pass
            for sock_obj in (ai_parent, ai_child):
                if sock_obj is not None:
                    try:
                        sock_obj.close()
                    except OSError:
                        pass
            raise
        os.close(slave_fd)
        ai_child.close()
        session = {
            "type": "pty", "proc": p, "master_fd": master_fd,
            "ai": ai_parent,
            "bridge": PtyBridge(
                lambda: os.read(master_fd, 4096),
                lambda d: _write_all(master_fd, d)),
        }
        try:
            _set_session(name, session)
        except Exception:
            _close_session_resources(session)
            raise
        threading.Thread(target=_monitor_session, args=(name,),
                         daemon=True).start()
    else:
        raise RuntimeError("no PTY support: pip install pywinpty (Windows)")

def kill_session(name: str) -> bool:
    """Terminate one named session and close all daemon-owned resources."""
    with _sessions_lock:
        s = sessions.get(name)
    if s is None:
        return False
    lock = _session_lock(s)
    locked = lock.acquire(timeout=3)
    if not locked:
        should_close = False
        with _sessions_lock:
            if sessions.get(name) is s:
                sessions.pop(name, None)
                should_close = True
        if should_close:
            return _close_session_resources(s)
        return False
    try:
        with _sessions_lock:
            if sessions.get(name) is not s:
                return False
            sessions.pop(name, None)
        return _close_session_resources(s)
    finally:
        lock.release()

def kill_session_if_current(name: str, expected: JsonDict) -> bool:
    """Kill name only if it still points at expected session object."""
    lock = _session_lock(expected)
    locked = lock.acquire(timeout=3)
    if not locked:
        should_close = False
        with _sessions_lock:
            if sessions.get(name) is expected:
                sessions.pop(name, None)
                should_close = True
        if should_close:
            return _close_session_resources(expected)
        return False
    try:
        with _sessions_lock:
            if sessions.get(name) is not expected:
                return False
            sessions.pop(name, None)
        return _close_session_resources(expected)
    finally:
        lock.release()

def _close_session_resources(s: JsonDict) -> bool:
    """Close resources for a session already removed from the session map."""
    with _session_close_lock(s):
        if s.get("_closed"):
            return True
        s["_closed"] = True
        return _close_session_resources_once(s)

def _close_session_resources_once(s: JsonDict) -> bool:
    """Close resources for a session. Caller holds the close-once lock."""
    if s["type"] == "remote":
        ws = s.get("_ws")
        if ws:
            try:
                ws.close()
            except Exception:
                pass  # cleanup must not raise -- resources may already be dead
            s["_ws"] = None
        return True
    if s["type"] == "pty":
        bridge = s.get("bridge")
        if bridge is not None:
            bridge.close()
            s["bridge"] = None
        winpty = s.get("winpty")
        if winpty is not None:
            try:
                if winpty.isalive():
                    winpty.terminate(force=True)
            except Exception:
                pass  # cleanup must not raise -- resources may already be dead
            s["winpty"] = None
        else:
            # Kill entire process group (worker + any fork children).
            # Popen created the worker in a new session, so its pgid == pid
            # before user code can run.
            proc = s.get("proc")
            try:
                pgid = (
                    os.getpgid(proc.pid)  # type: ignore[attr-defined]
                    if proc is not None else None
                )
            except OSError:
                pgid = None
            if proc is not None and pgid is not None:
                try:
                    os.killpg(pgid, signal.SIGTERM)  # type: ignore[attr-defined]
                    proc.wait(timeout=3)
                except (OSError, subprocess.TimeoutExpired):
                    try:
                        os.killpg(pgid, signal.SIGKILL)  # type: ignore[attr-defined]
                        proc.wait(timeout=1)
                    except Exception:
                        pass  # cleanup must not raise
                s["proc"] = None
            elif proc is not None:
                try:
                    proc.terminate()
                    proc.wait(timeout=3)
                except (OSError, subprocess.TimeoutExpired):
                    try:
                        proc.kill()
                        proc.wait(timeout=1)
                    except Exception:
                        pass  # cleanup must not raise
                s["proc"] = None
        if s.get("master_fd") is not None:
            try:
                os.close(s["master_fd"])
            except OSError:
                pass  # cleanup must not raise -- resources may already be dead
            s["master_fd"] = None
        for resource in ("ai",):
            try:
                handle = s.get(resource)
                if handle is not None:
                    handle.close()
                    s[resource] = None
            except OSError:
                pass  # cleanup must not raise -- resources may already be dead
        for resource in ("ai_rf", "ai_wf"):
            try:
                handle = s.get(resource)
                if handle is not None:
                    handle.close()
                    s[resource] = None
            except OSError:
                pass  # cleanup must not raise -- resources may already be dead
    return True

def _monitor_session(name: str) -> None:
    """Wait for a session's worker to exit, then auto-reap."""
    s = _get_session(name)
    if not s:
        return
    try:
        if s["type"] == "pty":
            winpty = s.get("winpty")
            proc = s.get("proc")
            if winpty is not None:
                winpty.wait()
            elif proc is not None:
                proc.wait()
    except Exception as e:
        print(f"WARN: session {name} exited abnormally: {e}",
              file=sys.stderr)
    with _session_lock(s):
        with _sessions_lock:
            if sessions.get(name) is not s:
                return
            sessions.pop(name, None)
        _close_session_resources(s)

def _recv_session_line(s: JsonDict) -> str | None:
    """Read one newline-delimited JSON response from the worker socket."""
    buf = typing.cast(bytes, s.get("_ai_buf", b""))
    try:
        while b"\n" not in buf:
            chunk = s["ai"].recv(_BUFFER_CHUNK)
            if not chunk:
                return None
            buf += chunk
            if len(buf) > _MAX_WORKER_RESPONSE:
                raise ValueError("worker response too large")
        line, buf = buf.split(b"\n", 1)
        return line.decode("utf-8", "replace")
    finally:
        s["_ai_buf"] = buf

def send_session(name: str, msg: JsonDict, timeout: float = 30) -> JsonDict:
    """Send one AI command to a session and wait for its response.

    A per-session lock serializes concurrent callers (multiple WebSocket
    handlers hitting the same session).  Remote sessions already had this
    via _send_remote's lock; local sessions now get the same protection.
    """
    s = _get_session(name)
    if s is None:
        return {"error": f"session '{name}' not found"}
    with _session_lock(s):
        if _get_session(name) is not s:
            return {"error": f"session '{name}' not found"}
        if s.get("_unhealthy"):
            return {"error": f"session '{name}' command channel out of sync after timeout; "
                    f"use pysh kill {name}"}
        if s["type"] == "remote":
            return _send_remote(s, msg, timeout)
        if s["type"] == "pty":
            try:
                s["ai"].settimeout(timeout)
                s["ai"].sendall((json.dumps(msg) + "\n").encode("utf-8"))
                line = _recv_session_line(s)
                if not line:
                    return {"error": f"session '{name}' dead -- pysh new {name} to restart"}
                resp: dict[str, typing.Any] = json.loads(line)
                return resp
            except socket.timeout:
                s["_unhealthy"] = True
                return {"error": "timeout -- command channel may be out of sync; "
                        "use pysh int or pysh kill if stuck"}
            except ValueError:
                s["_unhealthy"] = True
                return {"error": "worker response too large; use pysh kill to restart"}
            except (OSError, json.JSONDecodeError):
                s["_unhealthy"] = True
                return {"error": "session command failed"}
            finally:
                try:
                    s["ai"].settimeout(None)
                except OSError:
                    pass  # socket may be dead
        return {"error": f"unsupported session type: {s.get('type')}"}

# -----------------------------------------------
# REMOTE PROXY -- persistent TCP to remote daemon
# -----------------------------------------------

def _client_ssl_ctx() -> _ssl.SSLContext:
    """Create TLS client context with directional trust.

    Loads client cert for mTLS (proving identity to server).
    Uses trusted_servers/ fingerprints for exact server verification.
    If no server pins exist, falls back to the system CA bundle.
    """
    ctx = _ssl.SSLContext(_ssl.PROTOCOL_TLS_CLIENT)
    ctx.minimum_version = _ssl.TLSVersion.TLSv1_2
    # If pins exist, certificate identity is checked after the handshake by
    # exact SHA-256 fingerprint, not by OpenSSL CA/path validation.
    if _trusted_fingerprints(_trusted_servers_dir()):
        ctx.check_hostname = False
        ctx.verify_mode = _ssl.CERT_NONE
    else:
        # no pinned certs -- fall back to system CA bundle
        ctx.check_hostname = True
        ctx.set_default_verify_paths()
        ctx.verify_mode = _ssl.CERT_REQUIRED
    # load client cert for mTLS
    try:
        cert = os.path.join(_tls_dir(), "cert.pem")
        key = os.path.join(_tls_dir(), "key.pem")
        if os.path.exists(cert) and os.path.exists(key):
            ctx.load_cert_chain(cert, key)
    except (_ssl.SSLError, OSError) as e:
        print(f"WARN: client cert load failed: {e}", file=sys.stderr)
    return ctx


class _WsproClient:
    """WebSocket client over TLS socket, framing by wsproto."""

    def __init__(self, sock: SocketLike, ws: wsproto.WSConnection) -> None:
        self.sock = sock
        self.ws = ws
        self._text_parts: list[str] = []
        self._bytes_parts: list[bytes] = []
        self._pending_events: collections.deque[ws_events.Event] = collections.deque()
        self._message_size = 0

    @classmethod
    def connect(
        cls,
        host: str,
        port: int,
        ssl_ctx: _ssl.SSLContext,
        token: str | None = None,
        timeout: float = 10,
    ) -> "_WsproClient":
        raw = socket.create_connection((host, port), timeout=timeout)
        sock = None
        try:
            sock = ssl_ctx.wrap_socket(raw, server_hostname=host)
            server_trust_dir = _trusted_servers_dir()
            if _trusted_fingerprints(server_trust_dir):
                _verify_peer_fingerprint(sock, server_trust_dir, "server")
            sock.settimeout(timeout)
            ws = wsproto.WSConnection(ConnectionType.CLIENT)
            headers: list[tuple[bytes, bytes]] = []
            if token:
                headers.append((b"Authorization", f"Bearer {token}".encode("ascii")))
            sock.sendall(ws.send(ws_events.Request(
                host=f"{host}:{port}",
                target="/",
                subprotocols=[_WS_PROTO],
                extra_headers=headers,
            )))
            while True:
                data = sock.recv(_BUFFER_CHUNK)
                if not data:
                    raise RuntimeError("connection closed during handshake")
                ws.receive_data(data)
                for event in ws.events():
                    if isinstance(event, ws_events.AcceptConnection):
                        if event.subprotocol != _WS_PROTO:
                            raise RuntimeError("bad websocket protocol")
                        return cls(sock, ws)
                    if isinstance(event, ws_events.RejectConnection):
                        raise RuntimeError(
                            f"websocket rejected: {event.status_code}"
                        )
                    if isinstance(event, ws_events.RejectData):
                        raise RuntimeError("websocket rejected")
        except Exception:
            if sock is not None:
                sock.close()
            else:
                raw.close()
            raise

    def send(self, data: str | bytes) -> None:
        if isinstance(data, bytes):
            payload = self.ws.send(ws_events.BytesMessage(data=data))
        elif isinstance(data, str):
            payload = self.ws.send(ws_events.TextMessage(data=data))
        else:
            raise TypeError("websocket payload must be str or bytes")
        self.sock.sendall(payload)

    def recv(self, timeout: float | None = None) -> str | bytes | object:
        old_timeout = self.sock.gettimeout()
        if timeout is not None:
            self.sock.settimeout(timeout)
        try:
            while True:
                while self._pending_events:
                    result = self._handle_event(self._pending_events.popleft())
                    if result is not None:
                        return result
                data = self.sock.recv(_BUFFER_CHUNK)
                if not data:
                    raise RuntimeError("websocket closed")
                self.ws.receive_data(data)
                events = list(self.ws.events())
                for i, event in enumerate(events):
                    result = self._handle_event(event)
                    if result is not None:
                        self._pending_events.extend(events[i + 1:])
                        return result
        except (TimeoutError, socket.timeout):
            self._clear_message()
            raise
        finally:
            if timeout is not None:
                self.sock.settimeout(old_timeout)

    def close(self) -> None:
        try:
            self.sock.sendall(self.ws.send(ws_events.CloseConnection(code=1000)))
        except (OSError, LocalProtocolError):
            pass
        try:
            self.sock.close()
        except OSError:
            pass

    def _handle_event(self, event: ws_events.Event) -> str | bytes | object | None:
        if isinstance(event, ws_events.TextMessage):
            text_data = typing.cast(str, event.data)
            self._text_parts.append(text_data)
            self._message_size += len(text_data.encode("utf-8"))
            self._check_message_size()
            if event.message_finished:
                out_text = "".join(self._text_parts)
                self._clear_message()
                return out_text
            return None
        if isinstance(event, ws_events.BytesMessage):
            bytes_data = bytes(event.data)
            self._bytes_parts.append(bytes_data)
            self._message_size += len(bytes_data)
            self._check_message_size()
            if event.message_finished:
                out_bytes = b"".join(self._bytes_parts)
                self._clear_message()
                return out_bytes
            return None
        if isinstance(event, ws_events.CloseConnection):
            try:
                self.sock.sendall(self.ws.send(ws_events.CloseConnection(
                    code=1000,
                )))
            except (OSError, LocalProtocolError):
                pass
            return _WS_CLOSE
        if isinstance(event, ws_events.Ping):
            try:
                self.sock.sendall(self.ws.send(ws_events.Pong(payload=event.payload)))
            except (OSError, LocalProtocolError):
                pass
            return None
        if isinstance(event, ws_events.Pong):
            return None
        if isinstance(event, ws_events.RejectConnection):
            raise RuntimeError(f"websocket rejected: {event.status_code}")
        if isinstance(event, ws_events.RejectData):
            raise RuntimeError("websocket rejected")
        return None

    def _check_message_size(self) -> None:
        if self._message_size > _MAX_WS_PAYLOAD:
            self._clear_message()
            with contextlib.suppress(OSError):
                self.sock.close()
            raise RuntimeError("websocket message too large")

    def _clear_message(self) -> None:
        self._text_parts.clear()
        self._bytes_parts.clear()
        self._message_size = 0


def _connect_wss(
    host: str,
    port: int,
    token: str | None,
    timeout: float = 10,
) -> _WsproClient:
    return _WsproClient.connect(host, port, _client_ssl_ctx(), token, timeout)


def _parse_host_port(value: str, default_port: int = 7399) -> tuple[str, int]:
    """Parse HOST[:PORT] without treating IPv6 as supported syntax."""
    if ":" in value:
        host, _, port_s = value.rpartition(":")
        port = int(port_s)
    else:
        host = value
        port = int(os.environ.get("PYTHOND_PORT", str(default_port)))
    if not host:
        raise ValueError("host required")
    if not (1 <= port <= 65535):
        raise ValueError("port out of range")
    return host, port


def _open_remote_ws(
    host: str,
    port: int,
    token: str | None,
    use_tls: bool = False,
    timeout: float = 10,
) -> WebSocketLike:
    """Open one daemon WebSocket to HOST:PORT using the project TLS rules."""
    if use_tls:
        return _connect_wss(host, port, token, timeout=timeout)
    try:
        from websockets.sync.client import connect as ws_connect
    except ImportError:
        raise RuntimeError("websockets required: pip install pythond")
    return ws_connect(f"ws://{host}:{port}/",
                      additional_headers=_auth_headers(token),
                      proxy=None,
                      open_timeout=timeout,
                      close_timeout=2,
                      subprotocols=[_WS_PROTO])


def _connect_daemon(timeout: float = 5) -> WebSocketLike:
    """Open a client connection to the configured local or remote daemon."""
    host = os.environ.get("PYTHOND_HOST")
    use_tls = os.environ.get("PYTHOND_TLS", "").lower() in ("1", "true", "yes")
    token = os.environ.get("PYTHOND_TOKEN")

    if host:
        h, port = _parse_host_port(host)
        return _open_remote_ws(h, port, token, use_tls=use_tls, timeout=timeout)
    if _HAS_AF_UNIX:
        try:
            from websockets.sync.client import unix_connect as ws_unix_connect
        except ImportError:
            raise RuntimeError("websockets required for local AF_UNIX connections: pip install pythond")
        return ws_unix_connect(SOCK, open_timeout=timeout, close_timeout=2,
                               subprotocols=[_WS_PROTO])

    meta = _read_daemon_meta()
    port = int(os.environ.get("PYTHOND_PORT") or meta.get("port") or "7399")
    if not (1 <= port <= 65535):
        raise ValueError("port out of range")
    token = token or meta.get("token", "")
    return _open_remote_ws("127.0.0.1", port, token, use_tls=False,
                           timeout=timeout)


def _build_wire_message(cmd: str, args: list[str]) -> str:
    """Build daemon text-frame protocol: header args, newline body for code."""
    args = list(args)
    if cmd in ("run", "fire", "fork") and len(args) >= 2:
        header = " ".join([cmd] + args[:-1])
        return header + "\n" + args[-1]
    return " ".join([cmd] + args)


def _send_remote(
    session: JsonDict,
    msg: JsonDict,
    timeout: float = 30,
) -> JsonDict:
    """Forward one command to a remote daemon via persistent WebSocket.

    The local daemon is long-lived, so we cache one WebSocket per remote
    session.  Caller must hold the per-session lock.
    Connection is reopened automatically on failure.

    Retry policy: one retry on connection failure, then give up.
    Intentionally minimal -- the agent (or user) controls retry at the
    pysh level.  The daemon shouldn't hide network failures behind
    aggressive retries that add latency and hide the real problem.
    """
    cmd = msg.get("cmd", "")
    args = msg.get("args", [])
    if cmd in ("run", "fire", "fork") and len(args) < 2:
        alias = session.get("alias")
        if len(args) == 1 and isinstance(alias, str) and alias:
            args = [alias, args[0]]
        else:
            return {"error": f"remote {cmd} needs target session and code"}
    ws_msg = _build_wire_message(cmd, args)
    for attempt in range(2):
        ws = session.get("_ws")
        if ws is None:
            host, port, token = session["host"], session["port"], session["token"]
            try:
                ws = _open_remote_ws(host, port, token,
                                     use_tls=session.get("tls", False),
                                     timeout=10)
                session["_ws"] = ws
            except Exception:
                if attempt == 0:
                    continue
                return {"error": "remote connect failed"}
        try:
            ws.send(ws_msg)
        except Exception:
            session["_ws"] = None
            try:
                ws.close()
            except Exception:
                pass  # stale connection -- clear it
            if attempt == 0:
                continue
            return {"error": "remote send failed"}
        try:
            resp = ws.recv(timeout=timeout)
        except Exception:
            session["_ws"] = None
            try:
                ws.close()
            except Exception:
                pass  # stale connection -- clear it
            return {"error": "remote response failed"}
        if resp is _WS_CLOSE:
            session["_ws"] = None
            try:
                ws.close()
            except Exception:
                pass  # stale connection -- clear it
            if attempt == 0:
                continue
            return {"error": "remote closed"}
        if isinstance(resp, str) and resp.startswith("ERR "):
            return {"error": resp[4:]}
        if cmd == "run":
            return {"output": resp}
        try:
            parsed: dict[str, typing.Any] = json.loads(resp)
            return parsed
        except json.JSONDecodeError:
            return {"output": resp}
    return {"error": "remote unreachable"}

def connect_remote(
    name: str,
    host: str,
    port: int,
    token: str,
    use_tls: bool = False,
) -> str:
    """Register a remote daemon as a named session in the local daemon."""
    try:
        _ensure_session_capacity(name)
    except (ValueError, RuntimeError) as e:
        return f"ERR {_public_error(e)}"
    # test connectivity + auth now; actual data goes through _send_remote
    # which reconnects lazily.  This test catches bad host/port/token early
    # but doesn't guarantee future requests succeed (network can change).
    ws = None
    try:
        ws = _open_remote_ws(host, port, token, use_tls=use_tls, timeout=10)
        ws.send("ls")
        resp = ws.recv(timeout=5)
        if isinstance(resp, bytes):
            resp = resp.decode("utf-8", "replace")
        if resp is _WS_CLOSE:
            return "ERR remote closed during probe"
        if resp == "ERR auth failed":
            return "ERR auth failed on remote"
    except Exception as e:
        print(f"WARN: remote connect probe failed for {name}: {e.__class__.__name__}",
              file=sys.stderr)
        return "ERR cannot reach remote"
    finally:
        if ws is not None:
            try:
                ws.close()
            except Exception:
                pass
    try:
        _set_session(name, {
            "type": "remote",
            "alias": name,
            "host": host, "port": port, "token": token,
            "tls": use_tls,
        })
    except RuntimeError as e:
        return f"ERR {_public_error(e)}"
    return f"OK connected {name} -> {host}:{port}{' tls' if use_tls else ''}"

def _handle_stop(args: list[str]) -> str:
    if args:
        return "ERR usage: pyctl stop"
    if _daemon_server is not None:
        threading.Thread(target=_daemon_server.shutdown, daemon=True).start()
    return "OK stopping daemon"


def _handle_connect(args: list[str]) -> str:
    if len(args) not in (3, 4) or (len(args) == 4 and args[3] != "--tls"):
        return "ERR usage: pyctl connect <name> <host:port> <token> [--tls]"
    name, addr, token = args[0], args[1], args[2]
    use_tls = len(args) == 4
    try:
        host, port = _parse_host_port(addr)
    except ValueError:
        return f"ERR invalid address: {addr}"
    return connect_remote(name, host, port, token, use_tls)


def _handle_disconnect(args: list[str]) -> str:
    if len(args) != 1:
        return "ERR usage: pyctl disconnect <name>"
    name = args[0]
    s = _get_session(name)
    if s is None:
        return f"ERR no session '{name}'"
    if s["type"] != "remote":
        return f"ERR '{name}' is local, use kill"
    if not kill_session_if_current(name, s):
        return f"ERR session '{name}' changed during disconnect"
    return f"OK disconnected {name}"


def _handle_new(args: list[str]) -> str:
    if not args:
        return "ERR usage: pysh new <name>"
    name = args[0]
    try:
        _validate_session_name(name)
    except ValueError:
        return "ERR invalid session name"
    if len(args) > 1:
        return (f"ERR pysh new takes a name only"
                f" (got extra: {' '.join(args[1:])})."
                f" sessions are always Python")
    try:
        new_session(name)
    except (ValueError, RuntimeError) as e:
        return f"ERR {_public_error(e)}"
    s = _get_session(name)
    if s is None:
        return f"ERR failed to create session '{name}'"
    winpty = s.get("winpty")
    proc = s.get("proc")
    if winpty is not None:
        return f"OK {name} pid={winpty.pid} (winpty)"
    if proc is not None:
        return f"OK {name} pid={proc.pid}"
    return f"ERR failed to create session '{name}'"


def _handle_int(args: list[str]) -> str:
    if len(args) != 1:
        return "ERR usage: pysh int <name>"
    name = args[0]
    if _get_session(name) is None:
        return f"ERR no session '{name}'"
    resp = send_session(name, {"cmd": "int", "args": []})
    if not isinstance(resp, dict):
        return f"ERR int failed for {name}"
    if "error" in resp:
        return (f"ERR int failed for {name}: {resp['error']}. "
                "Session may be stuck in run or C code; use pysh kill.")
    t = resp.get("threads", 0)
    p = resp.get("processes", 0)
    parts: list[str] = []
    if t:
        parts.append(f"{t} {'thread' if t == 1 else 'threads'} (best-effort)")
    if p:
        parts.append(f"{p} {'process' if p == 1 else 'processes'} (killed)")
    if not parts:
        return f"OK no running cells in {name}"
    return f"OK int {name}: {', '.join(parts)}"


def _handle_kill(args: list[str]) -> str:
    if len(args) != 1:
        return "ERR usage: pysh kill <name>"
    name = args[0]
    if kill_session(name):
        return f"OK killed {name}"
    return f"ERR no session '{name}'"


def _handle_resize(args: list[str]) -> str:
    if len(args) != 3:
        return "ERR usage: resize <name> <rows> <cols>"
    try:
        name, rows, cols = args[0], int(args[1]), int(args[2])
    except ValueError:
        return "ERR rows/cols must be integers"
    if not (1 <= rows <= 65535 and 1 <= cols <= 65535):
        return "ERR rows/cols out of range"
    s = _get_session(name)
    if s is None:
        return f"ERR no session '{name}'"
    if s["type"] == "remote":
        return "ERR resize not supported for remote sessions"
    with _session_lock(s):
        if _get_session(name) is not s:
            return f"ERR no session '{name}'"
        if s["type"] == "pty" and "winpty" in s:
            s["winpty"].setwinsize(rows, cols)
        elif s["type"] == "pty" and s.get("master_fd") is not None:
            import struct
            fcntl.ioctl(  # type: ignore[name-defined]
                s["master_fd"],
                termios.TIOCSWINSZ,  # type: ignore[name-defined]
                struct.pack("HHHH", rows, cols, 0, 0),
            )
        else:
            return "ERR session has no live PTY"
    return "OK"


def _handle_ls(args: list[str]) -> str:
    if args:
        return "ERR usage: pysh ls"
    lines: list[str] = []
    for n, s in _session_snapshot():
        if s["type"] == "remote":
            tls_tag = " tls" if s.get("tls") else ""
            lines.append(f"  {n}: -> {s['host']}:{s['port']}{tls_tag} (remote)")
        elif s["type"] == "pty":
            winpty = s.get("winpty")
            proc = s.get("proc")
            if winpty is not None:
                alive = "alive" if winpty.isalive() else "DEAD"
                lines.append(f"  {n}: {alive} (winpty)")
            elif proc is not None:
                alive = "DEAD" if proc.poll() is not None else "alive"
                lines.append(f"  {n}: {alive} pid={proc.pid} (pty)")
            else:
                lines.append(f"  {n}: DEAD (pty)")
    return "\n".join(lines) or "(no sessions)"


def _log_cell_launch(name: str, src: str, resp: JsonDict) -> None:
    _log_session(name, src, json.dumps(resp), error=False)
    cid = resp.get("cell_id")
    if cid:
        current = _get_session(name)
        if current is not None:
            current.setdefault("_async_src", {})[cid] = src


def _log_cell_poll(name: str, resp: JsonDict, exec_error: bool) -> None:
    cid = resp.get("cell_id")
    current = _get_session(name)
    src = None
    if current is not None:
        src = current.setdefault("_async_src", {}).pop(cid, None)
    if src:
        output = resp.get("output", "")
        _log_session(name, src, output, error=exec_error)
        if not exec_error and src.strip():
            _log_history(name, src)


def _command_source(args: list[str]) -> str:
    return args[-1] if args else ""

def _handle_session_command(cmd: str, args: list[str]) -> str:
    if not args:
        return "ERR need session name"
    name = args[0]
    s = _get_session(name)
    if s is None:
        return f"ERR no session '{name}' -- pysh new {name}"
    inner_args = args[1:]
    if cmd in ("run", "fire", "fork") and inner_args:
        if s["type"] == "remote":
            if len(inner_args) >= 2:
                inner_args = [inner_args[0], " ".join(inner_args[1:])]
        else:
            inner_args = [" ".join(inner_args)]
    src = _command_source(inner_args) if cmd in ("run", "fire", "fork") else ""
    if src:
        lines = src.strip().splitlines()
        pfx = f"{name}>>> " if len(_session_snapshot()) > 1 else ">>> "
        cont = "." * len(pfx.rstrip()) + " "
        for i, ln in enumerate(lines):
            print(f"{pfx if i == 0 else cont}{ln}", file=sys.stderr)
    resp = send_session(name, {"cmd": cmd, "args": inner_args})
    if not isinstance(resp, dict):
        return str(resp)

    exec_error = bool(resp.pop("_error", False))
    if cmd == "run" and inner_args and "error" not in resp:
        output = resp.get("output", "")
        _log_session(name, src, output, error=exec_error)
        if not exec_error and src.strip():
            _log_history(name, src)
    elif cmd in ("fire", "fork") and inner_args and "error" not in resp:
        _log_cell_launch(name, src, resp)
    elif cmd == "poll" and "error" not in resp and resp.get("status") == "done":
        if exec_error:
            resp["error"] = True
        _log_cell_poll(name, resp, exec_error)

    if list(resp.keys()) == ["output"]:
        result = resp["output"]
        if cmd == "run" and result:
            print(result, file=sys.stderr)
        return str(result)
    return json.dumps(resp)


_CONTROL_HANDLERS: dict[str, typing.Callable[[list[str]], str]] = {
    "stop": _handle_stop,
    "connect": _handle_connect,
    "disconnect": _handle_disconnect,
    "new": _handle_new,
    "int": _handle_int,
    "kill": _handle_kill,
    "resize": _handle_resize,
    "ls": _handle_ls,
}
_SESSION_COMMANDS: set[str] = {
    "run", "fire", "fork", "poll", "status", "vars", "complete"
}


def handle_client(cmd: str, args: list[str]) -> str:
    """Handle one daemon control command from a client process."""
    handler = _CONTROL_HANDLERS.get(cmd)
    if handler is not None:
        return handler(args)
    if cmd in _SESSION_COMMANDS:
        return _handle_session_command(cmd, args)
    return f"ERR unknown: {cmd}"

def daemon(show_token: bool = False, listen_addr: str | None = None, tls: bool = False) -> None:
    """Run the daemon event loop with WebSocket protocol.

    Local POSIX: ws:// over AF_UNIX socket.
    Local Windows: ws://127.0.0.1:PORT with token auth.
    Remote: wss://HOST:PORT with token auth, plus mTLS when trusted_clients/ has certs.

    Protocol: text frames, first line = command, rest = code body.
      run name\\ncode    -> raw output
      fire name\\ncode   -> JSON {"cell_id":..., "status":"fired"}
      ls                 -> text listing
    Python code is never escaped -- it goes after the first \\n as-is.
    """
    try:
        from websockets.sync.server import serve as ws_serve
        if _HAS_AF_UNIX:
            from websockets.sync.server import unix_serve as ws_unix_serve
    except ImportError:
        print("ERR websockets required: pip install pythond",
              file=sys.stderr)
        raise SystemExit(1)

    global _daemon_token, _daemon_server
    _daemon_server = None
    ssl_ctx = None
    trusted_client_dir = None

    # --- resolve address & auth ---
    if listen_addr:
        if ":" in listen_addr:
            host, _, port_s = listen_addr.rpartition(":")
            host = host or "0.0.0.0"
            port = int(port_s)
        elif listen_addr.isdigit():
            host = "0.0.0.0"
            port = int(listen_addr)
        else:
            host = listen_addr
            port = int(os.environ.get("PYTHOND_PORT", "7399"))
        use_unix = False
        _use_mtls = False
        # RCE safety: non-localhost requires TLS
        if host not in ("127.0.0.1", "localhost", "::1") and not tls:
            print("ERR: --listen on non-localhost requires --tls (this is RCE)",
                  file=sys.stderr)
            print("     use --listen 127.0.0.1:PORT for localhost without TLS",
                  file=sys.stderr)
            raise SystemExit(1)
        if tls:
            cert, key = _generate_cert()
            fp = _cert_fingerprint(cert)
            ssl_ctx = _ssl.SSLContext(_ssl.PROTOCOL_TLS_SERVER)
            ssl_ctx.load_cert_chain(cert, key)
            ssl_ctx.minimum_version = _ssl.TLSVersion.TLSv1_2
            _daemon_token = secrets.token_hex(16)
            # mTLS: if trusted_clients/ has certs -> require client cert in
            # addition to token auth.  The local TLS terminator forwards to an
            # inner loopback WebSocket, so token auth remains mandatory there.
            trusted_client_dir = _trusted_clients_dir()
            n = _load_trusted_certs(ssl_ctx, trusted_client_dir)
            if _trusted_fingerprints(trusted_client_dir):
                ssl_ctx.verify_mode = _ssl.CERT_REQUIRED
                _use_mtls = True
        else:
            _daemon_token = secrets.token_hex(16)
    else:
        use_unix = _HAS_AF_UNIX
        if use_unix:
            _ensure_private_dir(os.path.dirname(SOCK))
            if os.path.exists(SOCK):
                os.unlink(SOCK)
        else:
            port = int(os.environ.get("PYTHOND_PORT", "7399"))
            _daemon_token = secrets.token_hex(16)
            try:
                _write_daemon_meta(port, _daemon_token)
            except RuntimeError as e:
                print(f"ERR {_public_error(e)}", file=sys.stderr)
                raise SystemExit(1)

    # --- connection handler (one thread per connection) ---
    def _ws_handler(ws: WebSocketLike) -> None:
        peer = getattr(ws, "remote_address", None)
        conn_id = next(_ACCESS_CONN_SEQ)
        _access_log("connect", conn_id=conn_id, peer=peer)
        # auth check for TCP mode
        if _daemon_token:
            auth = ws.request.headers.get("Authorization", "")
            token = ""
            if auth.startswith("Bearer "):
                token = auth[len("Bearer "):]
            if not hmac.compare_digest(token or "", _daemon_token):
                _access_log("auth", conn_id=conn_id, peer=peer, status="rejected")
                try:
                    ws.send("ERR auth failed")
                except Exception as e:
                    print(f"WARN: auth failure response failed: {e.__class__.__name__}",
                          file=sys.stderr)
                print(f"WARN: auth rejected from {peer}", file=sys.stderr)
                return
            _access_log("auth", conn_id=conn_id, peer=peer, status="ok")
        # keep-alive: handle multiple messages per connection
        try:
            for raw in ws:
                if isinstance(raw, bytes):
                    _access_log("command", conn_id=conn_id, peer=peer, status="rejected",
                                detail="binary-frame")
                    ws.send("ERR binary frame not allowed in command mode")
                    continue
                # protocol: "cmd arg1 arg2\nbody"
                if "\n" in raw:
                    header, body = raw.split("\n", 1)
                    has_body = True
                else:
                    header, body = raw, ""
                    has_body = False
                parts = header.split()
                cmd = parts[0] if parts else ""
                args = parts[1:]
                session_name = args[0] if args else ""
                if has_body:
                    args.append(body)
                body_len = len(body.encode("utf-8", "replace")) if has_body else 0
                _access_log("command", conn_id=conn_id, peer=peer, cmd=cmd,
                            session=session_name, body_bytes=body_len)

                # attach: switch to binary frame mode for PTY
                if cmd == "attach" and args:
                    aname = args[0]
                    s = _get_session(aname)
                    if s is None:
                        _access_log("attach", conn_id=conn_id, peer=peer, session=aname,
                                    status="no-session")
                        ws.send(f"ERR no session '{aname}'")
                        continue
                    bridge = s.get("bridge")
                    if not bridge:
                        _access_log("attach", conn_id=conn_id, peer=peer, session=aname,
                                    status="no-pty")
                        ws.send(f"ERR session '{aname}' has no PTY")
                        continue
                    if len(args) >= 3:
                        resize_resp = _handle_resize([aname, args[1], args[2]])
                        if resize_resp != "OK":
                            _access_log("attach", conn_id=conn_id, peer=peer, session=aname,
                                        status="resize-failed")
                            ws.send(resize_resp)
                            continue
                    owner = bridge.attach(lambda data: ws.send(data),
                                          lambda: ws.close())
                    if owner is None:
                        _access_log("attach", conn_id=conn_id, peer=peer, session=aname,
                                    status="busy")
                        ws.send(f"ERR session '{aname}' already attached")
                        continue
                    try:
                        _access_log("attach", conn_id=conn_id, peer=peer, session=aname,
                                    status="ok")
                        ws.send("OK attached")
                        if not bridge.flush_scrollback(owner):
                            _access_log("attach", conn_id=conn_id, peer=peer, session=aname,
                                        status="scrollback-failed")
                            return
                        for frame in ws:
                            if isinstance(frame, str):
                                if frame.strip() in ("detach", ""):
                                    break
                                continue
                            bridge.write(frame)  # binary -> PTY
                    finally:
                        bridge.detach(owner)
                        _access_log("detach", conn_id=conn_id, peer=peer, session=aname,
                                    status="ok")
                        try:
                            ws.send("OK detached")
                        except Exception:
                            pass  # detach ack failed -- connection closing anyway
                    return  # connection done after attach/detach

                try:
                    resp = handle_client(cmd, args)
                    status = "error" if resp.startswith("ERR ") else "ok"
                except Exception:
                    traceback.print_exc(file=sys.stderr)
                    resp = "ERR internal error"
                    status = "internal-error"
                _access_log("result", conn_id=conn_id, peer=peer, cmd=cmd,
                            session=session_name, status=status)
                try:
                    ws.send(resp or "")
                except Exception as e:
                    _access_log("result", conn_id=conn_id, peer=peer, cmd=cmd,
                                session=session_name, status="send-failed",
                                detail=e.__class__.__name__)
                    break
        finally:
            _access_log("disconnect", conn_id=conn_id, peer=peer)

    # --- start server ---
    mode = "winpty" if _WinPty else "pty"
    server: _Servable | None = None

    def _set_server(created: _Servable) -> _Servable:
        nonlocal server
        global _daemon_server
        server = created
        _daemon_server = created
        return created

    def _stop(signum: int, frame: typing.Any) -> None:
        if server:
            threading.Thread(target=server.shutdown, daemon=True).start()

    old_sigterm = None
    old_sigbreak = None
    try:
        old_sigterm = signal.signal(signal.SIGTERM, _stop)
    except (AttributeError, ValueError):
        pass  # signal not available on this platform
    if hasattr(signal, "SIGBREAK"):
        try:
            old_sigbreak = signal.signal(signal.SIGBREAK, _stop)
        except (AttributeError, ValueError):
            pass  # signal not available on this platform

    try:
        if use_unix:
            print(f"pythond pid={os.getpid()} ws://{SOCK} mode={mode}",
                  file=sys.stderr)
            old_umask = os.umask(0o177)
            try:
                server = _set_server(
                    ws_unix_serve(_ws_handler, SOCK,
                                  subprotocols=[_WS_PROTO])
                )
            finally:
                os.umask(old_umask)
            os.chmod(SOCK, 0o600)
        elif listen_addr:
            scheme = "wss" if tls else "ws"
            auth = "mtls" if _use_mtls else "token"
            print(f"pythond pid={os.getpid()} {scheme}://{host}:{port} mode={mode} auth={auth}",
                  file=sys.stderr)
            if _daemon_token and show_token:
                print(f"token={_daemon_token}", file=sys.stderr)
            elif _daemon_token:
                print("auth=token (use --show-token to print it)", file=sys.stderr)
            if tls:
                print(f"fingerprint={fp}", file=sys.stderr)
            if _use_mtls:
                print(f"mtls: {n} trusted client cert(s)", file=sys.stderr)
            if tls:
                assert ssl_ctx is not None
                server = _set_server(
                    _TlsTerminatedServer(ws_serve, _ws_handler, host,
                                         port, ssl_ctx, [_WS_PROTO],
                                         trusted_client_dir if _use_mtls else None)
                )
            else:
                server = _set_server(
                    ws_serve(_ws_handler, host, port,
                             subprotocols=[_WS_PROTO])
                )
        else:
            print(f"pythond pid={os.getpid()} ws://127.0.0.1:{port} mode={mode}",
                  file=sys.stderr)
            if show_token:
                tok_cmd = "set" if sys.platform == "win32" else "export"
                print(f"{tok_cmd} PYTHOND_TOKEN={_daemon_token}", file=sys.stderr)
            server = _set_server(
                ws_serve(_ws_handler, "127.0.0.1", port,
                         subprotocols=[_WS_PROTO])
            )

        if server is None:
            raise RuntimeError("server did not start")
        server.serve_forever()

    except KeyboardInterrupt:
        pass  # normal shutdown path
    except OSError as e:
        print(f"ERR {_public_error(e)}", file=sys.stderr)
        raise SystemExit(1)
    finally:
        print(f"\npythond stopped -- {_WS_HELLO}", file=sys.stderr)
        if old_sigterm is not None:
            try:
                signal.signal(signal.SIGTERM, old_sigterm)
            except (AttributeError, ValueError):
                pass  # signal not available on this platform
        if old_sigbreak is not None and hasattr(signal, "SIGBREAK"):
            try:
                signal.signal(signal.SIGBREAK, old_sigbreak)
            except (AttributeError, ValueError):
                pass  # signal not available on this platform
        try:
            if server is not None:
                server.shutdown()
        except Exception:
            pass  # server already stopped
        for name in list(sessions):
            kill_session(name)
        if use_unix and os.path.exists(SOCK):
            os.unlink(SOCK)
        if not use_unix and not listen_addr:
            _remove_daemon_meta()
        _daemon_server = None
        _daemon_token = None

# =============================================
# CLIENT
# =============================================

def _send(cmd: str, args: list[str]) -> str | None:
    """Send one command to daemon via WebSocket, return response string."""
    try:
        ws = _connect_daemon(timeout=5)
    except Exception as e:
        return f"ERR cannot connect: {_public_error(e)}"

    msg = _build_wire_message(cmd, args)
    try:
        ws.send(msg)
        resp = ws.recv(timeout=30)
        if resp is _WS_CLOSE:
            ws.close()
            return None
        if isinstance(resp, bytes):
            ws.close()
            return "ERR binary response not allowed in command mode"
        if not isinstance(resp, str):
            ws.close()
            return "ERR invalid daemon response"
        ws.close()
        return resp
    except Exception as e:
        try:
            ws.close()
        except Exception:
            pass  # connection close failed while reporting original error
        return f"ERR cannot connect: {_public_error(e)}"

def client(cmd: str, args: list[str], fail_on_err: bool = False) -> None:
    """CLI client for non-interactive commands."""
    resp = _send(cmd, args)
    if resp is None:
        print("ERR daemon not running -- start: pythond daemon", file=sys.stderr)
        sys.exit(1)
    if resp:
        print(resp, file=sys.stderr if resp.startswith("ERR ") else sys.stdout)
    if resp and resp.startswith("ERR ") and fail_on_err:
        sys.exit(1)

def attach(name: str) -> bool:
    """Connect a human terminal to a session REPL via WebSocket binary frames.
    Ctrl-] detaches. Session stays alive."""
    try:
        ws = _connect_daemon(timeout=5)
    except Exception as e:
        print(f"ERR connect failed: {_public_error(e)}", file=sys.stderr)
        return False

    # request attach
    resize_args = ""
    try:
        cols, rows = os.get_terminal_size()
        resize_args = f" {rows} {cols}"
    except OSError:
        pass  # non-interactive or detached terminal -- attach can still try
    try:
        ws.send(f"attach {name}{resize_args}")
        resp = ws.recv(timeout=5)
    except Exception as e:
        try:
            ws.close()
        except Exception:
            pass  # connection closing -- send/close may fail
        print(f"ERR attach failed: {_public_error(e)}", file=sys.stderr)
        return False
    if resp is _WS_CLOSE:
        resp = "ERR daemon closed connection"
    if isinstance(resp, bytes):
        resp = "ERR invalid attach response"
    if not resp.startswith("OK"):
        print(resp, file=sys.stderr)
        ws.close()
        return False

    try:
        if sys.platform == "win32":
            _attach_ws_win(ws, name)
        else:
            _attach_ws_pty(ws, name)
    except Exception as e:
        try:
            ws.send("detach")
        except Exception:
            pass  # connection closing -- send/close may fail
        try:
            ws.close()
        except Exception:
            pass  # connection closing -- send/close may fail
        print(f"ERR attach failed: {_public_error(e)}", file=sys.stderr)
        return False
    return True

def _attach_reader(ws: WebSocketLike, stopped: threading.Event) -> None:
    """WebSocket output -> stdout for both POSIX and Windows attach."""
    try:
        while not stopped.is_set():
            try:
                frame = ws.recv(timeout=2)
            except (TimeoutError, socket.timeout):
                continue
            if frame is _WS_CLOSE:
                break
            if isinstance(frame, bytes):
                os.write(sys.stdout.fileno(), frame)
            elif isinstance(frame, str):
                if frame == "OK detached":
                    break
                print(frame, file=sys.stderr)
    except Exception as e:
        if not stopped.is_set():
            print(f"WARN: attach reader failed: {e.__class__.__name__}",
                  file=sys.stderr)
    stopped.set()


def _attach_ws_loop(
    ws: WebSocketLike,
    name: str,
    read_input: typing.Callable[[threading.Event], bytes | None],
    restore_terminal: typing.Callable[[], None],
) -> None:
    """Shared attach loop. read_input returns bytes, None, or b'' for EOF."""
    if name:
        print(f"attached to {name} (Ctrl-] to detach)", file=sys.stderr)
    stopped = threading.Event()
    t: threading.Thread | None = None
    try:
        t = threading.Thread(target=_attach_reader, args=(ws, stopped),
                             daemon=True)
        t.start()
        while not stopped.is_set():
            data = read_input(stopped)
            if data is None:
                continue
            if not data or b"\x1d" in data:  # Ctrl-]
                before, _, _after = data.partition(b"\x1d")
                if before:
                    with contextlib.suppress(Exception):
                        ws.send(before)
                break
            try:
                ws.send(data)
            except Exception:
                break
    except (KeyboardInterrupt, OSError):
        pass  # user interrupted -- normal exit
    finally:
        stopped.set()
        if t is not None:
            t.join(timeout=3)
        restore_terminal()
        try:
            ws.send("detach")
        except Exception:
            pass  # connection closing -- send/close may fail
        try:
            ws.close()
        except Exception:
            pass  # connection closing -- send/close may fail
        print()


def _attach_ws_pty(ws: WebSocketLike, name: str = "") -> None:
    """POSIX raw terminal attach via WebSocket."""
    if not sys.stdin.isatty():
        raise RuntimeError("attach requires a TTY")
    old = termios.tcgetattr(sys.stdin)  # type: ignore[name-defined]
    tty.setraw(sys.stdin)  # type: ignore[name-defined]

    def read_input(_stopped: threading.Event) -> bytes | None:
        r, _, _ = _sel.select([sys.stdin], [], [], 0.1)  # type: ignore[name-defined]
        if sys.stdin not in r:
            return None
        return os.read(sys.stdin.fileno(), _ATTACH_READ_SIZE)

    def restore_terminal() -> None:
        termios.tcsetattr(  # type: ignore[name-defined]
            sys.stdin,
            termios.TCSADRAIN,  # type: ignore[name-defined]
            old,
        )

    _attach_ws_loop(ws, name, read_input, restore_terminal)


def _attach_ws_win(ws: WebSocketLike, name: str = "") -> None:
    """Windows raw terminal attach via WebSocket."""
    if not sys.stdin.isatty():
        raise RuntimeError("attach requires a TTY")
    import ctypes, msvcrt
    kernel32 = ctypes.windll.kernel32
    # argtypes: HANDLE is pointer-sized (64-bit on x64), not c_int
    kernel32.GetStdHandle.argtypes = [ctypes.c_uint32]
    kernel32.GetStdHandle.restype = ctypes.c_void_p
    kernel32.GetConsoleMode.argtypes = [ctypes.c_void_p, ctypes.POINTER(ctypes.c_uint32)]
    kernel32.GetConsoleMode.restype = ctypes.c_int
    kernel32.SetConsoleMode.argtypes = [ctypes.c_void_p, ctypes.c_uint32]
    kernel32.SetConsoleMode.restype = ctypes.c_int
    stdin_h = kernel32.GetStdHandle(-10)
    stdout_h = kernel32.GetStdHandle(-11)
    old_in = ctypes.c_uint32()
    old_out = ctypes.c_uint32()
    if not kernel32.GetConsoleMode(stdin_h, ctypes.byref(old_in)):
        raise RuntimeError("GetConsoleMode failed")
    if not kernel32.GetConsoleMode(stdout_h, ctypes.byref(old_out)):
        raise RuntimeError("GetConsoleMode failed")
    kernel32.SetConsoleMode(
        stdin_h,
        (old_in.value & ~0x0007) | _WIN_ENABLE_VIRTUAL_TERMINAL_INPUT,
    )
    kernel32.SetConsoleMode(
        stdout_h,
        old_out.value | _WIN_ENABLE_PROCESSED_OUTPUT |
        _WIN_ENABLE_WRAP_AT_EOL_OUTPUT | _WIN_ENABLE_VIRTUAL_TERMINAL_PROCESSING,
    )

    def read_input(stopped: threading.Event) -> bytes | None:
        if not msvcrt.kbhit():
            time.sleep(0.01)
            return None
        first = msvcrt.getch()
        if first in (b"\x00", b"\xe0"):
            while not stopped.is_set() and not msvcrt.kbhit():
                time.sleep(0.01)
            if stopped.is_set():
                return None
            return first + msvcrt.getch()
        return first

    def restore_terminal() -> None:
        kernel32.SetConsoleMode(stdin_h, old_in.value)
        kernel32.SetConsoleMode(stdout_h, old_out.value)

    _attach_ws_loop(ws, name, read_input, restore_terminal)

def _worker_entry(argv: list[str]) -> bool:
    """Handle internal worker subprocess entry points."""
    if argv[0] == "_worker_pty":
        slave_fd = int(argv[1])
        ai_fd = int(argv[2])
        try:
            if os.getsid(0) != os.getpid():  # type: ignore[attr-defined]
                os.setsid()  # type: ignore[attr-defined]
        except OSError as e:
            print(f"warn: setsid: {e}", file=sys.stderr)
        try:
            TIOCSCTTY = getattr(termios, 'TIOCSCTTY', 0x540E)  # type: ignore[name-defined]
            fcntl.ioctl(slave_fd, TIOCSCTTY, 0)  # type: ignore[name-defined]
        except (OSError, NameError) as e:
            print(f"warn: TIOCSCTTY: {e}", file=sys.stderr)
        os.dup2(slave_fd, 0)
        os.dup2(slave_fd, 1)
        os.dup2(slave_fd, 2)
        if slave_fd > 2:
            os.close(slave_fd)
        ai_sock = socket.socket(fileno=ai_fd)
        session_worker_pty(ai_sock)
        sys.exit(0)
    if argv[0] == "_worker_winpty":
        ai_port = int(argv[1])
        ai_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        ai_sock.connect(("127.0.0.1", ai_port))
        session_worker_pty(ai_sock)
        sys.exit(0)
    return False

def _add_session_subparsers(sub: argparse._SubParsersAction) -> None:
    p_attach = sub.add_parser("attach", help="attach terminal to session")
    p_attach.add_argument("name", nargs="?", default="default")
    p_new = sub.add_parser("new", help="create session")
    p_new.add_argument("name")
    for cname, chelp in (
        ("run", "sync exec, raw output"),
        ("fire", "async thread exec"),
        ("fork", "async process exec"),
    ):
        p_cmd = sub.add_parser(cname, help=chelp)
        p_cmd.add_argument("name")
        p_cmd.add_argument("code", nargs=argparse.REMAINDER)
    p_poll = sub.add_parser("poll", help="check async result")
    p_poll.add_argument("name")
    p_poll.add_argument("cell_id", nargs="?")
    for cname, chelp in (
        ("int", "interrupt running cells"),
        ("kill", "terminate session"),
        ("status", "session health"),
        ("vars", "namespace names"),
    ):
        p_cmd = sub.add_parser(cname, help=chelp)
        p_cmd.add_argument("name")
    sub.add_parser("ls", help="list sessions")
    p_complete = sub.add_parser("complete", help="tab completions")
    p_complete.add_argument("name")
    p_complete.add_argument("text")

def main() -> None:
    """Entry point for `pythond` command -- full command set."""
    argv = sys.argv[1:]
    if argv and argv[0].startswith("_worker"):
        if os.environ.get(_WORKER_ENV) != "1":
            print("ERR internal worker entry point", file=sys.stderr)
            sys.exit(1)
        if not _worker_entry(argv):
            print(f"ERR unknown worker command: {argv[0]}", file=sys.stderr)
            sys.exit(1)
        return

    parser = argparse.ArgumentParser(
        prog="pythond",
        description="Persistent Python REPL daemon.",
        epilog="Use pysh for session commands and pyctl for daemon management.",
    )
    parser.add_argument("-V", "--version", action="version",
                        version=f"pythond {__version__}")
    sub = parser.add_subparsers(dest="command")

    p_daemon = sub.add_parser("daemon", help="start daemon in foreground")
    p_daemon.add_argument("--listen", metavar="HOST:PORT",
                          help="listen address")
    p_daemon.add_argument("--tls", action="store_true",
                          help="enable TLS")
    p_daemon.add_argument("--show-token", action="store_true",
                          help="print auth token")

    _add_session_subparsers(sub)

    if not argv:
        parser.print_help()
        sys.exit(0)
    if argv[0] == "version":
        print(f"pythond {__version__}")
        sys.exit(0)

    args = parser.parse_args(argv)
    if args.command == "daemon":
        daemon(show_token=args.show_token, listen_addr=args.listen,
               tls=args.tls)
    elif args.command == "attach":
        if not attach(args.name):
            sys.exit(1)
    else:
        client(args.command, argv[1:], fail_on_err=True)

def pysh_main() -> None:
    """Entry point for `pysh` command -- session commands."""
    argv = sys.argv[1:]
    parser = argparse.ArgumentParser(
        prog="pysh",
        description="Python Shell: client for pythond daemon.",
        epilog="Remote sessions are managed by pyctl connect/disconnect.",
    )
    parser.add_argument("-V", "--version", action="version",
                        version=f"pythond {__version__}")
    sub = parser.add_subparsers(dest="command")

    _add_session_subparsers(sub)

    if not argv:
        parser.print_help()
        sys.exit(0)
    if argv[0] == "version":
        print(f"pythond {__version__}")
        sys.exit(0)

    args = parser.parse_args(argv)
    if args.command == "attach":
        if not attach(args.name):
            sys.exit(1)
    else:
        client(args.command, argv[1:], fail_on_err=True)


def _pyctl_env_status() -> bool:
    """Print liveness for the daemon selected by PYTHOND_* environment."""
    endpoint = os.environ.get("PYTHOND_HOST")
    try:
        ws = _connect_daemon(timeout=2)
    except Exception as e:
        print(f"endpoint: {endpoint}")
        print("alive: False")
        print(f"error: {_public_error(e)}")
        return False
    try:
        ws.send("ls")
        resp = ws.recv(timeout=2)
        alive = resp is not _WS_CLOSE and not (
            isinstance(resp, str) and resp.startswith("ERR ")
        )
        print(f"endpoint: {endpoint}")
        print(f"alive: {alive}")
        return alive
    except Exception as e:
        print(f"endpoint: {endpoint}")
        print("alive: False")
        print(f"error: {_public_error(e)}")
        return False
    finally:
        try:
            ws.close()
        except Exception:
            pass  # status probing should not fail because close failed


def pyctl_main() -> None:
    """Entry point for `pyctl` command -- daemon management."""
    argv = sys.argv[1:]
    parser = argparse.ArgumentParser(
        prog="pyctl",
        description="pythond daemon control.",
        epilog="pysh sends code; pyctl manages daemon lifecycle, proxy, and certs.",
    )
    parser.add_argument("-V", "--version", action="version",
                        version=f"pythond {__version__}")
    sub = parser.add_subparsers(dest="command")

    p_start = sub.add_parser("start", help="start daemon")
    p_start.add_argument("--listen", metavar="HOST:PORT")
    p_start.add_argument("--tls", action="store_true")
    p_start.add_argument("--show-token", action="store_true")
    sub.add_parser("stop", help="stop daemon gracefully")
    sub.add_parser("status", help="daemon process info")
    p_connect = sub.add_parser("connect", help="proxy to remote daemon")
    p_connect.add_argument("name")
    p_connect.add_argument("addr", metavar="host:port")
    p_connect.add_argument("token")
    p_connect.add_argument("--tls", action="store_true")
    p_disconnect = sub.add_parser("disconnect", help="drop remote proxy")
    p_disconnect.add_argument("name")
    p_trust = sub.add_parser("trust", help="trust a client cert")
    p_trust.add_argument("cert", metavar="cert.pem")
    p_pin = sub.add_parser("pin", help="pin a server cert")
    p_pin.add_argument("cert", metavar="cert.pem")
    sub.add_parser("cert", help="show/generate this machine's cert")

    if not argv:
        parser.print_help()
        sys.exit(0)
    if argv[0] == "version":
        print(f"pythond {__version__}")
        sys.exit(0)

    args = parser.parse_args(argv)
    if args.command == "start":
        daemon(show_token=args.show_token, listen_addr=args.listen,
               tls=args.tls)
    elif args.command == "stop":
        client("stop", [], fail_on_err=True)
    elif args.command == "connect":
        connect_args = [args.name, args.addr, args.token]
        if args.tls:
            connect_args.append("--tls")
        client("connect", connect_args, fail_on_err=True)
    elif args.command == "disconnect":
        client("disconnect", [args.name], fail_on_err=True)
    elif args.command == "trust":
        if not _HAS_CRYPTO:
            print("ERR: pip install pythond", file=sys.stderr)
            sys.exit(1)
        try:
            dest, fp = trust_cert(args.cert, direction="client")
        except RuntimeError as e:
            print(f"ERR {_public_error(e)}", file=sys.stderr)
            sys.exit(1)
        print(f"trusted client: {fp}")
        print(f"  -> {dest}")
    elif args.command == "pin":
        if not _HAS_CRYPTO:
            print("ERR: pip install pythond", file=sys.stderr)
            sys.exit(1)
        try:
            dest, fp = trust_cert(args.cert, direction="server")
        except RuntimeError as e:
            print(f"ERR {_public_error(e)}", file=sys.stderr)
            sys.exit(1)
        print(f"pinned server: {fp}")
        print(f"  -> {dest}")
    elif args.command == "cert":
        if not _HAS_CRYPTO:
            print("ERR: pip install pythond", file=sys.stderr)
            sys.exit(1)
        cert, key = _generate_cert()
        fp = _cert_fingerprint(cert)
        print("this machine's TLS certificate:")
        print(f"cert: {cert}")
        print(f"key:  {key}")
        print(f"fingerprint: {fp}")
        print("\nIf this machine is the client:")
        print(f"  copy {cert} to the server and run: pyctl trust <copied-client-cert.pem>")
        print("If this machine is the server:")
        print(f"  copy {cert} to the client and run: pyctl pin <copied-server-cert.pem>")
    elif args.command == "status":
        meta = _read_daemon_meta()
        alive = False
        if os.environ.get("PYTHOND_HOST"):
            alive = _pyctl_env_status()
        elif _HAS_AF_UNIX:
            alive = _unix_daemon_alive()
            print(f"socket: {SOCK}")
            print(f"alive: {alive}")
        elif meta:
            alive = _tcp_daemon_alive(meta)
            print(f"port: {meta.get('port')}")
            print(f"pid: {meta.get('pid')}")
            print(f"alive: {alive}")
        else:
            print("no daemon metadata found")
        if not alive:
            sys.exit(1)
    else:
        parser.print_help(sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
