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
    pyctl start [--listen HOST:PORT] [--tls]
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

Security:
  Not a sandbox: code runs with the daemon user's OS permissions.
  Local POSIX:   AF_UNIX socket mode 0o600.
  Local Windows: OWNER RIGHTS DACL via icacls (process-tree isolation).
  Remote token:  wss:// + shared token (symmetric, password-like).
  Remote mTLS:   wss:// + mutual cert verification, plus token.
    pyctl trust  = authorized_keys (server lets client in).
    pyctl pin    = known_hosts (client verifies server).
  Crash containment: per-session worker processes; daemon tries to reap failed sessions.

Auto-checkpoint:
  ~/.pythond/sessions/<name>/history.py -- successful execs only, replayable.
  ~/.pythond/sessions/<name>/session.log -- all activity including errors.

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
import sys, os, socket, json, threading, uuid, io, traceback, time, tempfile, code
import select
import signal, subprocess
import multiprocessing as mp
import pickle
import secrets
import base64
import hmac
import re
import typing

__version__ = "0.3.0"
JsonDict = dict[str, typing.Any]
MaybeJson = JsonDict | str
WebSocketLike = typing.Any
SocketLike = typing.Any
_WS_PROTO: typing.Any = f"pythond.{__version__[:3]}"   # e.g. "pythond.0.3"
_WS_HELLO = "tis but a scratch"
_MAX_SESSIONS = int(os.environ.get("PYTHOND_MAX_SESSIONS", "128"))
_MAX_WS_PAYLOAD = int(os.environ.get("PYTHOND_MAX_WS_PAYLOAD", str(16 * 1024 * 1024)))
_SESSION_NAME_RE = re.compile(r"^[A-Za-z0-9_.-]{1,80}$")
_BUFFER_CHUNK = 64 * 1024
_ASYNC_CELL_TTL = 300
_WS_HANDSHAKE_LIMIT = 64 * 1024
_ATTACH_READ_SIZE = 1024
_WIN_ENABLE_PROCESSED_INPUT = 0x0001
_WIN_ENABLE_VIRTUAL_TERMINAL_INPUT = 0x0200
_WIN_ENABLE_PROCESSED_OUTPUT = 0x0001
_WIN_ENABLE_WRAP_AT_EOL_OUTPUT = 0x0002
_WIN_ENABLE_VIRTUAL_TERMINAL_PROCESSING = 0x0004
_WS_CLOSE = object()

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
    Falls back to $TMPDIR/pythond-$UID.sock with UID to prevent squatting.
    """
    xdg = os.environ.get("XDG_RUNTIME_DIR")
    if xdg and os.path.isdir(xdg):
        return os.path.join(xdg, "pythond.sock")
    uid = os.getuid() if hasattr(os, "getuid") else os.getpid()
    return os.path.join(tempfile.gettempdir(), f"pythond-{uid}.sock")

SOCK = os.environ.get("PYTHOND_SOCK", _default_sock())

# -----------------------------------------------
# SESSION HISTORY (auto-checkpoint)
# -----------------------------------------------

def _validate_session_name(name: str) -> str:
    """Validate a session/proxy name before it becomes a filesystem path."""
    if (not isinstance(name, str) or not _SESSION_NAME_RE.fullmatch(name)
            or ".." in name):
        raise ValueError("invalid session name")
    return name

def _ensure_private_dir(path: str) -> str:
    """Create a daemon data directory and restrict it to the current user."""
    created = not os.path.isdir(path)
    os.makedirs(path, exist_ok=True)
    if sys.platform == "win32":
        if created:
            _secure_path_win32(path)
    else:
        try:
            os.chmod(path, 0o700)
        except OSError:
            pass  # chmod hardening -- dir still works without it
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
        new_file = not os.path.exists(path)
        with open(path, "a", encoding="utf-8") as f:
            if new_file:
                try:
                    os.chmod(path, 0o600)
                except OSError:
                    pass  # permission hardening -- keep logging best-effort
            f.write(f"\n# [{time.strftime('%Y-%m-%d %H:%M:%S')}]\n{src}\n")
    except OSError:
        pass  # best-effort -- don't crash if log dir missing

def _log_session(name: str, src: str, output: str = "", error: bool = False) -> None:
    """Append all exec activity to session.log (human readable)."""
    try:
        path = os.path.join(_session_dir(name), "session.log")
        new_file = not os.path.exists(path)
        with open(path, "a", encoding="utf-8") as f:
            if new_file:
                try:
                    os.chmod(path, 0o600)
                except OSError:
                    pass  # permission hardening -- keep logging best-effort
            tag = "ERROR" if error else "OK"
            f.write(f"\n# [{time.strftime('%Y-%m-%d %H:%M:%S')}] {tag}\n")
            f.write(f"{src}\n")
            if output:
                for line in output.splitlines():
                    f.write(f"# > {line}\n")
    except OSError:
        pass  # best-effort -- don't crash if log dir missing

# -----------------------------------------------
# SOCKET helpers
# -----------------------------------------------

def _secure_path_win32(path: str) -> None:
    """Set OWNER RIGHTS DACL on a Windows path intentionally.

    This gives process-tree-level isolation: only the process that created
    the path (and its children) can access it.  Other processes under the
    same user account cannot.  This is stronger than Unix user-level isolation.

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
    except (FileNotFoundError, subprocess.CalledProcessError):
        # icacls not available (shouldn't happen on Win10+)
        # fall back to user-level: at least restrict via token auth
        pass

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
    if not token:
        return False
    try:
        from websockets.sync.client import connect as ws_connect
        ws = ws_connect(f"ws://127.0.0.1:{port}/",
                        additional_headers=_auth_headers(token),
                        proxy=None,
                        open_timeout=2, close_timeout=1,
                        subprotocols=[_WS_PROTO])
        ws.send("ls")
        resp = ws.recv(timeout=2)
        ws.close()
        return resp != "ERR auth failed"
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
    fd = os.open(tmp, flags, 0o600) if sys.platform != "win32" else os.open(tmp, flags)
    try:
        os.write(fd, json.dumps(data).encode())
        os.fsync(fd)
    finally:
        os.close(fd)
    if sys.platform != "win32":
        os.chmod(tmp, 0o600)
    os.replace(tmp, path)

def _read_daemon_meta() -> JsonDict:
    """Read daemon metadata, returning {} when absent or invalid."""
    try:
        with open(_daemon_meta_path(), encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError):
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
    except OSError:
        pass  # cleanup -- don't crash on unlink failure

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
        return cert_path, key_path

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

    for fpath, data, mode in [
        (key_path, key_pem, 0o600),
        (cert_path, cert_pem, 0o644),
    ]:
        fd = os.open(fpath,
                     os.O_WRONLY | os.O_CREAT | os.O_TRUNC,
                     mode if sys.platform != "win32" else 0o666)
        try:
            os.write(fd, data)
            os.fsync(fd)
        finally:
            os.close(fd)
        if sys.platform != "win32":
            os.chmod(fpath, mode)

    return cert_path, key_path

def _cert_fingerprint(cert_path: str) -> str:
    """Return SHA-256 fingerprint of cert for pinning."""
    try:
        with open(cert_path, "rb") as f:
            der = _ssl.PEM_cert_to_DER_cert(f.read().decode())
        digest = _hashlib.sha256(der).hexdigest().upper()
        return ":".join(digest[i:i+2] for i in range(0, len(digest), 2))
    except (OSError, ValueError):
        return "unknown"

def _trusted_clients_dir() -> str:
    """Return ~/.pythond/tls/trusted_clients/ -- server trusts these clients."""
    return _ensure_private_dir(os.path.join(_tls_dir(), "trusted_clients"))

def _trusted_servers_dir() -> str:
    """Return ~/.pythond/tls/trusted_servers/ -- client trusts these servers."""
    return _ensure_private_dir(os.path.join(_tls_dir(), "trusted_servers"))

def _load_trusted_certs(ssl_ctx: _ssl.SSLContext, directory: str) -> int:
    """Load all .pem certs from a directory into SSLContext. Returns count."""
    count = 0
    for f in os.listdir(directory):
        if f.endswith(".pem"):
            try:
                ssl_ctx.load_verify_locations(os.path.join(directory, f))
                count += 1
            except _ssl.SSLError:
                print(f"warn: skipping malformed cert {f}", file=sys.stderr)
    return count

def trust_cert(cert_path: str, direction: str = "client") -> tuple[str, str]:
    """Copy a cert into the appropriate trusted dir.

    direction="client" -> server trusts this client (pyctl trust)
    direction="server" -> client trusts this server (pyctl pin)
    """
    td = _trusted_clients_dir() if direction == "client" else _trusted_servers_dir()
    fp = _cert_fingerprint(cert_path)
    name = fp.replace(":", "")[:16] + ".pem"
    dest = os.path.join(td, name)
    import shutil
    shutil.copy2(cert_path, dest)
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
    ) -> None:
        self._stopped = threading.Event()
        self._ssl_ctx = ssl_ctx
        self._inner = ws_serve(handler, "127.0.0.1", 0,
                               subprotocols=subprotocols)
        inner_addr = self._inner.socket.getsockname()
        self._inner_port = inner_addr[1]
        self._sock = socket.create_server((host, port))
        self._sock.settimeout(1.0)
        self._threads: list[threading.Thread] = []

    def serve_forever(self) -> None:
        t = threading.Thread(target=self._inner.serve_forever, daemon=True)
        t.start()
        self._threads.append(t)
        while not self._stopped.is_set():
            self._reap_threads()
            try:
                raw, _addr = self._sock.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            worker = threading.Thread(target=self._handle, args=(raw,),
                                      daemon=True)
            worker.start()
            self._threads.append(worker)

    def _reap_threads(self) -> None:
        """Drop finished bridge threads so accepted connections do not leak."""
        self._threads = [t for t in self._threads if t.is_alive()]

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
            inner_sock = socket.create_connection(("127.0.0.1",
                                                   self._inner_port))
            self._bridge(tls_sock, inner_sock)
        except Exception:
            pass
        finally:
            for sock in (tls_sock, inner_sock, raw):
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
                if not readable:
                    readable, _, _ = select.select(list(peers), [], [], 1.0)
            except (OSError, ValueError):
                return
            for src in readable:
                try:
                    data = src.recv(_BUFFER_CHUNK)
                except (_ssl.SSLWantReadError, _ssl.SSLWantWriteError,
                        BlockingIOError):
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
        try:
            while view:
                try:
                    sent = sock.send(view)
                    if sent == 0:
                        select.select([], [sock], [], 1.0)
                        return False
                    view = view[sent:]
                except _ssl.SSLWantReadError:
                    select.select([sock], [], [], 1.0)
                except (_ssl.SSLWantWriteError, BlockingIOError):
                    select.select([], [sock], [], 1.0)
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
    def flush(self) -> None:
        buf = getattr(self._local, "buf", None)
        (buf or self._real).flush()
    def fileno(self) -> int:
        return self._real.fileno()
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
    if isinstance(last, _ast.Expr) and len(tree.body) > 1:
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
    # install thread-local stdout/stderr once per session
    if not isinstance(sys.stdout, _ThreadStdout):
        sys.stdout = _ThreadStdout(sys.stdout)
    if not isinstance(sys.stderr, _ThreadStdout):
        sys.stderr = _ThreadStdout(sys.stderr)

    def _exec(src: str) -> _ExecOutput:
        with lock:
            buf = io.StringIO()
            had_error = False
            stdout = typing.cast(_ThreadStdout, sys.stdout)
            stderr = typing.cast(_ThreadStdout, sys.stderr)
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
            output = buf.getvalue().rstrip()
            if on_done:
                on_done(src, output)
            return _ExecOutput(output, had_error)
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
        cid = uuid.uuid4().hex[:12]
        res = {"output": "", "status": "running", "tid": None}
        def _bg(c: str = args[0], r: JsonDict = res) -> None:
            r["tid"] = threading.current_thread().ident
            try:
                out = _exec(c)
                r["output"] = str(out)
                r["_error"] = bool(getattr(out, "error", False))
            except BaseException:
                r["output"] = traceback.format_exc().rstrip()
                r["_error"] = True
            finally:
                r["status"] = "done"
                r["_done_at"] = time.time()
                r["tid"] = None
        with _cells_lock:
            cells[cid] = res
            _evict_stale_cells(cells)
        threading.Thread(target=_bg, daemon=True).start()
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
        cid = uuid.uuid4().hex[:12]
        # Use os.fork() + os._exit() instead of mp.Process.
        # mp.Process does Python cleanup after fork (join threads, atexit,
        # flush buffers) which deadlocks on locks held by threads that
        # don't exist in the child.  os._exit() skips all of that.
        r_fd, w_fd = os.pipe()
        # Prevent fork child's subprocess from inheriting pipe fds.
        # Without this, a grandchild process holds w_fd open -> parent's
        # read never sees EOF -> monitor hangs.
        try:
            os.set_inheritable(r_fd, False)
            os.set_inheritable(w_fd, False)
        except OSError:
            pass
        # Snapshot under lock for consistent namespace view, but fork OUTSIDE
        # the lock.  Child inherits lock state -- if locked, no owning thread
        # exists in child -> any code touching the lock deadlocks forever.
        if lock:
            with lock:
                ns_snap = {k: id(v) for k, v in ns.items()}
        else:
            ns_snap = {k: id(v) for k, v in ns.items()}
        # Small race window: fire thread could modify ns between snapshot
        # and fork.  Acceptable -- fork merge is already last-writer-wins.
        child_pid = os.fork()
        if child_pid == 0:
            # --- child process (exits via os._exit, no Python cleanup) ---
            os.close(r_fd)
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
                        except (pickle.PicklingError, TypeError, AttributeError):
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
        res = {"output": "", "status": "running", "pid": child_pid}
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
            except OSError:
                pass  # pipe broken -- child died before finishing write
            finally:
                try:
                    os.close(fd)
                except OSError:
                    pass
            try:
                os.waitpid(pid, 0)
            except ChildProcessError:
                pass  # already reaped
            try:
                if chunks:
                    data = pickle.loads(b"".join(chunks))
                    r["output"] = data.get("output", "")
                    r["_error"] = bool(data.get("_error", False))
                    merged = data.get("diff", {})
                    if lock:
                        with lock:
                            ns.update(merged)
                    else:
                        ns.update(merged)
                    r["_merged"] = list(merged.keys())
                    r["_skipped"] = data.get("skipped", [])
                else:
                    r["output"] = "(killed)"
                    r["_error"] = True
                    r["_merged"] = []
                    r["_skipped"] = []
            except (EOFError, OSError, pickle.UnpicklingError):
                r["output"] = r.get("output", "") or "(killed)"
                r["_error"] = True
                r["_merged"] = []
                r["_skipped"] = []
            except Exception:
                r["output"] = r.get("output", "") or "(fork result read failed)"
                r["_error"] = True
                r["_merged"] = []
                r["_skipped"] = []
            finally:
                r["status"] = "done"
                r["_done_at"] = time.time()
            r["pid"] = None
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
        import ctypes
        _set_async = ctypes.pythonapi.PyThreadState_SetAsyncExc
        _set_async.argtypes = [ctypes.c_ulong, ctypes.py_object]
        _set_async.restype = ctypes.c_int
        threads = 0
        processes = 0
        with _cells_lock:
            snapshot = list(cells.items())
        for cid, r in snapshot:
            if r["status"] != "running":
                continue
            tid = r.get("tid")
            pid = r.get("pid")
            if tid:
                rc = _set_async(ctypes.c_ulong(tid),
                                ctypes.py_object(KeyboardInterrupt))
                if rc > 1:
                    _set_async(ctypes.c_ulong(tid), ctypes.py_object(None))
                if rc >= 1:
                    threads += 1
            elif pid:
                try:
                    os.kill(pid, signal.SIGKILL)  # type: ignore[attr-defined]
                    processes += 1
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
            last_id = list(cells)[-1]
            r = cells[last_id]
        resp = {"cell_id": last_id, "status": r["status"],
                 "output": r["output"]}
        if r.get("_error"):
            resp["_error"] = True
        if "_merged" in r:
            resp["merged"] = r["_merged"]
            resp["skipped"] = r["_skipped"]
        return resp
    elif cmd == "status":
        vs = len([v for v in ns if not v.startswith("_")])
        with _cells_lock:
            _evict_stale_cells(cells)
            running = [cid for cid, r in cells.items()
                       if r["status"] == "running"]
            ncells = len(cells)
        return {"state": "running" if running else "idle",
                "running": running, "vars": vs, "cells": ncells}
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
            except Exception:
                try:
                    wf.write(json.dumps({"error": "worker protocol error"}) + "\n")
                    wf.flush()
                except BaseException:
                    break

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

def _get_session(name: str) -> JsonDict | None:
    """Return a session object by name, or None if absent."""
    with _sessions_lock:
        return sessions.get(name)

def _set_session(name: str, session: JsonDict) -> None:
    """Publish a newly created session atomically."""
    _validate_session_name(name)
    with _sessions_lock:
        _check_session_capacity_locked(name)
        sessions[name] = session

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
        self._owner: object | None = None
        self._lock = threading.Lock()
        self._scrollback = bytearray()
        self._MAX = _BUFFER_CHUNK
        threading.Thread(target=self._reader, daemon=True).start()

    def attach(self, send_fn: typing.Callable[[bytes], typing.Any]) -> object | None:
        """Attach one client. send_fn(bytes) sends binary data to client."""
        with self._lock:
            if self._send_fn is not None:
                return None
            owner = object()
            self._send_fn = send_fn
            self._owner = owner
            if self._scrollback:
                try:
                    send_fn(bytes(self._scrollback))
                except Exception:
                    pass  # scrollback flush failed -- not critical
                self._scrollback.clear()
            return owner

    def detach(self, owner: object | None = None) -> None:
        """Detach current client. PTY output goes to scrollback buffer."""
        with self._lock:
            if owner is not None and self._owner is not owner:
                return
            self._send_fn = None
            self._owner = None

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
            with self._lock:
                if self._send_fn:
                    try:
                        self._send_fn(data)
                    except Exception:
                        self._send_fn = None
                else:
                    self._scrollback.extend(data)
                    if len(self._scrollback) > self._MAX:
                        del self._scrollback[:-self._MAX]

def new_session(name: str) -> None:
    """Create or replace one named Python session."""
    _ensure_session_capacity(name)
    if _get_session(name) is not None:
        kill_session(name)
    if _HAS_PTY and _WinPty is not None:
        ai_srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        if hasattr(socket, "SO_EXCLUSIVEADDRUSE"):
            ai_srv.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
        ai_srv.bind(("127.0.0.1", 0))
        ai_port = ai_srv.getsockname()[1]
        ai_srv.listen(1)
        ai_srv.settimeout(10)
        proc = _WinPty.spawn(
            [sys.executable, os.path.abspath(__file__),
             "_worker_winpty", str(ai_port)]
        )
        try:
            ai_conn, _ = ai_srv.accept()
        except socket.timeout:
            proc.terminate(force=True)
            ai_srv.close()
            raise RuntimeError("winpty worker failed to connect")
        ai_srv.close()
        def _read() -> bytes:
            try:
                return proc.read().encode()
            except EOFError:
                return b""
        def _write(data: bytes) -> None:
            proc.write(data.decode(errors="replace"))
        session = {
            "type": "pty", "winpty": proc,
            "ai": ai_conn, "bridge": PtyBridge(_read, _write),
        }
        try:
            _set_session(name, session)
        except Exception:
            _close_session_resources(session)
            raise
        threading.Thread(target=_monitor_session, args=(name,),
                         daemon=True).start()
    elif _HAS_PTY:
        master_fd, slave_fd = pty.openpty()  # type: ignore[name-defined]
        ai_parent, ai_child = socket.socketpair()
        p = subprocess.Popen(
            [sys.executable, os.path.abspath(__file__),
             "_worker_pty", str(slave_fd), str(ai_child.fileno())],
            close_fds=True,
            pass_fds=(slave_fd, ai_child.fileno()),
        )
        os.close(slave_fd)
        ai_child.close()
        session = {
            "type": "pty", "proc": p, "master_fd": master_fd,
            "ai": ai_parent,
            "bridge": PtyBridge(
                lambda: os.read(master_fd, 4096),
                lambda d: os.write(master_fd, d)),
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
    with _session_lock(s):
        with _sessions_lock:
            if sessions.get(name) is not s:
                return False
            sessions.pop(name, None)
        return _close_session_resources(s)

def _close_session_resources(s: JsonDict) -> bool:
    """Close resources for a session already removed from the session map."""
    if s["type"] == "remote":
        ws = s.get("_ws")
        if ws:
            try:
                ws.close()
            except Exception:
                pass  # cleanup must not raise -- resources may already be dead
        return True
    if s["type"] == "pty":
        if "winpty" in s:
            try:
                if s["winpty"].isalive():
                    s["winpty"].terminate(force=True)
            except Exception:
                pass  # cleanup must not raise -- resources may already be dead
        else:
            # Kill entire process group (worker + any fork children).
            # Worker called os.setsid(), so its pgid == its pid.
            try:
                pgid = os.getpgid(s["proc"].pid)  # type: ignore[attr-defined]
            except OSError:
                pgid = None
            if pgid is not None:
                try:
                    os.killpg(pgid, signal.SIGTERM)  # type: ignore[attr-defined]
                    s["proc"].wait(timeout=3)
                except (OSError, subprocess.TimeoutExpired):
                    try:
                        os.killpg(pgid, signal.SIGKILL)  # type: ignore[attr-defined]
                        s["proc"].wait(timeout=1)
                    except Exception:
                        pass  # cleanup must not raise
            else:
                try:
                    s["proc"].terminate()
                    s["proc"].wait(timeout=3)
                except (OSError, subprocess.TimeoutExpired):
                    try:
                        s["proc"].kill()
                        s["proc"].wait(timeout=1)
                    except Exception:
                        pass  # cleanup must not raise
        if s.get("master_fd") is not None:
            try:
                os.close(s["master_fd"])
            except OSError:
                pass  # cleanup must not raise -- resources may already be dead
        for resource in ("ai",):
            try:
                handle = s.get(resource)
                if handle is not None:
                    handle.close()
            except OSError:
                pass  # cleanup must not raise -- resources may already be dead
    else:
        if s["proc"].is_alive():
            s["proc"].terminate()
            s["proc"].join(timeout=3)
            if s["proc"].is_alive():
                s["proc"].kill()
                s["proc"].join(timeout=1)
    return True

def _monitor_session(name: str) -> None:
    """Wait for a session's worker to exit, then auto-reap."""
    s = _get_session(name)
    if not s:
        return
    try:
        if s["type"] == "pty":
            if "winpty" in s:
                s["winpty"].wait()
            elif "proc" in s:
                s["proc"].wait()
        else:
            s["proc"].join()
    except Exception:
        pass  # process exited abnormally -- still need to reap
    kill_session(name)  # pop is atomic under _sessions_lock

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
        if s.get("_unhealthy") and msg.get("cmd") != "int":
            return {"error": f"session '{name}' command channel out of sync after timeout; "
                    f"use pysh int {name} or pysh kill {name}"}
        if s["type"] == "remote":
            return _send_remote(s, msg, timeout)
        if s["type"] == "pty":
            try:
                if "ai_wf" not in s:
                    s["ai_rf"] = s["ai"].makefile("r")
                    s["ai_wf"] = s["ai"].makefile("w")
                s["ai"].settimeout(timeout)
                s["ai_wf"].write(json.dumps(msg) + "\n")
                s["ai_wf"].flush()
                line = s["ai_rf"].readline()
                if not line:
                    return {"error": f"session '{name}' dead -- pysh new {name} to restart"}
                resp: dict[str, typing.Any] = json.loads(line)
                if msg.get("cmd") == "int":
                    s.pop("_unhealthy", None)
                return resp
            except socket.timeout:
                s["_unhealthy"] = True
                return {"error": "timeout -- command channel may be out of sync; "
                        "use pysh int or pysh kill if stuck"}
            except (OSError, json.JSONDecodeError):
                return {"error": "session command failed"}
            finally:
                try:
                    s["ai"].settimeout(None)
                except OSError:
                    pass  # socket may be dead
        else:
            if not s["proc"].is_alive():
                return {"error": f"session '{name}' dead -- pysh new {name} to restart"}
            s["tx"].send(msg)
            if s["rx"].poll(timeout):
                result: dict[str, typing.Any] = s["rx"].recv()
                return result
            return {"error": "timeout -- cell may still be running; "
                    "use pysh int or pysh kill if stuck"}

# -----------------------------------------------
# REMOTE PROXY -- persistent TCP to remote daemon
# -----------------------------------------------

def _client_ssl_ctx() -> _ssl.SSLContext:
    """Create TLS client context with directional trust.

    Loads client cert for mTLS (proving identity to server).
    Loads trusted_servers/ certs for server verification.
    Fails closed: if no pinned server certs and no system CA can verify,
    the connection will be rejected.  Use `pyctl pin <cert.pem>` to trust
    a self-signed remote daemon, or install a CA-signed cert on the server.
    """
    ctx = _ssl.SSLContext(_ssl.PROTOCOL_TLS_CLIENT)
    ctx.minimum_version = _ssl.TLSVersion.TLSv1_2
    # load pinned server certs if available
    n = _load_trusted_certs(ctx, _trusted_servers_dir())
    if n > 0:
        # Self-signed pinned daemon certs rarely match DNS names. The pin
        # itself is the server identity check.
        ctx.check_hostname = False
        ctx.verify_mode = _ssl.CERT_REQUIRED
    else:
        # no pinned certs -- fall back to system CA bundle
        ctx.check_hostname = True
        ctx.load_default_certs()
        ctx.verify_mode = _ssl.CERT_REQUIRED
    # load client cert for mTLS
    try:
        cert = os.path.join(_tls_dir(), "cert.pem")
        key = os.path.join(_tls_dir(), "key.pem")
        if os.path.exists(cert) and os.path.exists(key):
            ctx.load_cert_chain(cert, key)
    except (_ssl.SSLError, OSError):
        pass  # client cert optional -- skip if absent or broken
    return ctx


class _RawWssClient:
    """Minimal RFC6455 client for WSS command frames.

    websockets.sync.client can fail to send the HTTP upgrade on WSS on this
    Windows/Python stack.  This client is intentionally narrow: text command
    frames, text/binary receives, ping/pong, close.  Plain WS and AF_UNIX still
    use websockets directly.
    """

    def __init__(self, sock: SocketLike) -> None:
        self.sock = sock

    @classmethod
    def connect(
        cls,
        host: str,
        port: int,
        ssl_ctx: _ssl.SSLContext,
        token: str | None = None,
        timeout: float = 10,
    ) -> "_RawWssClient":
        raw = socket.create_connection((host, port), timeout=timeout)
        try:
            sock = ssl_ctx.wrap_socket(raw, server_hostname=host)
            sock.settimeout(timeout)
            key = base64.b64encode(secrets.token_bytes(16)).decode("ascii")
            headers = [
                "GET / HTTP/1.1",
                f"Host: {host}:{port}",
                "Upgrade: websocket",
                "Connection: Upgrade",
                f"Sec-WebSocket-Key: {key}",
                "Sec-WebSocket-Version: 13",
                f"Sec-WebSocket-Protocol: {_WS_PROTO}",
            ]
            if token:
                headers.append(f"Authorization: Bearer {token}")
            request = "\r\n".join(headers) + "\r\n\r\n"
            sock.sendall(request.encode("ascii"))
            response = cls._read_http_response(sock)
            if not response.startswith(b"HTTP/1.1 101 "):
                raise RuntimeError(response.split(b"\r\n", 1)[0].decode(
                    "latin1", "replace"))
            accept = base64.b64encode(_hashlib.sha1(
                (key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode(
                    "ascii")).digest()).decode("ascii")
            if f"sec-websocket-accept: {accept.lower()}".encode(
                    "ascii") not in response.lower():
                raise RuntimeError("bad websocket accept")
            return cls(sock)
        except Exception:
            raw.close()
            raise

    @staticmethod
    def _read_http_response(sock: SocketLike) -> bytes:
        data = b""
        while b"\r\n\r\n" not in data:
            chunk = sock.recv(4096)
            if not chunk:
                raise RuntimeError("connection closed during handshake")
            data += chunk
            if len(data) > _WS_HANDSHAKE_LIMIT:
                raise RuntimeError("websocket handshake too large")
        return data

    def send(self, data: str | bytes) -> None:
        if isinstance(data, bytes):
            opcode = 0x2
            payload = data
        else:
            opcode = 0x1
            payload = str(data).encode("utf-8")
        self._send_frame(opcode, payload)

    def recv(self, timeout: float | None = None) -> str | bytes | object:
        old_timeout = self.sock.gettimeout()
        if timeout is not None:
            self.sock.settimeout(timeout)
        try:
            while True:
                opcode, payload = self._recv_frame()
                if opcode == 0x1:
                    return payload.decode("utf-8", "replace")
                if opcode == 0x2:
                    return payload
                if opcode == 0x8:
                    return _WS_CLOSE
                if opcode == 0x9:
                    self._send_frame(0xA, payload)
        finally:
            if timeout is not None:
                self.sock.settimeout(old_timeout)

    def close(self) -> None:
        try:
            self._send_frame(0x8, b"")
        except OSError:
            pass
        try:
            self.sock.close()
        except OSError:
            pass

    def _send_frame(self, opcode: int, payload: bytes) -> None:
        first = 0x80 | opcode
        length = len(payload)
        mask = secrets.token_bytes(4)
        if length < 126:
            header = bytes([first, 0x80 | length])
        elif length < (1 << 16):
            header = bytes([first, 0x80 | 126]) + length.to_bytes(2, "big")
        else:
            header = bytes([first, 0x80 | 127]) + length.to_bytes(8, "big")
        masked = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
        self.sock.sendall(header + mask + masked)

    def _recv_frame(self) -> tuple[int, bytes]:
        header = self._recv_exact(2)
        opcode = header[0] & 0x0F
        length = header[1] & 0x7F
        masked = bool(header[1] & 0x80)
        if length == 126:
            length = int.from_bytes(self._recv_exact(2), "big")
        elif length == 127:
            length = int.from_bytes(self._recv_exact(8), "big")
        if length > _MAX_WS_PAYLOAD:
            raise RuntimeError("websocket frame too large")
        mask = self._recv_exact(4) if masked else b""
        payload = self._recv_exact(length)
        if masked:
            payload = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
        return opcode, payload

    def _recv_exact(self, n: int) -> bytes:
        data = b""
        while len(data) < n:
            chunk = self.sock.recv(n - len(data))
            if not chunk:
                raise RuntimeError("websocket closed")
            data += chunk
        return data


def _connect_wss(
    host: str,
    port: int,
    token: str | None,
    timeout: float = 10,
) -> _RawWssClient:
    return _RawWssClient.connect(host, port, _client_ssl_ctx(), token, timeout)


def _parse_host_port(value: str, default_port: int = 7399) -> tuple[str, int]:
    """Parse HOST[:PORT] without treating IPv6 as supported syntax."""
    if ":" in value:
        host, _, port_s = value.rpartition(":")
        return host, int(port_s)
    return value, int(os.environ.get("PYTHOND_PORT", str(default_port)))


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
    try:
        if _HAS_AF_UNIX:
            from websockets.sync.client import unix_connect as ws_unix_connect
    except ImportError:
        raise RuntimeError("websockets required: pip install pythond")

    host = os.environ.get("PYTHOND_HOST")
    use_tls = os.environ.get("PYTHOND_TLS", "").lower() in ("1", "true", "yes")
    token = os.environ.get("PYTHOND_TOKEN")

    if host:
        h, port = _parse_host_port(host)
        return _open_remote_ws(h, port, token, use_tls=use_tls, timeout=timeout)
    if _HAS_AF_UNIX:
        return ws_unix_connect(SOCK, open_timeout=timeout, close_timeout=2,
                               subprotocols=[_WS_PROTO])

    meta = _read_daemon_meta()
    port = int(os.environ.get("PYTHOND_PORT") or meta.get("port") or "7399")
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
                return {"error": "remote connect failed"}
        try:
            ws.send(ws_msg)
            resp = ws.recv(timeout=timeout)
            if resp is _WS_CLOSE:
                raise RuntimeError("remote closed")
            if cmd == "run":
                return {"output": resp}
            try:
                parsed: dict[str, typing.Any] = json.loads(resp)
                return parsed
            except json.JSONDecodeError:
                return {"output": resp}
        except Exception:
            session["_ws"] = None
            try:
                ws.close()
            except Exception:
                pass  # reconnect attempt -- clear and retry
            if attempt == 0:
                continue
            return {"error": "remote unreachable"}
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
        return f"ERR {e}"
    if _get_session(name) is not None:
        kill_session(name)
    # test connectivity + auth now; actual data goes through _send_remote
    # which reconnects lazily.  This test catches bad host/port/token early
    # but doesn't guarantee future requests succeed (network can change).
    try:
        ws = _open_remote_ws(host, port, token, use_tls=use_tls, timeout=10)
        ws.send("ls")
        resp = ws.recv(timeout=5)
        ws.close()
        if resp == "ERR auth failed":
            return "ERR auth failed on remote"
    except Exception:
        return "ERR cannot reach remote"
    try:
        _set_session(name, {
            "type": "remote",
            "host": host, "port": port, "token": token,
            "tls": use_tls,
        })
    except RuntimeError as e:
        return f"ERR {e}"
    return f"OK connected {name} -> {host}:{port}{' tls' if use_tls else ''}"

def _handle_stop(args: list[str]) -> str:
    if args:
        return "ERR usage: pyctl stop"
    if _daemon_server is not None:
        threading.Thread(target=_daemon_server.shutdown, daemon=True).start()
    return "OK stopping daemon"


def _handle_connect(args: list[str]) -> str:
    if len(args) < 3:
        return "ERR usage: pyctl connect <name> <host:port> <token> [--tls]"
    name, addr, token = args[0], args[1], args[2]
    use_tls = "--tls" in args
    try:
        host, port = _parse_host_port(addr)
    except ValueError:
        return f"ERR invalid address: {addr}"
    return connect_remote(name, host, port, token, use_tls)


def _handle_disconnect(args: list[str]) -> str:
    if not args:
        return "ERR usage: pyctl disconnect <name>"
    name = args[0]
    s = _get_session(name)
    if s is None:
        return f"ERR no session '{name}'"
    if s["type"] != "remote":
        return f"ERR '{name}' is local, use kill"
    kill_session(name)
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
        return f"ERR {e}"
    s = _get_session(name)
    if s is None:
        return f"ERR failed to create session '{name}'"
    if "winpty" in s:
        return f"OK {name} pid={s['winpty'].pid} (winpty)"
    return f"OK {name} pid={s['proc'].pid}"


def _handle_int(args: list[str]) -> str:
    if not args:
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
    if not args:
        return "ERR usage: pysh kill <name>"
    name = args[0]
    if kill_session(name):
        return f"OK killed {name}"
    return f"ERR no session '{name}'"


def _handle_resize(args: list[str]) -> str:
    if len(args) < 3:
        return "ERR usage: resize <name> <rows> <cols>"
    try:
        name, rows, cols = args[0], int(args[1]), int(args[2])
    except ValueError:
        return "ERR rows/cols must be integers"
    s = _get_session(name)
    if s is None:
        return f"ERR no session '{name}'"
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
            if "winpty" in s:
                alive = "alive" if s["winpty"].isalive() else "DEAD"
                lines.append(f"  {n}: {alive} (winpty)")
            else:
                alive = "DEAD" if s["proc"].poll() is not None else "alive"
                lines.append(f"  {n}: {alive} pid={s['proc'].pid} (pty)")
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


def _handle_session_command(cmd: str, args: list[str]) -> str:
    if not args:
        return "ERR need session name"
    name = args[0]
    s = _get_session(name)
    if s is None:
        return f"ERR no session '{name}' -- pysh new {name}"
    inner_args = args[1:]
    if cmd in ("run", "fire", "fork") and inner_args:
        code_str = inner_args[0]
        lines = code_str.strip().splitlines()
        pfx = f"{name}>>> " if len(_session_snapshot()) > 1 else ">>> "
        cont = "." * len(pfx.rstrip()) + " "
        for i, ln in enumerate(lines):
            print(f"{pfx if i == 0 else cont}{ln}", file=sys.stderr)
    resp = send_session(name, {"cmd": cmd, "args": inner_args})
    if not isinstance(resp, dict):
        return str(resp)

    exec_error = bool(resp.pop("_error", False))
    if cmd == "run" and inner_args and "error" not in resp:
        src = inner_args[0]
        output = resp.get("output", "")
        _log_session(name, src, output, error=exec_error)
        if not exec_error and src.strip():
            _log_history(name, src)
    elif cmd in ("fire", "fork") and inner_args and "error" not in resp:
        _log_cell_launch(name, inner_args[0], resp)
    elif cmd == "poll" and "error" not in resp and resp.get("status") == "done":
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
            n = _load_trusted_certs(ssl_ctx, _trusted_clients_dir())
            if n > 0:
                ssl_ctx.verify_mode = _ssl.CERT_REQUIRED
                _use_mtls = True
        else:
            _daemon_token = secrets.token_hex(16)
    else:
        use_unix = _HAS_AF_UNIX
        if use_unix:
            if os.path.exists(SOCK):
                os.unlink(SOCK)
        else:
            port = int(os.environ.get("PYTHOND_PORT", "7399"))
            _daemon_token = secrets.token_hex(16)
            try:
                _write_daemon_meta(port, _daemon_token)
            except RuntimeError as e:
                print(f"ERR {e}", file=sys.stderr)
                raise SystemExit(1)

    # --- connection handler (one thread per connection) ---
    def _ws_handler(ws: WebSocketLike) -> None:
        # auth check for TCP mode
        if _daemon_token:
            auth = ws.request.headers.get("Authorization", "")
            token = ""
            if auth.startswith("Bearer "):
                token = auth[len("Bearer "):]
            if not hmac.compare_digest(token or "", _daemon_token):
                try:
                    ws.send("ERR auth failed")
                except Exception:
                    pass  # connection already dead -- can't send auth error
                return
        # keep-alive: handle multiple messages per connection
        for raw in ws:
            if isinstance(raw, bytes):
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
            if has_body:
                args.append(body)

            # attach: switch to binary frame mode for PTY
            if cmd == "attach" and args:
                aname = args[0]
                s = _get_session(aname)
                if s is None:
                    ws.send(f"ERR no session '{aname}'")
                    continue
                bridge = s.get("bridge")
                if not bridge:
                    ws.send(f"ERR session '{aname}' has no PTY")
                    continue
                owner = bridge.attach(lambda data: ws.send(data))
                if owner is None:
                    ws.send(f"ERR session '{aname}' already attached")
                    continue
                ws.send("OK attached")
                try:
                    for frame in ws:
                        if isinstance(frame, str):
                            if frame.strip() in ("detach", ""):
                                break
                            continue
                        bridge.write(frame)  # binary -> PTY
                finally:
                    bridge.detach(owner)
                    try:
                        ws.send("OK detached")
                    except Exception:
                        pass  # detach ack failed -- connection closing anyway
                return  # connection done after attach/detach

            try:
                resp = handle_client(cmd, args)
            except Exception:
                traceback.print_exc(file=sys.stderr)
                resp = "ERR internal error"
            try:
                ws.send(resp or "")
            except Exception:
                break

    # --- start server ---
    mode = "winpty" if _WinPty else "pty"
    server: _Servable | None = None

    def _stop(signum: int, frame: typing.Any) -> None:
        if server:
            server.shutdown()

    old_sigterm = None
    try:
        old_sigterm = signal.signal(signal.SIGTERM, _stop)
    except (AttributeError, ValueError):
        pass  # signal not available on this platform
    if hasattr(signal, "SIGBREAK"):
        try:
            signal.signal(signal.SIGBREAK, _stop)
        except (AttributeError, ValueError):
            pass  # signal not available on this platform

    try:
        if use_unix:
            print(f"pythond pid={os.getpid()} ws://{SOCK} mode={mode}",
                  file=sys.stderr)
            server = ws_unix_serve(_ws_handler, SOCK,
                                     subprotocols=[_WS_PROTO])
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
                server = _TlsTerminatedServer(ws_serve, _ws_handler, host,
                                              port, ssl_ctx, [_WS_PROTO])
            else:
                server = ws_serve(_ws_handler, host, port,
                                  subprotocols=[_WS_PROTO])
        else:
            print(f"pythond pid={os.getpid()} ws://127.0.0.1:{port} mode={mode}",
                  file=sys.stderr)
            if show_token:
                tok_cmd = "set" if sys.platform == "win32" else "export"
                print(f"{tok_cmd} PYTHOND_TOKEN={_daemon_token}", file=sys.stderr)
            server = ws_serve(_ws_handler, "127.0.0.1", port,
                              subprotocols=[_WS_PROTO])

        _daemon_server = server
        server.serve_forever()

    except KeyboardInterrupt:
        pass  # normal shutdown path
    except OSError as e:
        print(f"ERR {e}", file=sys.stderr)
        raise SystemExit(1)
    finally:
        print(f"\npythond stopped -- {_WS_HELLO}", file=sys.stderr)
        if old_sigterm is not None:
            try:
                signal.signal(signal.SIGTERM, old_sigterm)
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

# =============================================
# CLIENT
# =============================================

def _send(cmd: str, args: list[str]) -> str | None:
    """Send one command to daemon via WebSocket, return response string."""
    try:
        ws = _connect_daemon(timeout=5)
    except Exception:
        return None

    msg = _build_wire_message(cmd, args)
    try:
        ws.send(msg)
        resp = ws.recv()
        if resp is _WS_CLOSE:
            return None
        ws.close()
        return typing.cast(str, resp)
    except Exception:
        try:
            ws.close()
        except Exception:
            pass  # connection closed -- return None to caller
        return None

def client(cmd: str, args: list[str]) -> None:
    """CLI client for non-interactive commands."""
    resp = _send(cmd, args)
    if resp is None:
        print("ERR daemon not running -- start: pythond daemon", file=sys.stderr)
        sys.exit(1)
    if resp:
        print(resp)

def attach(name: str) -> None:
    """Connect a human terminal to a session REPL via WebSocket binary frames.
    Ctrl-] detaches. Session stays alive."""
    try:
        ws = _connect_daemon(timeout=5)
    except Exception as e:
        print(f"ERR connect failed: {e}", file=sys.stderr)
        return

    # request attach
    try:
        ws.send(f"attach {name}")
        resp = ws.recv(timeout=5)
    except Exception as e:
        try:
            ws.close()
        except Exception:
            pass  # connection closing -- send/close may fail
        print(f"ERR attach failed: {e}", file=sys.stderr)
        return
    if resp is _WS_CLOSE:
        resp = "ERR daemon closed connection"
    if not resp.startswith("OK"):
        print(resp, file=sys.stderr)
        ws.close()
        return

    # resize
    try:
        rows, cols = os.get_terminal_size()
        _send("resize", [name, str(rows), str(cols)])
    except OSError:
        pass  # terminal restore -- best effort on exit

    if sys.platform == "win32":
        _attach_ws_win(ws, name)
    else:
        _attach_ws_pty(ws, name)

def _attach_reader(ws: WebSocketLike, stopped: threading.Event) -> None:
    """WebSocket output -> stdout for both POSIX and Windows attach."""
    try:
        while not stopped.is_set():
            frame = ws.recv()
            if frame is _WS_CLOSE:
                break
            if isinstance(frame, bytes):
                os.write(sys.stdout.fileno(), frame)
            elif isinstance(frame, str) and "detached" in frame:
                break
    except Exception:
        pass  # connection closing -- send/close may fail
    stopped.set()


def _attach_ws_loop(
    ws: WebSocketLike,
    name: str,
    read_input: typing.Callable[[], bytes | None],
    restore_terminal: typing.Callable[[], None],
) -> None:
    """Shared attach loop. read_input returns bytes, None, or b'' for EOF."""
    if name:
        print(f"attached to {name} (Ctrl-] to detach)", file=sys.stderr)
    stopped = threading.Event()
    try:
        t = threading.Thread(target=_attach_reader, args=(ws, stopped),
                             daemon=True)
        t.start()
        while not stopped.is_set():
            data = read_input()
            if data is None:
                continue
            if not data or b"\x1d" in data:  # Ctrl-]
                break
            try:
                ws.send(data)
            except Exception:
                break
    except (KeyboardInterrupt, OSError):
        pass  # user interrupted -- normal exit
    finally:
        stopped.set()
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
    old = termios.tcgetattr(sys.stdin)  # type: ignore[name-defined]
    tty.setraw(sys.stdin)  # type: ignore[name-defined]

    def read_input() -> bytes | None:
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
    kernel32.GetConsoleMode(stdin_h, ctypes.byref(old_in))
    kernel32.GetConsoleMode(stdout_h, ctypes.byref(old_out))
    kernel32.SetConsoleMode(stdin_h, _WIN_ENABLE_VIRTUAL_TERMINAL_INPUT)
    kernel32.SetConsoleMode(
        stdout_h,
        old_out.value | _WIN_ENABLE_PROCESSED_OUTPUT |
        _WIN_ENABLE_WRAP_AT_EOL_OUTPUT | _WIN_ENABLE_VIRTUAL_TERMINAL_PROCESSING,
    )

    def read_input() -> bytes | None:
        if not msvcrt.kbhit():
            time.sleep(0.01)
            return None
        return msvcrt.getch()

    def restore_terminal() -> None:
        kernel32.SetConsoleMode(stdin_h, old_in.value)
        kernel32.SetConsoleMode(stdout_h, old_out.value)

    _attach_ws_loop(ws, name, read_input, restore_terminal)

def _mp_init() -> None:
    try:
        mp.set_start_method("fork", force=True)
    except ValueError:
        pass  # fork not available -- use platform default

def _worker_entry(argv: list[str]) -> bool:
    """Handle internal worker subprocess entry points."""
    if argv[0] == "_worker_pty":
        slave_fd = int(argv[1])
        ai_fd = int(argv[2])
        os.setsid()  # type: ignore[attr-defined]
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

def main() -> None:
    """Entry point for `pythond` command -- full command set."""
    _mp_init()
    argv = sys.argv[1:]
    if not argv or argv[0] in ("-h", "--help"):
        print(__doc__.encode(sys.stdout.encoding or "utf-8", "replace").decode(sys.stdout.encoding or "utf-8", "replace"))
        sys.exit(0)
    if argv[0] in ("--version", "-V", "version"):
        print(f"pythond {__version__}")
        sys.exit(0)
    if argv[0].startswith("_worker"):
        _worker_entry(argv)
        return
    if argv[0] == "daemon":
        show = "--show-token" in argv
        listen = None
        use_tls = "--tls" in argv
        for i, a in enumerate(argv):
            if a == "--listen" and i + 1 < len(argv):
                listen = argv[i + 1]
        daemon(show_token=show, listen_addr=listen, tls=use_tls)
    elif argv[0] == "attach":
        name = argv[1] if len(argv) > 1 else "default"
        attach(name)
    else:
        client(argv[0], argv[1:])

_PYSH_HELP = """\
pysh -- Python Shell. Client for pythond daemon.

  pysh run <name> "code"       sync exec, raw output
  pysh fire <name> "code"      async (thread) -- shares namespace, can't kill C
  pysh fork <name> "code"      async process (POSIX only) -- killable, pickles back
  pysh poll <name> [cell_id]   check async result
  pysh attach <name>           human REPL (Ctrl-] detach)
  pysh new <name>              create session
  pysh int <name>              best-effort interrupt (fire=best effort, fork=kill)
  pysh kill <name>             terminate session
  pysh ls                      list sessions
  pysh status <name>           session health (JSON)
  pysh vars <name>             namespace names (JSON)
  pysh complete <name> "text"  tab completions (JSON)
  pysh --version               print version

Remote sessions are managed by pyctl (connect/disconnect).
Once connected, pysh run/fire/fork/poll work transparently.
"""

def pysh_main() -> None:
    """Entry point for `pysh` command -- session commands."""
    _mp_init()
    argv = sys.argv[1:]
    if not argv or argv[0] in ("-h", "--help"):
        print(_PYSH_HELP)
        sys.exit(0)
    if argv[0] in ("--version", "-V", "version"):
        print(f"pythond {__version__}")
        sys.exit(0)
    if argv[0] == "attach":
        name = argv[1] if len(argv) > 1 else "default"
        attach(name)
    else:
        client(argv[0], argv[1:])

_PYCTL_HELP = """\
pyctl -- pythond daemon control.

  pyctl start [--show-token]               start daemon (local)
  pyctl start --listen HOST:PORT [--tls]   start daemon (remote)
  pyctl stop                               stop daemon gracefully
  pyctl status                             daemon process info
  pyctl connect <name> <host:port> <token> [--tls]
                                           proxy to remote pythond daemon
  pyctl disconnect <name>                  drop remote proxy
  pyctl trust <cert.pem>                   let this client connect (server-side)
  pyctl pin <cert.pem>                     verify this server is real (client-side)
  pyctl cert                               show/generate this machine's cert
  pyctl --version                          print version

Architecture:
  pysh   = send code to sessions (local or remote, transparent)
  pyctl  = manage the daemon itself (start, stop, proxy, certs)
  daemon = execute code + reverse-proxy to remote daemons
"""

def pyctl_main() -> None:
    """Entry point for `pyctl` command -- daemon management."""
    _mp_init()
    argv = sys.argv[1:]
    if not argv or argv[0] in ("-h", "--help"):
        print(_PYCTL_HELP)
        sys.exit(0)
    if argv[0] in ("--version", "-V", "version"):
        print(f"pythond {__version__}")
        sys.exit(0)
    if argv[0] == "start":
        show = "--show-token" in argv
        listen = None
        use_tls = "--tls" in argv
        for i, a in enumerate(argv):
            if a == "--listen" and i + 1 < len(argv):
                listen = argv[i + 1]
        daemon(show_token=show, listen_addr=listen, tls=use_tls)
    elif argv[0] == "stop":
        client("stop", argv[1:])
    elif argv[0] == "connect":
        # pyctl connect <name> <host:port> <token> [--tls]
        # -> tells the daemon to proxy to a remote pythond
        client("connect", argv[1:])
    elif argv[0] == "disconnect":
        # pyctl disconnect <name>
        # -> tells the daemon to drop a remote proxy
        client("disconnect", argv[1:])
    elif argv[0] == "trust":
        if len(argv) < 2:
            print("usage: pyctl trust <cert.pem>  (let this client in)", file=sys.stderr)
            sys.exit(1)
        if not _HAS_CRYPTO:
            print("ERR: pip install pythond", file=sys.stderr)
            sys.exit(1)
        dest, fp = trust_cert(argv[1], direction="client")
        print(f"trusted client: {fp}")
        print(f"  -> {dest}")
    elif argv[0] == "pin":
        if len(argv) < 2:
            print("usage: pyctl pin <cert.pem>  (verify this server)", file=sys.stderr)
            sys.exit(1)
        if not _HAS_CRYPTO:
            print("ERR: pip install pythond", file=sys.stderr)
            sys.exit(1)
        dest, fp = trust_cert(argv[1], direction="server")
        print(f"pinned server: {fp}")
        print(f"  -> {dest}")
    elif argv[0] == "cert":
        if not _HAS_CRYPTO:
            print("ERR: pip install pythond", file=sys.stderr)
            sys.exit(1)
        cert, key = _generate_cert()
        fp = _cert_fingerprint(cert)
        print(f"cert: {cert}")
        print(f"key:  {key}")
        print(f"fingerprint: {fp}")
        print(f"\nOn server:  pyctl trust {cert}")
        print(f"On client:  pyctl pin {cert}")
    elif argv[0] == "status":
        meta = _read_daemon_meta()
        if _HAS_AF_UNIX:
            alive = os.path.exists(SOCK)
            print(f"socket: {SOCK}")
            print(f"alive: {alive}")
        elif meta:
            alive = _tcp_daemon_alive(meta)
            print(f"port: {meta.get('port')}")
            print(f"pid: {meta.get('pid')}")
            print(f"alive: {alive}")
        else:
            print("no daemon metadata found")
    else:
        print(f"ERR unknown pyctl command: {argv[0]}", file=sys.stderr)
        print(_PYCTL_HELP, file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
