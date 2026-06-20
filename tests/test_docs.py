#!/usr/bin/env python3
"""Documentation drift tests for README, SKILL, man page, site, examples, and packaging."""

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
README = (ROOT / "README.md").read_text(encoding="utf-8")
SKILL = (ROOT / "SKILL.md").read_text(encoding="utf-8")
HTML = (ROOT / "docs" / "index.html").read_text(encoding="utf-8")
MAN = (ROOT / "man" / "agent-tty.1").read_text(encoding="utf-8")
EXAMPLES = (ROOT / "EXAMPLES.md").read_text(encoding="utf-8")
PYPROJECT = (ROOT / "pyproject.toml").read_text(encoding="utf-8")
INIT = (ROOT / "src" / "agent_tty" / "__init__.py").read_text(encoding="utf-8")
K_HELP = (ROOT / "src" / "agent_tty" / "cli.py").read_text(encoding="utf-8")
KM_HELP = (ROOT / "src" / "agent_tty" / "monitor.py").read_text(encoding="utf-8")
TEST_SH = (ROOT / "tests" / "test.sh").read_text(encoding="utf-8")
CI = (ROOT / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8") if (ROOT / ".github" / "workflows" / "ci.yml").exists() else ""
FAILURES: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    if not cond:
        FAILURES.append(f"{name}: {detail}".rstrip(": "))


def max_runtime_checks() -> int:
    check_calls = len(re.findall(r'^\s*check(?:_exact)?\s+"', TEST_SH, flags=re.M))
    manual_checks = len(re.findall(r'&&\s*PASS=\$\(\(PASS\+1\)\)', TEST_SH))
    return check_calls + manual_checks


expected_checks = max_runtime_checks()
project_version_match = re.search(r'^version = "([^"]+)"$', PYPROJECT, flags=re.M)
init_version_match = re.search(r'^__version__ = "([^"]+)"$', INIT, flags=re.M)
check("pyproject: version exists", project_version_match is not None)
check("__init__: version exists", init_version_match is not None)
project_version = project_version_match.group(1) if project_version_match else ""
init_version = init_version_match.group(1) if init_version_match else ""
cli_commands = sorted(set(re.findall(r'^\s*k\s+([a-z]+)\b', K_HELP, flags=re.M)))
DOCS = (("README.md", README), ("SKILL.md", SKILL), ("man/agent-tty.1", MAN))
USER_DOCS = (("SKILL.md", SKILL), ("docs/index.html", HTML), ("man/agent-tty.1", MAN))
MAINTAINER_DOCS = (("README.md", README),)
ERRORS_WITH_CELL = (
    "interrupted",
    "unknown cell",
    "watcher died",
    "result missing",
    "lock update failed; use k int or k kill",
    "lock release failed",
    "interrupt failed; use k kill",
)
NO_SESSION_HINT = "no session 'x'; use k new x bash"
ERRORS_WITHOUT_CELL = (
    NO_SESSION_HINT,
    "active cell",
    "pipe failed",
    "send failed",
    "no active cell on 'x'",
    "invalid cell_id",
)
NO_LOG_HINT = "no log for 'x'; use k status x"
NO_SESSION_AUTO_HINT = "no session found; use k ls or k new <session> bash"
WATCHER_KILL_HINT = "watcher kill failed; use k kill"

# ── command coverage ──
check("k help: commands discovered", cli_commands == ["await", "fire", "history", "int", "kill", "ls", "new", "notify", "poll", "run", "status", "watch"],
      f"got {cli_commands}")
for doc_name, text in DOCS:
    for command in cli_commands:
        check(f"{doc_name}: command {command}", f"k {command}" in text)
    check(f"{doc_name}: mentions km", "km <session>" in text or "km SESSION" in text)
    check(f"{doc_name}: status next action", "k status" in text and ("next action" in text or "next useful command" in text))
check("README.md: status output example", "state=running" in README and "next='k poll work a1b2c3d4e5f6 or k int work'" in README)

for doc_name, text in MAINTAINER_DOCS:
    check(f"{doc_name}: runtime check count", f"{expected_checks} tests" in text, f"expected '{expected_checks} tests'")
    for script in (
        "tests/test_contracts.py",
        "tests/test_bridge_contracts.py",
        "tests/test_docs.py",
        "tests/test.sh",
        "tests/test_regressions.py",
        "tests/run_all.py",
    ):
        check(f"{doc_name}: mentions {script}", script in text)

check("SKILL.md: no maintainer Testing section", "## Testing" not in SKILL and "tests/test_" not in SKILL)
check("man/agent-tty.1: no maintainer Testing section", ".SH TESTING" not in MAN and "tests/test_" not in MAN)
check("man/agent-tty.1: no implementation atomic detail", "os.replace" not in MAN)
check("man/agent-tty.1: no internal state files", "_lock.json" not in MAN and "_session.json" not in MAN)
for internal in (
    "## Architecture",
    "## Safety Invariants",
    "## Metadata on Disk",
    "os.replace",
    "os.killpg",
    "_lock.json",
    "bg_pgid",
    "O_EXCL",
    "fsync",
    "echo_count",
    "paste-buffer",
    "send-keys",
    "pipe-pane",
    "process group",
    "stream processor",
    "direct to log",
    "re-frame",
    "O(1)",
):
    for doc_name, text in USER_DOCS:
        check(f"{doc_name}: no internal detail {internal}", internal not in text)
check("README.md: docs/package drift wording", "docs/package drift" in README)
check("README.md: no stale docs drift scope", "README/SKILL/man/HTML drift" not in README)

check("README.md: no stale line counts", not re.search(r"scripts/(?:k|km)\s+\d+\s+lines", README))
check("README.md: no stale test.sh line count", not re.search(r"test\.sh\s+\d+\s+lines", README))
check("repo: runtime test lives under tests", (ROOT / "tests" / "test.sh").exists())
check("repo: no root test.sh", not (ROOT / "test.sh").exists())
check("repo: Codex bridge exists", (ROOT / "vendor" / "codex_bridge.py").exists())
check("repo: bridge contract test exists", (ROOT / "tests" / "test_bridge_contracts.py").exists())
check("repo: man page exists", (ROOT / "man" / "agent-tty.1").exists())
check("docs/index.html: POSIX requirement", "Requires POSIX" in HTML and "native Windows fails fast" in HTML)

# ── package metadata surfaces ──
check("pyproject: package name", 'name = "agent-tty"' in PYPROJECT)
check("pyproject: shared human description", "shared live terminal for humans" in PYPROJECT)
check("pyproject: readme", 'readme = "README.md"' in PYPROJECT)
check("pyproject: POSIX classifier", "Operating System :: POSIX" in PYPROJECT)
check("__init__: version matches pyproject", init_version == project_version)
check("__init__: shared human description", "shared live terminal for humans" in INIT)
check("k help: version command", "k --version" in K_HELP and "print agent-tty version" in K_HELP)
check("km help: version command", "km --version" in KM_HELP)
check("runtime tests: version command", "k-version" in TEST_SH and "km-version" in TEST_SH)
check("runtime tests: user ergonomic commands", all(name in TEST_SH for name in (
    "new-idempotent", "ls", "run-text", "notify", "history", "watch", "km-oneshot-done", "invalid-cell-id"
)))
for script_name, target in (
    ("agent-tty", "agent_tty.cli:main"),
    ("k", "agent_tty.cli:main"),
    ("km", "agent_tty.monitor:main"),
):
    check(f"pyproject: script {script_name}", f'{script_name} = "{target}"' in PYPROJECT)
for doc_name, text in (("README.md", README), ("SKILL.md", SKILL), ("docs/index.html", HTML), ("man/agent-tty.1", MAN)):
    check(f"{doc_name}: package name", "agent-tty" in text)
    check(f"{doc_name}: shared human positioning", "shared live terminal for humans" in text.lower())
    check(f"{doc_name}: pip install", "pip install agent-tty" in text or doc_name == "man/agent-tty.1")
# diagnostics (reinstall hint, version commands) — not required on the landing page
for doc_name, text in (("README.md", README), ("SKILL.md", SKILL), ("man/agent-tty.1", MAN)):
    check(f"{doc_name}: reinstall stale entrypoint", "--force-reinstall agent-tty" in text)
    check(f"{doc_name}: version verification", "k --version" in text)
    check(f"{doc_name}: km version verification", "km --version" in text)
    check(f"{doc_name}: agent-tty version verification", "agent-tty --version" in text)
    check(f"{doc_name}: module version verification", "python -m agent_tty --version" in text)
check("man/agent-tty.1: version matches pyproject", f"agent-tty {project_version}" in MAN)
for doc_name, text in DOCS:
    check(f"{doc_name}: version command", "k --version" in text)
    check(f"{doc_name}: version aliases", "-V" in text and "version" in text)
check("man/agent-tty.1: km version command", "km --version" in MAN)
check("man/agent-tty.1: roff header", MAN.startswith(".TH AGENT-TTY 1"))

# ── CI coverage ──
check("CI: workflow exists", bool(CI))
for os_name in ("ubuntu-latest", "macos-latest", "windows-latest"):
    check(f"CI: covers {os_name}", os_name in CI)
for action in ("actions/checkout@v6", "actions/setup-python@v6"):
    check(f"CI: uses {action}", action in CI)
for needle in (
    "python -m mypy src vendor tests",
    "python tests/test_contracts.py",
    "python tests/test_bridge_contracts.py",
    "python tests/test_docs.py",
    "python -m py_compile vendor/codex_bridge.py",
    "python -m build",
    "python -m twine check dist/*",
    "python -m agent_tty --version",
    "k --version",
    "km --version",
    "agent-tty --version",
    "python tests/run_all.py k",
    "bash",
    "tmux",
):
    check(f"CI: includes {needle}", needle in CI)

check("README.md: Codex bridge documented", "vendor/codex_bridge.py" in README and "turn/start" in README and "k poll" in README and "queued" in README)
check("README.md: Codex bridge caveat documented", "Monitor-like" in README and "thread/inject_items" in README and "turn/steer" in README and "Codex Desktop may also fail to live-refresh" in README)

# ── option order: flags before positional args ──
for doc_name, text in DOCS + (("k help", K_HELP),):
    # fire: -t must come before [session] <code>
    check(f"{doc_name}: fire option order", bool(re.search(r"k fire\s+\[-t", text)),
          "should be 'k fire [-t N] [session] <code>'")
    # history: -n must come before [session]
    check(f"{doc_name}: history option order", bool(re.search(r"k history\s+\[-n", text)),
          "should be 'k history [-n N] [session]'")

# ── removed/outdated patterns ──
for doc_name, text in DOCS:
    check(f"{doc_name}: no old frame-only claim", "Frame delimiter = repeated prompt lines" not in text)
    # k JSON uses "status":"error","output":"interrupted" — NOT "status":"interrupted"
    # but km events legitimately emit "status":"interrupted" (terminal status)
    # so only check the k JSON schema section, not the km events section
    check(f"{doc_name}: k schema uses error+interrupted not status=interrupted",
          '"status": "error", "output": "interrupted"' in text)
    check(f"{doc_name}: no pipe-pane -o", "pipe-pane -o" not in text)
    check(f"{doc_name}: no /proc orphan docs", "/proc" not in text)
    check(f"{doc_name}: no bash-wrapped Python tests", "test_contracts.sh" not in text and "test_docs.sh" not in text)
    check(f"{doc_name}: hook uses path separator wording", "path separator" in text)
    check(f"{doc_name}: timeout recovery documented", "use k int or k kill" in text)
    check(f"{doc_name}: interrupted is error schema", '"status": "error", "output": "interrupted"' in text)

# ── km event monitor ──
for doc_name, text in DOCS:
    check(f"{doc_name}: km CLI documented", "km <session>" in text or "km SESSION" in text)
    check(f"{doc_name}: km -1 flag documented", "-1" in text and "one-shot" in text.lower())
    for km_status in ("fired", "done", "timeout", "interrupted", "notify", "closed", "error"):
        check(f"{doc_name}: km event '{km_status}'", f'"status": "{km_status}"' in text)
    check(f"{doc_name}: km completion includes session", '"session": "work"' in text or '"session": "..."' in text)
check("docs/index.html: km completion includes cell_id", '"cell_id"' in HTML and "a1b2c3d4e5f6" in HTML)
check("docs/index.html: km completion includes session", '"session"' in HTML and '"work"' in HTML)

# ── install + safety ──
check("README.md: install section", "## Install" in README)
for doc_name, text in DOCS:
    check(f"{doc_name}: requires python/tmux", "python" in text.lower() and "tmux" in text.lower())
    check(f"{doc_name}: tmux version requirement", "tmux 3.0+" in text)
for doc_name, text in MAINTAINER_DOCS:
    check(f"{doc_name}: atomic result writes documented", "os.replace" in text)
check("EXAMPLES.md: core analogy still present", "bash_tool is curl. k is a socket." in EXAMPLES)
check("EXAMPLES.md: persistent tmux model", "tmux session" in EXAMPLES and "stateful" in EXAMPLES)

# ── k help text consistency ──
for err in ERRORS_WITH_CELL:
    check(f"k help: error '{err}'", err in K_HELP)
check("k help: no-session hint", NO_SESSION_HINT in K_HELP)
check("k help: auto no-session hint", NO_SESSION_AUTO_HINT in K_HELP)
check("k help: no-log hint", NO_LOG_HINT in K_HELP)
check("k help: watcher kill hint", WATCHER_KILL_HINT in K_HELP)
for km_status in ("timeout", "interrupted"):
    check(f"k help: km event '{km_status}'", km_status in K_HELP)

# ── k JSON error outputs ──
for doc_name, text in (("README.md", README), ("SKILL.md", SKILL), ("man/agent-tty.1", MAN)):
    check(f"{doc_name}: no-session hint", NO_SESSION_HINT in text)
    check(f"{doc_name}: auto no-session hint", NO_SESSION_AUTO_HINT in text)
    check(f"{doc_name}: no-log text hint", NO_LOG_HINT in text)
    check(f"{doc_name}: watcher kill text hint", WATCHER_KILL_HINT in text)
    for err in ERRORS_WITHOUT_CELL:
        check(f"{doc_name}: error output '{err}'", err in text)
    for err in ERRORS_WITH_CELL:
        check(f"{doc_name}: error output '{err}'", err in text)
    check(f"{doc_name}: no-log is text-only", "Text-only errors" in text and "no log for" in text)
    # cell errors (with cell_id) vs errors (without) are distinguished
    check(f"{doc_name}: cell error schema", "cell error" in text)
    if doc_name != "man/agent-tty.1":
        without = re.search(r"JSON errors without `cell_id`: ([^\n]+)", text)
        with_cell = re.search(r"JSON errors with `cell_id`: ([^\n]+)", text)
        check(f"{doc_name}: errors without cell_id line exists", without is not None)
        check(f"{doc_name}: errors with cell_id line exists", with_cell is not None)
        if without and with_cell:
            check(f"{doc_name}: no-active-cell is without cell_id", "no active cell" in without.group(1))
            check(f"{doc_name}: no-active-cell not with cell_id", "no active cell" not in with_cell.group(1))
            check(f"{doc_name}: no-log not JSON", "no log" not in without.group(1) and "no log" not in with_cell.group(1))
    check(f"{doc_name}: no duplicate default 5", "(default 5) (default 5)" not in text)
check("man/agent-tty.1: poll does not claim fired result", "Results may be\n.BR fired" not in MAN)
check("man/agent-tty.1: int stdout is not JSON", "Stdout is" in MAN and "not JSON" in MAN)

if FAILURES:
    print("documentation drift failures:")
    for failure in FAILURES:
        print(f"  - {failure}")
    raise SystemExit(1)

print("documentation drift tests passed")
