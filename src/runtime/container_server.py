"""Container-side runtime daemon.

Receives JSON requests on stdin, writes one JSON response per line to stdout.
"""

from __future__ import annotations

import base64
import json
import os
import socket
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from pathlib import Path

_WORKSPACE_ROOT = Path("/workspace")
_SKILLS_ROOT = Path("/skills")
_CONFIG_ROOT = Path("/config")
_SOUL_PATH = _CONFIG_ROOT / "SOUL.md"
_CACHE_ROOT = Path("/cache")
_IMAGE_LIMIT_DEFAULT = 200_000

_skills_writable = os.environ.get("RUNTIME_WRITABLE_SKILLS", "").strip() in ("1", "true", "yes")

_sessions: dict[str, dict[str, object]] = {}

# ── secrets ──────────────────────────────────────────────────────────
_secrets: dict[str, str] = {}
_SECRET_SOCKET_PATH = "/tmp/.qb_s"

# Python script used by the injected get_secret bash function.
_GET_SECRET_SCRIPT = r'''
import socket, sys

with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as c:
    c.connect(sys.argv[1])
    c.sendall(sys.argv[2].encode("utf-8"))
    c.shutdown(socket.SHUT_WR)
    d = []
    while True:
        b = c.recv(4096)
        if not b:
            break
        d.append(b)
sys.stdout.write(b"".join(d).decode("utf-8"))
'''.strip()

_GET_SECRET_PREAMBLE = (
    f'__qb_ss="{_SECRET_SOCKET_PATH}"\n'
    f'get_secret() {{ python3 -c \'{_GET_SECRET_SCRIPT}\' "$__qb_ss" "$1"; }}\n'
    "export -f get_secret 2>/dev" + "/null\n"
)


def _serve_secrets() -> None:
    """Background thread: serve get_secret queries via Unix socket."""
    try:
        if os.path.exists(_SECRET_SOCKET_PATH):
            os.remove(_SECRET_SOCKET_PATH)
        srv = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        srv.bind(_SECRET_SOCKET_PATH)
        os.chmod(_SECRET_SOCKET_PATH, 0o600)
        srv.listen(8)
        srv.settimeout(0.5)
        while True:
            try:
                conn, _ = srv.accept()
            except socket.timeout:
                continue
            try:
                conn.settimeout(2.0)
                data = b""
                while len(data) < 4096:
                    chunk = conn.recv(4096 - len(data))
                    if not chunk:
                        break
                    data += chunk
                key = data.decode("utf-8").strip()
                value = _secrets.get(key, "")
                conn.sendall(value.encode("utf-8"))
            except Exception:
                pass
            finally:
                conn.close()
    except Exception:
        pass


def _init(params: dict[str, object]) -> dict[str, object]:
    """Receive secrets from broker and start socket server."""
    global _secrets
    raw = params.get("secrets")
    if isinstance(raw, dict):
        _secrets = {str(k): str(v) for k, v in raw.items()}
    if _secrets:
        t = threading.Thread(target=_serve_secrets, daemon=True, name="secrets")
        t.start()
    return _response(True)


def _response(ok: bool, **payload: object) -> dict[str, object]:
    result = {"ok": ok}
    result.update(payload)
    return result


def _allowed_roots() -> tuple[Path, ...]:
    return (_WORKSPACE_ROOT, _SKILLS_ROOT, _SOUL_PATH, _CACHE_ROOT)


_WRITABLE_ROOTS_ADMIN = (_WORKSPACE_ROOT, _SKILLS_ROOT, _SOUL_PATH, _CACHE_ROOT)
_WRITABLE_ROOTS_USER = (_WORKSPACE_ROOT, _CACHE_ROOT)


def _writable_roots() -> tuple[Path, ...]:
    return _WRITABLE_ROOTS_ADMIN if _skills_writable else _WRITABLE_ROOTS_USER


def _ensure_allowed(path_str: str, *, access: str = "read") -> Path:
    raw = Path(str(path_str or "").strip())
    if not raw.is_absolute():
        raise ValueError(f"path must be absolute runtime path: {path_str}")
    resolved = raw.resolve()
    for root in _allowed_roots():
        base = root if root.is_dir() else root.parent
        try:
            resolved.relative_to(base.resolve())
            if access == "write":
                if not any(_matches(resolved, wr) for wr in _writable_roots()):
                    raise ValueError(f"write not allowed (read-only): {path_str}")
                if root == _SOUL_PATH and resolved != _SOUL_PATH:
                    raise ValueError(f"write target not allowed: {path_str}")
            return resolved
        except ValueError:
            continue
    raise ValueError(f"path outside runtime roots: {path_str}")


def _matches(resolved: Path, root: Path) -> bool:
    base = root if root.is_dir() else root.parent
    try:
        resolved.relative_to(base.resolve())
        return True
    except ValueError:
        return False


def _inspect(path: Path, *, include_bytes: bool, max_bytes: int) -> dict[str, object]:
    result: dict[str, object] = {"ok": True, "exists": path.exists()}
    if not path.exists():
        return result
    result["is_dir"] = path.is_dir()
    if path.is_file():
        result["size"] = path.stat().st_size
        if include_bytes:
            limit = max(0, int(max_bytes))
            data = path.read_bytes() if limit <= 0 else path.read_bytes()[: limit + 1]
            truncated = False
            if limit > 0 and len(data) > limit:
                data = data[:limit]
                truncated = True
            result["data_b64"] = base64.b64encode(data).decode("ascii")
            result["truncated"] = truncated
    return result


def _atomic_write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(dir=str(path.parent), prefix=f".{path.name}.", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="") as handle:
            handle.write(content)
        os.replace(tmp_path, path)
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)


def _read_stream(stream: object, max_chars: int) -> tuple[str, bool]:
    if stream is None or max_chars <= 0:
        return "", False
    chunks: list[str] = []
    total = 0
    while total < max_chars:
        try:
            chunk = stream.read(max_chars - total)
        except BlockingIOError:
            break
        if not chunk:
            break
        chunks.append(chunk)
        total += len(chunk)
    return "".join(chunks), total >= max_chars


def _run_shell(command: str, cwd: Path, timeout_ms: int, max_output_chars: int) -> dict[str, object]:
    env = os.environ.copy()
    env["HOME"] = str(_WORKSPACE_ROOT)
    effective_cmd = (_GET_SECRET_PREAMBLE + command) if _secrets else command
    completed = subprocess.run(
        effective_cmd,
        shell=True,
        executable="/bin/bash",
        cwd=str(cwd),
        env=env,
        capture_output=True,
        text=True,
        timeout=max(timeout_ms, 1) / 1000,
        check=False,
    )
    stdout = completed.stdout[:max_output_chars]
    stderr = completed.stderr[:max_output_chars]
    return _response(
        completed.returncode == 0,
        exit_code=completed.returncode,
        stdout=stdout,
        stderr=stderr,
        stdout_truncated=len(completed.stdout) > len(stdout),
        stderr_truncated=len(completed.stderr) > len(stderr),
        timed_out=False,
        cwd=str(cwd),
    )


def _session_payload(session_id: str, session: dict[str, object], *, max_chars: int) -> dict[str, object]:
    proc = session["process"]
    stdout, stdout_truncated = _read_stream(getattr(proc, "stdout", None), max_chars)
    stderr, stderr_truncated = _read_stream(getattr(proc, "stderr", None), max_chars)
    return _response(
        True,
        session_id=session_id,
        running=proc.poll() is None,
        exit_code=proc.poll(),
        stdout=stdout,
        stderr=stderr,
        stdout_truncated=stdout_truncated,
        stderr_truncated=stderr_truncated,
        cwd=str(session["cwd"]),
    )


def _handle_bash(method: str, params: dict[str, object]) -> dict[str, object]:
    max_chars = max(1, int(params.get("max_chars", 20_000)))
    if method == "bash.list":
        items = []
        for session_id, session in _sessions.items():
            proc = session["process"]
            items.append({
                "session_id": session_id,
                "running": proc.poll() is None,
                "exit_code": proc.poll(),
                "pid": proc.pid,
                "cwd": str(session["cwd"]),
                "command": session["command"],
                "created_at": session["created_at"],
            })
        return _response(True, items=items)

    session_id = str(params.get("session_id", "")).strip()
    if method == "bash.start":
        cwd = _ensure_allowed(str(params.get("cwd", "/workspace")), access="read")
        command = str(params.get("command", ""))
        effective_cmd = (_GET_SECRET_PREAMBLE + command) if _secrets else command
        env = os.environ.copy()
        env["HOME"] = str(_WORKSPACE_ROOT)
        proc = subprocess.Popen(
            effective_cmd,
            shell=True,
            executable="/bin/bash",
            cwd=str(cwd),
            env=env,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
        )
        if proc.stdout is not None:
            os.set_blocking(proc.stdout.fileno(), False)
        if proc.stderr is not None:
            os.set_blocking(proc.stderr.fileno(), False)
        session_id = f"s-{uuid.uuid4().hex[:10]}"
        _sessions[session_id] = {
            "process": proc,
            "cwd": str(cwd),
            "command": command,
            "created_at": int(time.time()),
        }
        return _response(True, session_id=session_id, running=True, cwd=str(cwd), pid=proc.pid)

    session = _sessions.get(session_id)
    if session is None:
        return _response(False, error=f"session not found: {session_id}")

    proc = session["process"]
    if method == "bash.read":
        return _session_payload(session_id, session, max_chars=max_chars)
    if method == "bash.write":
        if proc.poll() is not None:
            return _response(False, error="session already exited")
        data = str(params.get("data", ""))
        stdin = getattr(proc, "stdin", None)
        if stdin is None:
            return _response(False, error="session stdin unavailable")
        stdin.write(data)
        stdin.flush()
        return _response(True, session_id=session_id, written_chars=len(data))
    if method == "bash.wait":
        timeout_ms = int(params.get("timeout_ms", 0))
        try:
            proc.wait(timeout=None if timeout_ms <= 0 else max(timeout_ms, 1) / 1000)
        except subprocess.TimeoutExpired:
            return _response(False, session_id=session_id, timed_out=True, running=True, timeout_ms=timeout_ms)
        payload = _session_payload(session_id, session, max_chars=max_chars)
        _sessions.pop(session_id, None)
        return payload
    if method == "bash.stop":
        timeout_ms = int(params.get("timeout_ms", 1000))
        if proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=max(timeout_ms, 1) / 1000)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=1)
        payload = _session_payload(session_id, session, max_chars=max_chars)
        _sessions.pop(session_id, None)
        return payload
    return _response(False, error=f"unknown bash method: {method}")


def _handle_request(request: dict[str, object]) -> dict[str, object]:
    method = str(request.get("method", "")).strip()
    params = request.get("params", {})
    if not isinstance(params, dict):
        params = {}

    if method == "_init":
        return _init(params)

    if method in {"inspect", "write", "delete", "move"}:
        path = _ensure_allowed(str(params.get("path", "")), access="write" if method != "inspect" else "read")
        if method == "inspect":
            return _inspect(
                path,
                include_bytes=bool(params.get("include_bytes", False)),
                max_bytes=int(params.get("max_bytes", _IMAGE_LIMIT_DEFAULT)),
            )
        if method == "write":
            _atomic_write(path, str(params.get("content", "")))
            return _response(True)
        if method == "delete":
            if path.exists() and path.is_file():
                path.unlink()
            return _response(True)
        target = _ensure_allowed(str(params.get("target", "")), access="write")
        target.parent.mkdir(parents=True, exist_ok=True)
        os.replace(path, target)
        return _response(True)

    if method == "bash.run":
        cwd = _ensure_allowed(str(params.get("cwd", "/workspace")), access="read")
        return _run_shell(
            str(params.get("command", "")),
            cwd,
            int(params.get("timeout_ms", 30_000)),
            int(params.get("max_output_chars", 20_000)),
        )

    if method.startswith("bash."):
        return _handle_bash(method, params)

    return _response(False, error=f"unknown method: {method}")


def main() -> None:
    for line in sys.stdin:
        raw = line.strip()
        if not raw:
            continue
        try:
            request = json.loads(raw)
            response = _handle_request(request)
        except Exception as exc:
            response = _response(False, error=str(exc))
        sys.stdout.write(json.dumps(response, ensure_ascii=False) + "\n")
        sys.stdout.flush()


if __name__ == "__main__":
    main()
