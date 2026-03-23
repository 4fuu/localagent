"""Host-side runtime broker for task container execution."""

from __future__ import annotations

import json
import logging
import os
import subprocess
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from websockets.sync.client import ClientConnection, connect

from ..config import cfg
from ..core.runtime_fs import (
    RUNTIME_CONFIG_ROOT,
    RUNTIME_ROOT,
    RUNTIME_SKILLS_ROOT,
    ensure_runtime_layout,
    resolve_runtime_workspace_root,
)
from ..core.secrets import conversation_scope, load_all_decrypted, person_scope
from ..core.store import Store
from ..retry import RetryPolicy

logger = logging.getLogger(__name__)

_RUNTIME_TOPICS = ["runtime.call"]
_SERVER_PATH = Path(__file__).resolve().parent / "container_server.py"


@dataclass
class _RuntimeSession:
    task_id: str
    process: subprocess.Popen[str]
    stdin_lock: threading.Lock
    stdout_lock: threading.Lock
    stderr_thread: threading.Thread | None


class RuntimeBrokerService:
    def __init__(self, hub_url: str):
        self._hub_url = hub_url
        self._thread: threading.Thread | None = None
        self._ready = threading.Event()
        self._stopping = threading.Event()
        self._ws: ClientConnection | None = None
        self._retry = RetryPolicy.for_service("runtime_broker")
        self._sessions: dict[str, _RuntimeSession] = {}
        self._sessions_lock = threading.Lock()

    def start(self) -> None:
        ensure_runtime_layout()
        self._thread = threading.Thread(target=self._run, daemon=True, name="runtime-broker")
        self._thread.start()
        if not self._ready.wait(timeout=5):
            raise RuntimeError("RuntimeBrokerService failed to start within 5 seconds")
        logger.info("RuntimeBrokerService started")

    def stop(self) -> None:
        self._stopping.set()
        if self._ws is not None:
            try:
                self._ws.close()
            except Exception:
                pass
        if self._thread is not None:
            self._thread.join(timeout=5)
            self._thread = None
        with self._sessions_lock:
            sessions = list(self._sessions.values())
            self._sessions.clear()
        for session in sessions:
            self._stop_session(session)
        logger.info("RuntimeBrokerService stopped")

    def _run(self) -> None:
        reconnect_attempt = 0
        while not self._stopping.is_set():
            try:
                self._ws = connect(self._hub_url, open_timeout=self._retry.connect_timeout)
                self._ws.send(json.dumps({
                    "type": "register",
                    "name": "runtime",
                    "topics": _RUNTIME_TOPICS,
                }))
                if not self._ready.is_set():
                    self._ready.set()
                reconnect_attempt = 0

                for raw in self._ws:
                    msg = json.loads(raw)
                    if msg.get("type") != "request":
                        continue
                    try:
                        resp = self._handle_call(msg.get("payload", {}))
                    except Exception as exc:
                        logger.exception("RuntimeBrokerService request failed")
                        resp = {"ok": False, "error": str(exc)}
                    assert self._ws is not None
                    self._ws.send(json.dumps({
                        "type": "response",
                        "id": msg["id"],
                        "payload": resp,
                    }))
            except Exception as exc:
                if self._stopping.is_set():
                    break
                delay = self._retry.backoff_delay(reconnect_attempt)
                reconnect_attempt += 1
                logger.warning("RuntimeBrokerService hub connection lost, retry in %.2fs: %s", delay, exc)
                time.sleep(delay)
            finally:
                if self._ws is not None:
                    try:
                        self._ws.close()
                    except Exception:
                        pass
                    self._ws = None

    def _handle_call(self, payload: dict[str, Any]) -> dict[str, Any]:
        task_id = str(payload.get("task_id", "")).strip()
        method = str(payload.get("method", "")).strip()
        params = payload.get("params", {})
        if not task_id:
            return {"ok": False, "error": "task_id 不能为空"}
        if not method:
            return {"ok": False, "error": "method 不能为空"}
        if not isinstance(params, dict):
            return {"ok": False, "error": "params 必须是对象"}
        if method == "shutdown":
            self._shutdown_session(task_id)
            return {"ok": True}
        session = self._ensure_session(task_id)
        return self._call_session(session, method=method, params=params)

    def _ensure_session(self, task_id: str) -> _RuntimeSession:
        with self._sessions_lock:
            existing = self._sessions.get(task_id)
            if existing is not None and existing.process.poll() is None:
                return existing
            session = self._spawn_session(task_id)
            self._sessions[task_id] = session
            return session

    def _spawn_session(self, task_id: str) -> _RuntimeSession:
        workspace_root, is_admin, secrets = self._resolve_task_context(task_id)
        ensure_runtime_layout()
        sandbox_cfg = cfg.sandbox
        image = str(sandbox_cfg.get("image", "")).strip()
        if not image:
            raise ValueError("sandbox.image 未配置")
        command = str(sandbox_cfg.get("command", "podman")).strip() or "podman"
        skills_writable = is_admin or bool(sandbox_cfg.get("user_writable_skills", False))
        cache_root = (RUNTIME_ROOT / "cache").resolve()
        cache_root.mkdir(parents=True, exist_ok=True)
        args = [
            command,
            "run",
            "--rm",
            "-i",
            "--userns=keep-id",
            "--cap-drop=ALL",
            "--security-opt=no-new-privileges",
            "--pids-limit",
            str(max(16, int(sandbox_cfg.get("pids_limit", 256)))),
            "--workdir",
            "/workspace",
            "-v",
            f"{workspace_root}:/workspace:rw,rbind",
            "-v",
            f"{RUNTIME_SKILLS_ROOT.resolve()}:/skills:{'rw' if skills_writable else 'ro'},rbind",
            "-v",
            f"{RUNTIME_CONFIG_ROOT.resolve()}:/config:{'rw' if skills_writable else 'ro'},rbind",
            "-v",
            f"{cache_root}:/cache:rw,rbind",
            "-v",
            f"{_SERVER_PATH.resolve()}:/opt/localagent/container_server.py:ro,rbind",
        ]
        pull = str(sandbox_cfg.get("pull", "missing")).strip()
        if pull:
            args.append(f"--pull={pull}")
        network = str(sandbox_cfg.get("network", "")).strip()
        if network:
            args.extend(["--network", network])
        if bool(sandbox_cfg.get("read_only_rootfs", True)):
            args.append("--read-only")
        for target in sandbox_cfg.get("tmpfs", []) or []:
            current = str(target).strip()
            if current:
                args.extend(["--tmpfs", current])
        for current in ("/etc/localtime", "/etc/timezone"):
            if os.path.exists(current):
                args.extend(["-v", f"{current}:{current}:ro"])
        for key, value in self._container_env(cache_root, skills_writable=skills_writable).items():
            args.extend(["-e", f"{key}={value}"])
        args.extend([image, "python", "/opt/localagent/container_server.py"])
        process = subprocess.Popen(
            args,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
        )
        stderr_thread = None
        if process.stderr is not None:
            stderr_thread = threading.Thread(
                target=self._log_stderr,
                args=(task_id, process.stderr),
                daemon=True,
                name=f"runtime-stderr-{task_id}",
            )
            stderr_thread.start()
        logger.info("Spawned runtime container for task=%s pid=%s", task_id, process.pid)
        session = _RuntimeSession(
            task_id=task_id,
            process=process,
            stdin_lock=threading.Lock(),
            stdout_lock=threading.Lock(),
            stderr_thread=stderr_thread,
        )
        # Send _init with secrets so container can serve get_secret.
        if secrets:
            try:
                self._call_session(
                    session,
                    method="_init",
                    params={"secrets": secrets},
                )
            except Exception:
                logger.warning("Failed to send secrets to container for task=%s", task_id, exc_info=True)
        return session

    def _call_session(self, session: _RuntimeSession, *, method: str, params: dict[str, Any]) -> dict[str, Any]:
        if session.process.poll() is not None:
            raise RuntimeError(f"runtime exited for task={session.task_id} code={session.process.returncode}")
        request = {"method": method, "params": params}
        assert session.process.stdin is not None
        assert session.process.stdout is not None
        with session.stdin_lock, session.stdout_lock:
            session.process.stdin.write(json.dumps(request, ensure_ascii=False) + "\n")
            session.process.stdin.flush()
            raw = session.process.stdout.readline()
        if not raw:
            raise RuntimeError(f"runtime closed stdout for task={session.task_id}")
        return json.loads(raw)

    def _stop_session(self, session: _RuntimeSession) -> None:
        if session.process.poll() is None:
            session.process.terminate()
            try:
                session.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                session.process.kill()
                session.process.wait(timeout=5)

    def _shutdown_session(self, task_id: str) -> None:
        with self._sessions_lock:
            session = self._sessions.pop(task_id, None)
        if session is not None:
            self._stop_session(session)

    @staticmethod
    def _log_stderr(task_id: str, stream: Any) -> None:
        try:
            for line in stream:
                text = str(line).rstrip()
                if text:
                    logger.warning("[runtime:%s] %s", task_id, text)
        except Exception:
            logger.debug("Runtime stderr reader stopped for task=%s", task_id, exc_info=True)

    @staticmethod
    def _container_env(cache_root: Path, *, skills_writable: bool = False) -> dict[str, str]:
        env: dict[str, str] = {}
        env["PYTHONIOENCODING"] = "utf-8"
        env["HOME"] = "/workspace"
        env["RUNTIME_WRITABLE_SKILLS"] = "1" if skills_writable else "0"
        cache = "/cache"
        env["XDG_CACHE_HOME"] = f"{cache}/xdg"
        env["UV_CACHE_DIR"] = f"{cache}/uv"
        env["PIP_CACHE_DIR"] = f"{cache}/pip"
        env["NPM_CONFIG_CACHE"] = f"{cache}/npm"
        env["PUPPETEER_CACHE_DIR"] = f"{cache}/puppeteer"
        for key in ("LANG", "LC_ALL", "TERM", "TZ"):
            value = os.environ.get(key, "")
            if value:
                env[key] = value
        return env

    @staticmethod
    def _resolve_task_context(task_id: str) -> tuple[Path, bool, dict[str, str]]:
        """Returns (workspace_root, is_admin, secrets)."""
        with Store() as store:
            task = store.task_read(task_id) or {}
            conversation_id = str(task.get("conversation_id", "")).strip()
            person_id = str(task.get("person_id", "")).strip()
            is_admin = bool(task.get("is_admin", False))
            state_row = store.conversation_state_read(conversation_id) if conversation_id else {}
        workspace = resolve_runtime_workspace_root(
            conversation_id=conversation_id,
            person_id=person_id,
            is_multi_party=bool((state_row or {}).get("is_multi_party", False)),
        ).resolve()
        # Load decrypted secrets for get_secret in container.
        scopes: list[str] = []
        if person_id:
            scopes.append(person_scope(person_id))
        if conversation_id:
            scopes.append(conversation_scope(conversation_id))
        secrets: dict[str, str] = {}
        if scopes:
            try:
                secrets = load_all_decrypted(scopes)
            except Exception:
                logger.warning("Failed to load secrets for task=%s", task_id)
        return workspace, is_admin, secrets
