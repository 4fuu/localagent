"""Container-backed execution for shell tools."""

from __future__ import annotations

import os
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from src.config import cfg

from ..provider import BaseState
from .sandbox import ToolSandbox

_CACHE_MOUNT_TARGET = Path("/localagent-cache")


class SandboxRunner(Protocol):
    def run(
        self,
        *,
        state: BaseState,
        command: str,
        cwd: Path,
        env: dict[str, str],
        timeout_ms: int,
    ) -> subprocess.CompletedProcess[str]: ...

    def start(
        self,
        *,
        state: BaseState,
        command: str,
        cwd: Path,
        env: dict[str, str],
    ) -> subprocess.Popen[str]: ...


@dataclass(frozen=True)
class PodmanSandboxRunner:
    executable: str
    image: str
    network: str
    pull: str
    read_only_rootfs: bool
    tmpfs: tuple[str, ...]
    pids_limit: int

    @classmethod
    def from_config(cls) -> "PodmanSandboxRunner":
        data = cfg.sandbox
        image = str(data.get("image", "")).strip()
        if not image:
            raise ValueError("sandbox.image 未配置")
        return cls(
            executable=str(data.get("command", "podman")).strip() or "podman",
            image=image,
            network=str(data.get("network", "slirp4netns")).strip(),
            pull=str(data.get("pull", "missing")).strip(),
            read_only_rootfs=bool(data.get("read_only_rootfs", True)),
            tmpfs=tuple(str(item).strip() for item in data.get("tmpfs", []) if str(item).strip()),
            pids_limit=max(16, int(data.get("pids_limit", 256))),
        )

    def run(
        self,
        *,
        state: BaseState,
        command: str,
        cwd: Path,
        env: dict[str, str],
        timeout_ms: int,
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            self._build_run_args(state=state, command=command, cwd=cwd, env=env),
            capture_output=True,
            text=True,
            timeout=timeout_ms / 1000,
            check=False,
            env=self._host_env_for_podman(),
        )

    def start(
        self,
        *,
        state: BaseState,
        command: str,
        cwd: Path,
        env: dict[str, str],
    ) -> subprocess.Popen[str]:
        return subprocess.Popen(
            self._build_run_args(state=state, command=command, cwd=cwd, env=env),
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
            env=self._host_env_for_podman(),
        )

    def _build_run_args(
        self,
        *,
        state: BaseState,
        command: str,
        cwd: Path,
        env: dict[str, str],
    ) -> list[str]:
        sandbox = ToolSandbox.from_state(state)
        args = [
            self.executable,
            "run",
            "--rm",
            "-i",
            "--userns=keep-id",
            "--cap-drop=ALL",
            "--security-opt=no-new-privileges",
            "--pids-limit",
            str(self.pids_limit),
            "--workdir",
            str(cwd),
        ]
        if self.pull:
            args.append(f"--pull={self.pull}")
        if self.network:
            args.extend(["--network", self.network])
        if self.read_only_rootfs:
            args.append("--read-only")
        for target in self.tmpfs:
            args.extend(["--tmpfs", target])
        for key, value in self._container_env(env).items():
            args.extend(["-e", f"{key}={value}"])
        for mount in self._build_time_mounts():
            args.extend(["-v", mount])
        for mount in self._build_mounts(sandbox):
            args.extend(["-v", mount])
        args.extend([self.image, "bash", "-lc", command])
        return args

    def _build_mounts(self, sandbox: ToolSandbox) -> list[str]:
        mounts: list[str] = []
        seen: set[str] = set()
        writable_roots = {str(path.resolve()) for path in sandbox.writable_roots}
        roots = [*sandbox.readable_roots, *sandbox.writable_roots]
        for root in self._dedupe_roots(roots):
            source = str(root.resolve())
            if source in seen:
                continue
            seen.add(source)
            mode = "rw,rbind" if source in writable_roots else "ro,rbind"
            mounts.append(f"{source}:{source}:{mode}")
        if sandbox.restricted:
            workspace_source = str(sandbox.workspace_root.resolve())
            for target in self._workspace_compat_targets(sandbox):
                mount_key = f"{workspace_source}->{target}"
                if mount_key in seen:
                    continue
                seen.add(mount_key)
                mounts.append(f"{workspace_source}:{target}:rw,rbind")
        cache_source = self._cache_source_dir(sandbox)
        mounts.append(f"{cache_source}:{_CACHE_MOUNT_TARGET}:rw,rbind")
        return mounts

    @staticmethod
    def _workspace_compat_targets(sandbox: ToolSandbox) -> list[str]:
        project_root = sandbox.project_root.resolve()
        workspace_root = sandbox.workspace_root.resolve()
        if project_root == workspace_root:
            return []

        target = str((project_root / "workspace").resolve())
        if target == str(workspace_root):
            return []
        return [target]

    @staticmethod
    def _dedupe_roots(roots: list[Path] | tuple[Path, ...]) -> list[Path]:
        unique: list[Path] = []
        for root in sorted({path.resolve() for path in roots}, key=lambda path: len(str(path))):
            if any(root == existing or root.is_relative_to(existing) for existing in unique):
                continue
            unique.append(root)
        return unique

    @staticmethod
    def _container_env(env: dict[str, str]) -> dict[str, str]:
        result: dict[str, str] = {}
        for key in (
            "HOME",
            "LANG",
            "LC_ALL",
            "TERM",
            "TZ",
            "XDG_CACHE_HOME",
            "UV_CACHE_DIR",
            "PIP_CACHE_DIR",
            "NPM_CONFIG_CACHE",
            "PUPPETEER_CACHE_DIR",
        ):
            value = env.get(key, "")
            if value:
                result[key] = value
        result.setdefault("PYTHONIOENCODING", "utf-8")
        result.setdefault("XDG_CACHE_HOME", str(_CACHE_MOUNT_TARGET / "xdg"))
        result.setdefault("UV_CACHE_DIR", str(_CACHE_MOUNT_TARGET / "uv"))
        result.setdefault("PIP_CACHE_DIR", str(_CACHE_MOUNT_TARGET / "pip"))
        result.setdefault("NPM_CONFIG_CACHE", str(_CACHE_MOUNT_TARGET / "npm"))
        result.setdefault("PUPPETEER_CACHE_DIR", str(_CACHE_MOUNT_TARGET / "puppeteer"))
        return result

    @staticmethod
    def _build_time_mounts() -> list[str]:
        mounts: list[str] = []
        for path in ("/etc/localtime", "/etc/timezone"):
            if os.path.exists(path):
                mounts.append(f"{path}:{path}:ro")
        return mounts

    @staticmethod
    def _cache_source_dir(sandbox: ToolSandbox) -> str:
        cache_root = (sandbox.project_root / ".localagent" / "sandbox-cache").resolve()
        cache_root.mkdir(parents=True, exist_ok=True)
        return str(cache_root)

    @staticmethod
    def _host_env_for_podman() -> dict[str, str]:
        env = os.environ.copy()
        env.setdefault("PYTHONIOENCODING", "utf-8")
        return env


def create_sandbox_runner() -> SandboxRunner:
    data = cfg.sandbox
    runtime = str(data.get("runtime", "podman")).strip().lower()
    if runtime == "podman":
        return PodmanSandboxRunner.from_config()
    raise ValueError(f"未知 sandbox runtime: {runtime}")
