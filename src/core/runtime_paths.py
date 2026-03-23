"""Shared host/runtime path mapping helpers."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any

RUNTIME_APP_ROOT = PurePosixPath("/app")
RUNTIME_WORKSPACE_ROOT = PurePosixPath("/workspace")
RUNTIME_SKILLS_ROOT = PurePosixPath("/skills")
RUNTIME_CONFIG_ROOT = PurePosixPath("/config")
RUNTIME_SOUL_PATH = RUNTIME_CONFIG_ROOT / "SOUL.md"
RUNTIME_CACHE_ROOT = PurePosixPath("/cache")

WORKSPACE_URI_PREFIX = "workspace://"
SKILLS_URI_PREFIX = "skills://"
CONFIG_URI_PREFIX = "config://"
CONFIG_SOUL_URI = "config://SOUL.md"


def _clean_locator_relative(value: str) -> str:
    raw = str(value or "").strip().replace("\\", "/")
    if raw.startswith("/"):
        raw = raw.lstrip("/")
    parts: list[str] = []
    for item in raw.split("/"):
        normalized = item.strip()
        if not normalized or normalized == ".":
            continue
        if normalized == "..":
            raise ValueError(f"非法路径跳转: {value}")
        parts.append(normalized)
    return "/".join(parts)


def _is_prefixed(value: str, prefix: str) -> bool:
    return str(value or "").strip().lower().startswith(prefix)


@dataclass(frozen=True)
class RuntimePathMap:
    host_project_root: Path
    host_workspace_root: Path
    host_skills_root: Path
    host_soul_path: Path
    runtime_project_root: PurePosixPath = RUNTIME_APP_ROOT
    runtime_workspace_root: PurePosixPath = RUNTIME_WORKSPACE_ROOT
    runtime_skills_root: PurePosixPath = RUNTIME_SKILLS_ROOT
    runtime_soul_path: PurePosixPath = RUNTIME_SOUL_PATH
    runtime_cache_root: PurePosixPath = RUNTIME_CACHE_ROOT

    @classmethod
    def from_values(
        cls,
        *,
        host_project_root: str,
        host_workspace_root: str,
        host_skills_root: str,
        host_soul_path: str,
        runtime_project_root: str = "",
        runtime_workspace_root: str = "",
        runtime_skills_root: str = "",
        runtime_soul_path: str = "",
        runtime_cache_root: str = "",
    ) -> "RuntimePathMap":
        return cls(
            host_project_root=Path(host_project_root).resolve(),
            host_workspace_root=Path(host_workspace_root).resolve(),
            host_skills_root=Path(host_skills_root).resolve(),
            host_soul_path=Path(host_soul_path).resolve(),
            runtime_project_root=PurePosixPath(runtime_project_root or str(RUNTIME_APP_ROOT)),
            runtime_workspace_root=PurePosixPath(runtime_workspace_root or str(RUNTIME_WORKSPACE_ROOT)),
            runtime_skills_root=PurePosixPath(runtime_skills_root or str(RUNTIME_SKILLS_ROOT)),
            runtime_soul_path=PurePosixPath(runtime_soul_path or str(RUNTIME_SOUL_PATH)),
            runtime_cache_root=PurePosixPath(runtime_cache_root or str(RUNTIME_CACHE_ROOT)),
        )

    @classmethod
    def from_state(cls, state: Any) -> "RuntimePathMap":
        host_project_root = (
            str(getattr(state, "host_project_root", "")).strip()
            or str(getattr(state, "project_root", "")).strip()
        )
        host_workspace_root = (
            str(getattr(state, "host_workspace_root", "")).strip()
            or str(getattr(state, "workspace_root", "")).strip()
        )
        host_skills_root = (
            str(getattr(state, "host_skills_path", "")).strip()
            or str(getattr(state, "skills_path", "")).strip()
        )
        host_soul_path = str(getattr(state, "host_soul_path", "")).strip()
        if not host_soul_path and host_project_root:
            host_soul_path = str((Path(host_project_root) / "SOUL.md").resolve())
        runtime_project_root = (
            str(getattr(state, "runtime_project_root", "")).strip()
            or str(getattr(state, "project_root", "")).strip()
            or str(RUNTIME_APP_ROOT)
        )
        runtime_workspace_root = (
            str(getattr(state, "runtime_workspace_root", "")).strip()
            or str(getattr(state, "workspace_root", "")).strip()
            or str(RUNTIME_WORKSPACE_ROOT)
        )
        runtime_skills_root = (
            str(getattr(state, "runtime_skills_path", "")).strip()
            or str(getattr(state, "skills_path", "")).strip()
            or str(RUNTIME_SKILLS_ROOT)
        )
        runtime_soul_path = (
            str(getattr(state, "runtime_soul_path", "")).strip()
            or str(RUNTIME_SOUL_PATH)
        )
        runtime_cache_root = (
            str(getattr(state, "runtime_cache_root", "")).strip()
            or str(RUNTIME_CACHE_ROOT)
        )
        return cls.from_values(
            host_project_root=host_project_root or runtime_project_root,
            host_workspace_root=host_workspace_root or runtime_workspace_root,
            host_skills_root=host_skills_root or runtime_skills_root,
            host_soul_path=host_soul_path or str(PurePosixPath(runtime_soul_path)),
            runtime_project_root=runtime_project_root,
            runtime_workspace_root=runtime_workspace_root,
            runtime_skills_root=runtime_skills_root,
            runtime_soul_path=runtime_soul_path,
            runtime_cache_root=runtime_cache_root,
        )

    def to_locator(self, value: str | Path) -> str:
        raw = str(value or "").strip()
        if not raw:
            return ""
        if "://" in raw:
            return raw
        if raw == str(self.runtime_soul_path):
            return CONFIG_SOUL_URI
        if raw.startswith("/"):
            runtime_path = PurePosixPath(raw)
            if runtime_path == self.runtime_workspace_root or runtime_path.is_relative_to(self.runtime_workspace_root):
                rel = runtime_path.relative_to(self.runtime_workspace_root)
                cleaned = _clean_locator_relative(str(rel))
                return WORKSPACE_URI_PREFIX + cleaned if cleaned else WORKSPACE_URI_PREFIX
            if runtime_path == self.runtime_skills_root or runtime_path.is_relative_to(self.runtime_skills_root):
                rel = runtime_path.relative_to(self.runtime_skills_root)
                cleaned = _clean_locator_relative(str(rel))
                return SKILLS_URI_PREFIX + cleaned if cleaned else SKILLS_URI_PREFIX
        if raw.startswith("/"):
            path = Path(raw).resolve()
            try:
                rel = path.relative_to(self.host_workspace_root)
                cleaned = _clean_locator_relative(str(rel))
                return WORKSPACE_URI_PREFIX + cleaned if cleaned else WORKSPACE_URI_PREFIX
            except ValueError:
                pass
            try:
                rel = path.relative_to(self.host_skills_root)
                cleaned = _clean_locator_relative(str(rel))
                return SKILLS_URI_PREFIX + cleaned if cleaned else SKILLS_URI_PREFIX
            except ValueError:
                pass
            if path == self.host_soul_path:
                return CONFIG_SOUL_URI
            return raw
        runtime_path = PurePosixPath(raw)
        if runtime_path == self.runtime_soul_path:
            return CONFIG_SOUL_URI
        if runtime_path == self.runtime_workspace_root or runtime_path.is_relative_to(self.runtime_workspace_root):
            rel = runtime_path.relative_to(self.runtime_workspace_root)
            cleaned = _clean_locator_relative(str(rel))
            return WORKSPACE_URI_PREFIX + cleaned if cleaned else WORKSPACE_URI_PREFIX
        if runtime_path == self.runtime_skills_root or runtime_path.is_relative_to(self.runtime_skills_root):
            rel = runtime_path.relative_to(self.runtime_skills_root)
            cleaned = _clean_locator_relative(str(rel))
            return SKILLS_URI_PREFIX + cleaned if cleaned else SKILLS_URI_PREFIX
        return raw

    def locator_to_runtime(self, value: str | Path) -> str:
        raw = str(value or "").strip()
        if not raw:
            return ""
        if raw.startswith("/"):
            current = PurePosixPath(raw)
            if current == self.runtime_soul_path:
                return raw
            if current == self.runtime_workspace_root or current.is_relative_to(self.runtime_workspace_root):
                return raw
            if current == self.runtime_skills_root or current.is_relative_to(self.runtime_skills_root):
                return raw
        locator = self.to_locator(raw)
        if _is_prefixed(locator, WORKSPACE_URI_PREFIX):
            rel = _clean_locator_relative(locator[len(WORKSPACE_URI_PREFIX):])
            path = self.runtime_workspace_root / rel if rel else self.runtime_workspace_root
            return str(path)
        if _is_prefixed(locator, SKILLS_URI_PREFIX):
            rel = _clean_locator_relative(locator[len(SKILLS_URI_PREFIX):])
            path = self.runtime_skills_root / rel if rel else self.runtime_skills_root
            return str(path)
        if _is_prefixed(locator, CONFIG_URI_PREFIX):
            rel = _clean_locator_relative(locator[len(CONFIG_URI_PREFIX):])
            if rel != "SOUL.md":
                raise ValueError(f"未知 config locator: {locator}")
            return str(self.runtime_soul_path)
        return raw

    def locator_to_host(self, value: str | Path) -> str:
        raw = str(value or "").strip()
        if not raw:
            return ""
        if raw.startswith("/"):
            candidate = Path(raw).resolve()
            if (
                candidate == self.host_workspace_root
                or candidate.is_relative_to(self.host_workspace_root)
                or candidate == self.host_skills_root
                or candidate.is_relative_to(self.host_skills_root)
                or candidate == self.host_soul_path
            ):
                return str(candidate)
        locator = self.to_locator(raw)
        if _is_prefixed(locator, WORKSPACE_URI_PREFIX):
            rel = _clean_locator_relative(locator[len(WORKSPACE_URI_PREFIX):])
            path = self.host_workspace_root / rel if rel else self.host_workspace_root
            return str(path.resolve())
        if _is_prefixed(locator, SKILLS_URI_PREFIX):
            rel = _clean_locator_relative(locator[len(SKILLS_URI_PREFIX):])
            path = self.host_skills_root / rel if rel else self.host_skills_root
            return str(path.resolve())
        if _is_prefixed(locator, CONFIG_URI_PREFIX):
            rel = _clean_locator_relative(locator[len(CONFIG_URI_PREFIX):])
            if rel != "SOUL.md":
                raise ValueError(f"未知 config locator: {locator}")
            return str(self.host_soul_path.resolve())
        return str(Path(raw).resolve()) if raw.startswith("/") else raw


def to_locator(
    value: str | Path,
    *,
    host_project_root: str,
    host_workspace_root: str,
    host_skills_root: str,
    host_soul_path: str,
) -> str:
    return RuntimePathMap.from_values(
        host_project_root=host_project_root,
        host_workspace_root=host_workspace_root,
        host_skills_root=host_skills_root,
        host_soul_path=host_soul_path,
    ).to_locator(value)


__all__ = [
    "CONFIG_SOUL_URI",
    "RUNTIME_APP_ROOT",
    "RUNTIME_CACHE_ROOT",
    "RUNTIME_CONFIG_ROOT",
    "RUNTIME_SKILLS_ROOT",
    "RUNTIME_SOUL_PATH",
    "RUNTIME_WORKSPACE_ROOT",
    "RuntimePathMap",
    "to_locator",
]
