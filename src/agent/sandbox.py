"""Shared sandbox rules for file and shell tools."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from src.config import cfg

from ..provider import BaseState

SandboxAccess = Literal["read", "write", "workdir"]


@dataclass(frozen=True)
class ToolSandbox:
    workspace_root: Path
    project_root: Path
    skills_root: Path | None
    soul_path: Path | None
    containerized: bool
    is_admin: bool
    restricted: bool

    @classmethod
    def from_state(cls, state: BaseState) -> "ToolSandbox":
        workspace_root = Path(getattr(state, "workspace_root", ".")).resolve()
        project_root = Path(getattr(state, "project_root", workspace_root)).resolve()

        raw_skills_path = str(getattr(state, "skills_path", "")).strip()
        skills_root: Path | None = None
        if raw_skills_path:
            skills_path = Path(raw_skills_path)
            if skills_path.is_absolute():
                skills_root = skills_path.resolve()
            else:
                skills_root = (project_root / skills_path).resolve()
        raw_soul_path = str(getattr(state, "soul_path", "")).strip()
        soul_path: Path | None = None
        if raw_soul_path:
            path = Path(raw_soul_path)
            if path.is_absolute():
                soul_path = path.resolve()
            else:
                soul_path = (project_root / path).resolve()
        containerized = bool(getattr(state, "containerized", False))
        is_admin = bool(getattr(state, "is_admin", False))

        return cls(
            workspace_root=workspace_root,
            project_root=project_root,
            skills_root=skills_root,
            soul_path=soul_path,
            containerized=containerized,
            is_admin=is_admin,
            restricted=bool(
                True
                if containerized
                else getattr(state, "sandboxed", not is_admin)
            ),
        )

    @property
    def readable_roots(self) -> tuple[Path, ...]:
        if self.containerized:
            roots: list[Path] = [self.project_root, self.workspace_root]
            if self.skills_root is not None:
                roots.append(self.skills_root)
            if self.soul_path is not None:
                roots.append(self.soul_path)
            return tuple(roots)
        if not self.restricted:
            return (self.project_root,)
        roots: list[Path] = [self.workspace_root]
        if self.skills_root is not None:
            roots.append(self.skills_root)
        return tuple(roots)

    @property
    def _skills_writable(self) -> bool:
        return self.is_admin or bool(cfg.sandbox.get("user_writable_skills", False))

    @property
    def writable_roots(self) -> tuple[Path, ...]:
        if self.containerized:
            roots: list[Path] = [self.workspace_root]
            if self._skills_writable:
                if self.skills_root is not None:
                    roots.append(self.skills_root)
                if self.soul_path is not None:
                    roots.append(self.soul_path)
            return tuple(roots)
        if not self.restricted:
            return (self.project_root,)
        return (self.workspace_root,)

    def effective_cwd(self, raw_cwd: str) -> Path:
        if raw_cwd:
            candidate = Path(raw_cwd)
            if candidate.is_dir():
                resolved = candidate.resolve()
                if self.is_allowed(resolved, access="workdir"):
                    return resolved
        return self.workspace_root if self.restricted else self.project_root

    def resolve_path(
        self,
        path: str,
        *,
        access: SandboxAccess,
        base_dir: Path,
    ) -> Path:
        if not path or not path.strip():
            raise ValueError("path 不能为空")
        raw = Path(path.strip())
        target = raw if raw.is_absolute() else (base_dir / raw)
        return self.ensure_allowed(target, access=access)

    def resolve_skills_root(self) -> Path:
        if self.skills_root is None:
            raise ValueError("skills_path 未配置")
        return self.ensure_allowed(self.skills_root, access="read")

    def ensure_allowed(self, target: Path, *, access: SandboxAccess) -> Path:
        resolved = target.resolve()
        if not self.restricted:
            return resolved

        roots = self._roots_for(access)
        if self._matches_any_root(resolved, roots):
            return resolved

        if self.containerized:
            raise ValueError(
                f"路径不在允许范围内: {resolved}；容器内仅允许访问 /app、/workspace、/skills、/config/SOUL.md"
            )
        if access == "read" and self.skills_root is not None:
            raise ValueError(
                f"路径不在允许范围内: {resolved}；非管理员只能访问 workspace 和全局 skills"
            )
        raise ValueError(f"路径不在当前会话 workspace 范围内: {resolved}")

    def is_allowed(self, target: Path, *, access: SandboxAccess) -> bool:
        resolved = target.resolve()
        if not self.restricted:
            return True
        return self._matches_any_root(resolved, self._roots_for(access))

    def _roots_for(self, access: SandboxAccess) -> tuple[Path, ...]:
        if access == "read":
            return self.readable_roots
        if access == "workdir" and self.containerized:
            roots = list(self.readable_roots)
            if self.soul_path is not None:
                roots.append(self.soul_path.parent)
            return tuple(roots)
        return self.writable_roots

    @staticmethod
    def _matches_any_root(target: Path, roots: tuple[Path, ...]) -> bool:
        for root in roots:
            try:
                target.relative_to(root)
                return True
            except ValueError:
                continue
        return False
