"""Internal host-side storage roots for task runtime data."""

from __future__ import annotations

import hashlib
import re
import shutil
from pathlib import Path

from .identity import workspace_scope

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
RUNTIME_ROOT = PROJECT_ROOT / ".localagent" / "runtime"
RUNTIME_WORKSPACES_ROOT = RUNTIME_ROOT / "workspaces"
RUNTIME_SKILLS_ROOT = RUNTIME_ROOT / "skills"
RUNTIME_CONFIG_ROOT = RUNTIME_ROOT / "config"
RUNTIME_SOUL_PATH = RUNTIME_CONFIG_ROOT / "SOUL.md"
RUNTIME_CONTROL_ROOT = RUNTIME_ROOT / "control"
REPO_SKILLS_ROOT = PROJECT_ROOT / "skills"
REPO_SOUL_PATH = PROJECT_ROOT / "SOUL.md"
NO_SCOPE = "__no_scope__"


def ensure_runtime_layout() -> None:
    RUNTIME_WORKSPACES_ROOT.mkdir(parents=True, exist_ok=True)
    RUNTIME_CONTROL_ROOT.mkdir(parents=True, exist_ok=True)
    if not RUNTIME_SKILLS_ROOT.exists() and REPO_SKILLS_ROOT.is_dir():
        shutil.copytree(REPO_SKILLS_ROOT, RUNTIME_SKILLS_ROOT)
    else:
        RUNTIME_SKILLS_ROOT.mkdir(parents=True, exist_ok=True)
    RUNTIME_CONFIG_ROOT.mkdir(parents=True, exist_ok=True)
    if not RUNTIME_SOUL_PATH.exists():
        if REPO_SOUL_PATH.is_file():
            shutil.copy2(REPO_SOUL_PATH, RUNTIME_SOUL_PATH)
        else:
            RUNTIME_SOUL_PATH.write_text("", encoding="utf-8")


def _slugify(value: str) -> str:
    normalized = re.sub(r"[^A-Za-z0-9._-]+", "-", value.strip()).strip("-._")
    return normalized[:48] or "conversation"


def _scoped_workspace_root(scope_kind: str, scope_id: str) -> Path:
    ensure_runtime_layout()
    normalized_kind = scope_kind.strip() or "conversation"
    normalized_scope_id = scope_id.strip() or NO_SCOPE
    normalized = f"{normalized_kind}:{normalized_scope_id}"
    digest = hashlib.sha1(normalized.encode("utf-8")).hexdigest()[:12]
    folder = f"{_slugify(normalized)}-{digest}"
    root = RUNTIME_WORKSPACES_ROOT / normalized_kind / folder
    root.mkdir(parents=True, exist_ok=True)
    return root.resolve()


def resolve_runtime_workspace_root(
    *,
    conversation_id: str,
    person_id: str = "",
    is_multi_party: bool = False,
) -> Path:
    ensure_runtime_layout()
    scope_kind, scope_id = workspace_scope(
        person_id=person_id,
        conversation_id=conversation_id,
        is_multi_party=is_multi_party,
    )
    if scope_kind == "person":
        return _scoped_workspace_root("person", scope_id)
    return _scoped_workspace_root("conversation", scope_id)
