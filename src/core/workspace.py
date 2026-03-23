"""Workspace helpers for person- or conversation-scoped isolation."""

import hashlib
import re
from pathlib import Path

from .identity import workspace_scope

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
WORKSPACES_ROOT = PROJECT_ROOT / ".localagent" / "workspaces"
NO_SCOPE = "__no_scope__"


def _slugify(value: str) -> str:
    normalized = re.sub(r"[^A-Za-z0-9._-]+", "-", value.strip()).strip("-._")
    return normalized[:48] or "conversation"


def _scoped_workspace_root(scope_kind: str, scope_id: str) -> Path:
    normalized_kind = scope_kind.strip() or "conversation"
    normalized_scope_id = scope_id.strip() or NO_SCOPE
    normalized = f"{normalized_kind}:{normalized_scope_id}"
    digest = hashlib.sha1(normalized.encode("utf-8")).hexdigest()[:12]
    folder = f"{_slugify(normalized)}-{digest}"
    root = WORKSPACES_ROOT / normalized_kind / folder
    root.mkdir(parents=True, exist_ok=True)
    return root.resolve()


def conversation_workspace_root(conversation_id: str) -> Path:
    return _scoped_workspace_root("conversation", conversation_id)


def person_workspace_root(person_id: str) -> Path:
    return _scoped_workspace_root("person", person_id)


def resolve_agent_workspace_root(
    *,
    conversation_id: str,
    person_id: str = "",
    is_multi_party: bool = False,
    is_admin: bool,
    sandboxed: bool = True,
) -> Path:
    if is_admin and not sandboxed:
        return PROJECT_ROOT
    scope_kind, scope_id = workspace_scope(
        person_id=person_id,
        conversation_id=conversation_id,
        is_multi_party=is_multi_party,
    )
    if scope_kind == "person":
        return person_workspace_root(scope_id)
    return conversation_workspace_root(scope_id)
