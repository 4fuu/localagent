"""Context refs: tool call payload references and task bindings."""

from __future__ import annotations

import re
from typing import Any

from ..core.store import Store

_SAFE_ID_RE = re.compile(r"^[A-Za-z0-9_-]+$")
_DEFAULT_MAX_CHARS = 6_000


def _is_safe_id(value: str) -> bool:
    return bool(value and _SAFE_ID_RE.match(value))


def _truncate_text(value: str, max_chars: int) -> tuple[str, bool]:
    if max_chars < 1:
        max_chars = 1
    if len(value) <= max_chars:
        return value, False
    return value[:max_chars], True


def _sanitize_for_storage(value: Any, max_chars: int = 120_000) -> Any:
    if value is None or isinstance(value, (bool, int, float)):
        return value
    if isinstance(value, str):
        clipped, truncated = _truncate_text(value, max_chars)
        if not truncated:
            return clipped
        return {"text": clipped, "truncated": True}
    if isinstance(value, dict):
        return {str(k): _sanitize_for_storage(v, max_chars=max_chars) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_sanitize_for_storage(v, max_chars=max_chars) for v in value]
    text = str(value)
    clipped, truncated = _truncate_text(text, max_chars)
    if not truncated:
        return clipped
    return {"text": clipped, "truncated": True}


def write_tool_ref(ref_id: str, payload: dict[str, Any]) -> None:
    normalized = ref_id.strip()
    if not _is_safe_id(normalized):
        raise ValueError(f"invalid ref_id: {ref_id}")
    safe_payload = _sanitize_for_storage(payload)
    with Store() as store:
        store.runtime_upsert_tool_ref(normalized, safe_payload)


def tool_ref_exists(ref_id: str) -> bool:
    normalized = ref_id.strip()
    if not _is_safe_id(normalized):
        return False
    with Store() as store:
        return store.runtime_tool_ref_exists(normalized)


def read_tool_ref(ref_id: str, max_chars: int = _DEFAULT_MAX_CHARS) -> dict[str, Any] | None:
    normalized = ref_id.strip()
    if not _is_safe_id(normalized):
        return None
    with Store() as store:
        return store.runtime_read_tool_ref(normalized, max_chars=max_chars)


def write_task_context_refs(task_id: str, ref_ids: list[str]) -> list[str]:
    normalized_task_id = task_id.strip()
    if not _is_safe_id(normalized_task_id):
        raise ValueError(f"invalid task_id: {task_id}")

    normalized_refs = [
        str(raw).strip()
        for raw in ref_ids
        if _is_safe_id(str(raw).strip())
    ]
    with Store() as store:
        return store.runtime_write_task_context_refs(
            normalized_task_id,
            normalized_refs,
        )


def read_task_context_refs(task_id: str) -> list[str]:
    normalized_task_id = task_id.strip()
    if not _is_safe_id(normalized_task_id):
        return []
    with Store() as store:
        return store.runtime_read_task_context_refs(normalized_task_id)
