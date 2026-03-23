"""Identity helpers for mapping external accounts to a stable person_id."""

from __future__ import annotations

from typing import Any

from ..config import cfg

_MULTI_PARTY_CHAT_TYPES = {"group", "supergroup"}


def build_account_key(gateway: str, user_id: str) -> str:
    normalized_gateway = str(gateway).strip()
    normalized_user_id = str(user_id).strip()
    if not normalized_gateway or not normalized_user_id:
        return ""
    return f"{normalized_gateway}:{normalized_user_id}"


def resolve_person_id(gateway: str, user_id: str) -> str:
    return cfg.resolve_person_id(gateway, user_id)


def infer_person_id(
    *,
    gateway: str,
    user_id: str,
    fallback_person_id: str = "",
) -> str:
    resolved = resolve_person_id(gateway, user_id)
    if resolved:
        return resolved
    return str(fallback_person_id).strip()


def is_multi_party_metadata(metadata: dict[str, Any] | None) -> bool:
    if not isinstance(metadata, dict):
        return False
    if bool(metadata.get("is_multi_party", False)):
        return True
    chat_type = str(metadata.get("chat_type", "")).strip().lower()
    return chat_type in _MULTI_PARTY_CHAT_TYPES


def workspace_scope(
    *,
    person_id: str = "",
    conversation_id: str = "",
    is_multi_party: bool = False,
) -> tuple[str, str]:
    normalized_person_id = str(person_id).strip()
    normalized_conversation_id = str(conversation_id).strip()
    if not is_multi_party and normalized_person_id:
        return "person", normalized_person_id
    return "conversation", normalized_conversation_id
