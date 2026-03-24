"""Topic snapshot indexing helpers."""

from __future__ import annotations

import hashlib
import json
from typing import Any

from ..index import IndexClient


def topic_entry_id(topic_id: str) -> str:
    normalized_topic_id = str(topic_id or "").strip()
    if not normalized_topic_id:
        raise ValueError("topic_id cannot be empty")
    return hashlib.sha256(f"topic:{normalized_topic_id}".encode()).hexdigest()


def _topic_snapshot_text(topic: dict[str, Any]) -> str:
    parts: list[str] = []
    topic_id = str(topic.get("id", "")).strip()
    if topic_id:
        parts.append(f"topic_id: {topic_id}")
    status = str(topic.get("status", "")).strip()
    if status:
        parts.append(f"status: {status}")
    goal = str(topic.get("goal", "")).strip()
    if goal:
        parts.append(f"goal: {goal}")
    if topic.get("replied"):
        parts.append("replied: true")
    last_task_id = str(topic.get("last_task_id", "")).strip()
    if last_task_id:
        parts.append(f"last_task_id: {last_task_id}")
    return "\n".join(parts).strip()


def archive_topic_snapshot(
    hub_url: str,
    *,
    conversation_id: str,
    topic: dict[str, Any] | None,
) -> str | None:
    if not hub_url or not conversation_id.strip() or not topic:
        return None
    topic_id = str(topic.get("id", "")).strip()
    if not topic_id:
        return None
    text = _topic_snapshot_text(topic)
    if not text:
        return None
    metadata = {
        "conversation_id": conversation_id.strip(),
        "topic_id": topic_id,
        "topic_status": str(topic.get("status", "")).strip(),
        "source": "topic",
        "last_task_id": str(topic.get("last_task_id", "")).strip(),
        "source_message_id": str(topic.get("source_message_id", "")).strip(),
        "updated_at": str(topic.get("updated_at", "")).strip(),
    }
    client = IndexClient(hub_url)
    try:
        return client.upsert_entry(
            topic_entry_id(topic_id),
            text=text,
            label=topic_id,
            source="topic",
            content=json.dumps(topic, ensure_ascii=False),
            metadata=json.dumps(metadata, ensure_ascii=False),
        )
    finally:
        client.close()
