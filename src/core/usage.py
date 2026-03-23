"""Usage 持久化到 SQLite runtime telemetry 表。"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import yaml

from .store import Store

logger = logging.getLogger(__name__)

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent


def parse_startup_payload(payload: str) -> dict[str, str]:
    """解析 startup payload 的头部 key=value 字段。"""
    if not payload:
        return {}
    lines = payload.splitlines()
    if not lines or lines[0].strip() != "[startup]":
        return {}

    data: dict[str, str] = {}
    for line in lines[1:]:
        raw = line.strip()
        if not raw:
            break
        if "=" not in raw:
            continue
        key, value = raw.split("=", 1)
        data[key.strip()] = value.strip()
    return data


def extract_conversation_id_from_payload(payload: str) -> str:
    """从 startup payload 中提取会话 ID。"""
    data = parse_startup_payload(payload)
    conv = data.get("conversation_id", "").strip()
    if conv:
        return conv

    for line in payload.splitlines():
        raw = line.strip()
        if raw.startswith("conversation_id="):
            return raw.split("=", 1)[1].strip()
    return ""


def read_task_conversation_id(task_id: str) -> str:
    """读取任务会话 ID：优先 DB，回退 task/<task_id>.md frontmatter。"""
    normalized_task_id = task_id.strip()
    if not normalized_task_id:
        return ""

    try:
        with Store() as store:
            task = store.task_read(normalized_task_id)
            if task:
                conv = str(task.get("conversation_id", "")).strip()
                if conv:
                    return conv
    except Exception:
        logger.exception(
            "Failed to read task conversation_id from Store: %s",
            normalized_task_id,
        )

    task_path = _PROJECT_ROOT / "task" / f"{normalized_task_id}.md"
    if not task_path.is_file():
        return ""

    try:
        content = task_path.read_text(encoding="utf-8")
    except Exception:
        logger.exception("Failed to read task file for usage: %s", task_path)
        return ""

    meta, _ = _parse_frontmatter(content)
    if not meta:
        return ""
    return str(meta.get("conversation_id", "")).strip()


def resolve_conversation_id(role: str, payload: str, task_id: str) -> str:
    """解析本次运行关联的 conversation_id。"""
    from_payload = extract_conversation_id_from_payload(payload).strip()
    if from_payload:
        return from_payload
    if role == "task":
        return read_task_conversation_id(task_id).strip()
    return ""


def persist_usage_record(record: dict[str, Any]) -> None:
    """写入 runtime_runs 表。"""
    conversation_id = str(record.get("conversation_id", "")).strip()
    record["conversation_id"] = conversation_id
    with Store() as store:
        store.runtime_record_run(record)


def _parse_frontmatter(content: str) -> tuple[dict | None, str]:
    if not content.startswith("---"):
        return None, content
    parts = content.split("---", 2)
    if len(parts) < 3:
        return None, content
    try:
        meta = yaml.safe_load(parts[1])
        return meta, parts[2].strip()
    except yaml.YAMLError:
        return None, content
