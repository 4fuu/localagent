"""Task: 任务创建/状态/停止 + TaskSource 索引源。"""

import uuid
from datetime import datetime, timezone
from pathlib import Path
from subprocess import Popen

from .inbox import format_frontmatter, parse_frontmatter, update_frontmatter
from ..index import IndexSource


def task_create(
    base: Path,
    goal: str,
    task_type: str = "general",
    notify_main_on_finish: bool | None = None,
    gateway: str = "",
    conversation_id: str = "",
    user_id: str = "",
    message_id: str = "",
    reply_to_message_id: str = "",
    parent_task_id: str = "",
    then: list[str] | None = None,
    then_task_types: list[str] | None = None,
    images: list[str] | None = None,
) -> dict:
    """创建任务 md 文件，返回 {id, path}。"""
    task_id = f"t-{uuid.uuid4().hex[:8]}"
    now = datetime.now(timezone.utc).isoformat()
    meta: dict = {
        "id": task_id,
        "status": "pending",
        "task_type": task_type,
        "goal": goal,
        "created_at": now,
    }
    if notify_main_on_finish is not None:
        meta["notify_main_on_finish"] = bool(notify_main_on_finish)
    if conversation_id.strip():
        meta["conversation_id"] = conversation_id.strip()
    if gateway.strip():
        meta["gateway"] = gateway.strip()
    if user_id.strip():
        meta["user_id"] = user_id.strip()
    if message_id.strip():
        meta["message_id"] = message_id.strip()
    if reply_to_message_id.strip():
        meta["reply_to_message_id"] = reply_to_message_id.strip()
    if parent_task_id.strip():
        meta["parent_task_id"] = parent_task_id.strip()
    if then:
        meta["then"] = then
    if then_task_types:
        meta["then_task_types"] = then_task_types
    if images:
        meta["images"] = images
    content = format_frontmatter(meta, "")
    task_path = base / f"{task_id}.md"
    task_path.write_text(content, encoding="utf-8")
    return {"id": task_id, "path": str(task_path)}


def task_stop(base: Path, task_id: str, process: Popen | None = None) -> dict:
    """终止子进程并更新状态为 stopped。"""
    task_path = base / f"{task_id}.md"
    if not task_path.is_file():
        return {"error": f"task {task_id} not found"}

    if process and process.poll() is None:
        process.terminate()
        process.wait(timeout=10)

    content = task_path.read_text(encoding="utf-8")
    updated = update_frontmatter(content, {"status": "stopped"})
    task_path.write_text(updated, encoding="utf-8")
    return {"id": task_id, "status": "stopped"}


class TaskSource(IndexSource):
    """task/ 目录的索引源。"""

    def __init__(self, path: str):
        self._base = Path(path).resolve()

    @property
    def name(self) -> str:
        return "task"

    def discover(self) -> list[Path]:
        if not self._base.is_dir():
            return []
        return sorted(
            p.resolve() for p in self._base.iterdir()
            if p.is_file() and p.suffix == ".md"
        )

    def extract_text(self, path: Path) -> str:
        content = path.read_text(encoding="utf-8")
        meta, body = parse_frontmatter(content)
        parts: list[str] = []
        if meta:
            if "goal" in meta:
                parts.append(f"goal: {meta['goal']}")
            if "status" in meta:
                parts.append(f"status: {meta['status']}")
        if body:
            parts.append(body)
        return "\n".join(parts)
