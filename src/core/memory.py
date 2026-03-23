"""Memory: 统一归档管道。"""

import json
from pathlib import Path

from .inbox import parse_frontmatter
from ..index import IndexClient


def archive(file_path: Path, source: str, hub_url: str) -> str:
    """归档文件到持久化存储（vec + sqlite），然后删除原文件。

    Args:
        file_path: 要归档的文件路径。
        source: 来源标识（"memory" / "inbox" / "task"）。
        hub_url: Hub WebSocket URL。

    Returns:
        entry_id，内容已存在时返回已有 ID。
    """
    content = file_path.read_text(encoding="utf-8")
    meta, body = parse_frontmatter(content)

    client = IndexClient(hub_url)
    try:
        entry_id = client.insert_entry(
            text=body or content,
            label=file_path.name,
            prefix=source,
            source=source,
            content=content,
            metadata=json.dumps(meta, ensure_ascii=False) if meta else None,
        )
        file_path.unlink()
    finally:
        client.close()

    return entry_id or ""
