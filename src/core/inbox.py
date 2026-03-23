"""Inbox: frontmatter 工具函数 + InboxSource 索引源。"""

from pathlib import Path

import yaml

from ..index import IndexSource


def parse_frontmatter(content: str) -> tuple[dict | None, str]:
    """解析 YAML frontmatter，返回 (meta, body)。"""
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


def format_frontmatter(meta: dict, body: str) -> str:
    """将 meta + body 组装为带 frontmatter 的 markdown。"""
    fm = yaml.dump(meta, default_flow_style=False, allow_unicode=True).strip()
    return f"---\n{fm}\n---\n\n# 内容开始\n{body}"


def update_frontmatter(content: str, updates: dict) -> str:
    """更新 frontmatter 字段，返回完整内容。"""
    meta, body = parse_frontmatter(content)
    if meta is None:
        meta = {}
    meta.update(updates)
    return format_frontmatter(meta, body)


class InboxSource(IndexSource):
    """inbox/ 目录的索引源。"""

    def __init__(self, path: str):
        self._base = Path(path).resolve()

    @property
    def name(self) -> str:
        return "inbox"

    def discover(self) -> list[Path]:
        if not self._base.is_dir():
            return []
        return sorted(
            p.resolve() for p in self._base.iterdir()
            if p.is_file() and (p.suffix == ".md" or p.name.endswith(".silent.md"))
        )

    def extract_text(self, path: Path) -> str:
        content = path.read_text(encoding="utf-8")
        meta, body = parse_frontmatter(content)
        parts: list[str] = []
        if meta:
            for key in ("from", "subject", "type", "priority"):
                if key in meta:
                    parts.append(f"{key}: {meta[key]}")
            attachments = meta.get("attachments")
            if isinstance(attachments, list):
                for att in attachments:
                    if isinstance(att, dict):
                        parts.append(
                            f"attachment: {att.get('file_name', 'unknown')} "
                            f"({att.get('mime_type', '')})"
                        )
        if body:
            parts.append(body)
        return "\n".join(parts)
