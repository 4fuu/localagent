from pathlib import Path

import yaml

from ..index import IndexSource


def _parse_frontmatter(content: str) -> dict | None:
    """解析 SKILL.md 的 YAML frontmatter。"""
    if not content.startswith("---"):
        return None
    parts = content.split("---", 2)
    if len(parts) < 3:
        return None
    try:
        return yaml.safe_load(parts[1])
    except yaml.YAMLError:
        return None


class SkillsSource(IndexSource):
    """skills 目录的索引源。"""

    def __init__(self, path: str):
        self._base = Path(path).resolve()

    @property
    def name(self) -> str:
        return "skills"

    def discover(self) -> list[Path]:
        if not self._base.is_dir():
            return []
        paths = []
        for entry in sorted(self._base.iterdir()):
            if not entry.is_dir():
                continue
            skill_file = entry / "SKILL.md"
            if skill_file.is_file():
                paths.append(skill_file.resolve())
        return paths

    def extract_text(self, path: Path) -> str:
        content = path.read_text(encoding="utf-8")
        meta = _parse_frontmatter(content)
        parts: list[str] = []
        if meta:
            if "name" in meta:
                parts.append(meta["name"])
            if "description" in meta:
                parts.append(meta["description"])
        if content.startswith("---"):
            body_parts = content.split("---", 2)
            if len(body_parts) >= 3:
                body = body_parts[2].strip()
                if body:
                    parts.append(body)
        return "\n".join(parts)


def _skill_slug(entry: Path) -> str:
    return entry.name.strip().lower().replace("_", "-")


def skills_catalog(path: str) -> list[dict[str, str]]:
    """返回本地 skills 元数据目录。"""
    base = Path(path)
    if not base.is_dir():
        return []

    items: list[dict[str, str]] = []
    for entry in sorted(base.iterdir()):
        if not entry.is_dir():
            continue
        skill_file = entry / "SKILL.md"
        if not skill_file.is_file():
            continue
        content = skill_file.read_text(encoding="utf-8")
        meta = _parse_frontmatter(content) or {}
        items.append({
            "skill": _skill_slug(entry),
            "name": str(meta.get("name") or entry.name).strip(),
            "description": str(meta.get("description") or "").strip(),
            "path": str(skill_file),
        })
    return items


def skills_list(path: str, page: int = 1, page_size: int = 10) -> dict:
    """载入路径下所有子文件夹的 SKILL.md 描述，分页返回。"""
    catalog = skills_catalog(path)
    if not catalog:
        return {
            "items": [],
            "total": 0,
            "page": page,
            "page_size": page_size,
            "total_pages": 0,
        }

    skills: list[dict[str, str]] = []
    for item in catalog:
        skills.append({
            "skill": item["skill"],
            "name": item["name"],
            "description": item["description"],
            "path": item["path"],
        })

    total = len(catalog)
    total_pages = (total + page_size - 1) // page_size if total > 0 else 0
    start = (page - 1) * page_size
    return {
        "items": skills[start : start + page_size],
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": total_pages,
    }
