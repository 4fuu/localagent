from abc import ABC, abstractmethod
from pathlib import Path


class IndexSource(ABC):
    """可索引的文件源。"""

    @property
    @abstractmethod
    def name(self) -> str:
        """源名称，如 'skills'。"""
        ...

    @abstractmethod
    def discover(self) -> list[Path]:
        """发现所有需要索引的文件，返回绝对路径列表。"""
        ...

    @abstractmethod
    def extract_text(self, path: Path) -> str:
        """从文件中提取用于 embedding 的文本。"""
        ...
