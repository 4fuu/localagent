from typing import Any
from msgspec import Struct, field


class BaseState(Struct, kw_only=True):
    messages: list[dict[str, Any]] = field(default_factory=list)
