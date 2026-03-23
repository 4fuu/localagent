from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

from .formatting import (
    MarkdownCapabilities,
    MarkdownDocument,
    RenderedText,
    parse_markdown,
    render_plain_text,
    should_render_markdown,
)


@dataclass(slots=True)
class Attachment:
    """附件。"""

    file_path: str
    file_name: str
    mime_type: str
    file_size: int = 0

    @property
    def is_image(self) -> bool:
        mime_type = self.mime_type.strip().lower()
        if mime_type.startswith("image/"):
            return True
        suffix = Path(self.file_name or self.file_path).suffix.lower()
        return suffix in {".png", ".jpg", ".jpeg", ".gif", ".webp", ".bmp"}


@dataclass(slots=True)
class InboundMessage:
    """统一的入站消息结构。"""

    gateway: str
    conversation_id: str
    text: str
    user_id: str = ""
    user_name: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)
    # True means "store only"; gateway service will skip wake notification.
    silent: bool = False
    attachments: list[Attachment] = field(default_factory=list)


MessageHandler = Callable[[InboundMessage], None]


class BaseGateway(ABC):
    """Gateway 适配器基类。"""

    def __init__(self, *, name: str, hub_url: str):
        self.name = name
        self.hub_url = hub_url

    @classmethod
    @abstractmethod
    def from_config(cls, *, hub_url: str, gateway_cfg: dict) -> "BaseGateway":
        """从配置构建实例。gateway_cfg 是该网关在 config.toml 中的配置段。"""

    @abstractmethod
    def start(self, on_message: MessageHandler) -> None:
        """启动网关并注册入站消息回调。

        子类可通过 InboundMessage.silent=True 标记静默消息，仅落盘不唤醒 agent。
        """

    @abstractmethod
    def stop(self) -> None:
        """停止网关。"""

    @abstractmethod
    def send_message(
        self,
        conversation_id: str,
        text: str,
        *,
        user_id: str = "",
        metadata: dict[str, Any] | None = None,
        artifact_refs: list[str] | None = None,
    ) -> dict[str, Any]:
        """发送消息到外部聊天软件。"""

    def send_action(self, conversation_id: str, action: str) -> None:
        """发送聊天动作（如 typing）到外部平台。默认空实现，子类按需覆盖。"""

    @property
    def markdown_capabilities(self) -> MarkdownCapabilities:
        """当前 gateway 支持的 Markdown 能力集合。"""
        return MarkdownCapabilities()

    def render_message_text(self, text: str) -> RenderedText:
        """将统一 Markdown 子集渲染为当前平台的发送文本。"""
        if not should_render_markdown(text):
            return RenderedText(text=text)
        rendered = self.render_markdown_document(parse_markdown(text))
        if text.strip() and not rendered.text.strip():
            return RenderedText(text=text.strip())
        return rendered

    def render_markdown_document(self, doc: MarkdownDocument) -> RenderedText:
        """子类可覆盖此方法，自定义平台级富文本映射。"""
        return render_plain_text(doc, self.markdown_capabilities)

    def filter_inbound(self, inbound: InboundMessage) -> InboundMessage | None:
        """入站筛选钩子（默认不处理）。

        返回:
        - InboundMessage: 继续写入 inbox，并由 `silent` 决定是否唤醒。
        - None: 丢弃消息，不写入 inbox，不唤醒。

        具体筛选/提醒策略由各 gateway 子类自行实现并覆盖此方法。
        """
        return inbound
