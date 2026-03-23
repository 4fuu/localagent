"""CLI gateway adapter.

Outbound messages are appended to a local mailbox file.
"""


import json
import re
from pathlib import Path
from typing import Any

from .base import BaseGateway, MessageHandler


_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
_DEFAULT_MAILBOX_DIR = _PROJECT_ROOT / ".localagent" / "gateway_cli"


class CliGateway(BaseGateway):
    def __init__(self, *, hub_url: str, mailbox_dir: Path):
        super().__init__(name="cli", hub_url=hub_url)
        self._mailbox_dir = mailbox_dir
        self._started = False

    @classmethod
    def from_config(cls, *, hub_url: str, gateway_cfg: dict) -> "CliGateway":
        if not gateway_cfg.get("enabled", False):
            raise ValueError("CLI gateway is not enabled")

        mailbox_raw = str(gateway_cfg.get("mailbox_dir", "")).strip()
        mailbox_dir = Path(mailbox_raw).expanduser() if mailbox_raw else _DEFAULT_MAILBOX_DIR
        return cls(hub_url=hub_url, mailbox_dir=mailbox_dir)

    def start(self, on_message: MessageHandler) -> None:
        del on_message
        self._mailbox_dir.mkdir(parents=True, exist_ok=True)
        self._started = True

    def stop(self) -> None:
        self._started = False

    def send_message(
        self,
        conversation_id: str,
        text: str,
        *,
        user_id: str = "",
        metadata: dict[str, Any] | None = None,
        artifact_refs: list[str] | None = None,
    ) -> dict[str, Any]:
        if not self._started:
            raise RuntimeError("CliGateway is not started")
        del artifact_refs

        path = self._mailbox_path(conversation_id)
        rendered = self.render_message_text(text)
        payload = {
            "gateway": self.name,
            "conversation_id": conversation_id,
            "user_id": user_id,
            "text": rendered.text,
            "metadata": metadata or {},
        }
        with path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(payload, ensure_ascii=False) + "\n")

        return {
            "gateway": self.name,
            "conversation_id": conversation_id,
            "delivered": True,
            "mailbox": str(path),
        }

    def _mailbox_path(self, conversation_id: str) -> Path:
        return self._mailbox_dir / f"{self._slug(conversation_id)}.jsonl"

    @staticmethod
    def _slug(value: str) -> str:
        raw = value.strip().lower() or "conversation"
        s = re.sub(r"[^a-z0-9._-]+", "-", raw)
        s = s.strip("-._")
        if not s:
            s = "conversation"
        return s[:80]
