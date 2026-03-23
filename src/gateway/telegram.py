"""Telegram gateway adapter (long polling)."""

import json
import logging
import threading
import time
import uuid
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib import parse, request

from .base import Attachment, BaseGateway, InboundMessage, MessageHandler
from .formatting import MarkdownCapabilities, MarkdownDocument, RenderedText, render_telegram_html
from ..core.artifacts import ArtifactStore
from ..retry import RetryPolicy

logger = logging.getLogger(__name__)

_GROUP_CHAT_TYPES = {"group", "supergroup"}
_SUPPORTED_GROUP_MESSAGE_MODES = {"all", "mention", "command", "mention_or_command"}
_PHOTO_EXTENSIONS = {".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp"}
_TELEGRAM_FILE_SIZE_LIMIT = 20 * 1024 * 1024  # 20MB
_DEFAULT_START_REPLY = "你好，我是 Qian。直接发送消息即可开始对话。"
_DEFAULT_GROUP_BACKLOG_LIMIT = 20
_META_REPLAYED_GROUP_BACKLOG = "_telegram_group_backlog_replay"


@dataclass(slots=True)
class _ChatRule:
    """Per-chat configuration."""

    silent: bool = False
    blocked_user_ids: set[str] = field(default_factory=set)
    blocked_user_ids_mode: str = "append"  # "append" | "override"
    group_message_mode: str | None = None  # None = use gateway default
    group_allow_media_only: bool | None = None  # None = use gateway default


def _to_bool(value: Any, default: bool) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        raw = value.strip().lower()
        if raw in {"1", "true", "yes", "on"}:
            return True
        if raw in {"0", "false", "no", "off"}:
            return False
    return default


def _parse_id_set(raw: Any) -> set[str]:
    if raw is None:
        return set()
    if isinstance(raw, (list, tuple, set)):
        items = raw
    else:
        items = str(raw).split(",")
    out: set[str] = set()
    for item in items:
        val = str(item).strip()
        if val:
            out.add(val)
    return out


def _parse_str_list(raw: Any) -> list[str]:
    if raw is None:
        return []
    if isinstance(raw, list):
        items = raw
    else:
        items = str(raw).split(",")
    out: list[str] = []
    for item in items:
        val = str(item).strip()
        if val:
            out.append(val)
    return out


def _safe_int(value: Any) -> int:
    try:
        return int(value)
    except Exception:
        return 0


class TelegramGateway(BaseGateway):
    def __init__(
        self,
        *,
        hub_url: str,
        bot_token: str,
        debug: bool,
        poll_timeout: int,
        drop_pending_updates: bool,
        blocked_user_ids: set[str],
        chat_rules: dict[str, _ChatRule],
        admin_user_ids: set[str],
        group_message_mode: str,
        group_allow_media_only: bool,
        command_prefixes: list[str],
        group_backlog_limit: int,
    ):
        super().__init__(name="telegram", hub_url=hub_url)
        self._bot_token = bot_token
        self._debug = debug
        self._poll_timeout = max(1, poll_timeout)
        self._drop_pending_updates = drop_pending_updates
        self._blocked_user_ids = blocked_user_ids
        self._chat_rules = chat_rules
        self._admin_user_ids = admin_user_ids
        self._group_message_mode = group_message_mode
        self._group_allow_media_only = group_allow_media_only
        self._command_prefixes = command_prefixes
        self._group_backlog_limit = max(1, group_backlog_limit)
        self._retry = RetryPolicy.for_service("telegram_api")
        self._group_backlog: dict[str, deque[InboundMessage]] = {}

        self._on_message: MessageHandler | None = None
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()
        self._offset: int | None = None
        self._bot_username = ""
        self._consecutive_errors = 0

    @property
    def markdown_capabilities(self) -> MarkdownCapabilities:
        return MarkdownCapabilities(
            bold=True,
            italic=True,
            strike=True,
            spoiler=True,
            inline_code=True,
            code_block=True,
            link=True,
            blockquote=True,
            heading=False,
            list=False,
        )

    def render_markdown_document(self, doc: MarkdownDocument) -> RenderedText:
        return render_telegram_html(doc)

    @classmethod
    def from_config(cls, *, hub_url: str, gateway_cfg: dict) -> "TelegramGateway":
        if not _to_bool(gateway_cfg.get("enabled"), False):
            raise ValueError("Telegram gateway is not enabled")

        bot_token = str(gateway_cfg.get("bot_token", "")).strip()
        if not bot_token:
            raise ValueError("Telegram bot_token is required")

        poll_timeout = int(gateway_cfg.get("poll_timeout", 25))
        debug = _to_bool(gateway_cfg.get("debug"), False)
        drop_pending_updates = _to_bool(gateway_cfg.get("drop_pending_updates"), True)
        blocked_user_ids = _parse_id_set(gateway_cfg.get("blocked_user_ids", []))
        admin_user_ids = _parse_id_set(gateway_cfg.get("admin_user_ids", []))
        group_message_mode = str(gateway_cfg.get("group_message_mode", "all")).strip().lower()
        if group_message_mode not in _SUPPORTED_GROUP_MESSAGE_MODES:
            raise ValueError(f"Invalid group_message_mode: {group_message_mode}")

        group_allow_media_only = _to_bool(gateway_cfg.get("group_allow_media_only"), False)

        command_prefixes = _parse_str_list(gateway_cfg.get("command_prefixes", ["/localagent"]))
        if not command_prefixes:
            command_prefixes = ["/localagent"]
        group_backlog_limit = int(
            gateway_cfg.get("group_backlog_limit", _DEFAULT_GROUP_BACKLOG_LIMIT)
        )

        chat_rules: dict[str, _ChatRule] = {}
        raw_chats = gateway_cfg.get("chat", {})
        if isinstance(raw_chats, dict):
            seen_ids: dict[str, str] = {}  # chat_id -> chat_name
            for name, chat_cfg in raw_chats.items():
                if not isinstance(chat_cfg, dict):
                    chat_cfg = {}
                chat_id = str(chat_cfg.get("id", "")).strip()
                if not chat_id:
                    continue
                if chat_id in seen_ids:
                    raise ValueError(
                        f"Duplicate Telegram chat ID '{chat_id}' found in "
                        f"gateway.telegram.chat.{name} (also defined in gateway.telegram.chat.{seen_ids[chat_id]})"
                    )
                seen_ids[chat_id] = name
                per_chat_mode = chat_cfg.get("group_message_mode")
                if per_chat_mode is not None:
                    per_chat_mode = str(per_chat_mode).strip().lower()
                    if per_chat_mode not in _SUPPORTED_GROUP_MESSAGE_MODES:
                        raise ValueError(
                            f"Invalid group_message_mode for chat {name}: {per_chat_mode}"
                        )
                per_chat_allow_media = chat_cfg.get("group_allow_media_only")
                if per_chat_allow_media is not None:
                    per_chat_allow_media = _to_bool(per_chat_allow_media, False)
                chat_rules[chat_id] = _ChatRule(
                    silent=_to_bool(chat_cfg.get("silent"), False),
                    blocked_user_ids=_parse_id_set(chat_cfg.get("blocked_user_ids", [])),
                    blocked_user_ids_mode=str(
                        chat_cfg.get("blocked_user_ids_mode", "append")
                    ).strip().lower(),
                    group_message_mode=per_chat_mode,
                    group_allow_media_only=per_chat_allow_media,
                )

        return cls(
            hub_url=hub_url,
            bot_token=bot_token,
            debug=debug,
            poll_timeout=poll_timeout,
            drop_pending_updates=drop_pending_updates,
            blocked_user_ids=blocked_user_ids,
            chat_rules=chat_rules,
            admin_user_ids=admin_user_ids,
            group_message_mode=group_message_mode,
            group_allow_media_only=group_allow_media_only,
            command_prefixes=command_prefixes,
            group_backlog_limit=group_backlog_limit,
        )

    def start(self, on_message: MessageHandler) -> None:
        self._on_message = on_message
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._poll_loop,
            daemon=True,
            name="gateway-telegram-poll",
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=5)
            self._thread = None
        self._on_message = None

    def send_action(self, conversation_id: str, action: str) -> None:
        try:
            self._api_call("sendChatAction", {"chat_id": conversation_id, "action": action})
        except Exception:
            logger.debug("[gateway-telegram] sendChatAction failed chat_id=%s", conversation_id)

    def send_message(
        self,
        conversation_id: str,
        text: str,
        *,
        user_id: str = "",
        metadata: dict[str, Any] | None = None,
        artifact_refs: list[str] | None = None,
    ) -> dict[str, Any]:
        del user_id
        results: list[dict[str, Any]] = []

        if artifact_refs:
            for ref in artifact_refs[:3]:
                try:
                    file_result = self._send_file(conversation_id, ref)
                    results.append(file_result)
                except Exception:
                    logger.exception(
                        "[gateway-telegram] failed to send file chat_id=%s artifact=%s",
                        conversation_id,
                        ref,
                    )
                    results.append({"artifact_ref": ref, "delivered": False})

        if text.strip():
            rendered = self.render_message_text(text)
            payload: dict[str, Any] = {
                "chat_id": conversation_id,
                "text": rendered.text,
            }
            if rendered.parse_mode:
                payload["parse_mode"] = rendered.parse_mode
            if metadata and metadata.get("reply_to_message_id"):
                payload["reply_to_message_id"] = metadata["reply_to_message_id"]
            result = self._api_call("sendMessage", payload)
            results.append({
                "type": "text",
                "delivered": bool(result.get("message_id")),
                "message_id": result.get("message_id"),
            })

        return {
            "gateway": self.name,
            "conversation_id": conversation_id,
            "delivered": all(r.get("delivered", False) for r in results),
            "results": results,
        }

    def filter_inbound(self, inbound: InboundMessage) -> InboundMessage | None:
        chat_id = inbound.conversation_id.strip()
        user_id = inbound.user_id.strip()
        chat_type = str(inbound.metadata.get("chat_type", "")).strip().lower()

        if not self._debug:
            if chat_id not in self._chat_rules:
                logger.warning(
                    "[gateway-telegram] unauthorized chat dropped chat_id=%s user_id=%s",
                    chat_id,
                    user_id or "-",
                )
                self._reply_auth_failed(chat_id=chat_id, reason="chat_not_allowed")
                return None

            rule = self._chat_rules[chat_id]

            if user_id:
                if rule.blocked_user_ids_mode == "override":
                    effective_blocked = rule.blocked_user_ids
                else:
                    effective_blocked = self._blocked_user_ids | rule.blocked_user_ids
                if user_id in effective_blocked:
                    logger.warning(
                        "[gateway-telegram] blocked user dropped chat_id=%s user_id=%s",
                        chat_id,
                        user_id,
                    )
                    return None

        rule = self._chat_rules.get(chat_id)

        if rule and rule.silent:
            # 管理员消息不作为静默消息，普通用户保持静默
            is_admin = inbound.metadata.get("is_admin", False)
            if not is_admin:
                inbound.silent = True

        if chat_type in _GROUP_CHAT_TYPES:
            replayed_backlog = bool(inbound.metadata.pop(_META_REPLAYED_GROUP_BACKLOG, False))
            mode = (
                rule.group_message_mode
                if rule and rule.group_message_mode
                else self._group_message_mode
            )
            if mode != "all" and not replayed_backlog:
                text = inbound.text.strip()
                has_media = bool(inbound.attachments)
                allow_media = (
                    rule.group_allow_media_only
                    if rule and rule.group_allow_media_only is not None
                    else self._group_allow_media_only
                )
                if has_media and allow_media:
                    pass  # 允许纯媒体消息通过
                elif not self._match_group_rule(text, mode):
                    logger.info(
                        "[gateway-telegram] group message dropped by mode=%s chat_id=%s user_id=%s",
                        mode,
                        chat_id,
                        user_id or "-",
                    )
                    return None

        return inbound

    def _reply_auth_failed(self, *, chat_id: str, reason: str) -> None:
        if not chat_id:
            return
        text = "鉴权失败：当前会话或用户未授权使用该机器人。"
        try:
            self._api_call(
                "sendMessage",
                {
                    "chat_id": chat_id,
                    "text": text,
                },
            )
        except Exception:
            logger.warning(
                "[gateway-telegram] failed to send auth error reply chat_id=%s reason=%s",
                chat_id,
                reason,
                exc_info=True,
            )

    def _reply_start(self, *, chat_id: str) -> None:
        if not chat_id:
            return
        try:
            self._api_call(
                "sendMessage",
                {
                    "chat_id": chat_id,
                    "text": _DEFAULT_START_REPLY,
                },
            )
        except Exception:
            logger.warning(
                "[gateway-telegram] failed to send /start reply chat_id=%s",
                chat_id,
                exc_info=True,
            )

    def _poll_loop(self) -> None:
        try:
            self._init_bot_profile()
            if self._drop_pending_updates:
                self._skip_pending_updates()

            while not self._stop.is_set():
                updates = self._fetch_updates()
                for upd in updates:
                    self._offset = int(upd.get("update_id", 0)) + 1
                    self._handle_update(upd)
        except Exception:
            logger.exception("[gateway-telegram] poll loop crashed")

    def _init_bot_profile(self) -> None:
        result = self._api_call("getMe", {})
        username = str(result.get("username", "")).strip().lower()
        self._bot_username = username
        logger.info(
            "[gateway-telegram] bot profile loaded username=%s",
            f"@{username}" if username else "(none)",
        )

    def _skip_pending_updates(self) -> None:
        updates = self._api_call("getUpdates", {"timeout": 0, "limit": 100})
        max_update_id = 0
        for upd in updates:
            update_id = int(upd.get("update_id", 0))
            if update_id > max_update_id:
                max_update_id = update_id
        if max_update_id > 0:
            self._offset = max_update_id + 1
            logger.info(
                "[gateway-telegram] skipped pending updates offset=%s",
                self._offset,
            )

    def _fetch_updates(self) -> list[dict[str, Any]]:
        payload: dict[str, Any] = {
            "timeout": self._poll_timeout,
            "allowed_updates": ["message"],
        }
        if self._offset is not None:
            payload["offset"] = self._offset
        try:
            result = self._api_call("getUpdates", payload)
        except Exception:
            self._consecutive_errors += 1
            backoff = min(2 ** self._consecutive_errors, 60)
            logger.warning(
                "[gateway-telegram] getUpdates failed (attempt %d); retrying in %ds",
                self._consecutive_errors,
                backoff,
            )
            self._stop.wait(backoff)
            return []
        self._consecutive_errors = 0
        if not isinstance(result, list):
            return []
        return result

    def _extract_message_text(self, msg: dict[str, Any]) -> str:
        raw_text = msg.get("text")
        if isinstance(raw_text, str) and raw_text.strip():
            return raw_text

        caption = msg.get("caption")
        if isinstance(caption, str):
            return caption
        return ""

    def _extract_attachment_infos(self, msg: dict[str, Any]) -> list[dict[str, Any]]:
        infos: list[dict[str, Any]] = []

        photo_list = msg.get("photo")
        if isinstance(photo_list, list) and photo_list:
            largest = max(photo_list, key=lambda p: p.get("file_size", 0))
            unique_id = str(largest.get("file_unique_id", "")).strip() or "unknown"
            file_size = _safe_int(largest.get("file_size", 0))
            infos.append({
                "kind": "photo",
                "file_id": str(largest.get("file_id", "")).strip(),
                "file_name": f"photo_{unique_id}.jpg",
                "mime_type": "image/jpeg",
                "file_size": file_size,
            })

        document = msg.get("document")
        if isinstance(document, dict):
            infos.append({
                "kind": "document",
                "file_id": str(document.get("file_id", "")).strip(),
                "file_name": str(document.get("file_name", "document")).strip() or "document",
                "mime_type": (
                    str(document.get("mime_type", "application/octet-stream")).strip()
                    or "application/octet-stream"
                ),
                "file_size": _safe_int(document.get("file_size", 0)),
            })

        return infos

    def _download_attachment_infos(
        self,
        attachment_infos: list[dict[str, Any]],
    ) -> list[Attachment]:
        attachments: list[Attachment] = []
        for info in attachment_infos:
            file_size = _safe_int(info.get("file_size", 0))
            if file_size > _TELEGRAM_FILE_SIZE_LIMIT:
                logger.warning(
                    "[gateway-telegram] %s too large (%d bytes), skipping",
                    str(info.get("kind", "attachment")),
                    file_size,
                )
                continue
            att = self._download_telegram_file(
                file_id=str(info.get("file_id", "")).strip(),
                file_name=str(info.get("file_name", "attachment")).strip() or "attachment",
                mime_type=(
                    str(info.get("mime_type", "application/octet-stream")).strip()
                    or "application/octet-stream"
                ),
                file_size=file_size,
            )
            if att:
                attachments.append(att)
        return attachments

    def _serialize_attachment_infos(
        self,
        attachment_infos: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        serialized: list[dict[str, Any]] = []
        for info in attachment_infos:
            file_size = _safe_int(info.get("file_size", 0))
            serialized.append({
                "kind": str(info.get("kind", "")).strip(),
                "file_name": str(info.get("file_name", "")).strip(),
                "mime_type": str(info.get("mime_type", "")).strip(),
                "file_size": file_size,
                "downloadable": bool(
                    file_size > 0 and file_size <= _TELEGRAM_FILE_SIZE_LIMIT
                ),
            })
        return serialized

    def _build_content_summary(
        self,
        *,
        text: str,
        attachment_infos: list[dict[str, Any]],
        max_len: int = 160,
    ) -> str:
        summary_parts: list[str] = []
        compact = " ".join(text.split())
        if compact:
            if len(compact) > max_len:
                compact = compact[: max_len - 3].rstrip() + "..."
            summary_parts.append(compact)
        if attachment_infos:
            attachment_names = ", ".join(
                str(info.get("file_name", "")).strip() or str(info.get("kind", "attachment"))
                for info in attachment_infos[:3]
            )
            if len(attachment_infos) > 3:
                attachment_names += f" (+{len(attachment_infos) - 3} more)"
            summary_parts.append(f"attachments: {attachment_names}")
        return " | ".join(summary_parts)

    def _extract_user_summary(self, user: Any) -> dict[str, Any]:
        if not isinstance(user, dict):
            return {}
        username = str(user.get("username", "")).strip()
        first_name = str(user.get("first_name", "")).strip()
        last_name = str(user.get("last_name", "")).strip()
        full_name = " ".join(part for part in (first_name, last_name) if part).strip()
        display_name = username or full_name or first_name
        return {
            "id": str(user.get("id", "")).strip(),
            "username": username,
            "display_name": display_name,
            "is_bot": bool(user.get("is_bot", False)),
        }

    def _extract_chat_summary(self, chat: Any) -> dict[str, Any]:
        if not isinstance(chat, dict):
            return {}
        return {
            "id": str(chat.get("id", "")).strip(),
            "type": str(chat.get("type", "")).strip(),
            "title": str(chat.get("title", "")).strip(),
            "username": str(chat.get("username", "")).strip(),
        }

    def _extract_reply_context(self, msg: dict[str, Any]) -> dict[str, Any] | None:
        reply_msg = msg.get("reply_to_message")
        if not isinstance(reply_msg, dict):
            return None

        reply_text = self._extract_message_text(reply_msg)
        reply_attachments = self._extract_attachment_infos(reply_msg)
        return {
            "message_id": reply_msg.get("message_id"),
            "text": reply_text,
            "summary": self._build_content_summary(
                text=reply_text,
                attachment_infos=reply_attachments,
            ),
            "has_text": bool(reply_text.strip()),
            "attachments": self._serialize_attachment_infos(reply_attachments),
            "sender": self._extract_user_summary(reply_msg.get("from")),
            "chat": self._extract_chat_summary(reply_msg.get("chat")),
        }

    def _extract_forward_context(
        self,
        msg: dict[str, Any],
        *,
        text: str,
        attachment_infos: list[dict[str, Any]],
    ) -> dict[str, Any] | None:
        forward_origin = msg.get("forward_origin")
        context: dict[str, Any] = {}

        if isinstance(forward_origin, dict):
            context["source_type"] = str(forward_origin.get("type", "")).strip()
            context["date"] = forward_origin.get("date")
            sender_user = self._extract_user_summary(forward_origin.get("sender_user"))
            if sender_user:
                context["source_user"] = sender_user
            sender_chat = self._extract_chat_summary(forward_origin.get("chat"))
            if sender_chat:
                context["source_chat"] = sender_chat
            sender_user_name = str(forward_origin.get("sender_user_name", "")).strip()
            if sender_user_name:
                context["source_name"] = sender_user_name
            author_signature = str(forward_origin.get("author_signature", "")).strip()
            if author_signature:
                context["author_signature"] = author_signature
            message_origin_id = forward_origin.get("message_id")
            if message_origin_id is not None:
                context["origin_message_id"] = message_origin_id
        else:
            forward_from = self._extract_user_summary(msg.get("forward_from"))
            if forward_from:
                context["source_type"] = "user"
                context["source_user"] = forward_from
            forward_from_chat = self._extract_chat_summary(msg.get("forward_from_chat"))
            if forward_from_chat:
                context["source_type"] = "chat"
                context["source_chat"] = forward_from_chat
            forward_sender_name = str(msg.get("forward_sender_name", "")).strip()
            if forward_sender_name:
                context["source_name"] = forward_sender_name
            forward_signature = str(msg.get("forward_signature", "")).strip()
            if forward_signature:
                context["author_signature"] = forward_signature
            if msg.get("forward_date") is not None:
                context["date"] = msg.get("forward_date")

        if not context:
            return None

        source_label = ""
        source_user = context.get("source_user")
        if isinstance(source_user, dict):
            source_label = str(source_user.get("display_name", "")).strip()
        if not source_label:
            source_chat = context.get("source_chat")
            if isinstance(source_chat, dict):
                source_label = (
                    str(source_chat.get("title", "")).strip()
                    or str(source_chat.get("username", "")).strip()
                )
        if not source_label:
            source_label = str(context.get("source_name", "")).strip()
        if source_label:
            context["source_label"] = source_label

        context["content"] = {
            "text": text,
            "has_text": bool(text.strip()),
            "attachments": self._serialize_attachment_infos(attachment_infos),
            "summary": self._build_content_summary(
                text=text,
                attachment_infos=attachment_infos,
            ),
        }
        return context

    def _handle_update(self, update: dict[str, Any]) -> None:
        msg = update.get("message")
        if not isinstance(msg, dict):
            return

        chat = msg.get("chat", {})
        if not isinstance(chat, dict):
            return
        chat_id = str(chat.get("id", "")).strip()
        if not chat_id:
            return

        text = self._extract_message_text(msg)
        attachment_infos = self._extract_attachment_infos(msg)
        attachments = self._download_attachment_infos(attachment_infos)

        # 无文本且无任何媒体信息时跳过
        if not text.strip() and not attachment_infos:
            return

        if self._is_start_command(text):
            self._reply_start(chat_id=chat_id)
            return

        sender = msg.get("from", {})
        if not isinstance(sender, dict):
            sender = {}
        user_id = str(sender.get("id", "")).strip()
        user_name = str(sender.get("username", "")).strip()
        if not user_name:
            user_name = str(sender.get("first_name", "")).strip()
        is_admin = user_id in self._admin_user_ids if user_id else False
        reply_context = self._extract_reply_context(msg)
        forward_context = self._extract_forward_context(
            msg,
            text=text,
            attachment_infos=attachment_infos,
        )

        metadata: dict[str, Any] = {
            "chat_type": str(chat.get("type", "")),
            "chat_title": str(chat.get("title", "")),
            "chat_username": str(chat.get("username", "")),
            "message_id": msg.get("message_id"),
            "update_id": update.get("update_id"),
            "bot_username": self._bot_username,
            "is_admin": is_admin,
            "content_summary": self._build_content_summary(
                text=text,
                attachment_infos=attachment_infos,
            ),
            "attachment_summaries": self._serialize_attachment_infos(attachment_infos),
        }
        if reply_context:
            metadata["reply_to_message"] = reply_context
        if forward_context:
            metadata["forwarded_message"] = forward_context

        inbound = InboundMessage(
            gateway=self.name,
            conversation_id=chat_id,
            text=text,
            user_id=user_id,
            user_name=user_name,
            metadata=metadata,
            attachments=attachments,
        )
        if not self._on_message:
            return

        outbound = self._expand_group_backlog(inbound)
        for item in outbound:
            self._on_message(item)

    def _expand_group_backlog(self, inbound: InboundMessage) -> list[InboundMessage]:
        chat_type = str(inbound.metadata.get("chat_type", "")).strip().lower()
        if chat_type not in _GROUP_CHAT_TYPES:
            return [inbound]

        chat_id = inbound.conversation_id.strip()
        rule = self._chat_rules.get(chat_id)
        mode = (
            rule.group_message_mode
            if rule and rule.group_message_mode
            else self._group_message_mode
        )
        if mode == "all":
            return [inbound]

        text = inbound.text.strip()
        has_media = bool(inbound.attachments)
        allow_media = (
            rule.group_allow_media_only
            if rule and rule.group_allow_media_only is not None
            else self._group_allow_media_only
        )
        matched = (has_media and allow_media) or self._match_group_rule(text, mode)
        if not matched:
            backlog = self._group_backlog.setdefault(chat_id, deque())
            if len(backlog) >= self._group_backlog_limit:
                backlog.popleft()
            backlog.append(inbound)
            logger.info(
                "[gateway-telegram] group message queued by mode=%s chat_id=%s queued=%d",
                mode,
                chat_id,
                len(backlog),
            )
            return []

        backlog = self._group_backlog.get(chat_id)
        if not backlog:
            return [inbound]

        replay = list(backlog)
        backlog.clear()
        for item in replay:
            item.metadata[_META_REPLAYED_GROUP_BACKLOG] = True
        inbound.metadata[_META_REPLAYED_GROUP_BACKLOG] = True
        logger.info(
            "[gateway-telegram] group backlog flushed chat_id=%s replayed=%d trigger_message_id=%s",
            chat_id,
            len(replay),
            str(inbound.metadata.get("message_id", "")),
        )
        return [*replay, inbound]

    def _match_group_rule(self, text: str, mode: str) -> bool:
        has_mention = self._has_bot_mention(text)
        has_command = self._has_command_prefix(text)
        if mode == "mention":
            return has_mention
        if mode == "command":
            return has_command
        return has_mention or has_command

    def _has_bot_mention(self, text: str) -> bool:
        if not self._bot_username:
            return False
        return f"@{self._bot_username}" in text.lower()

    def _has_command_prefix(self, text: str) -> bool:
        body = text.lstrip()
        lowered = body.lower()
        for prefix in self._command_prefixes:
            needle = prefix.strip().lower()
            if not needle:
                continue
            if lowered.startswith(needle):
                return True
        return False

    def _is_start_command(self, text: str) -> bool:
        head = text.strip().split(maxsplit=1)[0].lower()
        if head == "/start":
            return True
        if not head.startswith("/start@"):
            return False
        if not self._bot_username:
            return True
        return head == f"/start@{self._bot_username}"

    def _download_telegram_file(
        self,
        *,
        file_id: str,
        file_name: str,
        mime_type: str,
        file_size: int,
    ) -> Attachment | None:
        if not file_id:
            return None
        try:
            file_info = self._api_call("getFile", {"file_id": file_id})
            file_path = str(file_info.get("file_path", ""))
            if not file_path:
                logger.warning("[gateway-telegram] getFile returned no file_path")
                return None

            download_url = (
                f"https://api.telegram.org/file/bot{self._bot_token}/{file_path}"
            )

            req = request.Request(download_url, method="GET")
            with request.urlopen(req, timeout=60) as resp:
                data = resp.read()

            actual_size = len(data)
            logger.info(
                "[gateway-telegram] file downloaded to artifact store file_name=%s size=%d",
                file_name,
                actual_size,
            )
            artifact_ref = ArtifactStore().put_bytes(
                data,
                file_name=file_name,
                mime_type=mime_type,
            )
            return Attachment(
                file_path=artifact_ref,
                file_name=file_name,
                mime_type=mime_type,
                file_size=actual_size,
            )
        except Exception:
            logger.exception(
                "[gateway-telegram] failed to download file file_id=%s", file_id
            )
            return None

    def _send_file(
        self, conversation_id: str, artifact_ref: str
    ) -> dict[str, Any]:
        meta = ArtifactStore().stat(artifact_ref)
        file_name = str(meta.get("file_name", "")).strip() or "attachment"
        mime_type = str(meta.get("mime_type", "")).strip() or "application/octet-stream"
        ext = Path(file_name).suffix.lower()

        if ext in _PHOTO_EXTENSIONS:
            method = "sendPhoto"
            field_name = "photo"
        else:
            method = "sendDocument"
            field_name = "document"

        result = self._api_call_multipart(
            method,
            fields={"chat_id": conversation_id},
            file_field=field_name,
            artifact_ref=artifact_ref,
            file_name=file_name,
            mime_type=mime_type,
        )
        return {
            "type": "file",
            "artifact_ref": artifact_ref,
            "delivered": bool(result.get("message_id")),
            "message_id": result.get("message_id"),
        }

    def _api_call_multipart(
        self,
        method: str,
        *,
        fields: dict[str, str],
        file_field: str,
        artifact_ref: str,
        file_name: str,
        mime_type: str,
    ) -> Any:
        url = f"https://api.telegram.org/bot{self._bot_token}/{method}"
        boundary = uuid.uuid4().hex

        body_parts: list[bytes] = []
        for key, value in fields.items():
            body_parts.append(
                f"--{boundary}\r\n"
                f'Content-Disposition: form-data; name="{key}"\r\n\r\n'
                f"{value}\r\n".encode("utf-8")
            )

        file_data = ArtifactStore().read_bytes(artifact_ref)
        body_parts.append(
            f"--{boundary}\r\n"
            f'Content-Disposition: form-data; name="{file_field}"; filename="{file_name}"\r\n'
            f"Content-Type: {mime_type}\r\n\r\n".encode("utf-8")
        )
        body_parts.append(file_data)
        body_parts.append(f"\r\n--{boundary}--\r\n".encode("utf-8"))

        body = b"".join(body_parts)
        req = request.Request(url, data=body, method="POST")
        req.add_header("Content-Type", f"multipart/form-data; boundary={boundary}")

        last_exc: Exception | None = None
        for attempt in range(self._retry.max_retries + 1):
            try:
                with request.urlopen(req, timeout=self._poll_timeout + 30) as resp:
                    raw = resp.read().decode("utf-8")
                obj = json.loads(raw)
                if not obj.get("ok"):
                    description = obj.get("description", "telegram api error")
                    raise RuntimeError(f"telegram {method} failed: {description}")
                return obj.get("result")
            except Exception as exc:
                last_exc = exc
                if attempt >= self._retry.max_retries:
                    break
                time.sleep(self._retry.backoff_delay(attempt))
        raise RuntimeError(f"telegram {method} request failed") from last_exc

    def _api_call(self, method: str, payload: dict[str, Any]) -> Any:
        url = f"https://api.telegram.org/bot{self._bot_token}/{method}"
        data = parse.urlencode(payload, doseq=True).encode("utf-8")
        req = request.Request(url, data=data, method="POST")
        req.add_header("Content-Type", "application/x-www-form-urlencoded")
        last_exc: Exception | None = None
        for attempt in range(self._retry.max_retries + 1):
            try:
                with request.urlopen(req, timeout=self._poll_timeout + 10) as resp:
                    raw = resp.read().decode("utf-8")
                obj = json.loads(raw)
                if not obj.get("ok"):
                    description = obj.get("description", "telegram api error")
                    raise RuntimeError(f"telegram {method} failed: {description}")
                return obj.get("result")
            except Exception as exc:
                last_exc = exc
                if attempt >= self._retry.max_retries:
                    break
                time.sleep(self._retry.backoff_delay(attempt))
        raise RuntimeError(f"telegram {method} request failed") from last_exc
