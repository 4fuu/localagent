

import json
import logging
import re
import threading
import time
from pathlib import Path
from typing import Any

from websockets.sync.client import ClientConnection, connect

from .base import Attachment, BaseGateway, InboundMessage
from .registry import available_gateway_types, load_gateways
from ..core.artifacts import ArtifactStore, is_artifact_ref
from ..core.identity import infer_person_id, is_multi_party_metadata
from ..core.store import Store
from ..retry import RetryPolicy

logger = logging.getLogger(__name__)

_GATEWAY_TOPICS = ["gateway.send", "gateway.list", "gateway.inbound", "gateway.send_action"]
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
_USERS_FILE = _PROJECT_ROOT / ".localagent" / "users.json"


class GatewayService:
    """Gateway server. Runs in main process background thread."""

    def __init__(self, hub_url: str):
        self._hub_url = hub_url
        self._gateways: dict[str, BaseGateway] = {}
        self._thread: threading.Thread | None = None
        self._ready = threading.Event()
        self._stopping = threading.Event()
        self._ws: ClientConnection | None = None
        self._users: dict[str, str] = self._load_users()
        self._retry = RetryPolicy.for_service("gateway_service")
        # Typing indicator: set of (gateway_name, conversation_id)
        self._typing_active: set[tuple[str, str]] = set()
        self._typing_lock = threading.Lock()
        self._typing_thread: threading.Thread | None = None

    def start(self) -> None:
        self._thread = threading.Thread(
            target=self._run,
            daemon=True,
            name="gateway-service",
        )
        self._thread.start()
        if not self._ready.wait(timeout=5):
            raise RuntimeError("GatewayService failed to start within 5 seconds")
        logger.info("GatewayService started")

    def _run(self) -> None:
        self._load_gateways()
        self._typing_thread = threading.Thread(
            target=self._typing_ticker,
            daemon=True,
            name="gateway-typing-ticker",
        )
        self._typing_thread.start()

        try:
            reconnect_attempt = 0
            while not self._stopping.is_set():
                try:
                    self._ws = connect(
                        self._hub_url,
                        open_timeout=self._retry.connect_timeout,
                    )
                    self._ws.send(
                        json.dumps({
                            "type": "register",
                            "name": "gateway",
                            "topics": _GATEWAY_TOPICS,
                        })
                    )
                    if not self._ready.is_set():
                        self._ready.set()
                    reconnect_attempt = 0

                    for raw in self._ws:
                        msg = json.loads(raw)
                        if msg.get("type") != "request":
                            continue

                        cmd = msg["topic"].split(".", 1)[1]
                        try:
                            handler = getattr(self, f"_handle_{cmd}", None)
                            if handler is None:
                                resp = {"ok": False, "error": f"unknown command: {cmd}"}
                            else:
                                resp = handler(msg.get("payload", {}))
                        except Exception as exc:
                            logger.exception("GatewayService error handling %s", cmd)
                            resp = {"ok": False, "error": str(exc)}

                        assert self._ws is not None
                        self._ws.send(
                            json.dumps({
                                "type": "response",
                                "id": msg["id"],
                                "payload": resp,
                            })
                        )
                except Exception as exc:
                    if self._stopping.is_set():
                        break
                    delay = self._retry.backoff_delay(reconnect_attempt)
                    reconnect_attempt += 1
                    logger.warning(
                        "GatewayService hub connection lost, retry in %.2fs: %s",
                        delay,
                        exc,
                    )
                    time.sleep(delay)
                finally:
                    if self._ws:
                        try:
                            self._ws.close()
                        except Exception:
                            pass
                        self._ws = None
        finally:
            self._stop_gateways()

    def _load_gateways(self) -> None:
        gateways = load_gateways(hub_url=self._hub_url)
        self._gateways = {gw.name: gw for gw in gateways}

        for gw in self._gateways.values():
            gw.start(self._on_inbound_message)

        enabled = sorted(self._gateways.keys())
        logger.info(
            "GatewayService loaded gateways=%s available=%s",
            enabled,
            available_gateway_types(),
        )

    def _stop_gateways(self) -> None:
        for gw in self._gateways.values():
            try:
                gw.stop()
            except Exception:
                logger.exception("Failed to stop gateway %s", gw.name)
        self._gateways = {}

    def stop(self) -> None:
        self._stopping.set()
        if self._ws:
            try:
                self._ws.close()
            except Exception:
                pass
        if self._thread:
            self._thread.join(timeout=5)
            self._thread = None
        logger.info("GatewayService stopped")

    def _on_inbound_message(self, inbound: InboundMessage) -> None:
        gateway = self._gateways.get(inbound.gateway)
        if gateway is None:
            logger.warning("Drop inbound from unknown gateway: %s", inbound.gateway)
            return

        if inbound.user_id and inbound.user_name:
            self._update_user(inbound.gateway, inbound.user_id, inbound.user_name)
        elif inbound.user_id and not inbound.user_name:
            key = f"{inbound.gateway}:{inbound.user_id}"
            cached = self._users.get(key)
            if cached:
                inbound.user_name = cached
        inbound = self._enrich_inbound_identity(inbound)

        original_conversation_id = inbound.conversation_id
        filtered = gateway.filter_inbound(inbound)
        if filtered is None:
            logger.info(
                "[gateway-inbound] dropped by gateway filter gateway=%s conversation_id=%s",
                gateway.name,
                original_conversation_id,
            )
            return
        self._ingest_inbound_message(filtered)

    def _ingest_inbound_message(self, inbound: InboundMessage) -> dict[str, Any]:
        if self._stopping.is_set():
            raise RuntimeError("GatewayService is stopping")

        record = self._write_inbox_message(inbound)
        if inbound.silent:
            logger.info(
                "[gateway-inbound] silent message stored gateway=%s conversation_id=%s inbox_id=%s",
                inbound.gateway,
                inbound.conversation_id,
                record["id"],
            )
            return record

        wake_message = self._build_wake_message(inbound, record)
        self._emit_hub_event_with_retry(
            topic="agent.wake",
            payload={"message": wake_message},
        )
        return record

    def _build_wake_message(self, inbound: InboundMessage, record: dict[str, Any]) -> str:
        lines = [
            "[gateway-inbound]",
            f"gateway={inbound.gateway}",
            f"conversation_id={inbound.conversation_id}",
            f"inbox_id={record['id']}",
        ]
        if inbound.user_id:
            lines.append(f"user_id={inbound.user_id}")
        person_id = str(inbound.metadata.get("person_id", "")).strip()
        if person_id:
            lines.append(f"person_id={person_id}")
        if inbound.user_name:
            lines.append(f"user_name={inbound.user_name}")
        if bool(inbound.metadata.get("is_admin", False)):
            lines.append("role=admin")
        lines.append(f"summary={self._summarize_text(inbound.text)}")
        if inbound.attachments:
            att_names = ", ".join(a.file_name for a in inbound.attachments)
            lines.append(f"attachments={att_names}")
        return "\n".join(lines)

    def _emit_hub_event_with_retry(self, *, topic: str, payload: dict[str, Any]) -> None:
        last_exc: Exception | None = None
        for attempt in range(self._retry.max_retries + 1):
            try:
                with connect(
                    self._hub_url,
                    open_timeout=self._retry.connect_timeout,
                ) as ws:
                    ws.send(json.dumps({"type": "event", "topic": topic, "payload": payload}))
                return
            except Exception as exc:
                last_exc = exc
                if attempt >= self._retry.max_retries:
                    break
                time.sleep(self._retry.backoff_delay(attempt))
        logger.error("Failed to emit %s event: %s", topic, last_exc)

    def _write_inbox_message(self, inbound: InboundMessage) -> dict[str, Any]:
        """Write inbox message to DB. Returns the created record."""
        if inbound.attachments:
            inbound = self._store_inbound_attachments(inbound)
        with Store() as store:
            record = store.inbox_create(
                gateway=inbound.gateway,
                conversation_id=inbound.conversation_id,
                message_id=str(inbound.metadata.get("message_id", "")),
                user_id=inbound.user_id,
                person_id=str(inbound.metadata.get("person_id", "")).strip(),
                user_name=inbound.user_name,
                is_admin=bool(inbound.metadata.get("is_admin", False)),
                content=inbound.text.strip(),
                metadata=inbound.metadata,
                attachments=[
                    {
                        "file_path": a.file_path,
                        "file_name": a.file_name,
                        "mime_type": a.mime_type,
                        "file_size": a.file_size,
                        "is_image": a.is_image,
                    }
                    for a in inbound.attachments
                ],
                silent=inbound.silent,
            )
            return record

    def _store_inbound_attachments(self, inbound: InboundMessage) -> InboundMessage:
        artifact_store = ArtifactStore()
        updated: list[Attachment] = []
        for attachment in inbound.attachments:
            file_ref = attachment.file_path
            if is_artifact_ref(file_ref):
                updated.append(Attachment(
                    file_path=file_ref,
                    file_name=attachment.file_name,
                    mime_type=attachment.mime_type,
                    file_size=attachment.file_size,
                ))
                continue

            source = Path(attachment.file_path).resolve()
            if source.is_file():
                file_ref = artifact_store.import_file(
                    source,
                    file_name=attachment.file_name or source.name,
                    mime_type=attachment.mime_type,
                )
                try:
                    source.unlink()
                except OSError:
                    pass
            updated.append(Attachment(
                file_path=file_ref,
                file_name=attachment.file_name,
                mime_type=attachment.mime_type,
                file_size=attachment.file_size,
            ))

        inbound.attachments = updated
        return inbound

    @staticmethod
    def _summarize_text(text: str, max_len: int = 20) -> str:
        compact = " ".join(text.split())
        if not compact:
            return "(empty)"
        if len(compact) <= max_len:
            return compact
        return compact[: max_len - 1] + "…"

    @staticmethod
    def _slug(value: str, *, fallback: str) -> str:
        raw = value.strip().lower()
        if not raw:
            return fallback
        s = re.sub(r"[^a-z0-9._-]+", "-", raw)
        s = s.strip("-._")
        if not s:
            return fallback
        return s[:64]

    @staticmethod
    def _load_users() -> dict[str, str]:
        if _USERS_FILE.exists():
            try:
                return json.loads(_USERS_FILE.read_text(encoding="utf-8"))
            except Exception:
                logger.warning("Failed to load users file, starting fresh")
        return {}

    def _save_users(self) -> None:
        _USERS_FILE.parent.mkdir(parents=True, exist_ok=True)
        _USERS_FILE.write_text(
            json.dumps(self._users, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    def _update_user(self, gateway: str, user_id: str, user_name: str) -> None:
        key = f"{gateway}:{user_id}"
        if self._users.get(key) != user_name:
            self._users[key] = user_name
            self._save_users()
            logger.info("User mapping updated: %s -> %s", key, user_name)

    def _enrich_inbound_identity(self, inbound: InboundMessage) -> InboundMessage:
        metadata = inbound.metadata if isinstance(inbound.metadata, dict) else {}
        metadata["person_id"] = infer_person_id(
            gateway=inbound.gateway,
            user_id=inbound.user_id,
            fallback_person_id=str(metadata.get("person_id", "")).strip(),
        )
        metadata["is_multi_party"] = is_multi_party_metadata(metadata)
        inbound.metadata = metadata
        return inbound

    def _handle_send(self, payload: dict) -> dict:
        gateway_name = str(payload.get("gateway", "")).strip()
        conversation_id = str(payload.get("conversation_id", "")).strip()
        text = str(payload.get("text", "")).strip()
        user_id = str(payload.get("user_id", "")).strip()
        metadata = payload.get("metadata")
        artifact_refs = payload.get("artifact_refs")

        if not gateway_name:
            return {"ok": False, "error": "gateway 不能为空"}
        if not conversation_id:
            return {"ok": False, "error": "conversation_id 不能为空"}
        if not text and not artifact_refs:
            return {"ok": False, "error": "text 和 artifact_refs 不能同时为空"}

        gateway = self._gateways.get(gateway_name)
        if gateway is None:
            return {"ok": False, "error": f"gateway 未启用: {gateway_name}"}

        result = gateway.send_message(
            conversation_id,
            text,
            user_id=user_id,
            metadata=metadata if isinstance(metadata, dict) else None,
            artifact_refs=artifact_refs if isinstance(artifact_refs, list) else None,
        )
        return {"ok": True, "result": result}

    def _handle_send_action(self, payload: dict) -> dict:
        gateway_name = str(payload.get("gateway", "")).strip()
        conversation_id = str(payload.get("conversation_id", "")).strip()
        action = str(payload.get("action", "typing")).strip()

        if not gateway_name or not conversation_id:
            return {"ok": False, "error": "gateway 和 conversation_id 不能为空"}

        if action == "typing_start":
            with self._typing_lock:
                self._typing_active.add((gateway_name, conversation_id))
            return {"ok": True}
        if action == "typing_stop":
            with self._typing_lock:
                self._typing_active.discard((gateway_name, conversation_id))
            return {"ok": True}

        gateway = self._gateways.get(gateway_name)
        if gateway is None:
            return {"ok": False, "error": f"gateway 未启用: {gateway_name}"}

        gateway.send_action(conversation_id, action)
        return {"ok": True}

    def _typing_ticker(self) -> None:
        while not self._stopping.wait(timeout=4):
            with self._typing_lock:
                targets = list(self._typing_active)
            for gateway_name, conversation_id in targets:
                gw = self._gateways.get(gateway_name)
                if gw:
                    gw.send_action(conversation_id, "typing")

    def _handle_list(self, payload: dict) -> dict:
        del payload
        return {
            "ok": True,
            "enabled": sorted(self._gateways.keys()),
            "available": available_gateway_types(),
        }

    def _handle_inbound(self, payload: dict) -> dict:
        gateway_name = str(payload.get("gateway", "")).strip()
        conversation_id = str(payload.get("conversation_id", "")).strip()
        text = str(payload.get("text", "")).strip()
        user_id = str(payload.get("user_id", "")).strip()
        user_name = str(payload.get("user_name", "")).strip()
        metadata = payload.get("metadata")
        silent = bool(payload.get("silent", False))

        if not gateway_name:
            return {"ok": False, "error": "gateway 不能为空"}
        if not conversation_id:
            return {"ok": False, "error": "conversation_id 不能为空"}
        if not text:
            return {"ok": False, "error": "text 不能为空"}

        gateway = self._gateways.get(gateway_name)
        if gateway is None:
            return {"ok": False, "error": f"gateway 未启用: {gateway_name}"}

        inbound = InboundMessage(
            gateway=gateway_name,
            conversation_id=conversation_id,
            text=text,
            user_id=user_id,
            user_name=user_name,
            metadata=metadata if isinstance(metadata, dict) else {},
            silent=silent,
        )
        inbound = self._enrich_inbound_identity(inbound)
        filtered = gateway.filter_inbound(inbound)
        if filtered is None:
            return {"ok": True, "dropped": True, "silent": True}

        result = self._ingest_inbound_message(filtered)
        return {"ok": True, "inbox_id": result["id"], "silent": filtered.silent}
