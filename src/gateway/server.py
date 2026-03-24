
import json
import logging
import re
import threading
import time
import uuid
from pathlib import Path
from typing import Any

from websockets.sync.client import ClientConnection, connect

from .base import Attachment, BaseGateway, InboundMessage
from .registry import available_gateway_types, load_gateways
from ..core.artifacts import ArtifactStore, is_artifact_ref
from ..core.identity import infer_person_id, is_multi_party_metadata
from ..core.store import Store, conversation_state_active_topic, conversation_state_record_delivery
from ..index import IndexClient
from ..retry import RetryPolicy
from ..agent.topic_memory import archive_topic_snapshot

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
        self._delivery_queue: list[dict[str, Any]] = []
        self._delivery_lock = threading.Lock()
        self._delivery_event = threading.Event()
        self._delivery_thread: threading.Thread | None = None

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
        self._delivery_thread = threading.Thread(
            target=self._delivery_loop,
            daemon=True,
            name="gateway-delivery-worker",
        )
        self._delivery_thread.start()

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
        self._delivery_event.set()
        if self._delivery_thread:
            self._delivery_thread.join(timeout=5)
            self._delivery_thread = None
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
        task_id = str(payload.get("task_id", "")).strip()
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

        delivery_id = self._enqueue_delivery(
            gateway=gateway_name,
            conversation_id=conversation_id,
            text=text,
            user_id=user_id,
            task_id=task_id,
            metadata=metadata if isinstance(metadata, dict) else None,
            artifact_refs=artifact_refs if isinstance(artifact_refs, list) else None,
        )
        return {"ok": True, "result": {"queued": True, "delivery_id": delivery_id}}

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

    def _enqueue_delivery(
        self,
        *,
        gateway: str,
        conversation_id: str,
        text: str,
        user_id: str = "",
        task_id: str = "",
        metadata: dict[str, Any] | None = None,
        artifact_refs: list[str] | None = None,
    ) -> str:
        delivery_id = f"gd-{uuid.uuid4().hex[:12]}"
        item = {
            "id": delivery_id,
            "gateway": gateway,
            "conversation_id": conversation_id,
            "text": text,
            "user_id": user_id,
            "task_id": task_id,
            "metadata": dict(metadata or {}),
            "artifact_refs": list(artifact_refs or []),
            "attempts": 0,
            "next_attempt_at": time.monotonic(),
            "created_at": time.time(),
        }
        with self._delivery_lock:
            self._delivery_queue.append(item)
        self._delivery_event.set()
        return delivery_id

    def _delivery_loop(self) -> None:
        while not self._stopping.is_set():
            item, wait_seconds = self._claim_due_delivery()
            if item is None:
                self._delivery_event.wait(timeout=wait_seconds)
                self._delivery_event.clear()
                continue
            self._deliver_one(item)

    def _claim_due_delivery(self) -> tuple[dict[str, Any] | None, float]:
        now = time.monotonic()
        with self._delivery_lock:
            if not self._delivery_queue:
                return None, 1.0
            due_index: int | None = None
            due_at = 0.0
            next_due = None
            for index, item in enumerate(self._delivery_queue):
                candidate_due = float(item.get("next_attempt_at", 0.0))
                if candidate_due <= now:
                    due_index = index
                    due_at = candidate_due
                    break
                if next_due is None or candidate_due < next_due:
                    next_due = candidate_due
            if due_index is not None:
                return self._delivery_queue.pop(due_index), max(0.0, now - due_at)
        if next_due is None:
            return None, 1.0
        return None, max(0.1, next_due - now)

    def _deliver_one(self, item: dict[str, Any]) -> None:
        gateway_name = str(item.get("gateway", "")).strip()
        gateway = self._gateways.get(gateway_name)
        if gateway is None:
            self._reschedule_delivery(item, RuntimeError(f"gateway 未启用: {gateway_name}"))
            return
        try:
            result = gateway.send_message(
                str(item.get("conversation_id", "")).strip(),
                str(item.get("text", "")).strip(),
                user_id=str(item.get("user_id", "")).strip(),
                metadata=(item.get("metadata") if isinstance(item.get("metadata"), dict) else None),
                artifact_refs=(
                    item.get("artifact_refs")
                    if isinstance(item.get("artifact_refs"), list)
                    else None
                ),
            )
        except Exception as exc:
            self._reschedule_delivery(item, exc)
            return
        self._finalize_delivery_success(item, result if isinstance(result, dict) else {})

    def _reschedule_delivery(self, item: dict[str, Any], exc: Exception) -> None:
        attempts = int(item.get("attempts", 0)) + 1
        item["attempts"] = attempts
        max_attempts = self._retry.max_retries + 1
        if attempts >= max_attempts:
            self._finalize_delivery_failure(item, str(exc))
            return
        delay = self._retry.backoff_delay(attempts - 1)
        item["next_attempt_at"] = time.monotonic() + delay
        item["last_error"] = str(exc)
        with self._delivery_lock:
            self._delivery_queue.append(item)
        self._delivery_event.set()
        logger.warning(
            "Queued delivery retry scheduled id=%s attempt=%d delay=%.2fs error=%s",
            item.get("id", ""),
            attempts,
            delay,
            exc,
        )

    @staticmethod
    def _extract_sent_message_id(result: dict[str, Any]) -> str:
        results = result.get("results", [])
        if not isinstance(results, list):
            return ""
        for entry in results:
            if (
                isinstance(entry, dict)
                and entry.get("type") == "text"
                and str(entry.get("message_id", "")).strip()
            ):
                return str(entry.get("message_id", "")).strip()
        return ""

    def _finalize_delivery_success(self, item: dict[str, Any], result: dict[str, Any]) -> None:
        gateway = str(item.get("gateway", "")).strip()
        conversation_id = str(item.get("conversation_id", "")).strip()
        user_id = str(item.get("user_id", "")).strip()
        task_id = str(item.get("task_id", "")).strip()
        text = str(item.get("text", "")).strip()
        sent_message_id = self._extract_sent_message_id(result)
        task_data: dict[str, Any] | None = None
        if task_id:
            try:
                with Store() as store:
                    task_data = store.task_read(task_id)
            except Exception:
                logger.warning("Failed to read task for delivery success task=%s", task_id, exc_info=True)
        try:
            if text:
                index_metadata = json.dumps(
                    {
                        "gateway": gateway,
                        "conversation_id": conversation_id,
                        "user_id": user_id,
                        "topic_id": str((task_data or {}).get("topic_id", "")).strip(),
                        "task_id": task_id,
                    },
                    ensure_ascii=False,
                )
                idx = IndexClient(self._hub_url)
                try:
                    idx.insert_reply(text, content=text, metadata=index_metadata)
                finally:
                    idx.close()
        except Exception:
            logger.warning("Failed to save queued reply record task=%s", task_id, exc_info=True)
        try:
            with Store() as store:
                if conversation_id:
                    store.conversation_state_apply(
                        conversation_id,
                        lambda current: conversation_state_record_delivery(
                            current,
                            task_id=task_id,
                            topic_id=str((task_data or {}).get("topic_id", "")).strip(),
                            text=text,
                            message_id=sent_message_id or str(((item.get("metadata") or {}).get("reply_to_message_id", ""))).strip(),
                        ),
                        gateway=gateway,
                        user_id=user_id,
                        person_id=str((task_data or {}).get("person_id", "")).strip(),
                    )
                    store.conversation_event_append(
                        conversation_id,
                        "reply_sent",
                        payload={
                            "task_id": task_id,
                            "message_id": sent_message_id,
                            "text": text[:500],
                        },
                    )
                    latest_state = store.conversation_state_read(conversation_id) or {}
                    target_topic_id = str((task_data or {}).get("topic_id", "")).strip()
                    target_topic = next(
                        (
                            entry
                            for entry in (latest_state.get("topics", []) or [])
                            if str(entry.get("id", "")).strip() == target_topic_id
                        ),
                        None,
                    ) if target_topic_id else conversation_state_active_topic(latest_state)
                    archive_topic_snapshot(
                        self._hub_url,
                        conversation_id=conversation_id,
                        topic=target_topic,
                    )
        except Exception:
            logger.warning("Queued reply state writeback failed task=%s", task_id, exc_info=True)

    def _finalize_delivery_failure(self, item: dict[str, Any], error: str) -> None:
        gateway = str(item.get("gateway", "")).strip()
        conversation_id = str(item.get("conversation_id", "")).strip()
        user_id = str(item.get("user_id", "")).strip()
        task_id = str(item.get("task_id", "")).strip()
        text = str(item.get("text", "")).strip()
        logger.error(
            "Queued delivery failed permanently id=%s task=%s conversation=%s error=%s",
            item.get("id", ""),
            task_id,
            conversation_id,
            error,
        )
        if conversation_id:
            try:
                with Store() as store:
                    store.conversation_event_append(
                        conversation_id,
                        "reply_failed",
                        payload={
                            "task_id": task_id,
                            "error": error[:500],
                            "text": text[:500],
                        },
                    )
            except Exception:
                logger.warning("Failed to write reply_failed event task=%s", task_id, exc_info=True)
            wake_message = "\n".join([
                "[gateway-delivery-failed]",
                f"gateway={gateway}",
                f"conversation_id={conversation_id}",
                f"user_id={user_id}",
                f"task_id={task_id}",
                f"summary={self._summarize_text(error, max_len=80)}",
            ])
            self._emit_hub_event_with_retry(
                topic="agent.wake",
                payload={"message": wake_message},
            )

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
