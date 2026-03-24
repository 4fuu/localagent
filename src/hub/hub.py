"""WebSocket broadcast hub.

Runs an async WebSocket server in a background daemon thread.
Components (vec server, subagents, etc.) connect as clients,
register topics they handle, and exchange request/response messages.

Protocol messages (JSON):
    register : {"type":"register", "name":"<component>", "topics":["a","b"]}
    request  : {"type":"request",  "id":"<uuid>", "topic":"<topic>", "payload":{…}}
    response : {"type":"response", "id":"<uuid>", "payload":{…}}
    event    : {"type":"event",    "topic":"<topic>", "payload":{…}}
"""

import asyncio
import json
import logging
import select
import subprocess
import sys
import threading
import re
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from time import monotonic as monotonic_time, time as wall_time
from typing import Any

from websockets.asyncio.server import Server, ServerConnection, serve

from ..agent.context_refs import read_task_context_refs, write_task_context_refs, write_tool_ref

logger = logging.getLogger(__name__)

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 9600
DEFAULT_NOTIFY_DELAY = 0  # seconds; 0 = immediate
DEFAULT_MAIN_INBOX_BATCH_SIZE = 10
DEFAULT_MAIN_PER_CONVERSATION_LIMIT = 5
DEFAULT_MAX_MAIN_AGENTS = 2
DEFAULT_MAX_TASK_AGENTS = 16
DEFAULT_PRIORITIZE_ADMIN = True
DEFAULT_REAP_INTERVAL = 5  # seconds
DEFAULT_STARTUP_TIMEOUT = 5  # seconds
DEFAULT_SHUTDOWN_TIMEOUT = 5  # seconds
DEFAULT_AGENT_TERMINATE_TIMEOUT = 10  # seconds
DEFAULT_PENDING_REQUEST_TIMEOUT = 30  # seconds
DEFAULT_DB_SCRUB_INTERVAL = 0  # seconds; 0 = disabled
DEFAULT_TRACE_CLEANUP_INTERVAL = 0  # seconds; 0 = disabled
DEFAULT_TRACE_RETENTION_DAYS = 7
DEFAULT_POOL_SIZE_MAIN = 0  # 0 = disabled
DEFAULT_POOL_SIZE_TASK = 0  # 0 = disabled
_POOL_WORKER_READY_TIMEOUT = 30  # seconds to wait for worker ready signal
STARTUP_PAYLOAD_TOPIC = "hub.agent_startup_payload"


class Hub:
    def __init__(
        self,
        host: str = DEFAULT_HOST,
        port: int = DEFAULT_PORT,
        *,
        notify_delay: int = DEFAULT_NOTIFY_DELAY,
        main_inbox_batch_size: int = DEFAULT_MAIN_INBOX_BATCH_SIZE,
        main_per_conversation_limit: int = DEFAULT_MAIN_PER_CONVERSATION_LIMIT,
        max_main_agents: int = DEFAULT_MAX_MAIN_AGENTS,
        max_task_agents: int = DEFAULT_MAX_TASK_AGENTS,
        prioritize_admin: bool = DEFAULT_PRIORITIZE_ADMIN,
        reap_interval: int = DEFAULT_REAP_INTERVAL,
        startup_timeout: int = DEFAULT_STARTUP_TIMEOUT,
        shutdown_timeout: int = DEFAULT_SHUTDOWN_TIMEOUT,
        agent_terminate_timeout: int = DEFAULT_AGENT_TERMINATE_TIMEOUT,
        pending_request_timeout: int = DEFAULT_PENDING_REQUEST_TIMEOUT,
        db_scrub_interval: int = DEFAULT_DB_SCRUB_INTERVAL,
        trace_cleanup_interval: int = DEFAULT_TRACE_CLEANUP_INTERVAL,
        trace_retention_days: int = DEFAULT_TRACE_RETENTION_DAYS,
        pool_size_main: int = DEFAULT_POOL_SIZE_MAIN,
        pool_size_task: int = DEFAULT_POOL_SIZE_TASK,
    ):
        if notify_delay < 0:
            raise ValueError("notify_delay must be >= 0")
        if reap_interval <= 0:
            raise ValueError("reap_interval must be > 0")
        if startup_timeout <= 0:
            raise ValueError("startup_timeout must be > 0")
        if shutdown_timeout <= 0:
            raise ValueError("shutdown_timeout must be > 0")
        if agent_terminate_timeout <= 0:
            raise ValueError("agent_terminate_timeout must be > 0")
        if pending_request_timeout <= 0:
            raise ValueError("pending_request_timeout must be > 0")
        if db_scrub_interval < 0:
            raise ValueError("db_scrub_interval must be >= 0")
        if trace_cleanup_interval < 0:
            raise ValueError("trace_cleanup_interval must be >= 0")
        if trace_retention_days < 0:
            raise ValueError("trace_retention_days must be >= 0")
        if main_inbox_batch_size <= 0:
            raise ValueError("main_inbox_batch_size must be > 0")
        if main_per_conversation_limit < 0:
            raise ValueError("main_per_conversation_limit must be >= 0")
        if max_main_agents <= 0:
            raise ValueError("max_main_agents must be > 0")
        if max_task_agents <= 0:
            raise ValueError("max_task_agents must be > 0")

        self.host = host
        self.port = port
        self.components: dict[str, ServerConnection] = {}
        self.topic_handlers: dict[str, str] = {}
        self.pending: dict[str, tuple[ServerConnection, float]] = {}
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._ready = threading.Event()
        self._server: Server | None = None
        # Agent process management
        self._processes: dict[str, subprocess.Popen] = {}
        self._startup_payloads: dict[str, str] = {}
        self._pending_task_spawns: list[tuple[str, str]] = []
        self._main_conversations: dict[str, str] = {}
        self._notify_delay = notify_delay
        self._notify_deadlines: dict[str, float] = {}
        self._notify_timer_handles: dict[str, asyncio.TimerHandle] = {}
        # Typing indicator: running agent_key -> (gateway, conversation_id)
        self._typing_targets: dict[str, tuple[str, str]] = {}
        self._poll_task: asyncio.Task | None = None
        self._reap_interval = reap_interval
        self._startup_timeout = startup_timeout
        self._shutdown_timeout = shutdown_timeout
        self._agent_terminate_timeout = agent_terminate_timeout
        self._pending_request_timeout = pending_request_timeout
        self._db_scrub_interval = db_scrub_interval
        self._next_db_scrub_deadline = (
            monotonic_time() if db_scrub_interval > 0 else float("inf")
        )
        self._trace_cleanup_interval = trace_cleanup_interval
        self._trace_retention_days = trace_retention_days
        self._next_trace_cleanup_deadline = (
            monotonic_time() if trace_cleanup_interval > 0 else float("inf")
        )
        self._main_claim_batch_size = main_inbox_batch_size
        self._main_claim_lease_seconds = max(900, startup_timeout * 10, reap_interval * 10)
        self._max_main_agents = max_main_agents
        self._max_task_agents = max_task_agents
        self._prioritize_admin = prioritize_admin
        self._stopping = False
        # Agent process pool (pre-warmed workers)
        self._pool_size_main = max(0, pool_size_main)
        self._pool_size_task = max(0, pool_size_task)
        self._pool_main: list[subprocess.Popen] = []
        self._pool_task: list[subprocess.Popen] = []
        # Cron job management
        self._cron_path = Path(".localagent/cron.json")
        self._cron_jobs: list[dict] = []
        self._load_cron_jobs()

    @property
    def url(self) -> str:
        return f"ws://{self.host}:{self.port}"

    # ------------------------------------------------------------------
    # WebSocket handler
    # ------------------------------------------------------------------

    async def _handler(self, ws: ServerConnection) -> None:
        name: str | None = None
        try:
            async for raw in ws:
                msg = json.loads(raw)
                msg_type = msg.get("type")

                if msg_type == "register":
                    name = msg["name"]
                    assert name, "WebSocket handler: name is None"
                    previous = self.components.get(name)
                    self.components[name] = ws
                    for topic in msg.get("topics", []):
                        self.topic_handlers[topic] = name
                    if previous is not None and previous is not ws:
                        logger.warning(
                            "Component re-registered: %s (replacing previous connection)",
                            name,
                        )
                    logger.info("Component registered: %s", name)

                elif msg_type == "request":
                    await self._route_request(ws, msg)

                elif msg_type == "response":
                    pending_item = self.pending.pop(msg.get("id", ""), None)
                    if pending_item:
                        requester, _ = pending_item
                        await requester.send(
                            raw if isinstance(raw, str) else raw.decode()  # type: ignore
                        )

                elif msg_type == "event":
                    self._on_event(msg.get("topic", ""), msg.get("payload", {}))

        except Exception as exc:
            logger.debug("Connection closed (%s): %s", name or "unknown", exc)
        finally:
            if name:
                # 仅当断开的连接仍是当前活跃连接时，才清理映射。
                # 否则说明同名组件已经重连，不能误删新的 handler。
                if self.components.get(name) is ws:
                    self.components.pop(name, None)
                    self.topic_handlers = {
                        t: n for t, n in self.topic_handlers.items() if n != name
                    }
                    logger.info("Component disconnected: %s", name)
                else:
                    logger.info(
                        "Stale connection closed: %s (active connection preserved)",
                        name,
                    )

    async def _route_request(self, requester: ServerConnection, msg: dict) -> None:
        topic = msg["topic"]
        if topic == STARTUP_PAYLOAD_TOPIC:
            payload = msg.get("payload", {})
            agent_key = str(payload.get("agent_key", "")).strip()
            role = str(payload.get("role", "")).strip()
            task_id = str(payload.get("task_id", "")).strip()
            key = agent_key or ("main" if role == "main" else f"task-{task_id}")
            startup_payload = self._startup_payloads.pop(key, "")
            await requester.send(
                json.dumps({
                    "type": "response",
                    "id": msg["id"],
                    "payload": {"ok": True, "payload": startup_payload},
                })
            )
            return

        handler_name = self.topic_handlers.get(topic)
        if handler_name and handler_name in self.components:
            self.pending[msg["id"]] = (requester, monotonic_time())
            handler_ws = self.components[handler_name]
            await handler_ws.send(json.dumps(msg))
        else:
            await requester.send(
                json.dumps({
                    "type": "response",
                    "id": msg["id"],
                    "payload": {"ok": False, "error": f"no handler for topic: {topic}"},
                })
            )

    # ------------------------------------------------------------------
    # Agent event handling
    # ------------------------------------------------------------------

    def _on_event(self, topic: str, payload: dict) -> None:
        if topic == "agent.wake":
            self._handle_wake(str(payload.get("message", "")))
        elif topic == "agent.task_done":
            self._handle_task_done(payload)
        elif topic == "agent.spawn":
            self._handle_spawn(payload)
        elif topic == "agent.stop":
            self._handle_stop(payload)
        elif topic == "agent.main_done":
            self._handle_main_done()
        elif topic == "cron.set":
            self._handle_cron_set(payload)
        elif topic == "cron.cancel":
            self._handle_cron_cancel(payload)
        else:
            logger.debug("Event on topic %s", topic)

    def _running_agent_count(self, role: str) -> int:
        if role == "main":
            prefix = "main-"
        elif role == "task":
            prefix = "task-"
        else:
            return 0
        count = 0
        for key, proc in self._processes.items():
            if not key.startswith(prefix):
                continue
            if proc.poll() is None:
                count += 1
        return count

    def _has_main_capacity(self) -> bool:
        return self._running_agent_count("main") < self._max_main_agents

    def _has_task_capacity(self) -> bool:
        return self._running_agent_count("task") < self._max_task_agents

    @staticmethod
    def _extract_startup_conversation_id(payload: str) -> str:
        if not payload:
            return ""
        in_startup = False
        for raw_line in payload.splitlines():
            line = raw_line.strip()
            if not line:
                if in_startup:
                    break
                continue
            if line == "[startup]":
                in_startup = True
                continue
            if in_startup and line.startswith("conversation_id="):
                return line.split("=", 1)[1].strip()
            if in_startup and line.startswith("[") and line.endswith("]"):
                break
        return ""

    def _has_running_main_for_conversation(self, conversation_id: str) -> bool:
        normalized = conversation_id.strip()
        if not normalized:
            return False
        for key, proc in self._processes.items():
            if not key.startswith("main-"):
                continue
            if self._main_conversations.get(key) != normalized:
                continue
            if proc.poll() is None:
                return True
        return False

    @staticmethod
    def _extract_inline_field(message: str, field: str) -> str:
        pattern = rf"(?m)^{re.escape(field)}=(.+)$"
        match = re.search(pattern, message.strip())
        if not match:
            return ""
        return match.group(1).strip()

    def _infer_conversation_from_wake_message(self, message: str) -> tuple[str, str, str]:
        conversation_id = self._extract_inline_field(message, "conversation_id")
        gateway = self._extract_inline_field(message, "gateway")
        user_id = self._extract_inline_field(message, "user_id")
        inbox_ids = sorted(set(re.findall(r"inbox_id=(\S+)", message)))
        if conversation_id and (gateway or user_id or not inbox_ids):
            return conversation_id, gateway, user_id
        if not inbox_ids:
            return conversation_id, gateway, user_id

        from ..core.store import Store

        try:
            with Store() as store:
                inbox_records = [
                    store.inbox_read(inbox_id)
                    for inbox_id in inbox_ids
                ]
        except Exception:
            logger.warning("Failed to infer wake conversation from inbox refs", exc_info=True)
            return "", "", ""
        conversation_ids = {
            str((item or {}).get("conversation_id", "")).strip()
            for item in inbox_records
            if item is not None
        }
        conversation_ids.discard("")
        if len(conversation_ids) != 1:
            return "", "", ""
        record = next((item for item in inbox_records if item is not None), None) or {}
        return (
            next(iter(conversation_ids)),
            gateway or str(record.get("gateway", "")).strip(),
            user_id or str(record.get("user_id", "")).strip(),
        )

    def _sync_inbox_backlog(self) -> None:
        from ..core.store import Store

        try:
            with Store() as store:
                conversation_ids = store.inbox_list_unprocessed_conversations(include_silent=False)
                for conversation_id in conversation_ids:
                    store.conversation_work_ensure_backlog(conversation_id)
        except Exception:
            logger.exception("Failed to sync inbox backlog")

    def _schedule_main_agents(self) -> None:
        from ..core.store import Store

        while self._has_main_capacity() and not self._stopping:
            try:
                with Store() as store:
                    deferred_conversation_ids = self._pending_notify_delay_conversations()
                    claim = store.conversation_work_claim(
                        agent_key=f"main-{uuid.uuid4().hex[:12]}",
                        lease_seconds=self._main_claim_lease_seconds,
                        inbox_limit=self._main_claim_batch_size,
                        prioritize_admin=self._prioritize_admin,
                        exclude_conversation_ids=deferred_conversation_ids,
                    )
            except Exception:
                logger.exception("Failed to claim conversation work")
                return
            if not claim:
                return
            conversation_id = str(claim.get("conversation_id", "")).strip()
            if conversation_id and self._has_running_main_for_conversation(conversation_id):
                try:
                    with Store() as store:
                        store.conversation_work_finish(
                            str(claim.get("claimed_by", "")).strip(),
                            conversation_id,
                            mark_inbox_processed=False,
                            consumed_task_ids=[],
                        )
                except Exception:
                    logger.exception("Failed to release skipped conversation claim: %s", conversation_id)
                continue

            payload = self._build_main_claim_payload(claim)
            agent_key = str(claim.get("claimed_by", "")).strip()
            if not agent_key:
                continue
            typing_target = self._normalize_typing_target(
                str(claim.get("gateway", "")).strip(),
                conversation_id,
            )
            if not self._spawn_agent(
                "main",
                payload=payload,
                agent_key=agent_key,
                typing_target=typing_target,
            ):
                try:
                    with Store() as store:
                        store.conversation_work_finish(
                            agent_key,
                            conversation_id,
                            mark_inbox_processed=False,
                            consumed_task_ids=[],
                        )
                except Exception:
                    logger.exception("Failed to release main claim after spawn skip: %s", conversation_id)
                continue

    def _build_main_claim_payload(self, claim: dict[str, Any]) -> str:
        from ..core.store import Store

        inbox_ids = [
            str(item).strip()
            for item in list(claim.get("inbox_ids", []) or [])
            if str(item).strip()
        ]
        inbox_items: list[dict[str, Any]] = []
        if inbox_ids:
            try:
                with Store() as store:
                    for inbox_id in inbox_ids:
                        inbox = store.inbox_read(inbox_id)
                        if inbox is not None:
                            inbox_items.append(inbox)
            except Exception:
                logger.warning("Failed to read claimed inbox items for startup payload", exc_info=True)

        message_lines = [
            "[scheduler]",
            f"conversation_id={str(claim.get('conversation_id', '')).strip()}",
            f"work_version={int(claim.get('work_version', 0))}",
        ]
        if inbox_items:
            message_lines.extend([
                "",
                (
                    f"[inbox-batch] selected={len(inbox_items)} "
                    f"claimed_by={str(claim.get('claimed_by', '')).strip()}"
                ),
            ])
            for item in inbox_items:
                message_lines.append(
                    " ".join([
                        f"inbox_id={item.get('id', '')}",
                        f"gateway={item.get('gateway', '')}",
                        f"conversation_id={item.get('conversation_id', '')}",
                        f"message_id={item.get('message_id', '')}",
                        f"is_admin={'true' if bool(item.get('is_admin', False)) else 'false'}",
                        f"silent={'true' if bool(item.get('silent', False)) else 'false'}",
                    ])
                )
        return self._build_startup_payload(
            wake_mode=str(claim.get("wake_mode", "wake")).strip() or "wake",
            source_topic=str(claim.get("source_topic", "conversation.work")).strip(),
            message="\n".join(message_lines).strip(),
            completed_task_ids=list(claim.get("completed_task_ids", []) or []),
            conversation_id=str(claim.get("conversation_id", "")).strip(),
            inbox_ids=inbox_ids,
        )

    def _pending_notify_delay_conversations(self) -> set[str]:
        if self._notify_delay <= 0 or not self._notify_deadlines:
            return set()
        now = monotonic_time()
        expired = [
            conversation_id
            for conversation_id, deadline in self._notify_deadlines.items()
            if deadline <= now
        ]
        for conversation_id in expired:
            self._notify_deadlines.pop(conversation_id, None)
            handle = self._notify_timer_handles.pop(conversation_id, None)
            if handle is not None:
                handle.cancel()
        return set(self._notify_deadlines.keys())

    def _arm_notify_delay_timer(self, conversation_id: str) -> None:
        if self._notify_delay <= 0 or self._loop is None or self._stopping:
            return
        deadline = self._notify_deadlines.get(conversation_id)
        if deadline is None:
            return
        previous = self._notify_timer_handles.pop(conversation_id, None)
        if previous is not None:
            previous.cancel()
        delay = max(0.0, deadline - monotonic_time())
        self._notify_timer_handles[conversation_id] = self._loop.call_later(
            delay,
            self._on_notify_delay_elapsed,
            conversation_id,
            deadline,
        )

    def _on_notify_delay_elapsed(self, conversation_id: str, deadline: float) -> None:
        current_deadline = self._notify_deadlines.get(conversation_id)
        if current_deadline is None:
            self._notify_timer_handles.pop(conversation_id, None)
            return
        if abs(current_deadline - deadline) > 1e-6:
            return
        self._notify_timer_handles.pop(conversation_id, None)
        if current_deadline > monotonic_time():
            self._arm_notify_delay_timer(conversation_id)
            return
        self._notify_deadlines.pop(conversation_id, None)
        if not self._stopping:
            self._schedule_main_agents()

    def _handle_wake(self, message: str) -> None:
        conversation_id, gateway, user_id = self._infer_conversation_from_wake_message(message)
        if not conversation_id:
            logger.warning("Ignore wake without resolvable conversation_id")
            return
        from ..core.store import Store

        try:
            with Store() as store:
                store.conversation_work_touch(
                    conversation_id,
                    gateway=gateway,
                    user_id=user_id,
                )
        except Exception:
            logger.exception("Failed to touch conversation work from wake: %s", conversation_id)
            return
        if self._notify_delay > 0:
            self._notify_deadlines[conversation_id] = monotonic_time() + self._notify_delay
            self._arm_notify_delay_timer(conversation_id)
            return
        self._schedule_main_agents()

    def _handle_task_done(self, payload: dict) -> None:
        task_id = str(payload.get("task_id", "")).strip()

        # 检查任务文件中的 then 字段，自动创建后续任务
        if task_id and self._try_chain_task(task_id):
            return

        if not task_id:
            return
        from ..core.store import Store

        try:
            with Store() as store:
                task = store.task_read(task_id)
                if not task:
                    logger.warning("task_done ignored: task not found %s", task_id)
                    return
                conversation_id = str(task.get("conversation_id", "")).strip()
                if not conversation_id:
                    logger.warning("task_done ignored: conversation_id missing task=%s", task_id)
                    return
                store.conversation_work_touch(
                    conversation_id,
                    gateway=str(task.get("gateway", "")).strip(),
                    user_id=str(task.get("user_id", "")).strip(),
                    completed_task_id=task_id,
                )
        except Exception:
            logger.exception("Failed to touch conversation work from task_done: %s", task_id)
            return
        self._schedule_main_agents()

    @staticmethod
    def _build_startup_payload(
        *,
        wake_mode: str,
        source_topic: str = "",
        message: str = "",
        task_id: str = "",
        completed_task_ids: list[str] | None = None,
        conversation_id: str = "",
        inbox_ids: list[str] | None = None,
    ) -> str:
        local_now = datetime.now().astimezone().replace(microsecond=0)
        tzinfo = local_now.tzinfo
        tz_name = ""
        if tzinfo is not None:
            tz_name = getattr(tzinfo, "key", "") or (local_now.tzname() or "")
        if not tz_name:
            tz_name = "local"
        lines = [
            "[startup]",
            f"wake_mode={wake_mode}",
            f"triggered_at_local={local_now.isoformat()}",
            f"triggered_timezone={tz_name}"
        ]
        if source_topic:
            lines.append(f"source_topic={source_topic}")
        if task_id:
            lines.append(f"task_id={task_id}")
        normalized_completed_task_ids = [
            str(item).strip()
            for item in (completed_task_ids or [])
            if str(item).strip()
        ]
        if normalized_completed_task_ids:
            lines.append(
                f"completed_task_ids={','.join(normalized_completed_task_ids)}"
            )
        normalized_inbox_ids = [
            str(item).strip()
            for item in (inbox_ids or [])
            if str(item).strip()
        ]
        if normalized_inbox_ids:
            lines.append(f"inbox_ids={','.join(normalized_inbox_ids)}")
        if conversation_id:
            lines.append(f"conversation_id={conversation_id}")
        if message:
            lines.extend(["", message])
        return "\n".join(lines)

    def _queue_task_spawn(self, task_id: str, payload: str) -> None:
        normalized_task_id = task_id.strip()
        if not normalized_task_id:
            return
        if any(existing_task_id == normalized_task_id for existing_task_id, _ in self._pending_task_spawns):
            return
        self._pending_task_spawns.append((normalized_task_id, payload))

    def _drain_pending_task_spawns(self) -> None:
        while self._pending_task_spawns and self._has_task_capacity() and not self._stopping:
            task_id, payload = self._pending_task_spawns.pop(0)
            self._spawn_agent("task", task_id=task_id, payload=payload)

    def _try_chain_task(self, prev_task_id: str) -> bool:
        """检查已完成任务的 then_chain 字段，若存在则创建并启动后续任务。返回是否已处理。"""
        from ..core.store import Store

        try:
            with Store() as store:
                task = store.task_read(prev_task_id)
        except Exception:
            logger.warning("Failed to read task from store for chain check: %s", prev_task_id)
            return False

        if not task:
            return False

        task_result = str(task.get("result", "") or "")
        if "[chain-stop]" in task_result:
            logger.info("Chain stopped by task result marker: %s", prev_task_id)
            return False

        pending_then = task.get("then_chain") or []
        if not isinstance(pending_then, list):
            logger.warning("Invalid then_chain format in task %s: %r", prev_task_id, pending_then)
            return False
        pending_then = [str(item).strip() for item in pending_then if str(item).strip()]
        if not pending_then:
            return False
        pending_then_task_types = task.get("then_task_types") or []
        if not isinstance(pending_then_task_types, list):
            pending_then_task_types = []
        pending_then_task_types = [
            str(item).strip().lower()
            for item in pending_then_task_types
            if str(item).strip()
        ]

        then_goal = pending_then[0]
        remaining_then = pending_then[1:]
        next_task_type = (
            pending_then_task_types[0]
            if pending_then_task_types
            else "general"
        )
        remaining_then_task_types = pending_then_task_types[1:]
        conversation_id = str(task.get("conversation_id", ""))
        gateway = str(task.get("gateway", ""))
        user_id = str(task.get("user_id", ""))
        person_id = str(task.get("person_id", ""))
        message_id = str(task.get("message_id", ""))
        reply_to_message_id = str(task.get("reply_to_message_id", ""))
        memory_id = str(task.get("memory_id", ""))
        parent_task_id = str(task.get("id", ""))
        # 在 goal 中追加前置任务引用
        chained_goal = f"{then_goal}\n\n（前置任务 {prev_task_id} 已完成，结果：{(task.get('result') or '')[:500]}）"

        try:
            with Store() as store:
                result = store.task_create(
                    chained_goal,
                    task_type=next_task_type,
                    notify_main_on_finish=bool(task.get("notify_main_on_finish", True)),
                    gateway=gateway,
                    conversation_id=conversation_id,
                    user_id=user_id,
                    person_id=person_id,
                    message_id=message_id,
                    reply_to_message_id=reply_to_message_id,
                    parent_task_id=parent_task_id,
                    then=remaining_then or None,
                    then_task_types=remaining_then_task_types or None,
                    is_admin=bool(task.get("is_admin", False)),
                    memory_id=memory_id,
                )
        except Exception:
            logger.exception("Failed to create chain task from %s", prev_task_id)
            return False
        new_task_id = result["id"]
        previous_context_refs = read_task_context_refs(prev_task_id)
        if task_result and len(task_result) > 500:
            result_ref_id = f"chain-result-{prev_task_id}"
            try:
                write_tool_ref(result_ref_id, {
                    "source_task_id": prev_task_id,
                    "result": task_result,
                })
                previous_context_refs = list({*previous_context_refs, result_ref_id})
            except Exception:
                logger.warning("Failed to write chain result ref for %s", prev_task_id, exc_info=True)
        if previous_context_refs:
            try:
                write_task_context_refs(new_task_id, previous_context_refs)
            except Exception:
                logger.warning("Failed to copy context refs from %s", prev_task_id, exc_info=True)
        startup_payload = self._build_startup_payload(
            wake_mode="spawn",
            source_topic="agent.chain",
            task_id=new_task_id,
            message=f"[chain] 前置任务 {prev_task_id} 已完成，自动启动后续任务",
        )
        if self._has_task_capacity():
            self._spawn_agent("task", task_id=new_task_id, payload=startup_payload)
        else:
            self._queue_task_spawn(new_task_id, startup_payload)
        logger.info("Chain task spawned: %s -> %s", prev_task_id, new_task_id)
        return True

    def _handle_spawn(self, payload: dict) -> None:
        task_id = str(payload.get("task_id", ""))
        if task_id:
            startup_payload = self._build_startup_payload(
                wake_mode="spawn",
                source_topic="agent.spawn",
                task_id=task_id,
                message="[event] 收到新任务启动请求",
            )
            if self._has_task_capacity():
                self._spawn_agent("task", task_id=task_id, payload=startup_payload)
            else:
                self._queue_task_spawn(task_id, startup_payload)

    def _handle_stop(self, payload: dict) -> None:
        role = str(payload.get("role", "")).strip().lower()
        task_id = str(payload.get("task_id", "")).strip()
        agent_key = str(payload.get("agent_key", "")).strip()
        if role == "main" and not agent_key:
            for key, proc in list(self._processes.items()):
                if not key.startswith("main-"):
                    continue
                if proc.poll() is None:
                    proc.terminate()
                    logger.info("Termination requested for %s (pid=%d)", key, proc.pid)
                else:
                    self._processes.pop(key, None)
            return
        if role == "main":
            key = agent_key
        elif task_id:
            key = f"task-{task_id}"
        else:
            logger.warning("Ignore agent.stop without valid role/task_id payload=%s", payload)
            return

        proc = self._processes.get(key)
        if proc is None:
            logger.info("agent.stop ignored: process not found key=%s", key)
            return

        if proc.poll() is None:
            proc.terminate()
            logger.info("Termination requested for %s (pid=%d)", key, proc.pid)
        else:
            self._processes.pop(key, None)
            logger.info("Process already exited for %s (pid=%d)", key, proc.pid)

    def _handle_main_done(self) -> None:
        self._reap_processes()
        self._sync_inbox_backlog()
        self._schedule_main_agents()

    async def _expire_pending_requests(self) -> None:
        if not self.pending:
            return
        now = monotonic_time()
        expired: list[tuple[str, ServerConnection]] = []
        for request_id, (requester, started_at) in list(self.pending.items()):
            if now - started_at >= self._pending_request_timeout:
                self.pending.pop(request_id, None)
                expired.append((request_id, requester))
        for request_id, requester in expired:
            try:
                await requester.send(
                    json.dumps({
                        "type": "response",
                        "id": request_id,
                        "payload": {
                            "ok": False,
                            "error": (
                                "request timeout: "
                                f"exceeded {self._pending_request_timeout}s waiting for handler"
                            ),
                        },
                    })
                )
            except Exception:
                logger.debug("Skip timeout response for request_id=%s", request_id, exc_info=True)

    # ------------------------------------------------------------------
    # Agent process pool
    # ------------------------------------------------------------------

    def _spawn_pool_worker(self, role: str) -> subprocess.Popen | None:
        """Spawn a pre-warmed pool worker and wait for its ready signal."""
        cmd = [
            sys.executable, "-m", "src.agent.sub",
            f"--role={role}",
            "--hub=",  # placeholder, actual hub sent via stdin
            "--pool-worker",
        ]
        try:
            proc = subprocess.Popen(
                cmd,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                # stderr inherits parent — agent logging remains visible.
            )
        except Exception:
            logger.warning("Failed to spawn pool worker (role=%s)", role, exc_info=True)
            return None

        # Wait for the ready signal (blocking, but called from background thread).
        ready = False
        try:
            rlist, _, _ = select.select([proc.stdout], [], [], _POOL_WORKER_READY_TIMEOUT)
            if rlist:
                line = proc.stdout.readline()
                if line:
                    data = json.loads(line)
                    ready = bool(data.get("ready"))
        except Exception:
            logger.warning("Pool worker ready-check failed (role=%s)", role, exc_info=True)

        if not ready:
            proc.kill()
            proc.wait(timeout=2)
            return None
        # Ready signal consumed; close stdout pipe (agent logs via stderr).
        try:
            proc.stdout.close()
        except Exception:
            pass
        return proc

    def _warm_pool(self) -> None:
        """Fill both pools to their configured sizes (called from background thread)."""
        for _ in range(self._pool_size_main - len(self._pool_main)):
            worker = self._spawn_pool_worker("main")
            if worker:
                self._pool_main.append(worker)
        for _ in range(self._pool_size_task - len(self._pool_task)):
            worker = self._spawn_pool_worker("task")
            if worker:
                self._pool_task.append(worker)
        if self._pool_size_main or self._pool_size_task:
            logger.info(
                "Agent pool warmed (main=%d/%d, task=%d/%d)",
                len(self._pool_main), self._pool_size_main,
                len(self._pool_task), self._pool_size_task,
            )

    def _replenish_pool(self) -> None:
        """Top up pools in a background thread (non-blocking for event loop)."""
        if self._pool_size_main <= 0 and self._pool_size_task <= 0:
            return
        # Quick check: anything to do?
        main_alive = sum(1 for p in self._pool_main if p.poll() is None)
        task_alive = sum(1 for p in self._pool_task if p.poll() is None)
        if main_alive >= self._pool_size_main and task_alive >= self._pool_size_task:
            return
        threading.Thread(
            target=self._do_replenish_pool, daemon=True, name="hub-pool-replenish"
        ).start()

    def _do_replenish_pool(self) -> None:
        """Actually refill pools (runs in background thread)."""
        for pool, size, role in [
            (self._pool_main, self._pool_size_main, "main"),
            (self._pool_task, self._pool_size_task, "task"),
        ]:
            # Remove dead workers
            alive = [p for p in pool if p.poll() is None]
            pool.clear()
            pool.extend(alive)
            # Spawn replacements
            for _ in range(size - len(pool)):
                if self._stopping:
                    return
                worker = self._spawn_pool_worker(role)
                if worker:
                    pool.append(worker)

    def _claim_pool_worker(self, role: str) -> subprocess.Popen | None:
        """Take one warm worker from the pool, or return None."""
        pool = self._pool_main if role == "main" else self._pool_task
        while pool:
            proc = pool.pop(0)
            if proc.poll() is None:
                return proc
            # Dead worker, skip
        return None

    def _activate_pool_worker(
        self,
        proc: subprocess.Popen,
        *,
        hub: str,
        task: str = "",
        agent_key: str = "",
    ) -> bool:
        """Send assignment JSON to a pool worker's stdin. Returns True on success."""
        assignment = json.dumps({
            "hub": hub,
            "task": task,
            "agent_key": agent_key,
        }) + "\n"
        try:
            proc.stdin.write(assignment.encode())
            proc.stdin.flush()
            proc.stdin.close()
            return True
        except Exception:
            logger.warning("Failed to activate pool worker pid=%d", proc.pid, exc_info=True)
            proc.kill()
            proc.wait(timeout=2)
            return False

    def _kill_pool(self) -> None:
        """Gracefully shut down all idle pool workers by closing their stdin."""
        for pool in (self._pool_main, self._pool_task):
            for proc in pool:
                if proc.poll() is None:
                    try:
                        proc.stdin.close()
                    except Exception:
                        pass
            # Give workers a moment to exit, then force-kill stragglers.
            for proc in pool:
                try:
                    proc.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait(timeout=1)
                except Exception:
                    pass
            pool.clear()

    # ------------------------------------------------------------------
    # Agent process management
    # ------------------------------------------------------------------

    def _spawn_agent(
        self,
        role: str,
        *,
        payload: str = "",
        task_id: str = "",
        agent_key: str = "",
        typing_target: tuple[str, str] | None = None,
    ) -> bool:
        if self._stopping:
            logger.debug("Hub is stopping, skip spawning role=%s", role)
            return False

        if role == "main":
            if not self._has_main_capacity():
                return False
            key = agent_key.strip() or f"main-{uuid.uuid4().hex[:12]}"
            conversation_id = self._extract_startup_conversation_id(payload)
            if conversation_id and self._has_running_main_for_conversation(conversation_id):
                logger.info(
                    "Main agent already running for conversation=%s, skip spawn",
                    conversation_id,
                )
                return False
        else:
            if not self._has_task_capacity():
                return False
            key = f"task-{task_id}"
        existing = self._processes.get(key)
        if existing and existing.poll() is None:
            logger.warning("Agent %s already running (pid=%d), skip spawn", key, existing.pid)
            return False

        if payload:
            self._startup_payloads[key] = payload
        else:
            self._startup_payloads.pop(key, None)

        # Try to activate a pre-warmed pool worker first.
        pool_worker = self._claim_pool_worker(role)
        if pool_worker and self._activate_pool_worker(
            pool_worker,
            hub=self.url,
            task=task_id,
            agent_key=key,
        ):
            proc = pool_worker
            logger.info("Activated pool worker for %s agent (pid=%d)", key, proc.pid)
        else:
            # Cold start fallback.
            if role == "task":
                cmd = [
                    sys.executable, "-m", "src.agent.sub",
                    f"--role={role}",
                    f"--hub={self.url}",
                    f"--agent-key={key}",
                    f"--task={task_id}",
                ]
            else:
                cmd = [
                    sys.executable, "-m", "src.agent.sub",
                    f"--role={role}",
                    f"--hub={self.url}",
                    f"--agent-key={key}",
                ]
            proc = subprocess.Popen(cmd)
            logger.info("Spawned %s agent (pid=%d)", key, proc.pid)

        self._processes[key] = proc
        if role == "main":
            self._main_conversations[key] = conversation_id
        resolved_typing_target = typing_target
        if resolved_typing_target is None and role == "task":
            resolved_typing_target = self._resolve_task_typing_target(task_id)
        if resolved_typing_target is not None:
            self._register_typing_target(key, *resolved_typing_target)
        return True

    def _reap_processes(self) -> None:
        dead = [k for k, p in self._processes.items() if p.poll() is not None]
        for k in dead:
            proc = self._processes.pop(k)
            self._main_conversations.pop(k, None)
            self._unregister_typing_target(k)
            logger.info("Process %s (pid=%d) exited (code=%s)", k, proc.pid, proc.returncode)
        if dead:
            self._release_orphaned_main_claims()
        self._drain_pending_task_spawns()
        self._sync_inbox_backlog()
        self._schedule_main_agents()
        self._replenish_pool()

    # ------------------------------------------------------------------
    # Typing indicator
    # ------------------------------------------------------------------

    @staticmethod
    def _normalize_typing_target(gateway: str, conversation_id: str) -> tuple[str, str] | None:
        normalized_gateway = gateway.strip()
        normalized_conversation_id = conversation_id.strip()
        if not normalized_gateway or not normalized_conversation_id:
            return None
        return normalized_gateway, normalized_conversation_id

    def _resolve_task_typing_target(self, task_id: str) -> tuple[str, str] | None:
        from ..core.store import Store

        try:
            with Store() as store:
                task = store.task_read(task_id)
            if not task:
                return None
            gateway = str(task.get("gateway", "")).strip()
            conversation_id = str(task.get("conversation_id", "")).strip()
            return self._normalize_typing_target(gateway, conversation_id)
        except Exception:
            logger.debug("Failed to resolve typing target for task %s", task_id)
            return None

    def _register_typing_target(self, agent_key: str, gateway: str, conversation_id: str) -> None:
        target = self._normalize_typing_target(gateway, conversation_id)
        if target is None:
            return
        previous = self._typing_targets.get(agent_key)
        if previous == target:
            return
        other_targets = {
            value
            for key, value in self._typing_targets.items()
            if key != agent_key
        }
        self._typing_targets[agent_key] = target
        if previous and previous != target and previous not in other_targets:
            asyncio.ensure_future(
                self._send_typing_control(previous[0], previous[1], "typing_stop")
            )
        if target not in other_targets:
            asyncio.ensure_future(
                self._send_typing_control(target[0], target[1], "typing_start")
            )

    def _unregister_typing_target(self, agent_key: str) -> None:
        target = self._typing_targets.pop(agent_key, None)
        if not target:
            return
        gateway, conversation_id = target
        # Only stop if no other running agent targets the same conversation.
        if target not in self._typing_targets.values():
            asyncio.ensure_future(
                self._send_typing_control(gateway, conversation_id, "typing_stop")
            )

    async def _send_typing_control(self, gateway: str, conversation_id: str, action: str) -> None:
        gw_ws = self.components.get("gateway")
        if gw_ws is None:
            return
        try:
            await gw_ws.send(json.dumps({
                "type": "request",
                "id": str(uuid.uuid4()),
                "topic": "gateway.send_action",
                "payload": {
                    "gateway": gateway,
                    "conversation_id": conversation_id,
                    "action": action,
                },
            }))
        except Exception:
            logger.debug("Failed to send %s to %s:%s", action, gateway, conversation_id)

    def terminate_all_agents(self) -> None:
        for name, proc in list(self._processes.items()):
            if proc.poll() is None:
                logger.info("Terminating %s (pid=%d)", name, proc.pid)
                proc.terminate()
                try:
                    proc.wait(timeout=self._agent_terminate_timeout)
                except subprocess.TimeoutExpired:
                    proc.kill()
        self._processes.clear()
        self._typing_targets.clear()

    # ------------------------------------------------------------------
    # Cron job management
    # ------------------------------------------------------------------

    def _load_cron_jobs(self) -> None:
        if self._cron_path.is_file():
            try:
                self._cron_jobs = json.loads(
                    self._cron_path.read_text(encoding="utf-8")
                )
            except Exception:
                logger.warning("Failed to load cron jobs, starting fresh")
                self._cron_jobs = []

    def _save_cron_jobs(self) -> None:
        self._cron_path.parent.mkdir(parents=True, exist_ok=True)
        self._cron_path.write_text(
            json.dumps(self._cron_jobs, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    def _handle_cron_set(self, payload: dict) -> None:
        job: dict = {
            "id": payload["id"],
            "trigger_at": payload["trigger_at"],
            "goal": payload["goal"],
        }
        for key in ("gateway", "conversation_id", "user_id", "person_id"):
            value = str(payload.get(key, "")).strip()
            if value:
                job[key] = value
        if payload.get("interval"):
            job["interval"] = payload["interval"]
        self._cron_jobs.append(job)
        self._save_cron_jobs()
        logger.info("Cron job registered: %s at %s", job["id"], job["trigger_at"])

    def _handle_cron_cancel(self, payload: dict) -> None:
        cron_id = payload.get("id", "")
        before = len(self._cron_jobs)
        self._cron_jobs = [j for j in self._cron_jobs if j["id"] != cron_id]
        if len(self._cron_jobs) < before:
            self._save_cron_jobs()
            logger.info("Cron job cancelled: %s", cron_id)

    @staticmethod
    def _parse_interval(interval: str) -> timedelta:
        """解析间隔字符串如 '30m'、'1h'、'2d' 为 timedelta。"""
        match = re.match(r"^(\d+)([smhd])$", interval.strip())
        if not match:
            raise ValueError(f"无效的 interval 格式: {interval}")
        amount = int(match.group(1))
        unit = match.group(2)
        return {
            "s": timedelta(seconds=amount),
            "m": timedelta(minutes=amount),
            "h": timedelta(hours=amount),
            "d": timedelta(days=amount),
        }[unit]

    def _check_cron_jobs(self) -> None:
        if not self._cron_jobs:
            return
        now = datetime.now(timezone.utc)
        due = []
        remaining = []
        for job in self._cron_jobs:
            trigger_at = datetime.fromisoformat(job["trigger_at"])
            if trigger_at <= now:
                due.append(job)
            else:
                remaining.append(job)
        if not due:
            return
        # 对有 interval 的周期任务，重新计算下次触发时间并保留
        for job in due:
            interval = job.get("interval")
            if interval:
                try:
                    delta = self._parse_interval(interval)
                    next_at = datetime.fromisoformat(job["trigger_at"]) + delta
                    # 如果计算出的下次时间仍然已过期，从当前时间开始算
                    if next_at <= now:
                        next_at = now + delta
                    job["trigger_at"] = next_at.isoformat()
                    remaining.append(job)
                    logger.info("Cron job rescheduled: %s -> %s", job["id"], job["trigger_at"])
                except ValueError:
                    logger.warning("Invalid interval for cron %s: %s, not rescheduling", job["id"], interval)
        self._cron_jobs = remaining
        self._save_cron_jobs()
        for job in due:
            try:
                self._fire_cron(job)
            except Exception:
                logger.exception("Failed to fire cron job: %s", job.get("id", ""))

    def _fire_cron(self, job: dict) -> None:
        from ..core.store import Store

        with Store() as store:
            result = store.task_create(
                str(job.get("goal", "")).strip(),
                task_type="general",
                notify_main_on_finish=False,
                gateway=str(job.get("gateway", "")).strip(),
                conversation_id=str(job.get("conversation_id", "")).strip(),
                user_id=str(job.get("user_id", "")).strip(),
                person_id=str(job.get("person_id", "")).strip(),
            )
        task_id = str(result["id"]).strip()
        if not str(job.get("conversation_id", "")).strip():
            logger.warning(
                "Cron job fired without conversation routing: id=%s gateway=%s conversation_id=%s user_id=%s",
                job.get("id", ""),
                job.get("gateway", ""),
                job.get("conversation_id", ""),
                job.get("user_id", ""),
            )
        payload = self._build_startup_payload(
            wake_mode="cron",
            source_topic="cron.fire",
            task_id=task_id,
            message=f"[cron] {job['id']} 触发",
        )
        if self._has_task_capacity():
            self._spawn_agent("task", task_id=task_id, payload=payload)
        else:
            self._queue_task_spawn(task_id, payload)
        logger.info("Cron job fired: %s -> task %s", job["id"], task_id)

    def _release_orphaned_main_claims(self) -> None:
        from ..core.store import Store

        try:
            with Store() as store:
                claimed_rows = store.conversation_work_list_claimed()
        except Exception:
            logger.exception("Failed to list claimed conversation work")
            return

        for row in claimed_rows:
            agent_key = str(row.get("claimed_by", "")).strip()
            conversation_id = str(row.get("conversation_id", "")).strip()
            if not agent_key.startswith("main-") or not conversation_id:
                continue
            proc = self._processes.get(agent_key)
            if proc is not None and proc.poll() is None:
                continue
            try:
                with Store() as store:
                    store.conversation_work_finish(
                        agent_key,
                        conversation_id,
                        mark_inbox_processed=False,
                        consumed_task_ids=[],
                    )
                logger.info(
                    "Released orphaned main claim agent=%s conversation=%s",
                    agent_key,
                    conversation_id,
                )
            except Exception:
                logger.exception(
                    "Failed to release orphaned main claim agent=%s conversation=%s",
                    agent_key,
                    conversation_id,
                )

    # ------------------------------------------------------------------
    # Periodic poll loop
    # ------------------------------------------------------------------

    async def _poll_loop(self) -> None:
        while not self._stopping:
            await asyncio.sleep(self._reap_interval)
            if self._stopping:
                break
            await self._expire_pending_requests()
            self._reap_processes()
            self._check_cron_jobs()
            await self._maybe_run_db_scrub()
            await self._maybe_cleanup_traces()

    async def _maybe_run_db_scrub(self) -> None:
        if self._db_scrub_interval <= 0:
            return
        if monotonic_time() < self._next_db_scrub_deadline:
            return
        self._next_db_scrub_deadline = monotonic_time() + self._db_scrub_interval
        started = monotonic_time()
        try:
            from ..core.db_scrub import scrub_manifest_db

            result = await asyncio.to_thread(scrub_manifest_db)
            elapsed_ms = round((monotonic_time() - started) * 1000, 1)
            updated_rows = int(result.get("updated_rows", 0))
            updated_fields = int(result.get("updated_fields", 0))
            if updated_rows > 0:
                logger.warning(
                    "DB scrub applied: rows=%s fields=%s elapsed_ms=%s",
                    updated_rows,
                    updated_fields,
                    elapsed_ms,
                )
            else:
                logger.debug("DB scrub done: no updates elapsed_ms=%s", elapsed_ms)
        except Exception:
            logger.exception("DB scrub failed")

    async def _maybe_cleanup_traces(self) -> None:
        if self._trace_cleanup_interval <= 0:
            return
        if monotonic_time() < self._next_trace_cleanup_deadline:
            return
        self._next_trace_cleanup_deadline = monotonic_time() + self._trace_cleanup_interval
        try:
            removed = await asyncio.to_thread(
                self._cleanup_runtime_telemetry, self._trace_retention_days
            )
            if removed:
                logger.info("Runtime telemetry cleanup: removed %d stale item(s)", removed)
            else:
                logger.debug("Runtime telemetry cleanup: nothing to remove")
        except Exception:
            logger.exception("Runtime telemetry cleanup failed")

    @staticmethod
    def _cleanup_runtime_telemetry(retention_days: int) -> int:
        import os

        from ..core.store import Store

        trace_dir = Path(".localagent/tool_calls")
        ref_dir = Path(".localagent/tool_refs")
        task_ref_dir = Path(".localagent/task_context_refs")
        legacy_usage_files = [
            Path(".localagent/usage.jsonl"),
            Path(".localagent/usage.lock"),
            Path(".localagent/usage_total.json"),
            Path(".localagent/usage_by_conversation.json"),
        ]
        cutoff = wall_time() - retention_days * 86400
        removed = 0

        def _safe_unlink(path: Path, *, warn_prefix: str) -> int:
            try:
                if path.exists():
                    path.unlink()
                    return 1
            except Exception:
                logger.warning("%s: %s", warn_prefix, path)
            return 0

        with Store() as store:
            db_removed = store.runtime_cleanup(retention_days=retention_days)
        removed += sum(int(value) for value in db_removed.values())

        if trace_dir.is_dir():
            for f in trace_dir.iterdir():
                if f.suffix != ".jsonl":
                    continue
                try:
                    if os.path.getmtime(f) >= cutoff:
                        continue
                except Exception:
                    logger.warning("Failed to stat trace file: %s", f)
                    continue

                removed += _safe_unlink(f, warn_prefix="Failed to remove trace file")
                removed += _safe_unlink(
                    f.with_suffix(".lock"),
                    warn_prefix="Failed to remove trace lock file",
                )

        if ref_dir.is_dir():
            for f in ref_dir.iterdir():
                if f.suffix != ".json":
                    continue
                try:
                    if os.path.getmtime(f) < cutoff:
                        removed += _safe_unlink(
                            f, warn_prefix="Failed to remove stale tool ref file"
                        )
                except Exception:
                    logger.warning("Failed to stat tool ref file: %s", f)

        if task_ref_dir.is_dir():
            for f in task_ref_dir.iterdir():
                if f.suffix != ".json":
                    continue
                try:
                    if os.path.getmtime(f) < cutoff:
                        removed += _safe_unlink(
                            f, warn_prefix="Failed to remove stale task ref file"
                        )
                except Exception:
                    logger.warning("Failed to stat task refs file: %s", f)
        for path in legacy_usage_files:
            try:
                if path.exists() and os.path.getmtime(path) < cutoff:
                    removed += _safe_unlink(
                        path,
                        warn_prefix="Failed to remove legacy usage telemetry file",
                    )
            except Exception:
                logger.warning("Failed to stat legacy usage telemetry file: %s", path)
        return removed

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def _serve(self) -> None:
        self._server = await serve(self._handler, self.host, self.port)
        self._ready.set()
        self._release_orphaned_main_claims()
        self._sync_inbox_backlog()
        self._schedule_main_agents()
        self._poll_task = asyncio.create_task(self._poll_loop())
        await self._server.serve_forever()

    def _run(self) -> None:
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        try:
            self._loop.run_until_complete(self._serve())
        except asyncio.CancelledError:
            pass
        finally:
            pending = asyncio.all_tasks(self._loop)
            for task in pending:
                task.cancel()
            if pending:
                self._loop.run_until_complete(
                    asyncio.gather(*pending, return_exceptions=True)
                )
            self._loop.run_until_complete(self._loop.shutdown_asyncgens())
            self._loop.close()

    def start(self) -> None:
        """Start the hub in a background daemon thread (blocks until ready)."""
        self._thread = threading.Thread(target=self._run, daemon=True, name="hub")
        self._thread.start()
        if not self._ready.wait(timeout=self._startup_timeout):
            raise RuntimeError(
                f"Hub failed to start within {self._startup_timeout} seconds"
            )
        logger.info("Hub started on %s", self.url)
        # Warm agent pools in background (non-blocking).
        if self._pool_size_main > 0 or self._pool_size_task > 0:
            threading.Thread(
                target=self._warm_pool, daemon=True, name="hub-pool-warm"
            ).start()

    def stop(self) -> None:
        """Shut down the hub gracefully."""
        self._stopping = True
        for handle in list(self._notify_timer_handles.values()):
            handle.cancel()
        self._notify_timer_handles.clear()
        self._notify_deadlines.clear()
        if self._poll_task and self._loop:
            self._loop.call_soon_threadsafe(self._poll_task.cancel)
        self.terminate_all_agents()
        self._kill_pool()
        if self._server and self._loop:
            self._loop.call_soon_threadsafe(self._server.close)
        if self._thread:
            self._thread.join(timeout=self._shutdown_timeout)
            self._thread = None
        logger.info("Hub stopped")
