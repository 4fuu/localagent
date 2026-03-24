"""Unified data store backed by SQLite."""

import json
import logging
import re
import sqlite3
import threading
import time
import uuid
from collections import deque
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

from .runtime_fs import RUNTIME_SKILLS_ROOT, ensure_runtime_layout
from .skills import skills_catalog

logger = logging.getLogger(__name__)

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
_LOCALAGENT_DIR = _PROJECT_ROOT / ".localagent"
_DB_PATH = _LOCALAGENT_DIR / "manifest.db"
_SQLITE_CONNECT_TIMEOUT_SECONDS = 10.0
_SQLITE_BUSY_TIMEOUT_MS = 5000
_SQLITE_INIT_MAX_RETRIES = 5
_SQLITE_INIT_BASE_DELAY_SECONDS = 0.1
_DB_INIT_LOCK = threading.Lock()
_DB_INITIALIZED_PATHS: set[str] = set()


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _iso_after_seconds(seconds: int | float) -> str:
    return (datetime.now(timezone.utc) + timedelta(seconds=max(0.0, float(seconds)))).isoformat()


def _gen_id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


def _json_dumps(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False)


def _json_loads(text: str | None, default: Any = None) -> Any:
    if not text:
        return default
    try:
        return json.loads(text)
    except (json.JSONDecodeError, TypeError):
        return default


def _default_enabled_skills() -> list[str]:
    ensure_runtime_layout()
    return [
        str(item.get("skill", "")).strip()
        for item in skills_catalog(str(RUNTIME_SKILLS_ROOT))
        if str(item.get("skill", "")).strip()
    ]


def _is_locked_sqlite_error(exc: sqlite3.OperationalError) -> bool:
    message = str(exc).lower()
    return (
        "database is locked" in message
        or "database schema is locked" in message
        or "database table is locked" in message
    )


def _truncate_text(value: str, max_chars: int) -> tuple[str, bool]:
    if max_chars < 1:
        max_chars = 1
    if len(value) <= max_chars:
        return value, False
    return value[:max_chars], True


def _preview_json_text(text: str, max_chars: int = 400) -> str:
    clipped, truncated = _truncate_text(text, max_chars)
    return f"{clipped}...(truncated)" if truncated else clipped


def _head_tail_summary(text: str, max_chars: int = 500) -> str:
    value = str(text or "")
    if max_chars < 1 or len(value) <= max_chars:
        return value
    marker = "\n\n...[中间内容已截断]...\n\n"
    if len(marker) >= max_chars:
        clipped, _ = _truncate_text(value, max_chars)
        return clipped
    remaining = max_chars - len(marker)
    head_len = max(1, remaining // 2)
    tail_len = max(1, remaining - head_len)
    return value[:head_len] + marker + value[-tail_len:]


def _as_text(value: Any) -> str:
    if isinstance(value, str):
        return value
    return _json_dumps(value)


def _as_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _tokenize_topic_text(value: Any) -> set[str]:
    text = str(value or "").strip().lower()
    if not text:
        return set()
    return {
        item
        for item in re.split(r"[^0-9a-zA-Z_\u4e00-\u9fff]+", text)
        if item
    }


def _topic_overlap_score(left: Any, right: Any) -> float:
    left_tokens = _tokenize_topic_text(left)
    right_tokens = _tokenize_topic_text(right)
    if not left_tokens or not right_tokens:
        return 0.0
    shared = left_tokens & right_tokens
    if not shared:
        return 0.0
    return len(shared) / max(len(left_tokens), len(right_tokens))


def _normalize_unique_texts(values: list[Any], *, limit: int = 12, max_chars: int = 240) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for raw in values:
        item = str(raw or "").strip()
        if not item:
            continue
        if len(item) > max_chars:
            item = item[:max_chars]
        if item in seen:
            continue
        seen.add(item)
        result.append(item)
        if len(result) >= max(1, int(limit)):
            break
    return result


def normalize_topic_item(value: dict[str, Any] | None) -> dict[str, Any]:
    source = value or {}
    raw_status = str(source.get("status", "")).strip().lower()
    if raw_status not in {"active", "blocked", "done"}:
        if raw_status in {"delivered", "done", "waiting_user"}:
            raw_status = {"delivered": "done", "waiting_user": "blocked"}.get(raw_status, "active")
        else:
            raw_status = "active"
    goal = str(source.get("goal", "")).strip()
    if not goal:
        goal = str(source.get("subgoal", "")).strip()
    return {
        "id": str(source.get("id", "")).strip() or _gen_id("topic"),
        "status": raw_status,
        "goal": goal,
        "replied": bool(source.get("replied", False)),
        "source_message_id": str(source.get("source_message_id", "")).strip(),
        "last_task_id": str(source.get("last_task_id", "")).strip(),
        "updated_at": str(source.get("updated_at", "")).strip() or _now_iso(),
    }


def _extract_last_json_block(text: str) -> dict[str, Any] | None:
    if not text.strip():
        return None
    matches = re.findall(r"```json\s*(\{.*?\})\s*```", text, flags=re.DOTALL | re.IGNORECASE)
    candidates = list(matches)
    xml_matches = re.findall(r"<task_outcome>\s*(\{.*?\})\s*</task_outcome>", text, flags=re.DOTALL | re.IGNORECASE)
    candidates.extend(xml_matches)
    for raw in reversed(candidates):
        try:
            parsed = json.loads(raw)
        except Exception:
            continue
        if isinstance(parsed, dict):
            return parsed
    return None


def strip_structured_outcome_block(text: str) -> str:
    value = str(text or "")
    value = re.sub(r"\n?```json\s*\{.*?\}\s*```\s*$", "", value, flags=re.DOTALL | re.IGNORECASE)
    value = re.sub(r"\n?<task_outcome>\s*\{.*?\}\s*</task_outcome>\s*$", "", value, flags=re.DOTALL | re.IGNORECASE)
    return value.strip()


def normalize_task_outcome(
    value: dict[str, Any] | None,
    *,
    task_type: str = "",
    result: str = "",
) -> dict[str, Any]:
    source = value or {}
    normalized_status = str(source.get("status", "")).strip().lower()
    if normalized_status not in {"completed", "blocked", "waiting_user", "delivered", "done"}:
        lowered = str(result or "").lower()
        if any(token in lowered for token in ["等待用户", "需要用户", "请用户", "waiting user", "need user"]):
            normalized_status = "waiting_user"
        elif any(token in lowered for token in ["失败", "报错", "无法", "未完成", "error", "failed", "blocked"]):
            normalized_status = "blocked"
        elif str(task_type).strip().lower() in {"reply", "general"}:
            normalized_status = "delivered"
        else:
            normalized_status = "completed"
    delivery = str(source.get("delivery", "")).strip()
    if not delivery and normalized_status in {"delivered", "done"} and str(task_type).strip().lower() in {"reply", "general"}:
        delivery = strip_structured_outcome_block(result)[:400]
    return {
        "status": normalized_status,
        "summary": str(source.get("summary", "")).strip()[:500],
        "facts": _normalize_unique_texts(source.get("facts", []) or [], limit=12),
        "constraints": _normalize_unique_texts(source.get("constraints", []) or [], limit=12),
        "unknowns_open": _normalize_unique_texts(source.get("unknowns_open", []) or [], limit=12),
        "unknowns_resolved": _normalize_unique_texts(source.get("unknowns_resolved", []) or [], limit=12),
        "artifacts": _normalize_unique_texts(source.get("artifacts", []) or [], limit=12, max_chars=400),
        "next_action": str(source.get("next_action", "")).strip()[:240],
        "delivery": delivery,
        "user_visible_delivery": bool(source.get("user_visible_delivery", False)) or normalized_status in {"delivered", "done"},
    }


def parse_task_outcome(result: str, *, task_type: str = "") -> dict[str, Any]:
    parsed = _extract_last_json_block(result)
    return normalize_task_outcome(parsed, task_type=task_type, result=result)


def normalize_conversation_topics(values: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
    seen: set[str] = set()
    topics: list[dict[str, Any]] = []
    for raw in values or []:
        item = normalize_topic_item(raw)
        topic_id = str(item.get("id", "")).strip()
        if not topic_id or topic_id in seen:
            continue
        seen.add(topic_id)
        topics.append(item)
    topics.sort(key=lambda item: str(item.get("updated_at", "")), reverse=True)
    return topics[:8]


def conversation_state_active_topic(state: dict[str, Any] | None) -> dict[str, Any] | None:
    source = state or {}
    topics = normalize_conversation_topics(source.get("topics", []) or [])
    active_topic_id = str(source.get("active_topic_id", "")).strip()
    for item in topics:
        if str(item.get("id", "")).strip() == active_topic_id:
            return item
    if topics:
        return topics[0]
    return None


def conversation_state_topic_summaries(state: dict[str, Any] | None) -> list[dict[str, Any]]:
    source = state or {}
    active_topic_id = str(source.get("active_topic_id", "")).strip()
    summaries: list[dict[str, Any]] = []
    for item in normalize_conversation_topics(source.get("topics", []) or []):
        summaries.append({
            "id": item.get("id", ""),
            "status": item.get("status", ""),
            "goal": item.get("goal", ""),
            "replied": item.get("replied", False),
            "updated_at": item.get("updated_at", ""),
            "is_active": str(item.get("id", "")).strip() == active_topic_id,
        })
    return summaries


def conversation_state_bind_task(
    current: dict[str, Any],
    *,
    goal: str,
    task_type: str,
    message_id: str = "",
    task_id: str = "",
) -> tuple[list[dict[str, Any]], str]:
    normalized_goal = str(goal or "").strip()
    normalized_message_id = str(message_id or "").strip()
    topics = normalize_conversation_topics(current.get("topics", []) or [])
    active_topic = conversation_state_active_topic(current)
    chosen: dict[str, Any] | None = None

    if active_topic is not None:
        active_source_message_id = str(active_topic.get("source_message_id", "")).strip()
        overlap = _topic_overlap_score(normalized_goal, active_topic.get("goal", ""))
        if normalized_message_id and normalized_message_id == active_source_message_id:
            chosen = dict(active_topic)
        elif overlap >= 0.5:
            chosen = dict(active_topic)

    if chosen is None:
        chosen = normalize_topic_item({
            "goal": normalized_goal,
            "source_message_id": normalized_message_id,
            "last_task_id": task_id,
            "status": "active",
        })
        topics = [chosen, *topics]
    else:
        chosen["updated_at"] = _now_iso()
        chosen["status"] = "active"
        chosen["replied"] = False
        if normalized_goal and not str(chosen.get("goal", "")).strip():
            chosen["goal"] = normalized_goal
        if normalized_message_id:
            chosen["source_message_id"] = normalized_message_id
        if task_id:
            chosen["last_task_id"] = task_id
        topics = [chosen, *[item for item in topics if str(item.get("id", "")).strip() != str(chosen.get("id", "")).strip()]]

    return normalize_conversation_topics(topics), str(chosen.get("id", "")).strip()


def conversation_state_apply_task_result(
    current: dict[str, Any],
    *,
    task: dict[str, Any],
    summary: str,
    outcome: dict[str, Any] | None = None,
) -> dict[str, Any]:
    normalized_outcome = normalize_task_outcome(
        outcome,
        task_type=str(task.get("task_type", "")).strip(),
        result=summary,
    )
    topic_id = str(task.get("topic_id", "")).strip()
    topics = normalize_conversation_topics(current.get("topics", []) or [])
    updated_topics: list[dict[str, Any]] = []
    target: dict[str, Any] | None = None

    for item in topics:
        if topic_id and str(item.get("id", "")).strip() == topic_id:
            target = dict(item)
        else:
            updated_topics.append(item)

    if target is None:
        target = normalize_topic_item({
            "id": topic_id or "",
            "goal": str(task.get("goal", "")).strip(),
            "source_message_id": str(task.get("message_id", "")).strip(),
            "status": "active",
        })

    task_type = str(task.get("task_type", "")).strip().lower()
    target["updated_at"] = _now_iso()
    target["last_task_id"] = str(task.get("id", "")).strip()

    outcome_status = str(normalized_outcome.get("status", "")).strip()
    target_status = str(target.get("status", "")).strip()
    if task_type in {"execute", "general"} and outcome_status in {"blocked", "waiting_user"}:
        target["status"] = "blocked"
    elif (
        task_type in {"reply", "general"}
        and outcome_status in {"done", "delivered", "completed"}
        and bool(target.get("replied"))
        and target_status != "blocked"
    ):
        # A successful user-visible delivery closes the topic unless execution already proved it is blocked.
        target["status"] = "done"
    elif task_type != "reply":
        target["status"] = "active"

    updated_topics.insert(0, normalize_topic_item(target))
    return {
        "topics": normalize_conversation_topics(updated_topics),
        "active_topic_id": str(target.get("id", "")).strip(),
    }


def conversation_state_record_delivery(
    current: dict[str, Any],
    *,
    task_id: str = "",
    topic_id: str = "",
    text: str = "",
    message_id: str = "",
) -> dict[str, Any]:
    normalized_topic_id = str(topic_id or "").strip()
    topics = normalize_conversation_topics(current.get("topics", []) or [])
    updated_topics: list[dict[str, Any]] = []
    target: dict[str, Any] | None = None
    for item in topics:
        if normalized_topic_id and str(item.get("id", "")).strip() == normalized_topic_id:
            target = dict(item)
        else:
            updated_topics.append(item)
    if target is None:
        target = normalize_topic_item({"id": normalized_topic_id or "", "status": "active"})
    target["replied"] = True
    target["updated_at"] = _now_iso()
    target["last_task_id"] = str(task_id or "").strip()
    updated_topics.insert(0, normalize_topic_item(target))
    return {
        "topics": normalize_conversation_topics(updated_topics),
        "active_topic_id": str(target.get("id", "")).strip(),
        "last_bot_message_id": str(message_id or "").strip(),
    }


class Store:
    """SQLite-backed store for memories, tasks, and inbox messages.

    Usage:
        store = Store()          # uses default .localagent/manifest.db
        store = Store(db_path)   # custom path (for testing)
    """

    def __init__(self, db_path: str | Path | None = None):
        path = Path(db_path) if db_path else _DB_PATH
        path.parent.mkdir(parents=True, exist_ok=True)
        self._db_path_key = str(path.resolve())
        self._db = sqlite3.connect(
            str(path), check_same_thread=False, timeout=_SQLITE_CONNECT_TIMEOUT_SECONDS,
        )
        try:
            self._db.execute(f"PRAGMA busy_timeout={_SQLITE_BUSY_TIMEOUT_MS}")
            self._db.row_factory = sqlite3.Row
            self._initialize_database()
        except Exception:
            self.close()
            raise

    def _initialize_database(self) -> None:
        with _DB_INIT_LOCK:
            if self._db_path_key in _DB_INITIALIZED_PATHS:
                return
            self._run_init_step(
                lambda: self._db.execute("PRAGMA journal_mode=WAL").fetchone(),
                step_name="enable WAL",
                required=False,
            )
            self._run_init_step(self._init_tables, step_name="initialize schema")
            _DB_INITIALIZED_PATHS.add(self._db_path_key)

    def _run_init_step(
        self,
        action: Callable[[], Any],
        *,
        step_name: str,
        required: bool = True,
    ) -> Any:
        last_error: sqlite3.OperationalError | None = None
        for attempt in range(_SQLITE_INIT_MAX_RETRIES + 1):
            try:
                return action()
            except sqlite3.OperationalError as exc:
                if not _is_locked_sqlite_error(exc):
                    raise
                last_error = exc
                try:
                    self._db.rollback()
                except sqlite3.Error:
                    pass
                if attempt >= _SQLITE_INIT_MAX_RETRIES:
                    break
                time.sleep(min(_SQLITE_INIT_BASE_DELAY_SECONDS * (2**attempt), 1.0))
        if required and last_error is not None:
            raise last_error
        if last_error is not None:
            logger.warning(
                "Store init skipped step=%s db=%s after lock retries: %s",
                step_name,
                self._db_path_key,
                last_error,
            )
        return None

    # ------------------------------------------------------------------
    # Schema
    # ------------------------------------------------------------------

    def _init_tables(self) -> None:
        self._db.executescript("""
            CREATE TABLE IF NOT EXISTS memories (
                id              TEXT PRIMARY KEY,
                layer           TEXT NOT NULL DEFAULT 'archive',
                topic           TEXT NOT NULL,
                tags            TEXT NOT NULL DEFAULT '[]',
                summary         TEXT NOT NULL DEFAULT '',
                thoughts        TEXT NOT NULL DEFAULT '',
                raw_content     TEXT NOT NULL DEFAULT '',
                messages        TEXT NOT NULL DEFAULT '[]',
                task_results    TEXT NOT NULL DEFAULT '[]',
                source          TEXT NOT NULL DEFAULT '',
                images          TEXT NOT NULL DEFAULT '[]',
                cron_refs       TEXT NOT NULL DEFAULT '[]',
                created_at      TEXT NOT NULL,
                updated_at      TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS tasks (
                id              TEXT PRIMARY KEY,
                status          TEXT NOT NULL DEFAULT 'pending',
                task_type       TEXT NOT NULL DEFAULT 'general',
                notify_main_on_finish INTEGER NOT NULL DEFAULT 1,
                goal            TEXT NOT NULL,
                gateway         TEXT NOT NULL DEFAULT '',
                conversation_id TEXT NOT NULL DEFAULT '',
                user_id         TEXT NOT NULL DEFAULT '',
                person_id       TEXT NOT NULL DEFAULT '',
                message_id      TEXT NOT NULL DEFAULT '',
                reply_to_message_id TEXT NOT NULL DEFAULT '',
                parent_task_id  TEXT NOT NULL DEFAULT '',
                then_chain      TEXT NOT NULL DEFAULT '[]',
                then_task_types TEXT NOT NULL DEFAULT '[]',
                images          TEXT NOT NULL DEFAULT '[]',
                is_admin        INTEGER NOT NULL DEFAULT 0,
                result          TEXT NOT NULL DEFAULT '',
                outcome_json    TEXT NOT NULL DEFAULT '{}',
                memory_id       TEXT NOT NULL DEFAULT '',
                created_at      TEXT NOT NULL,
                updated_at      TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS inbox_messages (
                id              TEXT PRIMARY KEY,
                gateway         TEXT NOT NULL,
                conversation_id TEXT NOT NULL DEFAULT '',
                message_id      TEXT NOT NULL DEFAULT '',
                user_id         TEXT NOT NULL DEFAULT '',
                person_id       TEXT NOT NULL DEFAULT '',
                user_name       TEXT NOT NULL DEFAULT '',
                is_admin        INTEGER NOT NULL DEFAULT 0,
                content         TEXT NOT NULL DEFAULT '',
                metadata        TEXT NOT NULL DEFAULT '{}',
                attachments     TEXT NOT NULL DEFAULT '[]',
                silent          INTEGER NOT NULL DEFAULT 0,
                processed       INTEGER NOT NULL DEFAULT 0,
                claimed_by      TEXT NOT NULL DEFAULT '',
                claim_expires_at TEXT NOT NULL DEFAULT '',
                processed_by    TEXT NOT NULL DEFAULT '',
                processed_at    TEXT NOT NULL DEFAULT '',
                created_at      TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS conversation_work_queue (
                conversation_id TEXT PRIMARY KEY,
                gateway         TEXT NOT NULL DEFAULT '',
                user_id         TEXT NOT NULL DEFAULT '',
                dirty           INTEGER NOT NULL DEFAULT 0,
                work_version    INTEGER NOT NULL DEFAULT 0,
                pending_task_ids TEXT NOT NULL DEFAULT '[]',
                claimed_by      TEXT NOT NULL DEFAULT '',
                claim_expires_at TEXT NOT NULL DEFAULT '',
                last_started_at TEXT NOT NULL DEFAULT '',
                last_finished_at TEXT NOT NULL DEFAULT '',
                created_at      TEXT NOT NULL,
                updated_at      TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS conversation_states (
                conversation_id   TEXT PRIMARY KEY,
                version           INTEGER NOT NULL DEFAULT 0,
                gateway           TEXT NOT NULL DEFAULT '',
                user_id           TEXT NOT NULL DEFAULT '',
                person_id         TEXT NOT NULL DEFAULT '',
                is_multi_party    INTEGER NOT NULL DEFAULT 0,
                active_topic_id   TEXT NOT NULL DEFAULT '',
                topics            TEXT NOT NULL DEFAULT '[]',
                enabled_skills    TEXT NOT NULL DEFAULT '[]',
                session_constraints TEXT NOT NULL DEFAULT '[]',
                session_facts     TEXT NOT NULL DEFAULT '[]',
                current_focus     TEXT NOT NULL DEFAULT '',
                resolved_entities TEXT NOT NULL DEFAULT '[]',
                open_questions    TEXT NOT NULL DEFAULT '[]',
                active_task_ids   TEXT NOT NULL DEFAULT '[]',
                last_user_message_id TEXT NOT NULL DEFAULT '',
                last_bot_message_id  TEXT NOT NULL DEFAULT '',
                last_delivery        TEXT NOT NULL DEFAULT '',
                last_result_summary  TEXT NOT NULL DEFAULT '',
                recent_memory_ids    TEXT NOT NULL DEFAULT '[]',
                created_at        TEXT NOT NULL,
                updated_at        TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS conversation_events (
                id              TEXT PRIMARY KEY,
                conversation_id TEXT NOT NULL,
                event_type      TEXT NOT NULL,
                payload         TEXT NOT NULL DEFAULT '{}',
                created_at      TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS settings (
                key             TEXT PRIMARY KEY,
                value           TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS user_profiles (
                id                TEXT PRIMARY KEY,
                gateway           TEXT NOT NULL DEFAULT '',
                user_id           TEXT NOT NULL DEFAULT '',
                person_id         TEXT NOT NULL DEFAULT '',
                conversation_id   TEXT NOT NULL DEFAULT '',
                profile_key       TEXT NOT NULL DEFAULT '',
                profile_value     TEXT NOT NULL DEFAULT '',
                source_memory_id  TEXT NOT NULL DEFAULT '',
                source_message_id TEXT NOT NULL DEFAULT '',
                confidence        REAL NOT NULL DEFAULT 1.0,
                created_at        TEXT NOT NULL,
                updated_at        TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS runtime_runs (
                run_id            TEXT PRIMARY KEY,
                role              TEXT NOT NULL DEFAULT '',
                task_id           TEXT NOT NULL DEFAULT '',
                provider          TEXT NOT NULL DEFAULT '',
                model             TEXT NOT NULL DEFAULT '',
                base_url          TEXT NOT NULL DEFAULT '',
                hub_url           TEXT NOT NULL DEFAULT '',
                conversation_id   TEXT NOT NULL DEFAULT '',
                wake_mode         TEXT NOT NULL DEFAULT '',
                source_topic      TEXT NOT NULL DEFAULT '',
                status            TEXT NOT NULL DEFAULT '',
                error             TEXT NOT NULL DEFAULT '',
                total_iterations  INTEGER NOT NULL DEFAULT 0,
                total_tool_calls  INTEGER NOT NULL DEFAULT 0,
                total_retries     INTEGER NOT NULL DEFAULT 0,
                prompt_tokens     INTEGER NOT NULL DEFAULT 0,
                completion_tokens INTEGER NOT NULL DEFAULT 0,
                total_tokens      INTEGER NOT NULL DEFAULT 0,
                usage             TEXT NOT NULL DEFAULT '{}',
                elapsed_seconds   REAL NOT NULL DEFAULT 0,
                logged_at         TEXT NOT NULL,
                created_at        TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS runtime_tool_calls (
                event_id          TEXT PRIMARY KEY,
                timestamp         TEXT NOT NULL,
                run_id            TEXT NOT NULL DEFAULT '',
                role              TEXT NOT NULL DEFAULT '',
                task_id           TEXT NOT NULL DEFAULT '',
                tool_name         TEXT NOT NULL DEFAULT '',
                ok                INTEGER,
                error             TEXT NOT NULL DEFAULT '',
                traceback         TEXT NOT NULL DEFAULT '',
                duration_ms       REAL NOT NULL DEFAULT 0,
                ref_id            TEXT NOT NULL DEFAULT '',
                args              TEXT NOT NULL DEFAULT '{}',
                result            TEXT NOT NULL DEFAULT '',
                created_at        TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS runtime_tool_refs (
                ref_id            TEXT PRIMARY KEY,
                timestamp         TEXT NOT NULL,
                run_id            TEXT NOT NULL DEFAULT '',
                role              TEXT NOT NULL DEFAULT '',
                task_id           TEXT NOT NULL DEFAULT '',
                tool_name         TEXT NOT NULL DEFAULT '',
                args              TEXT NOT NULL DEFAULT '{}',
                args_preview      TEXT NOT NULL DEFAULT '',
                args_truncated    INTEGER NOT NULL DEFAULT 0,
                result            TEXT NOT NULL DEFAULT 'null',
                result_preview    TEXT NOT NULL DEFAULT '',
                result_truncated  INTEGER NOT NULL DEFAULT 0,
                created_at        TEXT NOT NULL,
                updated_at        TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS runtime_task_context_refs (
                task_id           TEXT NOT NULL,
                ref_id            TEXT NOT NULL,
                ordinal           INTEGER NOT NULL DEFAULT 0,
                created_at        TEXT NOT NULL,
                updated_at        TEXT NOT NULL,
                PRIMARY KEY (task_id, ref_id)
            );
        """)
        self._db.execute(
            "CREATE INDEX IF NOT EXISTS idx_runtime_runs_task_logged_at ON runtime_runs(task_id, logged_at DESC)"
        )
        self._db.execute(
            "CREATE INDEX IF NOT EXISTS idx_runtime_runs_conversation_logged_at ON runtime_runs(conversation_id, logged_at DESC)"
        )
        self._db.execute(
            "CREATE INDEX IF NOT EXISTS idx_runtime_runs_role_logged_at ON runtime_runs(role, logged_at DESC)"
        )
        self._db.execute(
            "CREATE INDEX IF NOT EXISTS idx_runtime_tool_calls_task_timestamp ON runtime_tool_calls(task_id, timestamp DESC)"
        )
        self._db.execute(
            "CREATE INDEX IF NOT EXISTS idx_runtime_tool_calls_tool_timestamp ON runtime_tool_calls(tool_name, timestamp DESC)"
        )
        self._db.execute(
            "CREATE INDEX IF NOT EXISTS idx_runtime_tool_calls_run_id ON runtime_tool_calls(run_id)"
        )
        self._db.execute(
            "CREATE INDEX IF NOT EXISTS idx_runtime_tool_refs_task_timestamp ON runtime_tool_refs(task_id, timestamp DESC)"
        )
        self._db.execute(
            "CREATE INDEX IF NOT EXISTS idx_runtime_task_context_refs_task_ordinal ON runtime_task_context_refs(task_id, ordinal ASC)"
        )
        self._db.execute(
            "CREATE INDEX IF NOT EXISTS idx_conversation_events_conversation_created_at ON conversation_events(conversation_id, created_at DESC)"
        )
        self._db.execute(
            "CREATE INDEX IF NOT EXISTS idx_conversation_work_queue_dirty_updated_at ON conversation_work_queue(dirty, updated_at ASC)"
        )
        self._db.execute(
            "CREATE INDEX IF NOT EXISTS idx_inbox_messages_pending_conversation ON inbox_messages(processed, silent, conversation_id, created_at ASC)"
        )
        self._db.execute(
            "CREATE INDEX IF NOT EXISTS idx_inbox_messages_claimed_by ON inbox_messages(claimed_by, conversation_id, created_at ASC)"
        )
        self._db.execute(
            "CREATE INDEX IF NOT EXISTS idx_user_profiles_person_scope ON user_profiles(person_id, conversation_id, profile_key)"
        )
        self._db.execute(
            "CREATE INDEX IF NOT EXISTS idx_user_profiles_legacy_scope ON user_profiles(gateway, user_id, conversation_id, profile_key)"
        )
        self._ensure_column(
            "tasks",
            "task_type",
            "TEXT NOT NULL DEFAULT 'general'",
        )
        self._ensure_column(
            "tasks",
            "notify_main_on_finish",
            "INTEGER NOT NULL DEFAULT 1",
        )
        self._ensure_column(
            "tasks",
            "then_task_types",
            "TEXT NOT NULL DEFAULT '[]'",
        )
        self._ensure_column(
            "inbox_messages",
            "claimed_by",
            "TEXT NOT NULL DEFAULT ''",
        )
        self._ensure_column(
            "inbox_messages",
            "claim_expires_at",
            "TEXT NOT NULL DEFAULT ''",
        )
        self._ensure_column(
            "inbox_messages",
            "processed_by",
            "TEXT NOT NULL DEFAULT ''",
        )
        self._ensure_column(
            "inbox_messages",
            "processed_at",
            "TEXT NOT NULL DEFAULT ''",
        )
        self._ensure_column(
            "tasks",
            "gateway",
            "TEXT NOT NULL DEFAULT ''",
        )
        self._ensure_column(
            "tasks",
            "user_id",
            "TEXT NOT NULL DEFAULT ''",
        )
        self._ensure_column(
            "tasks",
            "person_id",
            "TEXT NOT NULL DEFAULT ''",
        )
        self._ensure_column(
            "tasks",
            "message_id",
            "TEXT NOT NULL DEFAULT ''",
        )
        self._ensure_column(
            "tasks",
            "reply_to_message_id",
            "TEXT NOT NULL DEFAULT ''",
        )
        self._ensure_column(
            "tasks",
            "parent_task_id",
            "TEXT NOT NULL DEFAULT ''",
        )
        self._ensure_column(
            "tasks",
            "is_admin",
            "INTEGER NOT NULL DEFAULT 0",
        )
        self._ensure_column(
            "inbox_messages",
            "person_id",
            "TEXT NOT NULL DEFAULT ''",
        )
        self._ensure_column(
            "conversation_states",
            "person_id",
            "TEXT NOT NULL DEFAULT ''",
        )
        self._ensure_column(
            "conversation_states",
            "is_multi_party",
            "INTEGER NOT NULL DEFAULT 0",
        )
        self._ensure_column(
            "conversation_states",
            "active_topic_id",
            "TEXT NOT NULL DEFAULT ''",
        )
        self._ensure_column(
            "conversation_states",
            "topics",
            "TEXT NOT NULL DEFAULT '[]'",
        )
        self._ensure_column(
            "conversation_states",
            "enabled_skills",
            "TEXT NOT NULL DEFAULT '[]'",
        )
        self._ensure_column(
            "conversation_states",
            "session_constraints",
            "TEXT NOT NULL DEFAULT '[]'",
        )
        self._ensure_column(
            "conversation_states",
            "session_facts",
            "TEXT NOT NULL DEFAULT '[]'",
        )
        self._ensure_column(
            "conversation_states",
            "last_delivery",
            "TEXT NOT NULL DEFAULT ''",
        )
        self._ensure_column(
            "tasks",
            "topic_id",
            "TEXT NOT NULL DEFAULT ''",
        )
        self._ensure_column(
            "tasks",
            "outcome_json",
            "TEXT NOT NULL DEFAULT '{}'",
        )
        self._ensure_column(
            "user_profiles",
            "person_id",
            "TEXT NOT NULL DEFAULT ''",
        )
        self._db.execute("UPDATE memories SET layer = 'archive' WHERE layer IN ('l1', 'l2')")
        self._db.commit()

    def _ensure_column(self, table: str, column: str, spec: str) -> bool:
        rows = self._db.execute(f"PRAGMA table_info({table})").fetchall()
        existing = {str(row[1]) for row in rows}
        if column in existing:
            return False
        self._db.execute(f"ALTER TABLE {table} ADD COLUMN {column} {spec}")
        return True

    def setting_read(self, key: str, default: str = "") -> str:
        row = self._db.execute(
            "SELECT value FROM settings WHERE key = ?",
            (key.strip(),),
        ).fetchone()
        if row is None:
            return default
        return str(row["value"])

    def setting_write(self, key: str, value: str) -> None:
        self._db.execute(
            "INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)",
            (key.strip(), value),
        )
        self._db.commit()

    def enabled_skills_read(self) -> list[str]:
        stored = self.setting_read("enabled_skills", "")
        if stored:
            return _normalize_unique_texts(_json_loads(stored, []), limit=128)
        defaults = _normalize_unique_texts(_default_enabled_skills(), limit=128)
        self.setting_write("enabled_skills", _json_dumps(defaults))
        return defaults

    def enabled_skills_write(self, skills: list[str]) -> list[str]:
        normalized = _normalize_unique_texts(skills, limit=128)
        self.setting_write("enabled_skills", _json_dumps(normalized))
        return normalized

    # ------------------------------------------------------------------
    # Memory CRUD
    # ------------------------------------------------------------------

    def memory_append_task_result(
        self,
        memory_id: str,
        *,
        task_id: str,
        summary: str,
    ) -> bool:
        """Append a task result to a memory's task_results array."""
        row = self._db.execute(
            "SELECT task_results FROM memories WHERE id = ?", (memory_id,)
        ).fetchone()
        if row is None:
            return False
        results = _json_loads(row["task_results"], [])
        results.append({
            "task_id": task_id,
            "summary": summary,
            "time": _now_iso(),
        })
        self._db.execute(
            "UPDATE memories SET task_results = ?, updated_at = ? WHERE id = ?",
            (_json_dumps(results), _now_iso(), memory_id),
        )
        self._db.commit()
        return True

    # ------------------------------------------------------------------
    # Task CRUD
    # ------------------------------------------------------------------

    def task_create(
        self,
        goal: str,
        *,
        task_type: str = "general",
        notify_main_on_finish: bool,
        topic_id: str = "",
        gateway: str = "",
        conversation_id: str = "",
        user_id: str = "",
        person_id: str = "",
        message_id: str = "",
        reply_to_message_id: str = "",
        parent_task_id: str = "",
        then: list[str] | None = None,
        then_task_types: list[str] | None = None,
        images: list[str] | None = None,
        is_admin: bool = False,
        memory_id: str = "",
    ) -> dict[str, Any]:
        """Create a new task. Returns {id, status, goal, ...}."""
        tid = _gen_id("t")
        now = _now_iso()
        self._db.execute(
            """INSERT INTO tasks
               (id, status, task_type, notify_main_on_finish, goal, topic_id, gateway, conversation_id, user_id, person_id, message_id,
                reply_to_message_id, parent_task_id, then_chain, then_task_types, images,
                is_admin, result, outcome_json, memory_id, created_at, updated_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                tid, "pending", task_type, 1 if notify_main_on_finish else 0, goal,
                topic_id,
                gateway,
                conversation_id,
                user_id,
                person_id,
                message_id,
                reply_to_message_id,
                parent_task_id,
                _json_dumps(then or []),
                _json_dumps(then_task_types or []),
                _json_dumps(images or []),
                1 if is_admin else 0,
                "", _json_dumps({}), memory_id,
                now, now,
            ),
        )
        self._db.commit()
        return self.task_read(tid)  # type: ignore[return-value]

    def task_read(self, task_id: str) -> dict[str, Any] | None:
        """Read a single task by ID."""
        row = self._db.execute(
            "SELECT * FROM tasks WHERE id = ?", (task_id,)
        ).fetchone()
        if row is None:
            return None
        return self._row_to_task(row)

    def task_list(
        self, status: str = "", limit: int = 50
    ) -> list[dict[str, Any]]:
        """List tasks, optionally filtered by status."""
        if status:
            rows = self._db.execute(
                "SELECT * FROM tasks WHERE status = ? ORDER BY created_at DESC LIMIT ?",
                (status, limit),
            ).fetchall()
        else:
            rows = self._db.execute(
                "SELECT * FROM tasks ORDER BY created_at DESC LIMIT ?",
                (limit,),
            ).fetchall()
        return [self._row_to_task(r) for r in rows]

    def task_update(self, task_id: str, **fields: Any) -> bool:
        """Update task fields. Supported: status, result, then_chain, memory_id, routing fields."""
        allowed = {
            "status",
            "result",
            "outcome_json",
            "then_chain",
            "then_task_types",
            "notify_main_on_finish",
            "memory_id",
            "goal",
            "task_type",
            "gateway",
            "conversation_id",
            "user_id",
            "person_id",
            "message_id",
            "reply_to_message_id",
            "parent_task_id",
            "topic_id",
        }
        sets: list[str] = []
        params: list[Any] = []
        for k, v in fields.items():
            if k not in allowed:
                continue
            if k in {"then_chain", "then_task_types", "outcome_json"}:
                v = _json_dumps(v) if isinstance(v, list) else v
            if k == "outcome_json" and isinstance(v, dict):
                v = _json_dumps(v)
            sets.append(f"{k} = ?")
            params.append(v)
        if not sets:
            return False
        sets.append("updated_at = ?")
        params.append(_now_iso())
        params.append(task_id)
        result = self._db.execute(
            f"UPDATE tasks SET {', '.join(sets)} WHERE id = ?", params
        )
        self._db.commit()
        return result.rowcount > 0

    def task_complete(
        self,
        task_id: str,
        result: str,
        *,
        outcome: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        """Mark task as done and write result. Also auto-writes to associated memory."""
        task = self.task_read(task_id)
        if task is None:
            return None
        normalized_outcome = normalize_task_outcome(
            outcome,
            task_type=str(task.get("task_type", "")).strip(),
            result=result,
        )
        self.task_update(task_id, status="done", result=result, outcome_json=normalized_outcome)
        # Auto-append task result to associated memory
        if task["memory_id"]:
            self.memory_append_task_result(
                task["memory_id"],
                task_id=task_id,
                summary=_head_tail_summary(strip_structured_outcome_block(result), 500) if result else "",
            )
        return self.task_read(task_id)

    def task_stop(self, task_id: str) -> dict[str, Any] | None:
        """Mark task as stopped."""
        self.task_update(task_id, status="stopped")
        return self.task_read(task_id)

    @staticmethod
    def _row_to_task(row: sqlite3.Row) -> dict[str, Any]:
        return {
            "id": row["id"],
            "status": row["status"],
            "task_type": row["task_type"] if "task_type" in row.keys() else "general",
            "notify_main_on_finish": (
                bool(row["notify_main_on_finish"])
                if "notify_main_on_finish" in row.keys()
                else True
            ),
            "goal": row["goal"],
            "topic_id": row["topic_id"] if "topic_id" in row.keys() else "",
            "gateway": row["gateway"] if "gateway" in row.keys() else "",
            "conversation_id": row["conversation_id"],
            "user_id": row["user_id"] if "user_id" in row.keys() else "",
            "person_id": row["person_id"] if "person_id" in row.keys() else "",
            "message_id": row["message_id"] if "message_id" in row.keys() else "",
            "reply_to_message_id": (
                row["reply_to_message_id"] if "reply_to_message_id" in row.keys() else ""
            ),
            "parent_task_id": row["parent_task_id"] if "parent_task_id" in row.keys() else "",
            "then_chain": _json_loads(row["then_chain"], []),
            "then_task_types": _json_loads(
                row["then_task_types"] if "then_task_types" in row.keys() else "[]",
                [],
            ),
            "images": _json_loads(row["images"], []),
            "is_admin": bool(row["is_admin"]) if "is_admin" in row.keys() else False,
            "result": row["result"],
            "outcome_json": _json_loads(row["outcome_json"] if "outcome_json" in row.keys() else "{}", {}),
            "memory_id": row["memory_id"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    # ------------------------------------------------------------------
    # Inbox CRUD
    # ------------------------------------------------------------------

    def inbox_create(
        self,
        *,
        gateway: str,
        conversation_id: str = "",
        message_id: str = "",
        user_id: str = "",
        person_id: str = "",
        user_name: str = "",
        is_admin: bool = False,
        content: str = "",
        metadata: dict[str, Any] | None = None,
        attachments: list[dict[str, Any]] | None = None,
        silent: bool = False,
    ) -> dict[str, Any]:
        """Create a new inbox message. Returns the created record."""
        iid = _gen_id("inbox")
        now = _now_iso()
        self._db.execute(
            """INSERT INTO inbox_messages
               (id, gateway, conversation_id, message_id, user_id, person_id, user_name,
                is_admin, content, metadata, attachments, silent, processed,
                created_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                iid, gateway, conversation_id, message_id,
                user_id, person_id, user_name,
                1 if is_admin else 0,
                content,
                _json_dumps(metadata or {}),
                _json_dumps(attachments or []),
                1 if silent else 0,
                0,
                now,
            ),
        )
        self._db.commit()
        return self.inbox_read(iid)  # type: ignore[return-value]

    def inbox_read(self, inbox_id: str) -> dict[str, Any] | None:
        """Read a single inbox message."""
        row = self._db.execute(
            "SELECT * FROM inbox_messages WHERE id = ?", (inbox_id,)
        ).fetchone()
        if row is None:
            return None
        return self._row_to_inbox(row)

    def inbox_list_unprocessed(
        self,
        silent: bool | None = None,
        *,
        limit: int = 0,
        include_claimed: bool = True,
        prioritize_admin: bool = False,
    ) -> list[dict[str, Any]]:
        """List unprocessed inbox messages.

        Args:
            silent: None=all, True=only silent, False=only non-silent.
            limit: max returned rows; <=0 means no limit.
            prioritize_admin: place admin messages first while keeping FIFO inside same priority.
        """
        where = "processed = 0"
        params: list[Any] = []
        if silent is not None:
            where += " AND silent = ?"
            params.append(1 if silent else 0)
        if not include_claimed:
            now = _now_iso()
            where += " AND (claimed_by = '' OR claim_expires_at <= ?)"
            params.append(now)

        if prioritize_admin:
            order_by = "is_admin DESC, created_at ASC"
        else:
            order_by = "created_at ASC"

        sql = f"SELECT * FROM inbox_messages WHERE {where} ORDER BY {order_by}"
        if limit > 0:
            sql += " LIMIT ?"
            params.append(int(limit))

        rows = self._db.execute(sql, tuple(params)).fetchall()
        return [self._row_to_inbox(r) for r in rows]

    def inbox_mark_processed(self, inbox_id: str) -> bool:
        """Mark an inbox message as processed."""
        now = _now_iso()
        result = self._db.execute(
            """UPDATE inbox_messages
               SET processed = 1,
                   claimed_by = '',
                   claim_expires_at = '',
                   processed_at = CASE WHEN processed_at = '' THEN ? ELSE processed_at END
               WHERE id = ? AND processed = 0""",
            (now, inbox_id),
        )
        self._db.commit()
        return result.rowcount > 0

    def inbox_count_for_conversation(
        self,
        *,
        gateway: str,
        conversation_id: str,
        only_non_silent: bool = True,
    ) -> int:
        where = ["gateway = ?", "conversation_id = ?"]
        params: list[Any] = [gateway, conversation_id]
        if only_non_silent:
            where.append("silent = 0")
        row = self._db.execute(
            f"SELECT COUNT(*) FROM inbox_messages WHERE {' AND '.join(where)}",
            tuple(params),
        ).fetchone()
        return int(row[0] if row else 0)

    def inbox_set_silent(self, inbox_id: str, silent: bool) -> bool:
        """Update silent flag for a single inbox message."""
        result = self._db.execute(
            "UPDATE inbox_messages SET silent = ? WHERE id = ?",
            (1 if silent else 0, inbox_id),
        )
        self._db.commit()
        return result.rowcount > 0

    def inbox_list_for_conversation(
        self,
        conversation_id: str,
        *,
        limit: int = 10,
    ) -> list[dict[str, Any]]:
        rows = self._db.execute(
            """SELECT * FROM inbox_messages
               WHERE conversation_id = ?
               ORDER BY created_at DESC
               LIMIT ?""",
            (conversation_id.strip(), max(1, int(limit))),
        ).fetchall()
        return [self._row_to_inbox(row) for row in rows]

    def inbox_list_unprocessed_conversations(
        self,
        *,
        include_silent: bool = False,
    ) -> list[str]:
        where = ["processed = 0"]
        params: list[Any] = []
        if not include_silent:
            where.append("silent = 0")
        rows = self._db.execute(
            f"""SELECT DISTINCT conversation_id
                FROM inbox_messages
                WHERE {' AND '.join(where)} AND conversation_id != ''
                ORDER BY conversation_id ASC""",
            tuple(params),
        ).fetchall()
        return [str(row["conversation_id"]).strip() for row in rows if str(row["conversation_id"]).strip()]

    @staticmethod
    def _row_to_inbox(row: sqlite3.Row) -> dict[str, Any]:
        return {
            "id": row["id"],
            "gateway": row["gateway"],
            "conversation_id": row["conversation_id"],
            "message_id": row["message_id"],
            "user_id": row["user_id"],
            "person_id": row["person_id"] if "person_id" in row.keys() else "",
            "user_name": row["user_name"],
            "is_admin": bool(row["is_admin"]),
            "content": row["content"],
            "metadata": _json_loads(row["metadata"], {}),
            "attachments": _json_loads(row["attachments"], []),
            "silent": bool(row["silent"]),
            "processed": bool(row["processed"]),
            "claimed_by": row["claimed_by"] if "claimed_by" in row.keys() else "",
            "claim_expires_at": row["claim_expires_at"] if "claim_expires_at" in row.keys() else "",
            "processed_by": row["processed_by"] if "processed_by" in row.keys() else "",
            "processed_at": row["processed_at"] if "processed_at" in row.keys() else "",
            "created_at": row["created_at"],
        }

    # ------------------------------------------------------------------
    # Conversation Work Queue
    # ------------------------------------------------------------------

    def conversation_work_read(self, conversation_id: str) -> dict[str, Any] | None:
        normalized_conversation_id = conversation_id.strip()
        if not normalized_conversation_id:
            return None
        row = self._db.execute(
            "SELECT * FROM conversation_work_queue WHERE conversation_id = ?",
            (normalized_conversation_id,),
        ).fetchone()
        if row is None:
            return None
        return self._row_to_conversation_work(row)

    def conversation_work_list_claimed(self) -> list[dict[str, Any]]:
        rows = self._db.execute(
            """SELECT *
               FROM conversation_work_queue
               WHERE claimed_by != ''
               ORDER BY updated_at ASC"""
        ).fetchall()
        return [self._row_to_conversation_work(row) for row in rows]

    def conversation_work_touch(
        self,
        conversation_id: str,
        *,
        gateway: str = "",
        user_id: str = "",
        completed_task_id: str = "",
    ) -> dict[str, Any]:
        normalized_conversation_id = conversation_id.strip()
        if not normalized_conversation_id:
            raise ValueError("conversation_id cannot be empty")
        normalized_completed_task_id = completed_task_id.strip()
        now = _now_iso()
        self._db.execute("BEGIN IMMEDIATE")
        try:
            current = self.conversation_work_read(normalized_conversation_id)
            if current is None:
                pending_task_ids = [normalized_completed_task_id] if normalized_completed_task_id else []
                self._db.execute(
                    """INSERT INTO conversation_work_queue
                       (conversation_id, gateway, user_id, dirty, work_version, pending_task_ids,
                        claimed_by, claim_expires_at, last_started_at, last_finished_at,
                        created_at, updated_at)
                       VALUES (?, ?, ?, 1, 1, ?, '', '', '', '', ?, ?)""",
                    (
                        normalized_conversation_id,
                        gateway.strip(),
                        user_id.strip(),
                        _json_dumps(pending_task_ids),
                        now,
                        now,
                    ),
                )
            else:
                pending_task_ids = list(current.get("pending_task_ids", []) or [])
                if normalized_completed_task_id and normalized_completed_task_id not in pending_task_ids:
                    pending_task_ids.append(normalized_completed_task_id)
                self._db.execute(
                    """UPDATE conversation_work_queue
                       SET gateway = ?,
                           user_id = ?,
                           dirty = 1,
                           work_version = ?,
                           pending_task_ids = ?,
                           updated_at = ?
                       WHERE conversation_id = ?""",
                    (
                        gateway.strip() or str(current.get("gateway", "")).strip(),
                        user_id.strip() or str(current.get("user_id", "")).strip(),
                        int(current.get("work_version", 0)) + 1,
                        _json_dumps(pending_task_ids),
                        now,
                        normalized_conversation_id,
                    ),
                )
            self._db.commit()
            work = self.conversation_work_read(normalized_conversation_id)
            if work is None:
                raise RuntimeError("failed to touch conversation work")
            return work
        except Exception:
            self._db.rollback()
            raise

    def conversation_work_ensure_backlog(
        self,
        conversation_id: str,
        *,
        gateway: str = "",
        user_id: str = "",
    ) -> dict[str, Any] | None:
        normalized_conversation_id = conversation_id.strip()
        if not normalized_conversation_id:
            return None
        now = _now_iso()
        self._db.execute("BEGIN IMMEDIATE")
        try:
            current = self.conversation_work_read(normalized_conversation_id)
            if current is None:
                self._db.execute(
                    """INSERT INTO conversation_work_queue
                       (conversation_id, gateway, user_id, dirty, work_version, pending_task_ids,
                        claimed_by, claim_expires_at, last_started_at, last_finished_at,
                        created_at, updated_at)
                       VALUES (?, ?, ?, 1, 1, '[]', '', '', '', '', ?, ?)""",
                    (
                        normalized_conversation_id,
                        gateway.strip(),
                        user_id.strip(),
                        now,
                        now,
                    ),
                )
                self._db.commit()
                return self.conversation_work_read(normalized_conversation_id)
            claim_expires_at = str(current.get("claim_expires_at", "")).strip()
            claimed_by = str(current.get("claimed_by", "")).strip()
            if bool(current.get("dirty", False)) or (claimed_by and claim_expires_at > now):
                self._db.rollback()
                return current
            self._db.execute(
                """UPDATE conversation_work_queue
                   SET gateway = ?,
                       user_id = ?,
                       dirty = 1,
                       work_version = ?,
                       updated_at = ?
                   WHERE conversation_id = ?""",
                (
                    gateway.strip() or str(current.get("gateway", "")).strip(),
                    user_id.strip() or str(current.get("user_id", "")).strip(),
                    int(current.get("work_version", 0)) + 1,
                    now,
                    normalized_conversation_id,
                ),
            )
            self._db.commit()
            return self.conversation_work_read(normalized_conversation_id)
        except Exception:
            self._db.rollback()
            raise

    def conversation_work_claim(
        self,
        agent_key: str,
        *,
        lease_seconds: int,
        inbox_limit: int = 10,
        prioritize_admin: bool = False,
        exclude_conversation_ids: set[str] | None = None,
    ) -> dict[str, Any] | None:
        normalized_agent_key = agent_key.strip()
        if not normalized_agent_key:
            raise ValueError("agent_key cannot be empty")
        excluded_conversation_ids = {
            str(item).strip()
            for item in (exclude_conversation_ids or set())
            if str(item).strip()
        }
        candidate_rows = self._db.execute(
            """SELECT conversation_id
               FROM conversation_work_queue
               WHERE dirty = 1
               ORDER BY updated_at ASC
               LIMIT 32"""
        ).fetchall()
        order_by = "is_admin DESC, created_at ASC" if prioritize_admin else "created_at ASC"
        for row in candidate_rows:
            conversation_id = str(row["conversation_id"]).strip()
            if not conversation_id:
                continue
            if conversation_id in excluded_conversation_ids:
                continue
            now = _now_iso()
            lease_until = _iso_after_seconds(lease_seconds)
            self._db.execute("BEGIN IMMEDIATE")
            try:
                current = self.conversation_work_read(conversation_id)
                if current is None or not bool(current.get("dirty", False)):
                    self._db.rollback()
                    continue
                claimed_by = str(current.get("claimed_by", "")).strip()
                claim_expires_at = str(current.get("claim_expires_at", "")).strip()
                if claimed_by and claim_expires_at > now:
                    self._db.rollback()
                    continue

                inbox_rows = self._db.execute(
                    f"""SELECT *
                        FROM inbox_messages
                        WHERE conversation_id = ?
                          AND processed = 0
                          AND silent = 0
                          AND (claimed_by = '' OR claim_expires_at <= ?)
                        ORDER BY {order_by}
                        LIMIT ?""",
                    (conversation_id, now, max(1, int(inbox_limit))),
                ).fetchall()
                inbox_items = [self._row_to_inbox(item) for item in inbox_rows]
                inbox_ids = [str(item.get("id", "")).strip() for item in inbox_items if str(item.get("id", "")).strip()]
                pending_task_ids = list(current.get("pending_task_ids", []) or [])

                if not inbox_ids and not pending_task_ids:
                    self._db.execute(
                        """UPDATE conversation_work_queue
                           SET dirty = 0,
                               claimed_by = '',
                               claim_expires_at = '',
                               updated_at = ?
                           WHERE conversation_id = ?""",
                        (now, conversation_id),
                    )
                    self._db.commit()
                    continue

                if inbox_ids:
                    placeholders = ",".join("?" for _ in inbox_ids)
                    self._db.execute(
                        f"""UPDATE inbox_messages
                            SET claimed_by = ?, claim_expires_at = ?
                            WHERE id IN ({placeholders})
                              AND processed = 0
                              AND silent = 0
                              AND (claimed_by = '' OR claim_expires_at <= ?)""",
                        (normalized_agent_key, lease_until, *inbox_ids, now),
                    )

                gateway = str(current.get("gateway", "")).strip()
                user_id = str(current.get("user_id", "")).strip()
                if inbox_items:
                    gateway = str(inbox_items[0].get("gateway", "")).strip() or gateway
                    user_id = str(inbox_items[0].get("user_id", "")).strip() or user_id

                self._db.execute(
                    """UPDATE conversation_work_queue
                       SET gateway = ?,
                           user_id = ?,
                           claimed_by = ?,
                           claim_expires_at = ?,
                           last_started_at = ?,
                           updated_at = ?
                       WHERE conversation_id = ?""",
                    (
                        gateway,
                        user_id,
                        normalized_agent_key,
                        lease_until,
                        now,
                        now,
                        conversation_id,
                    ),
                )
                self._db.commit()

                wake_mode = "wake"
                source_topic = "conversation.work"
                if pending_task_ids and not inbox_ids:
                    wake_mode = "task_done" if len(pending_task_ids) == 1 else "task_done_batch"
                    source_topic = "agent.task_done"
                return {
                    "conversation_id": conversation_id,
                    "gateway": gateway,
                    "user_id": user_id,
                    "work_version": int(current.get("work_version", 0)),
                    "claimed_by": normalized_agent_key,
                    "claim_expires_at": lease_until,
                    "inbox_ids": inbox_ids,
                    "completed_task_ids": pending_task_ids,
                    "wake_mode": wake_mode,
                    "source_topic": source_topic,
                }
            except Exception:
                self._db.rollback()
                raise
        return None

    def conversation_work_finish(
        self,
        agent_key: str,
        conversation_id: str,
        *,
        mark_inbox_processed: bool,
        consumed_task_ids: list[str] | None = None,
    ) -> dict[str, Any]:
        normalized_agent_key = agent_key.strip()
        normalized_conversation_id = conversation_id.strip()
        if not normalized_agent_key or not normalized_conversation_id:
            raise ValueError("agent_key and conversation_id are required")
        consumed = [
            str(item).strip()
            for item in (consumed_task_ids or [])
            if str(item).strip()
        ]
        now = _now_iso()
        self._db.execute("BEGIN IMMEDIATE")
        try:
            current = self.conversation_work_read(normalized_conversation_id)
            if current is None:
                self._db.rollback()
                return {"processed_inbox_items": [], "dirty": False}

            claimed_rows = self._db.execute(
                """SELECT * FROM inbox_messages
                   WHERE conversation_id = ?
                     AND processed = 0
                     AND claimed_by = ?
                   ORDER BY created_at ASC""",
                (normalized_conversation_id, normalized_agent_key),
            ).fetchall()
            claimed_items = [self._row_to_inbox(row) for row in claimed_rows]
            processed_items = claimed_items if mark_inbox_processed else []

            if mark_inbox_processed and claimed_items:
                claimed_ids = [str(item.get("id", "")).strip() for item in claimed_items]
                placeholders = ",".join("?" for _ in claimed_ids)
                self._db.execute(
                    f"""UPDATE inbox_messages
                        SET processed = 1,
                            claimed_by = '',
                            claim_expires_at = '',
                            processed_by = ?,
                            processed_at = ?
                        WHERE id IN ({placeholders})""",
                    (normalized_agent_key, now, *claimed_ids),
                )

            self._db.execute(
                """UPDATE inbox_messages
                   SET claimed_by = '', claim_expires_at = ''
                   WHERE conversation_id = ?
                     AND processed = 0
                     AND claimed_by = ?""",
                (normalized_conversation_id, normalized_agent_key),
            )

            pending_task_ids = [
                item
                for item in list(current.get("pending_task_ids", []) or [])
                if item not in set(consumed)
            ]
            pending_inbox_row = self._db.execute(
                """SELECT 1
                   FROM inbox_messages
                   WHERE conversation_id = ?
                     AND processed = 0
                     AND silent = 0
                   LIMIT 1""",
                (normalized_conversation_id,),
            ).fetchone()
            dirty_after = bool(pending_task_ids) or pending_inbox_row is not None
            self._db.execute(
                """UPDATE conversation_work_queue
                   SET pending_task_ids = ?,
                       dirty = ?,
                       claimed_by = '',
                       claim_expires_at = '',
                       last_finished_at = ?,
                       updated_at = ?
                   WHERE conversation_id = ?
                     AND claimed_by = ?""",
                (
                    _json_dumps(pending_task_ids),
                    1 if dirty_after else 0,
                    now,
                    now,
                    normalized_conversation_id,
                    normalized_agent_key,
                ),
            )
            self._db.commit()
            return {
                "processed_inbox_items": processed_items,
                "dirty": dirty_after,
            }
        except Exception:
            self._db.rollback()
            raise

    @staticmethod
    def _row_to_conversation_work(row: sqlite3.Row) -> dict[str, Any]:
        return {
            "conversation_id": row["conversation_id"],
            "gateway": row["gateway"],
            "user_id": row["user_id"],
            "dirty": bool(row["dirty"]),
            "work_version": _as_int(row["work_version"]),
            "pending_task_ids": _json_loads(row["pending_task_ids"], []),
            "claimed_by": row["claimed_by"],
            "claim_expires_at": row["claim_expires_at"],
            "last_started_at": row["last_started_at"],
            "last_finished_at": row["last_finished_at"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    # ------------------------------------------------------------------
    # Conversation State + Events
    # ------------------------------------------------------------------

    def conversation_state_read(self, conversation_id: str) -> dict[str, Any] | None:
        normalized_conversation_id = conversation_id.strip()
        if not normalized_conversation_id:
            return None
        row = self._db.execute(
            "SELECT * FROM conversation_states WHERE conversation_id = ?",
            (normalized_conversation_id,),
        ).fetchone()
        if row is None:
            return None
        return self._row_to_conversation_state(row)

    def conversation_state_upsert(
        self,
        conversation_id: str,
        *,
        gateway: str = "",
        user_id: str = "",
        person_id: str = "",
        is_multi_party: bool | None = None,
        active_topic_id: str | None = None,
        topics: list[dict[str, Any]] | None = None,
        enabled_skills: list[str] | None = None,
        session_constraints: list[str] | None = None,
        session_facts: list[str] | None = None,
        active_task_ids: list[str] | None = None,
        last_user_message_id: str | None = None,
        last_bot_message_id: str | None = None,
        recent_memory_ids: list[str] | None = None,
        expected_version: int | None = None,
        # Legacy params kept for call-site compat but ignored:
        current_focus: str | None = None,
        resolved_entities: list[str] | None = None,
        open_questions: list[str] | None = None,
        last_delivery: str | None = None,
        last_result_summary: str | None = None,
    ) -> dict[str, Any]:
        normalized_conversation_id = conversation_id.strip()
        if not normalized_conversation_id:
            raise ValueError("conversation_id cannot be empty")

        existing = self.conversation_state_read(normalized_conversation_id)
        if existing is None:
            now = _now_iso()
            payload = {
                "conversation_id": normalized_conversation_id,
                "version": 0,
                "gateway": gateway.strip(),
                "user_id": user_id.strip(),
                "person_id": person_id.strip(),
                "is_multi_party": bool(is_multi_party),
                "active_topic_id": active_topic_id or "",
                "topics": normalize_conversation_topics(topics),
                "enabled_skills": _normalize_unique_texts(enabled_skills or [], limit=64),
                "session_constraints": _normalize_unique_texts(session_constraints or [], limit=16),
                "session_facts": _normalize_unique_texts(session_facts or [], limit=16),
                "active_task_ids": active_task_ids or [],
                "last_user_message_id": last_user_message_id or "",
                "last_bot_message_id": last_bot_message_id or "",
                "recent_memory_ids": recent_memory_ids or [],
                "created_at": now,
                "updated_at": now,
            }
            try:
                self._db.execute(
                    """INSERT INTO conversation_states
                       (conversation_id, version, gateway, user_id, person_id, is_multi_party,
                        active_topic_id, topics, enabled_skills, session_constraints, session_facts, current_focus,
                        resolved_entities, open_questions, active_task_ids,
                        last_user_message_id, last_bot_message_id, last_delivery, last_result_summary,
                        recent_memory_ids, created_at, updated_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        payload["conversation_id"],
                        payload["version"],
                        payload["gateway"],
                        payload["user_id"],
                        payload["person_id"],
                        1 if payload["is_multi_party"] else 0,
                        payload["active_topic_id"],
                        _json_dumps(payload["topics"]),
                        _json_dumps(payload["enabled_skills"]),
                        _json_dumps(payload["session_constraints"]),
                        _json_dumps(payload["session_facts"]),
                        "",  # current_focus (legacy)
                        "[]",  # resolved_entities (legacy)
                        "[]",  # open_questions (legacy)
                        _json_dumps(payload["active_task_ids"]),
                        payload["last_user_message_id"],
                        payload["last_bot_message_id"],
                        "",  # last_delivery (legacy)
                        "",  # last_result_summary (legacy)
                        _json_dumps(payload["recent_memory_ids"]),
                        payload["created_at"],
                        payload["updated_at"],
                    ),
                )
                self._db.commit()
                return self.conversation_state_read(normalized_conversation_id)  # type: ignore[return-value]
            except sqlite3.IntegrityError:
                self._db.rollback()
                existing = self.conversation_state_read(normalized_conversation_id)
                if existing is None:
                    raise

        if expected_version is not None and int(existing.get("version", 0)) != int(expected_version):
            raise ValueError("conversation_state version mismatch")

        next_version = int(existing.get("version", 0)) + 1
        updated = {
            "gateway": gateway.strip() or str(existing.get("gateway", "")),
            "user_id": user_id.strip() or str(existing.get("user_id", "")),
            "person_id": person_id.strip() or str(existing.get("person_id", "")),
            "is_multi_party": (
                bool(existing.get("is_multi_party", False))
                if is_multi_party is None
                else bool(is_multi_party)
            ),
            "active_topic_id": (
                str(existing.get("active_topic_id", ""))
                if active_topic_id is None else str(active_topic_id)
            ),
            "topics": (
                normalize_conversation_topics(existing.get("topics", []) or [])
                if topics is None else normalize_conversation_topics(topics)
            ),
            "enabled_skills": (
                existing.get("enabled_skills", [])
                if enabled_skills is None
                else _normalize_unique_texts(enabled_skills, limit=64)
            ),
            "session_constraints": (
                existing.get("session_constraints", [])
                if session_constraints is None
                else _normalize_unique_texts(session_constraints, limit=16)
            ),
            "session_facts": (
                existing.get("session_facts", [])
                if session_facts is None
                else _normalize_unique_texts(session_facts, limit=16)
            ),
            "active_task_ids": existing.get("active_task_ids", []) if active_task_ids is None else active_task_ids,
            "last_user_message_id": str(existing.get("last_user_message_id", "")) if last_user_message_id is None else last_user_message_id,
            "last_bot_message_id": str(existing.get("last_bot_message_id", "")) if last_bot_message_id is None else last_bot_message_id,
            "recent_memory_ids": existing.get("recent_memory_ids", []) if recent_memory_ids is None else recent_memory_ids,
        }
        query = """UPDATE conversation_states
               SET version = ?, gateway = ?, user_id = ?, person_id = ?, is_multi_party = ?,
                   active_topic_id = ?, topics = ?, enabled_skills = ?, session_constraints = ?, session_facts = ?,
                   active_task_ids = ?,
                   last_user_message_id = ?, last_bot_message_id = ?,
                   recent_memory_ids = ?, updated_at = ?
               WHERE conversation_id = ?"""
        params: list[Any] = [
            next_version,
            updated["gateway"],
            updated["user_id"],
            updated["person_id"],
            1 if updated["is_multi_party"] else 0,
            updated["active_topic_id"],
            _json_dumps(updated["topics"]),
            _json_dumps(updated["enabled_skills"]),
            _json_dumps(updated["session_constraints"]),
            _json_dumps(updated["session_facts"]),
            _json_dumps(updated["active_task_ids"]),
            updated["last_user_message_id"],
            updated["last_bot_message_id"],
            _json_dumps(updated["recent_memory_ids"]),
            _now_iso(),
            normalized_conversation_id,
        ]
        if expected_version is not None:
            query += " AND version = ?"
            params.append(int(expected_version))
        cursor = self._db.execute(query, tuple(params))
        if expected_version is not None and cursor.rowcount != 1:
            self._db.rollback()
            raise ValueError("conversation_state version mismatch")
        self._db.commit()
        return self.conversation_state_read(normalized_conversation_id)  # type: ignore[return-value]

    def conversation_state_apply(
        self,
        conversation_id: str,
        updater: Callable[[dict[str, Any]], dict[str, Any] | None],
        *,
        gateway: str = "",
        user_id: str = "",
        person_id: str = "",
        is_multi_party: bool | None = None,
        max_retries: int = 6,
    ) -> dict[str, Any]:
        normalized_conversation_id = conversation_id.strip()
        if not normalized_conversation_id:
            raise ValueError("conversation_id cannot be empty")
        retries = max(1, int(max_retries))
        last_exc: Exception | None = None
        for _ in range(retries):
            current = self.conversation_state_read(normalized_conversation_id)
            if current is None:
                current = self.conversation_state_upsert(
                    normalized_conversation_id,
                    gateway=gateway,
                    user_id=user_id,
                    person_id=person_id,
                    is_multi_party=is_multi_party,
                )
            expected_version = int(current.get("version", 0))
            patch = updater(dict(current))
            if patch is None:
                return current
            try:
                return self.conversation_state_upsert(
                    normalized_conversation_id,
                    gateway=gateway,
                    user_id=user_id,
                    person_id=str(patch.get("person_id", "")).strip() or person_id,
                    is_multi_party=(
                        bool(patch["is_multi_party"])
                        if "is_multi_party" in patch
                        else is_multi_party
                    ),
                    active_topic_id=patch.get("active_topic_id"),
                    topics=patch.get("topics"),
                    enabled_skills=patch.get("enabled_skills"),
                    session_constraints=patch.get("session_constraints"),
                    session_facts=patch.get("session_facts"),
                    active_task_ids=patch.get("active_task_ids"),
                    last_user_message_id=patch.get("last_user_message_id"),
                    last_bot_message_id=patch.get("last_bot_message_id"),
                    recent_memory_ids=patch.get("recent_memory_ids"),
                    expected_version=expected_version,
                )
            except ValueError as exc:
                if "version mismatch" not in str(exc):
                    raise
                last_exc = exc
                continue
        raise RuntimeError("conversation_state optimistic update failed") from last_exc

    def conversation_event_append(
        self,
        conversation_id: str,
        event_type: str,
        *,
        payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        normalized_conversation_id = conversation_id.strip()
        normalized_event_type = event_type.strip()
        if not normalized_conversation_id or not normalized_event_type:
            raise ValueError("conversation_id and event_type are required")
        event = {
            "id": _gen_id("cev"),
            "conversation_id": normalized_conversation_id,
            "event_type": normalized_event_type,
            "payload": payload or {},
            "created_at": _now_iso(),
        }
        self._db.execute(
            """INSERT INTO conversation_events
               (id, conversation_id, event_type, payload, created_at)
               VALUES (?, ?, ?, ?, ?)""",
            (
                event["id"],
                event["conversation_id"],
                event["event_type"],
                _json_dumps(event["payload"]),
                event["created_at"],
            ),
        )
        self._db.commit()
        return event

    def conversation_event_list(
        self,
        conversation_id: str,
        *,
        limit: int = 20,
    ) -> list[dict[str, Any]]:
        rows = self._db.execute(
            """SELECT * FROM conversation_events
               WHERE conversation_id = ?
               ORDER BY created_at DESC
               LIMIT ?""",
            (conversation_id.strip(), max(1, int(limit))),
        ).fetchall()
        return [
            {
                "id": row["id"],
                "conversation_id": row["conversation_id"],
                "event_type": row["event_type"],
                "payload": _json_loads(row["payload"], {}),
                "created_at": row["created_at"],
            }
            for row in rows
        ]

    def conversation_recent_tasks(
        self,
        conversation_id: str,
        *,
        limit: int = 6,
    ) -> list[dict[str, Any]]:
        rows = self._db.execute(
            """SELECT * FROM tasks
               WHERE conversation_id = ?
               ORDER BY updated_at DESC
               LIMIT ?""",
            (conversation_id.strip(), max(1, int(limit))),
        ).fetchall()
        return [self._row_to_task(row) for row in rows]

    def conversation_history_search(
        self,
        conversation_id: str,
        query: str,
        *,
        limit: int = 5,
    ) -> list[dict[str, Any]]:
        normalized_conversation_id = conversation_id.strip()
        normalized_query = query.strip().lower()
        if not normalized_conversation_id or not normalized_query:
            return []
        query_terms = [term for term in re.split(r"\s+", normalized_query) if term]
        candidates: list[dict[str, Any]] = []

        for inbox in self.inbox_list_for_conversation(normalized_conversation_id, limit=50):
            text = str(inbox.get("content", "")).strip()
            if not text:
                continue
            haystack = text.lower()
            score = sum(haystack.count(term) for term in query_terms)
            if score <= 0:
                continue
            candidates.append({
                "kind": "inbox",
                "id": inbox["id"],
                "created_at": inbox["created_at"],
                "score": score,
                "snippet": _head_tail_summary(text, 300),
                "message_id": inbox.get("message_id", ""),
            })

        for task in self.conversation_recent_tasks(normalized_conversation_id, limit=50):
            text = "\n".join([
                str(task.get("goal", "")).strip(),
                str(task.get("result", "")).strip(),
            ]).strip()
            if not text:
                continue
            haystack = text.lower()
            score = sum(haystack.count(term) for term in query_terms)
            if score <= 0:
                continue
            candidates.append({
                "kind": "task",
                "id": task["id"],
                "created_at": task["updated_at"],
                "score": score,
                "snippet": _head_tail_summary(text, 300),
                "task_type": task.get("task_type", ""),
                "status": task.get("status", ""),
            })

        ranked = sorted(
            candidates,
            key=lambda item: (-int(item.get("score", 0)), str(item.get("created_at", ""))),
        )
        return ranked[:max(1, int(limit))]

    def conversation_recent_window(
        self,
        conversation_id: str,
        *,
        inbox_limit: int = 3,
        task_limit: int = 3,
        event_limit: int = 6,
    ) -> dict[str, Any]:
        normalized_conversation_id = conversation_id.strip()
        if not normalized_conversation_id:
            return {"inbox_messages": [], "tasks": [], "events": []}
        return {
            "inbox_messages": self.inbox_list_for_conversation(
                normalized_conversation_id,
                limit=inbox_limit,
            ),
            "tasks": self.conversation_recent_tasks(
                normalized_conversation_id,
                limit=task_limit,
            ),
            "events": self.conversation_event_list(
                normalized_conversation_id,
                limit=event_limit,
            ),
        }

    @staticmethod
    def _row_to_conversation_state(row: sqlite3.Row) -> dict[str, Any]:
        topics = normalize_conversation_topics(
            _json_loads(row["topics"] if "topics" in row.keys() else "[]", [])
        )
        if not topics:
            legacy_focus = row["current_focus"] if "current_focus" in row.keys() else ""
            if legacy_focus:
                topics = normalize_conversation_topics([{
                    "goal": legacy_focus,
                    "updated_at": row["updated_at"] if "updated_at" in row.keys() else _now_iso(),
                }])
        return {
            "conversation_id": row["conversation_id"],
            "version": _as_int(row["version"]),
            "gateway": row["gateway"],
            "user_id": row["user_id"],
            "person_id": row["person_id"] if "person_id" in row.keys() else "",
            "is_multi_party": bool(row["is_multi_party"]) if "is_multi_party" in row.keys() else False,
            "active_topic_id": row["active_topic_id"] if "active_topic_id" in row.keys() else "",
            "topics": topics,
            "enabled_skills": _json_loads(
                row["enabled_skills"] if "enabled_skills" in row.keys() else "[]",
                [],
            ),
            "session_constraints": _json_loads(
                row["session_constraints"] if "session_constraints" in row.keys() else "[]",
                [],
            ),
            "session_facts": _json_loads(
                row["session_facts"] if "session_facts" in row.keys() else "[]",
                [],
            ),
            "active_task_ids": _json_loads(row["active_task_ids"], []),
            "last_user_message_id": row["last_user_message_id"],
            "last_bot_message_id": row["last_bot_message_id"],
            "recent_memory_ids": _json_loads(row["recent_memory_ids"], []),
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    # ------------------------------------------------------------------
    # User Profile CRUD
    # ------------------------------------------------------------------

    def user_profile_upsert(
        self,
        *,
        person_id: str = "",
        gateway: str = "",
        user_id: str = "",
        profile_key: str,
        profile_value: str,
        conversation_id: str = "",
        source_memory_id: str = "",
        source_message_id: str = "",
        confidence: float = 1.0,
    ) -> dict[str, Any]:
        """Create or update a user profile fact by person scope."""
        now = _now_iso()
        normalized_person_id = person_id.strip()
        normalized_gateway = gateway.strip()
        normalized_user_id = user_id.strip()
        normalized_conversation_id = conversation_id.strip()
        normalized_profile_key = profile_key.strip()
        existing = self._find_user_profile(
            person_id=normalized_person_id,
            gateway=normalized_gateway,
            user_id=normalized_user_id,
            conversation_id=normalized_conversation_id,
            profile_key=normalized_profile_key,
        )
        if existing is not None:
            self._db.execute(
                """UPDATE user_profiles
                   SET gateway = ?,
                       user_id = ?,
                       person_id = ?,
                       profile_value = ?,
                       source_memory_id = ?,
                       source_message_id = ?,
                       confidence = ?,
                       updated_at = ?
                   WHERE id = ?""",
                (
                    normalized_gateway or str(existing.get("gateway", "")),
                    normalized_user_id or str(existing.get("user_id", "")),
                    normalized_person_id or str(existing.get("person_id", "")),
                    profile_value,
                    source_memory_id,
                    source_message_id,
                    confidence,
                    now,
                    str(existing.get("id", "")).strip(),
                ),
            )
        else:
            self._db.execute(
                """INSERT INTO user_profiles
                   (id, gateway, user_id, person_id, conversation_id, profile_key, profile_value,
                    source_memory_id, source_message_id, confidence, created_at, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    _gen_id("up"),
                    normalized_gateway,
                    normalized_user_id,
                    normalized_person_id,
                    normalized_conversation_id,
                    normalized_profile_key,
                    profile_value,
                    source_memory_id,
                    source_message_id,
                    confidence,
                    now,
                    now,
                ),
            )
        self._db.commit()
        row = self._find_user_profile_row(
            person_id=normalized_person_id,
            gateway=normalized_gateway,
            user_id=normalized_user_id,
            conversation_id=normalized_conversation_id,
            profile_key=normalized_profile_key,
        )
        if row is None:
            raise RuntimeError("failed to upsert user profile")
        return self._row_to_user_profile(row)

    def user_profile_list(
        self,
        *,
        person_id: str = "",
        gateway: str = "",
        user_id: str = "",
        conversation_id: str = "",
        profile_key: str = "",
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        """List user profile facts with optional filters."""
        where: list[str] = []
        params: list[Any] = []
        if person_id:
            where.append("person_id = ?")
            params.append(person_id)
        if gateway:
            where.append("gateway = ?")
            params.append(gateway)
        if user_id:
            where.append("user_id = ?")
            params.append(user_id)
        if conversation_id:
            where.append("conversation_id = ?")
            params.append(conversation_id)
        if profile_key:
            where.append("profile_key = ?")
            params.append(profile_key)

        sql = "SELECT * FROM user_profiles"
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY updated_at DESC"
        if limit > 0:
            sql += " LIMIT ?"
            params.append(limit)

        rows = self._db.execute(sql, tuple(params)).fetchall()
        return [self._row_to_user_profile(r) for r in rows]

    def user_profile_delete(
        self,
        *,
        profile_id: str = "",
        person_id: str = "",
        gateway: str = "",
        user_id: str = "",
        conversation_id: str = "",
        profile_key: str = "",
    ) -> int:
        """Delete profile facts by id or by composite key. Returns deleted count."""
        if profile_id:
            result = self._db.execute(
                "DELETE FROM user_profiles WHERE id = ?",
                (profile_id,),
            )
            self._db.commit()
            return int(result.rowcount or 0)

        normalized_person_id = person_id.strip()
        normalized_gateway = gateway.strip()
        normalized_user_id = user_id.strip()
        normalized_conversation_id = conversation_id.strip()
        normalized_profile_key = profile_key.strip()
        if not normalized_profile_key:
            return 0
        where = ["conversation_id = ?", "profile_key = ?"]
        params: list[Any] = [normalized_conversation_id, normalized_profile_key]
        if normalized_person_id:
            where.append("person_id = ?")
            params.append(normalized_person_id)
        elif normalized_gateway and normalized_user_id:
            where.append("gateway = ?")
            where.append("user_id = ?")
            params.extend([normalized_gateway, normalized_user_id])
        else:
            return 0
        result = self._db.execute(
            f"DELETE FROM user_profiles WHERE {' AND '.join(where)}",
            tuple(params),
        )
        self._db.commit()
        return int(result.rowcount or 0)

    def user_profile_bind_person(
        self,
        *,
        gateway: str,
        user_id: str,
        person_id: str,
    ) -> int:
        normalized_gateway = gateway.strip()
        normalized_user_id = user_id.strip()
        normalized_person_id = person_id.strip()
        if not normalized_gateway or not normalized_user_id or not normalized_person_id:
            return 0
        result = self._db.execute(
            """UPDATE user_profiles
               SET person_id = ?, updated_at = ?
               WHERE gateway = ? AND user_id = ? AND person_id = ''""",
            (
                normalized_person_id,
                _now_iso(),
                normalized_gateway,
                normalized_user_id,
            ),
        )
        self._db.commit()
        return int(result.rowcount or 0)

    def _find_user_profile_row(
        self,
        *,
        person_id: str = "",
        gateway: str = "",
        user_id: str = "",
        conversation_id: str = "",
        profile_key: str = "",
    ) -> sqlite3.Row | None:
        normalized_person_id = person_id.strip()
        normalized_gateway = gateway.strip()
        normalized_user_id = user_id.strip()
        normalized_conversation_id = conversation_id.strip()
        normalized_profile_key = profile_key.strip()
        if normalized_person_id and normalized_profile_key:
            row = self._db.execute(
                """SELECT * FROM user_profiles
                   WHERE person_id = ? AND conversation_id = ? AND profile_key = ?
                   ORDER BY updated_at DESC
                   LIMIT 1""",
                (
                    normalized_person_id,
                    normalized_conversation_id,
                    normalized_profile_key,
                ),
            ).fetchone()
            if row is not None:
                return row
        if normalized_gateway and normalized_user_id and normalized_profile_key:
            return self._db.execute(
                """SELECT * FROM user_profiles
                   WHERE gateway = ? AND user_id = ? AND conversation_id = ? AND profile_key = ?
                   ORDER BY updated_at DESC
                   LIMIT 1""",
                (
                    normalized_gateway,
                    normalized_user_id,
                    normalized_conversation_id,
                    normalized_profile_key,
                ),
            ).fetchone()
        return None

    def _find_user_profile(
        self,
        *,
        person_id: str = "",
        gateway: str = "",
        user_id: str = "",
        conversation_id: str = "",
        profile_key: str = "",
    ) -> dict[str, Any] | None:
        row = self._find_user_profile_row(
            person_id=person_id,
            gateway=gateway,
            user_id=user_id,
            conversation_id=conversation_id,
            profile_key=profile_key,
        )
        if row is None:
            return None
        return self._row_to_user_profile(row)

    @staticmethod
    def _row_to_user_profile(row: sqlite3.Row) -> dict[str, Any]:
        return {
            "id": row["id"],
            "gateway": row["gateway"],
            "user_id": row["user_id"],
            "person_id": row["person_id"] if "person_id" in row.keys() else "",
            "conversation_id": row["conversation_id"],
            "profile_key": row["profile_key"],
            "profile_value": row["profile_value"],
            "source_memory_id": row["source_memory_id"],
            "source_message_id": row["source_message_id"],
            "confidence": float(row["confidence"]),
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    # ------------------------------------------------------------------
    # Runtime Telemetry
    # ------------------------------------------------------------------

    def runtime_record_run(self, record: dict[str, Any]) -> str:
        run_id = str(record.get("run_id", "")).strip() or _gen_id("run")
        now = _now_iso()
        logged_at = str(record.get("logged_at", "")).strip() or now
        usage = record.get("usage", {})
        if not isinstance(usage, dict):
            usage = {}
        self._db.execute(
            """INSERT INTO runtime_runs
               (run_id, role, task_id, provider, model, base_url, hub_url, conversation_id,
                wake_mode, source_topic, status, error, total_iterations, total_tool_calls,
                total_retries, prompt_tokens, completion_tokens, total_tokens, usage,
                elapsed_seconds, logged_at, created_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
               ON CONFLICT(run_id) DO UPDATE SET
                   role = excluded.role,
                   task_id = excluded.task_id,
                   provider = excluded.provider,
                   model = excluded.model,
                   base_url = excluded.base_url,
                   hub_url = excluded.hub_url,
                   conversation_id = excluded.conversation_id,
                   wake_mode = excluded.wake_mode,
                   source_topic = excluded.source_topic,
                   status = excluded.status,
                   error = excluded.error,
                   total_iterations = excluded.total_iterations,
                   total_tool_calls = excluded.total_tool_calls,
                   total_retries = excluded.total_retries,
                   prompt_tokens = excluded.prompt_tokens,
                   completion_tokens = excluded.completion_tokens,
                   total_tokens = excluded.total_tokens,
                   usage = excluded.usage,
                   elapsed_seconds = excluded.elapsed_seconds,
                   logged_at = excluded.logged_at
            """,
            (
                run_id,
                str(record.get("role", "")).strip(),
                str(record.get("task_id", "")).strip(),
                str(record.get("provider", "")).strip(),
                str(record.get("model", "")).strip(),
                str(record.get("base_url", "")).strip(),
                str(record.get("hub_url", "")).strip(),
                str(record.get("conversation_id", "")).strip(),
                str(record.get("wake_mode", "")).strip(),
                str(record.get("source_topic", "")).strip(),
                str(record.get("status", "")).strip(),
                str(record.get("error", "")).strip(),
                _as_int(record.get("total_iterations", 0)),
                _as_int(record.get("total_tool_calls", 0)),
                _as_int(record.get("total_retries", 0)),
                _as_int(usage.get("prompt_tokens", 0)),
                _as_int(usage.get("completion_tokens", 0)),
                _as_int(usage.get("total_tokens", 0)),
                _json_dumps(usage),
                _as_float(record.get("elapsed_seconds", 0)),
                logged_at,
                now,
            ),
        )
        self._db.commit()
        return run_id

    def runtime_record_tool_call(self, record: dict[str, Any]) -> str:
        event_id = str(record.get("event_id", "")).strip() or _gen_id("tc")
        now = _now_iso()
        ok = record.get("ok")
        ok_value = None
        if isinstance(ok, bool):
            ok_value = 1 if ok else 0
        self._db.execute(
            """INSERT OR REPLACE INTO runtime_tool_calls
               (event_id, timestamp, run_id, role, task_id, tool_name, ok, error, traceback,
                duration_ms, ref_id, args, result, created_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                event_id,
                str(record.get("timestamp", "")).strip() or now,
                str(record.get("run_id", "")).strip(),
                str(record.get("role", "")).strip(),
                str(record.get("task_id", "")).strip(),
                str(record.get("tool", "")).strip(),
                ok_value,
                str(record.get("error", "")).strip(),
                str(record.get("traceback", "")).strip(),
                _as_float(record.get("duration_ms", 0)),
                str(record.get("ref_id", "")).strip(),
                _as_text(record.get("args", {})),
                _as_text(record.get("result", "")),
                now,
            ),
        )
        self._db.commit()
        return event_id

    def runtime_upsert_tool_ref(self, ref_id: str, payload: dict[str, Any]) -> bool:
        normalized_ref_id = ref_id.strip()
        if not normalized_ref_id:
            return False
        now = _now_iso()
        self._db.execute(
            """INSERT INTO runtime_tool_refs
               (ref_id, timestamp, run_id, role, task_id, tool_name, args, args_preview,
                args_truncated, result, result_preview, result_truncated, created_at, updated_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
               ON CONFLICT(ref_id) DO UPDATE SET
                   timestamp = excluded.timestamp,
                   run_id = excluded.run_id,
                   role = excluded.role,
                   task_id = excluded.task_id,
                   tool_name = excluded.tool_name,
                   args = excluded.args,
                   args_preview = excluded.args_preview,
                   args_truncated = excluded.args_truncated,
                   result = excluded.result,
                   result_preview = excluded.result_preview,
                   result_truncated = excluded.result_truncated,
                   updated_at = excluded.updated_at
            """,
            (
                normalized_ref_id,
                str(payload.get("timestamp", "")).strip() or now,
                str(payload.get("run_id", "")).strip(),
                str(payload.get("role", "")).strip(),
                str(payload.get("task_id", "")).strip(),
                str(payload.get("tool", "")).strip(),
                _as_text(payload.get("args", {})),
                str(payload.get("args_preview", "")).strip(),
                1 if bool(payload.get("args_truncated", False)) else 0,
                _as_text(payload.get("result")),
                str(payload.get("result_preview", "")).strip(),
                1 if bool(payload.get("result_truncated", False)) else 0,
                now,
                now,
            ),
        )
        self._db.commit()
        return True

    def runtime_tool_ref_exists(self, ref_id: str) -> bool:
        row = self._db.execute(
            "SELECT 1 FROM runtime_tool_refs WHERE ref_id = ? LIMIT 1",
            (ref_id.strip(),),
        ).fetchone()
        return row is not None

    def runtime_read_tool_ref(
        self,
        ref_id: str,
        *,
        max_chars: int = 6_000,
    ) -> dict[str, Any] | None:
        row = self._db.execute(
            """SELECT ref_id, timestamp, run_id, role, task_id, tool_name, args, args_preview,
                      args_truncated, result, result_preview, result_truncated
               FROM runtime_tool_refs
               WHERE ref_id = ?""",
            (ref_id.strip(),),
        ).fetchone()
        if row is None:
            return None

        args_text = row["args"] or ""
        result_text = row["result"] or ""
        args_value = _json_loads(args_text, args_text)
        result_value = _json_loads(result_text, result_text)
        result: dict[str, Any] = {
            "ref_id": row["ref_id"],
            "timestamp": row["timestamp"],
            "run_id": row["run_id"],
            "role": row["role"],
            "task_id": row["task_id"],
            "tool": row["tool_name"],
            "args": args_value,
            "args_preview": row["args_preview"],
            "args_truncated": bool(row["args_truncated"]),
            "result": result_value,
            "result_preview": row["result_preview"],
            "result_truncated": bool(row["result_truncated"]),
        }
        preview_args, args_cut = _truncate_text(args_text, max_chars)
        result["args_preview"] = preview_args
        if args_cut:
            result["args_preview_truncated"] = True

        preview_result, result_cut = _truncate_text(result_text, max_chars)
        if isinstance(result_value, str):
            result["result"] = preview_result
            if result_cut:
                result["result_truncated"] = True
        else:
            result["result_preview"] = preview_result
            if result_cut:
                result["result_preview_truncated"] = True
        return result

    def runtime_write_task_context_refs(
        self,
        task_id: str,
        ref_ids: list[str],
    ) -> list[str]:
        normalized_task_id = task_id.strip()
        if not normalized_task_id:
            return []
        seen: set[str] = set()
        normalized: list[str] = []
        for raw in ref_ids:
            ref_id = str(raw).strip()
            if not ref_id or ref_id in seen:
                continue
            seen.add(ref_id)
            normalized.append(ref_id)

        now = _now_iso()
        self._db.execute(
            "DELETE FROM runtime_task_context_refs WHERE task_id = ?",
            (normalized_task_id,),
        )
        for ordinal, ref_id in enumerate(normalized):
            self._db.execute(
                """INSERT INTO runtime_task_context_refs
                   (task_id, ref_id, ordinal, created_at, updated_at)
                   VALUES (?,?,?,?,?)""",
                (normalized_task_id, ref_id, ordinal, now, now),
            )
        self._db.commit()
        return normalized

    def runtime_read_task_context_refs(self, task_id: str) -> list[str]:
        rows = self._db.execute(
            """SELECT ref_id FROM runtime_task_context_refs
               WHERE task_id = ?
               ORDER BY ordinal ASC""",
            (task_id.strip(),),
        ).fetchall()
        return [str(row["ref_id"]).strip() for row in rows if str(row["ref_id"]).strip()]

    def runtime_task_trace_stats(
        self,
        task_id: str,
        *,
        recent_limit: int = 20,
    ) -> dict[str, Any]:
        normalized_task_id = task_id.strip()
        if not normalized_task_id:
            return {
                "exists": False,
                "tool_calls_total": 0,
                "tool_calls_ok": 0,
                "tool_calls_failed": 0,
                "recent_calls": [],
            }
        row = self._db.execute(
            """SELECT COUNT(*) AS total,
                      SUM(CASE WHEN ok = 1 THEN 1 ELSE 0 END) AS ok_count,
                      SUM(CASE WHEN ok = 0 THEN 1 ELSE 0 END) AS failed_count,
                      COUNT(DISTINCT tool_name) AS unique_tool_count,
                      MAX(timestamp) AS last_tool_call_at
               FROM runtime_tool_calls
               WHERE task_id = ?""",
            (normalized_task_id,),
        ).fetchone()
        total = _as_int(row["total"] if row else 0)
        if total <= 0:
            return {
                "exists": False,
                "tool_calls_total": 0,
                "tool_calls_ok": 0,
                "tool_calls_failed": 0,
                "recent_calls": [],
            }

        recent_rows = self._db.execute(
            """SELECT timestamp, tool_name, ok, duration_ms, ref_id, args, result
               FROM runtime_tool_calls
               WHERE task_id = ?
               ORDER BY timestamp DESC
               LIMIT ?""",
            (normalized_task_id, max(1, int(recent_limit))),
        ).fetchall()
        recent: deque[dict[str, Any]] = deque()
        for item in reversed(recent_rows):
            ok_value = item["ok"]
            ok_flag = None if ok_value is None else bool(ok_value)
            recent.append({
                "timestamp": item["timestamp"],
                "tool": item["tool_name"],
                "ok": ok_flag,
                "duration_ms": item["duration_ms"],
                "ref_id": item["ref_id"],
                "args_preview": _preview_json_text(item["args"] or ""),
                "result_preview": _preview_json_text(item["result"] or ""),
            })
        return {
            "exists": True,
            "tool_calls_total": total,
            "tool_calls_ok": _as_int(row["ok_count"] if row else 0),
            "tool_calls_failed": _as_int(row["failed_count"] if row else 0),
            "unique_tool_count": _as_int(row["unique_tool_count"] if row else 0),
            "last_tool_call_at": row["last_tool_call_at"] if row else "",
            "recent_calls": list(recent),
        }

    def runtime_task_latest_run(
        self,
        task_id: str,
        *,
        role: str = "task",
    ) -> dict[str, Any] | None:
        row = self._db.execute(
            """SELECT * FROM runtime_runs
               WHERE task_id = ? AND role = ?
               ORDER BY logged_at DESC
               LIMIT 1""",
            (task_id.strip(), role.strip()),
        ).fetchone()
        if row is None:
            return None
        usage = _json_loads(row["usage"], {})
        if not isinstance(usage, dict):
            usage = {}
        return {
            "run_id": row["run_id"],
            "logged_at": row["logged_at"],
            "provider": row["provider"],
            "model": row["model"],
            "elapsed_seconds": row["elapsed_seconds"],
            "status": row["status"],
            "total_iterations": _as_int(row["total_iterations"]),
            "total_tool_calls": _as_int(row["total_tool_calls"]),
            "total_retries": _as_int(row["total_retries"]),
            "prompt_tokens": _as_int(row["prompt_tokens"]),
            "completion_tokens": _as_int(row["completion_tokens"]),
            "total_tokens": _as_int(row["total_tokens"]),
            "usage": usage,
        }

    def runtime_cleanup(self, *, retention_days: int) -> dict[str, int]:
        cutoff = (
            datetime.now(timezone.utc) - timedelta(days=max(0, retention_days))
        ).isoformat()
        removed_runs = self._db.execute(
            "DELETE FROM runtime_runs WHERE logged_at < ?",
            (cutoff,),
        ).rowcount or 0
        removed_tool_calls = self._db.execute(
            "DELETE FROM runtime_tool_calls WHERE timestamp < ?",
            (cutoff,),
        ).rowcount or 0
        removed_tool_refs = self._db.execute(
            "DELETE FROM runtime_tool_refs WHERE timestamp < ?",
            (cutoff,),
        ).rowcount or 0
        removed_task_refs = self._db.execute(
            """DELETE FROM runtime_task_context_refs
               WHERE updated_at < ?
                  OR ref_id NOT IN (SELECT ref_id FROM runtime_tool_refs)""",
            (cutoff,),
        ).rowcount or 0
        self._db.commit()
        return {
            "runtime_runs": int(removed_runs),
            "runtime_tool_calls": int(removed_tool_calls),
            "runtime_tool_refs": int(removed_tool_refs),
            "runtime_task_context_refs": int(removed_task_refs),
        }

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def __enter__(self) -> "Store":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def close(self) -> None:
        """Close the database connection."""
        try:
            self._db.close()
        except Exception:
            pass
