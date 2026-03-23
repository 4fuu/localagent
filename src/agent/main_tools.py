"""Main-agent tools for conversation-state orchestration."""

import json
import logging
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from websockets.sync.client import connect

from ..core.artifacts import ArtifactStore, is_artifact_ref
from ..core.identity import resolve_person_id
from ..core.runtime_paths import RuntimePathMap
from ..core.secrets import (
    conversation_scope,
    delete_secret,
    list_secrets,
    person_scope,
    set_secret,
)
from ..core.store import Store
from ..core.store import conversation_state_bind_task
from ..index import IndexClient
from ..provider import tool
from ..retry import RetryPolicy
from .context_refs import read_task_context_refs, tool_ref_exists, write_task_context_refs
from .state import AgentState
from .topic_memory import archive_topic_snapshot

logger = logging.getLogger(__name__)
_RETRY = RetryPolicy.for_service("main_tools")
_TASK_TYPES = {"general", "reply", "execute"}
_AUTO_CONTEXT_REF_TASK_TYPES = {"execute", "general", "reply"}
_MAX_AUTO_CONTEXT_REFS = 4


def _result(payload: dict[str, Any]) -> str:
    return json.dumps(payload, ensure_ascii=False)


def _send_event(hub_url: str, topic: str, payload: dict[str, Any]) -> None:
    last_exc: Exception | None = None
    for attempt in range(_RETRY.max_retries + 1):
        try:
            ws = connect(hub_url, open_timeout=_RETRY.connect_timeout)
            try:
                ws.send(json.dumps({"type": "event", "topic": topic, "payload": payload}))
            finally:
                ws.close()
            return
        except Exception as exc:
            last_exc = exc
            if attempt >= _RETRY.max_retries:
                break
            time.sleep(_RETRY.backoff_delay(attempt))
    raise RuntimeError(f"failed to send event {topic}") from last_exc


def _coerce_list_param(value: Any, field_name: str) -> tuple[list[Any], str]:
    if value is None:
        return [], ""
    if isinstance(value, list):
        return value, ""
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return [], ""
        try:
            parsed = json.loads(raw)
        except Exception as exc:
            return [], f"{field_name} 字符串解析失败: {exc}"
        if not isinstance(parsed, list):
            return [], f"{field_name} 必须是数组"
        return parsed, ""
    return [], f"{field_name} 必须是数组"


def _normalize_unique_strs(values: list[Any]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        item = str(value).strip()
        if item and item not in seen:
            seen.add(item)
            result.append(item)
    return result


def _normalize_task_type(value: str) -> str:
    normalized = str(value or "").strip().lower()
    return normalized if normalized in _TASK_TYPES else ""


def _pick_auto_context_refs(
    state: AgentState,
    task_type: str,
    *,
    conversation_id: str = "",
) -> list[str]:
    normalized_task_type = _normalize_task_type(task_type) or "general"
    if normalized_task_type not in _AUTO_CONTEXT_REF_TASK_TYPES:
        return []
    normalized_conversation_id = conversation_id.strip()
    if not normalized_conversation_id:
        return []
    current_conversation_id = str(getattr(state, "current_conversation_id", "")).strip()
    if current_conversation_id != normalized_conversation_id:
        return []
    candidates = [
        str(item).strip()
        for item in (
            getattr(state, "recent_tool_ref_ids_by_conversation", {}) or {}
        ).get(normalized_conversation_id, [])
        if str(item).strip()
    ]
    deduped: list[str] = []
    seen: set[str] = set()
    for ref_id in candidates:
        if ref_id in seen:
            continue
        seen.add(ref_id)
        deduped.append(ref_id)
    return deduped[-_MAX_AUTO_CONTEXT_REFS:]


def _read_task_trace_stats(task_id: str, recent_limit: int = 20) -> dict[str, Any]:
    with Store() as store:
        return store.runtime_task_trace_stats(task_id, recent_limit=recent_limit)


def _read_task_usage_stats(task_id: str) -> dict[str, Any]:
    with Store() as store:
        latest = store.runtime_task_latest_run(task_id, role="task")
    if latest is None:
        return {"exists": False}
    return {"exists": True, **latest}


def _ensure_conversation_state(
    store: Store,
    *,
    conversation_id: str,
    gateway: str = "",
    user_id: str = "",
    person_id: str = "",
    is_multi_party: bool | None = None,
) -> dict[str, Any] | None:
    normalized_conversation_id = conversation_id.strip()
    if not normalized_conversation_id:
        return None
    existing = store.conversation_state_read(normalized_conversation_id)
    if existing is not None:
        return existing
    return store.conversation_state_upsert(
        normalized_conversation_id,
        gateway=gateway,
        user_id=user_id,
        person_id=person_id,
        is_multi_party=is_multi_party,
    )


def _resolve_person_id_for_tool(
    state: AgentState,
    *,
    gateway: str = "",
    user_id: str = "",
    explicit_person_id: str = "",
) -> str:
    normalized_person_id = explicit_person_id.strip()
    if normalized_person_id:
        return normalized_person_id
    state_person_id = str(getattr(state, "current_person_id", "")).strip()
    if state_person_id:
        return state_person_id
    return resolve_person_id(gateway, user_id)


def _resolve_secret_scope(
    state: AgentState,
    scope: str,
) -> tuple[str, str]:
    normalized_scope = scope.strip().lower() or "person"
    if normalized_scope == "conversation":
        conversation_id = str(getattr(state, "current_conversation_id", "")).strip()
        if not conversation_id:
            return "", "manage_env 仅支持绑定 conversation_id 的会话"
        return conversation_scope(conversation_id), ""
    if normalized_scope == "person":
        person_id = str(getattr(state, "current_person_id", "")).strip()
        if not person_id:
            return "", "manage_env 缺少 person_id，无法写入统一用户作用域"
        return person_scope(person_id), ""
    return "", f"未知 scope: {scope}"


def _find_equivalent_pending_task(
    store: Store,
    *,
    goal: str,
    task_type: str,
    conversation_id: str,
    gateway: str = "",
    user_id: str = "",
    message_id: str = "",
    reply_to_message_id: str = "",
    parent_task_id: str = "",
) -> dict[str, Any] | None:
    normalized_conversation_id = conversation_id.strip()
    if not normalized_conversation_id:
        return None
    normalized_goal = goal.strip()
    normalized_task_type = _normalize_task_type(task_type) or "general"
    normalized_gateway = gateway.strip()
    normalized_user_id = user_id.strip()
    normalized_message_id = message_id.strip()
    normalized_reply_to_message_id = reply_to_message_id.strip()
    normalized_parent_task_id = parent_task_id.strip()

    for candidate in store.conversation_recent_tasks(normalized_conversation_id, limit=20):
        if str(candidate.get("status", "")).strip() != "pending":
            continue
        if _normalize_task_type(str(candidate.get("task_type", ""))) != normalized_task_type:
            continue
        if normalized_gateway and str(candidate.get("gateway", "")).strip() != normalized_gateway:
            continue
        if normalized_user_id and str(candidate.get("user_id", "")).strip() != normalized_user_id:
            continue
        if normalized_parent_task_id and str(candidate.get("parent_task_id", "")).strip() != normalized_parent_task_id:
            continue
        candidate_message_id = str(candidate.get("message_id", "")).strip()
        if normalized_message_id and candidate_message_id == normalized_message_id:
            return candidate
        if normalized_reply_to_message_id and str(candidate.get("reply_to_message_id", "")).strip() == normalized_reply_to_message_id:
            return candidate
        if normalized_goal and str(candidate.get("goal", "")).strip() == normalized_goal:
            return candidate
    return None


def _archive_note(
    *,
    hub_url: str,
    text: str,
    conversation_id: str = "",
    note_type: str = "note",
    tags: list[str] | None = None,
    source_message_id: str = "",
) -> str | None:
    metadata = {
        "conversation_id": conversation_id.strip(),
        "type": note_type,
        "tags": tags or [],
        "source_message_id": source_message_id.strip(),
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    client = IndexClient(hub_url)
    try:
        return client.insert_entry(
            text=text,
            label=text[:80] or note_type,
            prefix=note_type,
            source=note_type,
            content=text,
            metadata=json.dumps(metadata, ensure_ascii=False),
        )
    finally:
        client.close()


@tool
def add_note(
    state: AgentState,
    content: str,
    conversation_id: str = "",
    tags: list[str] | None = None,
    source_message_id: str = "",
) -> str:
    """将推理结论写入 archive note。

    适用场景：
    - 写入 inbox 原文和 task result 之外、但后续调度仍可能复用的推理结论。
    - 例如“用户已确认这里的 openclaw 指官方项目”“这一轮需要先查结果再决定是否继续”。

    不适用场景：
    - 稳定用户事实不要写到 note，应改用 `manage_user_profile`。
    - 当前轮尚未确认的候选实体、候选仓库名、候选作者名，不能写成确定事实。
    - 不要把大段原文搬进 note；原始内容应由系统归档或放在 task result/context ref 中。

    参数：
    - content (string): 要写入的结论文本，必须简短、可复用、可独立理解。
    - conversation_id (string, 可选): 所属会话；为空时默认绑定当前会话。
    - tags (array[string], 可选): 辅助标签，仅用于检索提示，不代表已确认事实。
    - source_message_id (string, 可选): 关联的原始消息 ID，便于追溯。

    返回：
    - 写入成功后返回 archive entry_id；该 note 会和其他 archive 条目一起参与召回。
    """
    hub_url = state.hub_url
    if not hub_url:
        return _result({"ok": False, "error": "hub_url 未配置"})
    text = content.strip()
    if not text:
        return _result({"ok": False, "error": "content 不能为空"})
    parsed_tags, tags_err = _coerce_list_param(tags, "tags")
    if tags_err:
        return _result({"ok": False, "error": tags_err})
    normalized_conversation_id = (
        conversation_id.strip() or str(getattr(state, "current_conversation_id", "")).strip()
    )
    try:
        entry_id = _archive_note(
            hub_url=hub_url,
            text=text,
            conversation_id=normalized_conversation_id,
            note_type="note",
            tags=_normalize_unique_strs(parsed_tags),
            source_message_id=source_message_id,
        )
    except Exception as exc:
        logger.exception("add_note failed")
        return _result({"ok": False, "error": str(exc)})
    return _result({
        "ok": True,
        "entry_id": entry_id,
        "conversation_id": normalized_conversation_id,
        "content": text,
    })


@tool
def search_archive(
    state: AgentState,
    query: str,
    conversation_id: str = "",
    source: str = "",
    limit: int = 6,
) -> str:
    """搜索 archive 中的历史记录，作为当前调度的补充证据。

    使用原则：
    - 默认用于补缺，不替代 `conversation_state` 和 `recent_window`。
    - 返回的是候选证据，不是已确认事实；命中结果仍需结合当前 inbox 和会话状态判断。
    - 优先限定当前 `conversation_id`，避免把别的会话旧内容拉进当前线程。

    参数：
    - query (string): 搜索语句，可用当前目标、focus、用户原话组合。
    - conversation_id (string, 可选): 会话过滤；为空时默认当前会话。
    - source (string, 可选): 仅搜索指定 source。
    - limit (integer, 可选): 返回条数，建议保持小而精。

    返回：
    - `items` 中包含 id/score/text/label/source/metadata，可作为调度证据或后续 task 的上下文。
    """
    hub_url = state.hub_url
    if not hub_url:
        return _result({"ok": False, "error": "hub_url 未配置"})
    normalized_query = query.strip()
    if not normalized_query:
        return _result({"ok": False, "error": "query 不能为空"})
    normalized_conversation_id = (
        conversation_id.strip() or str(getattr(state, "current_conversation_id", "")).strip()
    )
    client = IndexClient(hub_url)
    try:
        sources = [{"source": source.strip()}] if source.strip() else None
        results = client.search(normalized_query, topk=max(1, min(int(limit), 10)), sources=sources)
    except Exception as exc:
        return _result({"ok": False, "error": str(exc)})
    finally:
        client.close()

    items: list[dict[str, Any]] = []
    for item in results:
        fields = item.get("fields", {}) or {}
        metadata = fields.get("metadata", {})
        if isinstance(metadata, str):
            try:
                metadata = json.loads(metadata)
            except Exception:
                metadata = {"raw": metadata}
        if normalized_conversation_id:
            candidate_conversation_id = str(metadata.get("conversation_id", "")).strip()
            if candidate_conversation_id != normalized_conversation_id:
                continue
        items.append({
            "id": item.get("id", ""),
            "score": item.get("score", 0.0),
            "text": str(fields.get("text", ""))[:500],
            "label": fields.get("label", ""),
            "source": fields.get("source", ""),
            "metadata": metadata,
        })
    return _result({
        "ok": True,
        "query": normalized_query,
        "conversation_id": normalized_conversation_id,
        "items": items,
    })


@tool
def manage_user_profile(
    state: AgentState,
    action: str,
    person_id: str = "",
    gateway: str = "",
    user_id: str = "",
    conversation_id: str = "",
    profile_key: str = "",
    profile_value: str = "",
    profile_id: str = "",
    source_memory_id: str = "",
    source_message_id: str = "",
    confidence: float = 1.0,
    limit: int = 200,
) -> str:
    """管理稳定、可复用的用户画像事实。

    适用场景：
    - 用户长期偏好、称呼、时区、地区、常用语言等跨轮次稳定事实。
    - 这些事实既不属于单轮临时 focus，也不应依赖 embedding 召回猜测。

    不适用场景：
    - 当前轮临时状态、待确认推断、任务过程中的中间结论，不要写到 user profile。
    - 不要用它代替 `conversation_state`，也不要把候选线索升级成稳定事实。

    参数：
    - action (string): `upsert` / `list` / `delete`。
    - person_id: 统一用户主键；为空时优先使用当前会话绑定的 person_id，再回退 `gateway + user_id` 解析。
    - gateway, user_id, conversation_id: 来源账号与会话作用域。`conversation_id=""` 表示用户级全局事实。
    - profile_key / profile_value: 事实键值。
    - profile_id: 删除时可用的画像记录 ID。
    - source_memory_id / source_message_id: 来源追溯信息。
    - confidence (number): 0~1，仅表示写入置信度，不代表系统已自动确认。
    - limit (integer): 查询上限。
    """
    normalized_action = action.strip().lower()
    normalized_person_id = _resolve_person_id_for_tool(
        state,
        gateway=gateway,
        user_id=user_id,
        explicit_person_id=person_id,
    )
    normalized_gateway = gateway.strip()
    normalized_user_id = user_id.strip()
    normalized_conversation_id = conversation_id.strip()
    normalized_key = profile_key.strip()
    normalized_value = profile_value.strip()
    normalized_profile_id = profile_id.strip()

    if normalized_action == "upsert":
        if not normalized_person_id or not normalized_key or not normalized_value:
            return _result({"ok": False, "error": "upsert 缺少必填字段"})
        with Store() as store:
            if normalized_gateway and normalized_user_id:
                store.user_profile_bind_person(
                    gateway=normalized_gateway,
                    user_id=normalized_user_id,
                    person_id=normalized_person_id,
                )
            item = store.user_profile_upsert(
                person_id=normalized_person_id,
                gateway=normalized_gateway,
                user_id=normalized_user_id,
                conversation_id=normalized_conversation_id,
                profile_key=normalized_key,
                profile_value=normalized_value,
                source_memory_id=source_memory_id.strip(),
                source_message_id=source_message_id.strip(),
                confidence=max(0.0, min(float(confidence), 1.0)),
            )
        return _result({"ok": True, "item": item})

    if normalized_action == "list":
        with Store() as store:
            if normalized_gateway and normalized_user_id and normalized_person_id:
                store.user_profile_bind_person(
                    gateway=normalized_gateway,
                    user_id=normalized_user_id,
                    person_id=normalized_person_id,
                )
            items = store.user_profile_list(
                person_id=normalized_person_id,
                gateway=normalized_gateway,
                user_id=normalized_user_id,
                conversation_id=normalized_conversation_id,
                profile_key=normalized_key,
                limit=max(1, min(int(limit), 500)),
            )
        return _result({"ok": True, "items": items, "count": len(items)})

    if normalized_action == "delete":
        with Store() as store:
            deleted = store.user_profile_delete(
                profile_id=normalized_profile_id,
                person_id=normalized_person_id,
                gateway=normalized_gateway,
                user_id=normalized_user_id,
                conversation_id=normalized_conversation_id,
                profile_key=normalized_key,
            )
        return _result({"ok": True, "deleted": deleted})

    return _result({"ok": False, "error": f"未知 action: {action}"})


_IMAGE_ATTACHMENT_EXTENSIONS = {".png", ".jpg", ".jpeg", ".gif", ".webp", ".bmp"}


def _collect_inbox_images(state: AgentState) -> list[str]:
    """Extract image artifact refs from the main agent's current inbox batch."""
    inbox_ids = getattr(state, "pending_inbox_ids", None)
    if not inbox_ids:
        return []
    refs: list[str] = []
    seen: set[str] = set()
    try:
        with Store() as store:
            for inbox_id in inbox_ids:
                inbox = store.inbox_read(inbox_id)
                if not inbox:
                    continue
                for att in inbox.get("attachments") or []:
                    if not isinstance(att, dict):
                        continue
                    # Check if it's an image attachment
                    is_image = att.get("is_image")
                    if not isinstance(is_image, bool):
                        mime = str(att.get("mime_type", "")).strip().lower()
                        is_image = mime.startswith("image/")
                        if not is_image:
                            for key in ("file_name", "file_path"):
                                suffix = Path(str(att.get(key, "")).strip()).suffix.lower()
                                if suffix in _IMAGE_ATTACHMENT_EXTENSIONS:
                                    is_image = True
                                    break
                    if not is_image:
                        continue
                    ref = str(att.get("file_path", "")).strip()
                    if ref and is_artifact_ref(ref) and ref not in seen:
                        seen.add(ref)
                        refs.append(ref)
    except Exception:
        logger.warning("Failed to auto-collect inbox images")
    return refs


@tool
def manage_task(
    state: AgentState,
    action: str,
    goal: str = "",
    task_type: str = "",
    task_id: str = "",
    gateway: str = "",
    conversation_id: str = "",
    user_id: str = "",
    message_id: str = "",
    reply_to_message_id: str = "",
    memory_id: str = "",
    parent_task_id: str = "",
    then: list[str] | None = None,
    then_task_types: list[str] | None = None,
    images: list[str] | None = None,
    context_ref_ids: list[str] | None = None,
) -> str:
    """创建、停止、查询 task。

    这是 main agent 的核心调度工具。使用时必须把任务边界写窄、写清楚。

    `start` 的使用规则：
    - `goal` 必须短、单步、可验收，只描述当前步骤，不要塞入整条工作流。
    - 一次只围绕一个 focus 分发一个动作；未确认的对象不要写进 goal。
    - 若需求本身只是保存/删除会话密钥，直接调用 `manage_env`，不要额外创建执行任务去写 secrets 文件。
    - `task_type` 必须显式选择：
      - `reply`: 只用于回复/确认/澄清/交付，不得夹带执行动作。
      - `execute`: 只用于查询/读取/修改/运行等执行动作，不得要求 task 直接对外回复。
      - `general`: 仅在 goal 明确要求“执行后回复”且一步完成更稳时使用，不能作为默认兜底。
    - 对同会话已有的等价未完成任务，应优先复用或等待，不要重复创建。

    `then` / `then_task_types` 规则：
    - 仅在步骤顺序稳定、每步输入输出可预期时使用。
    - 存在关键不确定性时禁止预先写整条链，应改为单步调度，待 task_done 后再决定下一步。
    - `then_task_types` 若提供，必须与 `then` 等长，并逐步声明 reply/execute/general。

    参数：
    - action (string): `start` / `status` / `stop`。
    - goal (string, 可选): 任务目标；`action=start` 时必填。
    - task_type (string, 可选): `reply` / `execute` / `general`。
    - task_id (string, 可选): 查询或停止时使用。
    - gateway / conversation_id / user_id / message_id / reply_to_message_id:
      回复链路和会话追溯信息。`reply` 任务至少需要 gateway + conversation_id。
    - memory_id (string, 可选): 兼容字段；用于保留与旧数据的关联。
    - parent_task_id (string, 可选): 父任务 ID。
    - then / then_task_types (array, 可选): 稳定链式任务定义。
    - images (array[string], 可选): 任务启动时注入的图片路径。
    - context_ref_ids (array[string], 可选): task 可通过 `read_context_ref` 读取的授权上下文。

    返回：
    - `start` 返回任务信息与自动绑定的 context_ref。
    - `status` 返回任务、trace、usage 与授权 ref 列表。
    - `stop` 返回停止结果，并尝试给运行中的 task agent 发送 stop 信号。
    """
    hub_url = state.hub_url
    if not hub_url:
        return _result({"ok": False, "error": "hub_url 未配置"})

    normalized_action = action.strip().lower()
    if normalized_action == "start":
        normalized_goal = goal.strip()
        if not normalized_goal:
            return _result({"ok": False, "error": "goal 不能为空"})
        normalized_task_type = _normalize_task_type(task_type) or "general"
        parsed_then, then_err = _coerce_list_param(then, "then")
        parsed_then_types, then_types_err = _coerce_list_param(then_task_types, "then_task_types")
        parsed_images, images_err = _coerce_list_param(images, "images")
        parsed_refs, refs_err = _coerce_list_param(context_ref_ids, "context_ref_ids")
        for error in (then_err, then_types_err, images_err, refs_err):
            if error:
                return _result({"ok": False, "error": error})

        normalized_then = _normalize_unique_strs(parsed_then)
        normalized_then_types = [_normalize_task_type(str(item)) for item in parsed_then_types]
        if any(not item for item in normalized_then_types):
            return _result({"ok": False, "error": "then_task_types 包含无效值"})
        if normalized_then_types and len(normalized_then_types) != len(normalized_then):
            return _result({"ok": False, "error": "then_task_types 长度必须与 then 一致"})

        normalized_conversation_id = (
            conversation_id.strip() or str(getattr(state, "current_conversation_id", "")).strip()
        )
        normalized_gateway = gateway.strip()
        normalized_user_id = user_id.strip()
        normalized_person_id = _resolve_person_id_for_tool(
            state,
            gateway=normalized_gateway,
            user_id=normalized_user_id,
        )
        current_is_multi_party = bool(getattr(state, "current_is_multi_party", False))
        normalized_message_id = message_id.strip()
        normalized_reply_to_message_id = reply_to_message_id.strip()
        normalized_memory_id = memory_id.strip()
        normalized_parent_task_id = parent_task_id.strip()
        normalized_images = _normalize_unique_strs(parsed_images)
        if normalized_images:
            path_map = RuntimePathMap.from_state(state)
            artifact_store = ArtifactStore()
            converted_images: list[str] = []
            for item in normalized_images:
                raw = str(item).strip()
                if not raw:
                    continue
                if is_artifact_ref(raw):
                    converted_images.append(raw)
                    continue
                locator = path_map.to_locator(raw)
                host_path = path_map.locator_to_host(locator)
                target = Path(host_path)
                if target.is_file():
                    converted_images.append(artifact_store.import_file(target, file_name=target.name))
                else:
                    logger.warning("manage_task: image file not found, skipped: %s", raw)
            normalized_images = _normalize_unique_strs(converted_images)
        # Auto-inject images from current inbox when LLM didn't pass images explicitly.
        if not normalized_images:
            normalized_images = _collect_inbox_images(state)
        normalized_context_refs = _normalize_unique_strs(parsed_refs)
        auto_context_refs: list[str] = []

        if normalized_task_type == "reply":
            if not normalized_gateway or not normalized_conversation_id:
                return _result({"ok": False, "error": "reply 任务必须提供 gateway 和 conversation_id"})

        with Store() as store:
            if normalized_conversation_id:
                _ensure_conversation_state(
                    store,
                    conversation_id=normalized_conversation_id,
                    gateway=normalized_gateway,
                    user_id=normalized_user_id,
                    person_id=normalized_person_id,
                    is_multi_party=current_is_multi_party,
                )
                existing_task = _find_equivalent_pending_task(
                    store,
                    goal=normalized_goal,
                    task_type=normalized_task_type,
                    conversation_id=normalized_conversation_id,
                    gateway=normalized_gateway,
                    user_id=normalized_user_id,
                    message_id=normalized_message_id,
                    reply_to_message_id=normalized_reply_to_message_id,
                    parent_task_id=normalized_parent_task_id,
                )
                if existing_task is not None:
                    existing_task_id = str(existing_task.get("id", "")).strip()
                    existing_refs = read_task_context_refs(existing_task_id) if existing_task_id else []
                    merged_refs = list(existing_refs)
                    if normalized_context_refs and existing_task_id:
                        for ref_id in normalized_context_refs:
                            if not tool_ref_exists(ref_id):
                                return _result({"ok": False, "error": f"context ref 不存在: {ref_id}"})
                        merged_refs = _normalize_unique_strs([*existing_refs, *normalized_context_refs])
                        if merged_refs:
                            write_task_context_refs(existing_task_id, merged_refs)
                    payload: dict[str, Any] = {
                        "ok": True,
                        **existing_task,
                        "deduplicated": True,
                    }
                    if merged_refs:
                        payload["context_ref_ids"] = merged_refs
                    return _result(payload)
            if not normalized_context_refs:
                auto_context_refs = _pick_auto_context_refs(
                    state,
                    normalized_task_type,
                    conversation_id=normalized_conversation_id,
                )
                normalized_context_refs = list(auto_context_refs)
            for ref_id in normalized_context_refs:
                if not tool_ref_exists(ref_id):
                    return _result({"ok": False, "error": f"context ref 不存在: {ref_id}"})
            topic_id = ""
            topic_state_patch: dict[str, Any] = {}
            if normalized_conversation_id:
                current_state = _ensure_conversation_state(
                    store,
                    conversation_id=normalized_conversation_id,
                    gateway=normalized_gateway,
                    user_id=normalized_user_id,
                    person_id=normalized_person_id,
                    is_multi_party=current_is_multi_party,
                ) or {}
                bound_topics, topic_id = conversation_state_bind_task(
                    current_state,
                    goal=normalized_goal,
                    task_type=normalized_task_type,
                    message_id=normalized_message_id,
                    task_id="",
                )
                topic_state_patch = {
                    "topics": bound_topics,
                    "active_topic_id": topic_id,
                }
            task = store.task_create(
                normalized_goal,
                task_type=normalized_task_type,
                topic_id=topic_id,
                gateway=normalized_gateway,
                conversation_id=normalized_conversation_id,
                user_id=normalized_user_id,
                person_id=normalized_person_id,
                message_id=normalized_message_id,
                reply_to_message_id=normalized_reply_to_message_id,
                parent_task_id=normalized_parent_task_id,
                then=normalized_then or None,
                then_task_types=normalized_then_types or None,
                images=normalized_images or None,
                is_admin=bool(getattr(state, "is_admin", False)),
                memory_id=normalized_memory_id,
            )
            if normalized_context_refs:
                write_task_context_refs(task["id"], normalized_context_refs)
            topic_snapshot: dict[str, Any] | None = None
            if normalized_conversation_id:
                if topic_id and topic_state_patch.get("topics"):
                    topic_state_patch["topics"] = [
                        {
                            **item,
                            "last_task_id": task["id"],
                        }
                        if str(item.get("id", "")).strip() == topic_id
                        else item
                        for item in list(topic_state_patch.get("topics", []) or [])
                    ]
                updated_state = store.conversation_state_apply(
                    normalized_conversation_id,
                    lambda current: {
                        **topic_state_patch,
                        "active_task_ids": _normalize_unique_strs([
                            *(current.get("active_task_ids", []) or []),
                            task["id"],
                        ]),
                        "person_id": normalized_person_id or str(current.get("person_id", "")),
                        "last_user_message_id": normalized_message_id or None,
                    },
                    gateway=normalized_gateway,
                    user_id=normalized_user_id,
                    person_id=normalized_person_id,
                    is_multi_party=current_is_multi_party,
                )
                if topic_id:
                    topic_snapshot = next(
                        (
                            item
                            for item in (updated_state.get("topics", []) or [])
                            if str(item.get("id", "")).strip() == topic_id
                        ),
                        None,
                    )
                store.conversation_event_append(
                    normalized_conversation_id,
                    "task_started",
                    payload={
                        "task_id": task["id"],
                        "task_type": normalized_task_type,
                        "goal": normalized_goal,
                    },
                )
                if topic_snapshot is not None:
                    try:
                        archive_topic_snapshot(
                            hub_url,
                            conversation_id=normalized_conversation_id,
                            topic=topic_snapshot,
                        )
                    except Exception:
                        logger.debug("Failed to archive topic snapshot", exc_info=True)
        _send_event(
            hub_url,
            "agent.spawn",
            {
                "task_id": task["id"],
                "conversation_id": normalized_conversation_id,
                "goal": normalized_goal,
            },
        )
        payload: dict[str, Any] = {"ok": True, **task}
        if normalized_context_refs:
            payload["context_ref_ids"] = normalized_context_refs
        if auto_context_refs:
            payload["context_ref_ids_auto_bound"] = True
        return _result(payload)

    if normalized_action == "status":
        normalized_task_id = task_id.strip()
        if not normalized_task_id:
            return _result({"ok": False, "error": "task_id 不能为空"})
        with Store() as store:
            task = store.task_read(normalized_task_id)
        if task is None:
            return _result({"ok": False, "error": f"任务不存在: {normalized_task_id}"})
        return _result({
            "ok": True,
            "task": task,
            "context_ref_ids": read_task_context_refs(normalized_task_id),
            "trace": _read_task_trace_stats(normalized_task_id),
            "usage": _read_task_usage_stats(normalized_task_id),
        })

    if normalized_action == "stop":
        normalized_task_id = task_id.strip()
        if not normalized_task_id:
            return _result({"ok": False, "error": "task_id 不能为空"})
        with Store() as store:
            task = store.task_stop(normalized_task_id)
            if task is None:
                return _result({"ok": False, "error": f"任务不存在: {normalized_task_id}"})
            normalized_conversation_id = str(task.get("conversation_id", "")).strip()
            if normalized_conversation_id:
                store.conversation_state_apply(
                    normalized_conversation_id,
                    lambda current: {
                        "active_task_ids": [
                            item
                            for item in (current.get("active_task_ids", []) or [])
                            if str(item).strip() != normalized_task_id
                        ],
                    },
                )
                store.conversation_event_append(
                    normalized_conversation_id,
                    "task_stopped",
                    payload={"task_id": normalized_task_id},
                )
        try:
            _send_event(hub_url, "agent.stop", {"role": "task", "task_id": normalized_task_id})
            stop_signal_sent = True
        except Exception as exc:
            stop_signal_sent = False
            return _result({"ok": False, "error": str(exc), "stop_signal_sent": stop_signal_sent})
        return _result({"ok": True, **task, "stop_signal_sent": stop_signal_sent})

    return _result({"ok": False, "error": f"未知 action: {action}"})


def _parse_trigger(trigger: str) -> datetime:
    raw = trigger.strip()
    if not raw:
        raise ValueError("empty trigger")
    if raw.endswith(("s", "m", "h", "d")) and raw[:-1].isdigit():
        amount = int(raw[:-1])
        unit = raw[-1]
        delta = {
            "s": timedelta(seconds=amount),
            "m": timedelta(minutes=amount),
            "h": timedelta(hours=amount),
            "d": timedelta(days=amount),
        }[unit]
        return datetime.now(timezone.utc) + delta
    return datetime.fromisoformat(raw).astimezone(timezone.utc)


@tool
def manage_cron(
    state: AgentState,
    action: str,
    trigger: str = "",
    goal: str = "",
    cron_id: str = "",
    interval: str = "",
) -> str:
    """设置、取消或查看定时任务。

    使用原则：
    - 只在目标本身确实需要未来触发或周期触发时使用。
    - `goal` 仍然要写成单步、可执行的任务描述；不要把模糊提醒写成无法验收的句子。

    时间格式：
    - `trigger` 支持相对时间：`30s`、`10m`、`2h`、`3d`
    - `trigger` 也支持 ISO 绝对时间
    - `interval` 使用与 `trigger` 相同的相对时间格式，表示重复周期

    参数：
    - action (string): `set` / `cancel` / `status`。
    - trigger (string, 可选): 触发时间；`set` 时必填。
    - goal (string, 可选): 触发时要执行的任务目标；`set` 时必填。
    - cron_id (string, 可选): 取消时使用。
    - interval (string, 可选): 重复周期。
    """
    hub_url = state.hub_url
    if not hub_url:
        return _result({"ok": False, "error": "hub_url 未配置"})
    normalized_action = action.strip().lower()
    if normalized_action == "set":
        if not trigger.strip() or not goal.strip():
            return _result({"ok": False, "error": "trigger 和 goal 不能为空"})
        trigger_at = _parse_trigger(trigger).isoformat()
        payload: dict[str, Any] = {
            "id": f"cron-{uuid.uuid4().hex[:8]}",
            "trigger_at": trigger_at,
            "goal": goal.strip(),
        }
        if interval.strip():
            payload["interval"] = interval.strip()
        _send_event(hub_url, "cron.set", payload)
        return _result({"ok": True, **payload})
    if normalized_action == "cancel":
        if not cron_id.strip():
            return _result({"ok": False, "error": "cron_id 不能为空"})
        _send_event(hub_url, "cron.cancel", {"id": cron_id.strip()})
        return _result({"ok": True, "cron_id": cron_id.strip(), "cancelled": True})
    if normalized_action == "status":
        cron_path = Path(".localagent/cron.json")
        if not cron_path.is_file():
            return _result({"ok": True, "jobs": []})
        return _result({"ok": True, "jobs": json.loads(cron_path.read_text(encoding="utf-8"))})
    return _result({"ok": False, "error": f"未知 action: {action}"})


@tool
def manage_wake(
    state: AgentState,
    inbox_ids: list[str] | None = None,
    message: str = "",
    source_topic: str = "",
) -> str:
    """将指定 inbox 重新投递到 wake 流程。

    适用场景：
    - poll 或其他后台逻辑识别到某条 silent inbox 需要重新进入标准 wake 调度。
    - 需要把指定 inbox 强绑定回 main 的 wake 输入，而不是仅靠模糊事件描述。

    参数：
    - inbox_ids (array[string]): 需要重新投递的 inbox ID 列表。
    - message (string, 可选): 补充说明。
    - source_topic (string, 可选): 事件来源标记，便于排查。
    """
    hub_url = state.hub_url
    if not hub_url:
        return _result({"ok": False, "error": "hub_url 未配置"})
    parsed_inbox_ids, inbox_ids_err = _coerce_list_param(inbox_ids, "inbox_ids")
    if inbox_ids_err:
        return _result({"ok": False, "error": inbox_ids_err})
    normalized_ids = _normalize_unique_strs(parsed_inbox_ids)
    if not normalized_ids:
        return _result({"ok": False, "error": "inbox_ids 不能为空"})
    with Store() as store:
        for inbox_id in normalized_ids:
            if store.inbox_read(inbox_id) is None:
                return _result({"ok": False, "error": f"inbox 不存在: {inbox_id}"})
            store.inbox_set_silent(inbox_id, False)
    lines = [f"inbox_id={item}" for item in normalized_ids]
    if message.strip():
        lines.append(message.strip())
    if source_topic.strip():
        lines.append(f"[meta] source={source_topic.strip()}")
    _send_event(hub_url, "agent.wake", {"message": "\n".join(lines)})
    return _result({"ok": True, "inbox_ids": normalized_ids})


@tool
def manage_env(
    state: AgentState,
    action: str,
    key: str = "",
    value: str = "",
    scope: str = "person",
) -> str:
    """管理绑定到统一用户或当前 conversation 的加密环境变量。

    适用场景：
    - 统一用户需要跨聊天软件复用 API key、token 等敏感值。
    - 个别 conversation 需要覆盖统一用户的默认 secret。

    安全规则：
    - 返回结果中永远不包含密钥值本身。
    - 不要把密钥写进 note、task goal、reply 文本或普通文件。

    参数：
    - action (string): `set` / `list` / `delete`。
    - key (string, 可选): 变量名。
    - value (string, 可选): 变量值；仅 `set` 时使用。
    - scope (string, 可选): `person` / `conversation`，默认 `person`。
    """
    scope_key, scope_err = _resolve_secret_scope(state, scope)
    if scope_err:
        return _result({"ok": False, "error": scope_err})
    normalized_action = action.strip().lower()
    if normalized_action == "set":
        if not key.strip() or not value:
            return _result({"ok": False, "error": "key 和 value 不能为空"})
        try:
            set_secret(scope_key, key.strip(), value)
        except Exception as exc:
            logger.warning("manage_env set failed for scope=%s key=%s: %s", scope_key, key.strip(), exc)
            return _result({"ok": False, "error": str(exc)})
        return _result({"ok": True, "key": key.strip(), "scope": scope, "scope_key": scope_key})
    if normalized_action == "list":
        try:
            keys = list_secrets(scope_key)
        except Exception as exc:
            logger.warning("manage_env list failed for scope=%s: %s", scope_key, exc)
            return _result({"ok": False, "error": str(exc)})
        return _result({"ok": True, "keys": keys, "scope": scope, "scope_key": scope_key})
    if normalized_action == "delete":
        if not key.strip():
            return _result({"ok": False, "error": "key 不能为空"})
        try:
            deleted = delete_secret(scope_key, key.strip())
        except Exception as exc:
            logger.warning("manage_env delete failed for scope=%s key=%s: %s", scope_key, key.strip(), exc)
            return _result({"ok": False, "error": str(exc)})
        if not deleted:
            return _result({"ok": False, "error": f"变量不存在: {key.strip()}"})
        return _result({
            "ok": True,
            "key": key.strip(),
            "deleted": True,
            "scope": scope,
            "scope_key": scope_key,
        })
    return _result({"ok": False, "error": f"未知 action: {action}"})


__all__ = [
    "add_note",
    "search_archive",
    "manage_user_profile",
    "manage_task",
    "manage_cron",
    "manage_wake",
    "manage_env",
]
