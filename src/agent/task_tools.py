"""Task agent context tools."""

import json
from typing import Any

from ..core.secrets import conversation_scope, list_secrets, person_scope
from ..core.store import Store
from ..provider import tool
from .context_refs import read_task_context_refs, read_tool_ref
from .reply_tools import send_reply
from .state import AgentState


def _result(payload: dict[str, Any]) -> str:
    return json.dumps(payload, ensure_ascii=False)


@tool
def read_task(
    state: AgentState,
    task_id: str,
) -> str:
    """读取任务的完整信息，包括目标、状态、执行结果等。

    适用场景：
    - 当前注入的 task/result 被截断，需要读取完整版本。
    - 需要查看父任务、前序任务或其他关联任务的完整 goal/result。

    使用限制：
    - 这是定向读取工具，不是扫描全部历史的入口。
    - 优先读取与当前 goal 直接相关的任务，不要发散到无关任务链。
    """
    if not task_id.strip():
        return _result({"ok": False, "error": "task_id 不能为空"})
    try:
        with Store() as store:
            task = store.task_read(task_id.strip())
    except Exception as exc:
        return _result({"ok": False, "error": f"读取任务失败: {exc}"})
    if task is None:
        return _result({"ok": False, "error": f"任务不存在: {task_id}"})

    sanitized = dict(task)
    sanitized.pop("then_chain", None)
    return _result({"ok": True, **sanitized})


@tool
def read_context_ref(
    state: AgentState,
    ref_id: str,
    max_chars: int = 6000,
) -> str:
    """读取当前任务已授权的上下文引用（ref）。

    适用场景：
    - main 已将某次工具调用结果、结构化资料或长文本片段绑定给当前 task。
    - 需要按需读取较长上下文，而不是把大段内容直接塞进 prompt。

    使用限制：
    - 只能读取当前 task 授权过的 ref_id。
    - 这是精确读取入口，不要把它当成“遍历所有历史”的工具。
    """
    normalized_ref_id = ref_id.strip()
    if not normalized_ref_id:
        return _result({"ok": False, "error": "ref_id 不能为空"})
    task_id = str(getattr(state, "task_id", "")).strip()
    if not task_id:
        return _result({"ok": False, "error": "仅 task agent 可读取 context ref"})

    allowed_ref_ids = read_task_context_refs(task_id)
    if normalized_ref_id not in allowed_ref_ids:
        return _result({
            "ok": False,
            "error": f"ref_id 不在当前任务授权列表中: {normalized_ref_id}",
            "allowed_ref_ids": allowed_ref_ids,
        })

    normalized_max_chars = max(200, min(int(max_chars), 20_000))
    data = read_tool_ref(normalized_ref_id, max_chars=normalized_max_chars)
    if data is None:
        return _result({"ok": False, "error": f"ref 不存在或已失效: {normalized_ref_id}"})
    return _result({"ok": True, "ref": data})


@tool
def search_conversation_history(
    state: AgentState,
    query: str,
    limit: int = 5,
) -> str:
    """搜索当前会话内的聊天与任务历史，用于补当前 goal 缺失的细节。

    使用原则：
    - 只搜索当前 `conversation_id`，不会跨会话扩散。
    - 仅用于补缺，不替代 `task.goal`、`recent_window` 和已绑定的 context ref。
    - 命中结果仍然是历史片段，不代表应该把旧话题重新拉成当前 focus。

    参数：
    - query (string): 与当前 goal 直接相关的检索语句。
    - limit (integer, 可选): 返回条数，建议保持较小范围。
    """
    normalized_query = query.strip()
    if not normalized_query:
        return _result({"ok": False, "error": "query 不能为空"})
    conversation_id = str(getattr(state, "current_conversation_id", "")).strip()
    if not conversation_id:
        return _result({"ok": False, "error": "当前任务缺少 conversation_id"})
    try:
        with Store() as store:
            items = store.conversation_history_search(
                conversation_id,
                normalized_query,
                limit=max(1, min(int(limit), 10)),
            )
    except Exception as exc:
        return _result({"ok": False, "error": f"搜索会话历史失败: {exc}"})
    return _result({
        "ok": True,
        "conversation_id": conversation_id,
        "query": normalized_query,
        "items": items,
    })


@tool
def inspect_env(
    state: AgentState,
    key: str = "",
) -> str:
    """读取当前任务可见 secret 的键名信息，不返回密钥值。

    适用场景：
    - reply/general task 需要确认统一用户级或当前会话级 secret 是否已经存在。
    - 需要告诉用户“变量名是什么”或“是否已保存”，但不应暴露 value。

    使用限制：
    - 返回当前任务可见的 secret key 名列表；统一用户级会被当前 conversation 覆盖。
    - 不会返回 secret value。
    """
    conversation_id = str(getattr(state, "current_conversation_id", "")).strip()
    person_id = str(getattr(state, "current_person_id", "")).strip()
    try:
        keys: list[str] = []
        seen: set[str] = set()
        for scope in [person_scope(person_id), conversation_scope(conversation_id)]:
            if not scope:
                continue
            for item in list_secrets(scope):
                if item in seen:
                    continue
                seen.add(item)
                keys.append(item)
    except Exception as exc:
        return _result({"ok": False, "error": f"读取 secret 键名失败: {exc}"})

    normalized_key = key.strip()
    if normalized_key:
        return _result({
            "ok": True,
            "conversation_id": conversation_id,
            "person_id": person_id,
            "key": normalized_key,
            "exists": normalized_key in keys,
        })
    return _result({
        "ok": True,
        "conversation_id": conversation_id,
        "person_id": person_id,
        "keys": keys,
    })


__all__ = [
    "read_task",
    "read_context_ref",
    "search_conversation_history",
    "inspect_env",
    "send_reply",
]
