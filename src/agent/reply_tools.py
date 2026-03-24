"""Shared reply tools used by main/task agents."""

import json
from pathlib import Path
from typing import Any

from ..core.artifacts import ArtifactStore, is_artifact_ref
from ..core.runtime_paths import RuntimePathMap
from ..core.store import Store
from ..gateway import GatewayClient
from ..provider import tool
from .sandbox import ToolSandbox
from .state import AgentState


def _result(payload: dict[str, Any]) -> str:
    return json.dumps(payload, ensure_ascii=False)


def _coerce_list_param(value: Any, field_name: str) -> tuple[list[Any], str]:
    """将参数归一化为 list。支持直接传 list，或传 JSON 数组字符串。"""
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
            return [], f"{field_name} 字符串解析失败，请传数组或 JSON 数组字符串: {exc}"
        if not isinstance(parsed, list):
            return [], f"{field_name} 必须是数组，或可解析为数组的 JSON 字符串"
        return parsed, ""
    return [], f"{field_name} 必须是数组，或可解析为数组的 JSON 字符串"


@tool
def send_reply(
    state: AgentState,
    gateway: str,
    conversation_id: str,
    text: str = "",
    user_id: str = "",
    message_id: str = "",
    file_paths: list[str] | None = None,
) -> str:
    """通过 gateway 向外部聊天会话发送回复。

    适用场景：
    - `reply` 或 `general` task 需要向用户发送文本、引用回复或文件。
    - 当 task goal 明确要求“交付结果”“确认收到”“发起澄清”时使用。

    使用规则：
    - 不要输出密钥、token 或其他敏感值。
    - `message_id` 用于引用回复；若当前 task 已绑定 `reply_to_message_id`，留空也会自动回填。
    - 长文本应按自然段拆分多次发送，不要把明显会超长的内容硬塞进一次回复。
    - `file_paths` 仅用于发送已存在的运行时文件或 artifact 引用；不要把“尚未生成”的文件描述成已发送。

    参数：
    - gateway / conversation_id: 目标会话路由；若 task 已绑定，可留空让系统回填。
    - text (string, 可选): 要发送的文本。
    - user_id (string, 可选): 网关侧用户标识。
    - message_id (string, 可选): 被引用的原消息 ID。
    - file_paths (array[string], 可选): 要发送的运行时文件路径或 artifact 引用列表；普通用户仅允许当前 runtime 范围内文件。

    返回：
    - 发送成功后返回 gateway 结果；系统会顺带写入 reply 记录，并更新 conversation_state / conversation_events。
    """
    hub_url = state.hub_url
    if not hub_url:
        return _result({"ok": False, "error": "hub_url 未配置"})
    parsed_file_paths, file_paths_err = _coerce_list_param(file_paths, "file_paths")
    if file_paths_err:
        return _result({"ok": False, "error": file_paths_err})

    normalized_artifacts: list[str] = []
    if parsed_file_paths:
        sandbox = ToolSandbox.from_state(state)
        path_map = RuntimePathMap.from_state(state)
        artifact_store = ArtifactStore()
        for fp in parsed_file_paths[:3]:
            raw_fp = str(fp).strip()
            if is_artifact_ref(raw_fp):
                try:
                    artifact_store.stat(raw_fp)
                except Exception:
                    return _result({"ok": False, "error": f"artifact 不存在: {raw_fp}"})
                normalized_artifacts.append(raw_fp)
                continue
            runtime_path = path_map.locator_to_runtime(str(fp).strip())
            runtime_candidate = Path(runtime_path)
            if not sandbox.is_allowed(runtime_candidate, access="read"):
                return _result({
                    "ok": False,
                    "error": f"文件路径不在当前沙箱范围内: {fp}",
                })
            host_path = path_map.locator_to_host(runtime_path)
            host_file = Path(host_path).resolve()
            if not host_file.is_file():
                return _result({"ok": False, "error": f"文件不存在: {fp}"})
            normalized_artifacts.append(artifact_store.import_file(host_file, file_name=host_file.name))

    if not text.strip() and not normalized_artifacts:
        return _result({"ok": False, "error": "text 和 file_paths 不能同时为空"})

    gw = gateway.strip()
    cid = conversation_id.strip()
    uid = user_id.strip()
    rtm_id = message_id.strip()
    task_id = getattr(state, "task_id", "")
    task_data: dict[str, Any] | None = None
    if task_id:
        try:
            with Store() as store:
                task_data = store.task_read(task_id)
        except Exception:
            task_data = None
    if task_data:
        gw = gw or str(task_data.get("gateway", "")).strip()
        cid = cid or str(task_data.get("conversation_id", "")).strip()
        uid = uid or str(task_data.get("user_id", "")).strip()
        rtm_id = rtm_id or str(task_data.get("reply_to_message_id", "")).strip()

    if not gw:
        return _result({"ok": False, "error": "gateway 不能为空"})
    if not cid:
        return _result({"ok": False, "error": "conversation_id 不能为空"})

    metadata: dict[str, Any] = {}
    if rtm_id:
        metadata["reply_to_message_id"] = rtm_id

    client = GatewayClient(hub_url)
    try:
        result = client.send(
            gateway=gw,
            conversation_id=cid,
            text=text,
            user_id=uid,
            task_id=task_id,
            metadata=metadata if metadata else None,
            artifact_refs=normalized_artifacts if normalized_artifacts else None,
        )
    except Exception as exc:
        return _result({"ok": False, "error": str(exc)})
    finally:
        client.close()

    del result
    return _result({"ok": True})
