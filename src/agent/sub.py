"""统一 Agent 子进程入口。

用法：
    python -m src.agent.sub --role=main --hub=ws://127.0.0.1:9600
    python -m src.agent.sub --role=task --hub=ws://127.0.0.1:9600 --task=t-abc12345
"""

import argparse
import asyncio
import inspect
import json
import logging
import os
import re
import sys
import time
import traceback
import uuid
from datetime import datetime, timezone
from functools import wraps
from pathlib import Path
from typing import Any, Callable

from websockets.sync.client import connect

from ..config import cfg
from ..core.identity import infer_person_id, is_multi_party_metadata, workspace_scope
from ..core.runtime_paths import (
    RUNTIME_CACHE_ROOT,
    RUNTIME_SKILLS_ROOT,
    RUNTIME_SOUL_PATH,
    RUNTIME_WORKSPACE_ROOT,
    RuntimePathMap,
)
from ..core.secrets import load_all_decrypted, scrub_text
from ..core.store import (
    Store,
    conversation_state_active_topic,
    conversation_state_apply_task_result,
    parse_task_outcome,
    strip_structured_outcome_block,
)
from ..core.runtime_fs import (
    PROJECT_ROOT,
    RUNTIME_ROOT,
    RUNTIME_SKILLS_ROOT as HOST_RUNTIME_SKILLS_ROOT,
    RUNTIME_SOUL_PATH as HOST_RUNTIME_SOUL_PATH,
    ensure_runtime_layout,
    resolve_runtime_workspace_root,
)
from ..index import IndexClient
from ..runtime import RuntimeClient
from ..core.usage import (
    parse_startup_payload,
    persist_usage_record,
    resolve_conversation_id,
)
from ..provider import Mimo, Qwen
from ..retry import RetryPolicy
from .context_refs import write_tool_ref
from .main_tools import (
    add_note,
    manage_cron,
    manage_env,
    manage_skills,
    manage_task,
    manage_user_profile,
    manage_wake,
    search_archive,
)
from .prompts import build_role_messages
from .reply_tools import send_reply
from .state import AgentState
from .task_tools import inspect_cron, inspect_env, read_context_ref, read_task, search_conversation_history
from .topic_memory import archive_topic_snapshot
from .tools import (
    apply_patch,
    bash_list,
    bash_read,
    bash_run,
    bash_start,
    bash_stop,
    bash_wait,
    bash_write,
    read,
    write,
)

logger = logging.getLogger("src.agent.sub")
STARTUP_PAYLOAD_TOPIC = "hub.agent_startup_payload"
_RETRY = RetryPolicy.for_service("agent_sub")
_CHAT_PROVIDER_MAP = {
    "qwen": Qwen,
    "mimo": Mimo,
}
_TOOL_REF_RESULT_MAX_CHARS = 20_000
_TOOL_REF_ARGS_MAX_CHARS = 8_000
_AUTO_CONTEXT_REF_TOOLS = {
    "search_archive",
    "manage_user_profile",
}
_MAX_RECENT_CONTEXT_REFS = 6
_IMAGE_ATTACHMENT_EXTENSIONS = {
    ".png",
    ".jpg",
    ".jpeg",
    ".gif",
    ".webp",
    ".bmp",
}
_TRUE_VALUES = {"1", "true", "yes", "on"}


def _env_truthy(name: str) -> bool:
    return os.environ.get(name, "").strip().lower() in _TRUE_VALUES


def _replace_stdout_with_devnull() -> None:
    try:
        sys.stdout = open(os.devnull, "w", encoding="utf-8")
    except OSError:
        pass


def _resolve_agent_roots(
    *,
    role: str,
    conversation_id: str,
    runtime_identity: dict[str, Any],
) -> dict[str, str | bool]:
    ensure_runtime_layout()
    if role == "task":
        host_project_root = str(RUNTIME_ROOT.resolve())
        host_workspace_root = str(resolve_runtime_workspace_root(
            conversation_id=conversation_id,
            person_id=str(runtime_identity.get("person_id", "")).strip(),
            is_multi_party=bool(runtime_identity.get("is_multi_party", False)),
        ))
        host_skills_root = str(HOST_RUNTIME_SKILLS_ROOT.resolve())
        host_soul_path = str(HOST_RUNTIME_SOUL_PATH.resolve())
        host_cache_root = str((RUNTIME_ROOT / "cache").resolve())
        Path(host_cache_root).mkdir(parents=True, exist_ok=True)
        return {
            "containerized": True,
            "project_root": str(RUNTIME_WORKSPACE_ROOT),
            "workspace_root": str(RUNTIME_WORKSPACE_ROOT),
            "cwd": str(RUNTIME_WORKSPACE_ROOT),
            "skills_root": str(RUNTIME_SKILLS_ROOT),
            "soul_path": str(RUNTIME_SOUL_PATH),
            "cache_root": str(RUNTIME_CACHE_ROOT),
            "host_project_root": host_project_root,
            "host_workspace_root": host_workspace_root,
            "host_skills_root": host_skills_root,
            "host_soul_path": host_soul_path,
            "host_cache_root": host_cache_root,
        }

    host_project_root = str(PROJECT_ROOT.resolve())
    host_skills_root = str(HOST_RUNTIME_SKILLS_ROOT.resolve())
    host_soul_path = str((PROJECT_ROOT / "SOUL.md").resolve())
    host_cache_root = str((PROJECT_ROOT / ".localagent" / "sandbox-cache").resolve())
    host_workspace_root = str(resolve_runtime_workspace_root(
        conversation_id=conversation_id,
        person_id=str(runtime_identity.get("person_id", "")).strip(),
        is_multi_party=bool(runtime_identity.get("is_multi_party", False)),
    ))
    return {
        "containerized": False,
        "project_root": str(RUNTIME_WORKSPACE_ROOT),
        "workspace_root": str(RUNTIME_WORKSPACE_ROOT),
        "cwd": str(RUNTIME_WORKSPACE_ROOT),
        "skills_root": str(RUNTIME_SKILLS_ROOT),
        "soul_path": str(RUNTIME_SOUL_PATH),
        "cache_root": str(RUNTIME_CACHE_ROOT),
        "host_project_root": host_project_root,
        "host_workspace_root": host_workspace_root,
        "host_skills_root": host_skills_root,
        "host_soul_path": host_soul_path,
        "host_cache_root": host_cache_root,
    }


def _resolve_is_admin(role: str, payload: str, task_id: str) -> bool:
    try:
        with Store() as store:
            if role == "task" and task_id:
                task = store.task_read(task_id)
                return bool((task or {}).get("is_admin", False))
            if role == "main":
                inbox_ids = _extract_inbox_ids(payload)
                if inbox_ids:
                    return any(bool((store.inbox_read(inbox_id) or {}).get("is_admin", False)) for inbox_id in inbox_ids)
                conversation_id = resolve_conversation_id(role, payload, task_id)
                if conversation_id:
                    recent_inbox = store.inbox_list_for_conversation(conversation_id, limit=1)
                    if recent_inbox:
                        return bool(recent_inbox[0].get("is_admin", False))
    except Exception:
        logger.warning("Failed to resolve admin state", exc_info=True)
    return False


def _resolve_runtime_identity(
    role: str,
    payload: str,
    task_id: str,
    conversation_id: str,
) -> dict[str, Any]:
    resolved: dict[str, Any] = {
        "gateway": "",
        "user_id": "",
        "person_id": "",
        "is_multi_party": False,
    }
    try:
        with Store() as store:
            inbox_items: list[dict[str, Any]] = []
            if role == "main":
                for inbox_id in _extract_inbox_ids(payload):
                    inbox = store.inbox_read(inbox_id)
                    if inbox is not None:
                        inbox_items.append(inbox)
            task = store.task_read(task_id) if role == "task" and task_id else None
            state_row = store.conversation_state_read(conversation_id) if conversation_id else None
            latest_inbox = None
            if conversation_id:
                recent_inbox = store.inbox_list_for_conversation(conversation_id, limit=1)
                latest_inbox = recent_inbox[0] if recent_inbox else None

            for source in [task, state_row, *inbox_items, latest_inbox]:
                if not isinstance(source, dict):
                    continue
                metadata = source.get("metadata", {}) if isinstance(source.get("metadata", {}), dict) else {}
                if not resolved["gateway"]:
                    resolved["gateway"] = str(source.get("gateway", "")).strip()
                if not resolved["user_id"]:
                    resolved["user_id"] = str(source.get("user_id", "")).strip()
                if not resolved["person_id"]:
                    resolved["person_id"] = (
                        str(source.get("person_id", "")).strip()
                        or str(metadata.get("person_id", "")).strip()
                    )
                if not resolved["is_multi_party"]:
                    resolved["is_multi_party"] = bool(source.get("is_multi_party", False)) or is_multi_party_metadata(metadata)

            resolved["person_id"] = infer_person_id(
                gateway=str(resolved.get("gateway", "")).strip(),
                user_id=str(resolved.get("user_id", "")).strip(),
                fallback_person_id=str(resolved.get("person_id", "")).strip(),
            )
    except Exception:
        logger.warning("Failed to resolve runtime identity", exc_info=True)
    return resolved


def _append_scoped_ref(
    bucket: dict[str, list[str]] | None,
    *,
    scope_key: str,
    ref_id: str,
) -> dict[str, list[str]]:
    normalized_scope = scope_key.strip()
    if bucket is None:
        bucket = {}
    if not normalized_scope or not ref_id:
        return bucket
    existing = [
        str(item).strip()
        for item in bucket.get(normalized_scope, [])
        if str(item).strip()
    ]
    updated = [item for item in existing if item != ref_id]
    updated.append(ref_id)
    bucket[normalized_scope] = updated[-_MAX_RECENT_CONTEXT_REFS:]
    return bucket


def _safe_jsonable(value: Any, depth: int = 0) -> Any:
    if depth >= 6:
        return f"<max-depth:{type(value).__name__}>"
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, dict):
        result: dict[str, Any] = {}
        for k, v in value.items():
            result[str(k)] = _safe_jsonable(v, depth + 1)
        return result
    if isinstance(value, (list, tuple, set)):
        return [_safe_jsonable(v, depth + 1) for v in value]
    return str(value)


def _scrub_trace_text(text: str) -> str:
    safe_text = text
    try:
        secrets = load_all_decrypted()
        safe_text = scrub_text(safe_text, secrets)
    except Exception:
        pass
    return safe_text


def _append_trace_record(record: dict[str, Any]) -> None:
    with Store() as store:
        store.runtime_record_tool_call(record)


def _build_trace_record_base(
    *,
    run_id: str,
    role: str,
    task_id: str,
    tool_name: str,
) -> dict[str, Any]:
    return {
        "event_id": f"tc-{uuid.uuid4().hex[:12]}",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "run_id": run_id,
        "role": role,
        "task_id": task_id,
        "tool": tool_name,
    }


def _truncate_text(value: str, max_chars: int) -> tuple[str, bool]:
    if max_chars < 1:
        max_chars = 1
    if len(value) <= max_chars:
        return value, False
    return value[:max_chars], True


def _inject_ref_id_into_result(result_text: str) -> tuple[str, str, dict[str, Any] | None]:
    """Try to attach ref_id to JSON dict tool results."""
    try:
        parsed = json.loads(result_text)
    except Exception:
        return result_text, "", None
    if not isinstance(parsed, dict):
        return result_text, "", None

    ref_id = str(parsed.get("ref_id", "")).strip() or f"ref-{uuid.uuid4().hex[:12]}"
    parsed["ref_id"] = ref_id
    return json.dumps(parsed, ensure_ascii=False), ref_id, parsed


def _build_tool_ref_payload(
    *,
    ref_id: str,
    run_id: str,
    role: str,
    task_id: str,
    tool_name: str,
    call_args: dict[str, Any],
    result_text: str,
    parsed_result: dict[str, Any] | None,
) -> dict[str, Any]:
    args_text = json.dumps(_safe_jsonable(call_args), ensure_ascii=False)
    args_preview, args_truncated = _truncate_text(args_text, _TOOL_REF_ARGS_MAX_CHARS)
    result_preview, result_truncated = _truncate_text(
        _scrub_trace_text(result_text), _TOOL_REF_RESULT_MAX_CHARS
    )
    payload: dict[str, Any] = {
        "ref_id": ref_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "run_id": run_id,
        "role": role,
        "task_id": task_id,
        "tool": tool_name,
        "args": _safe_jsonable(call_args),
        "args_preview": args_preview,
        "args_truncated": args_truncated,
        "result": _safe_jsonable(parsed_result if parsed_result is not None else result_text),
        "result_preview": result_preview,
        "result_truncated": result_truncated,
    }
    return payload


def _extract_state(args: tuple[Any, ...], kwargs: dict[str, Any]) -> AgentState | None:
    candidate = kwargs.get("state")
    if isinstance(candidate, AgentState):
        return candidate
    if args:
        first = args[0]
        if isinstance(first, AgentState):
            return first
    return None


def _should_remember_context_ref(
    *,
    role: str,
    tool_name: str,
    call_args: dict[str, Any],
    parsed_result: dict[str, Any] | None,
) -> bool:
    if role != "main":
        return False
    if tool_name not in _AUTO_CONTEXT_REF_TOOLS:
        return False
    if not isinstance(parsed_result, dict) or parsed_result.get("ok") is not True:
        return False
    if tool_name == "manage_user_profile":
        action = str(call_args.get("action", "")).strip().lower()
        return action == "list"
    return True


def _remember_context_ref(
    state: AgentState | None,
    *,
    ref_id: str,
    role: str,
    tool_name: str,
    call_args: dict[str, Any],
    parsed_result: dict[str, Any] | None,
) -> None:
    if state is None or not ref_id:
        return
    if not _should_remember_context_ref(
        role=role,
        tool_name=tool_name,
        call_args=call_args,
        parsed_result=parsed_result,
    ):
        return
    existing = [str(item).strip() for item in (state.recent_tool_ref_ids or []) if str(item).strip()]
    updated = [item for item in existing if item != ref_id]
    updated.append(ref_id)
    state.recent_tool_ref_ids = updated[-_MAX_RECENT_CONTEXT_REFS:]
    current_conversation_id = str(getattr(state, "current_conversation_id", "")).strip()
    if current_conversation_id:
        state.recent_tool_ref_ids_by_conversation = _append_scoped_ref(
            getattr(state, "recent_tool_ref_ids_by_conversation", None),
            scope_key=current_conversation_id,
            ref_id=ref_id,
        )
    current_memory_id = str(getattr(state, "last_memory_id", "")).strip()
    if current_memory_id:
        state.recent_tool_ref_ids_by_memory = _append_scoped_ref(
            getattr(state, "recent_tool_ref_ids_by_memory", None),
            scope_key=current_memory_id,
            ref_id=ref_id,
        )


def _wrap_tool_with_trace(
    tool_func: Callable[..., Any],
    *,
    run_id: str,
    role: str,
    task_id: str,
) -> Callable[..., Any]:
    tool_name = str(getattr(tool_func, "_tool_name", tool_func.__name__))

    if inspect.iscoroutinefunction(tool_func):

        @wraps(tool_func)
        async def _wrapped_async(*args: Any, **kwargs: Any) -> Any:
            started_at = time.monotonic()
            call_args = dict(kwargs)
            call_args.pop("state", None)
            state_obj = _extract_state(args, kwargs)
            record = _build_trace_record_base(
                run_id=run_id, role=role, task_id=task_id, tool_name=tool_name
            )
            record["args"] = _safe_jsonable(call_args)
            try:
                result = await tool_func(*args, **kwargs)
            except Exception as exc:
                record["ok"] = False
                record["error"] = str(exc)
                record["traceback"] = _scrub_trace_text(traceback.format_exc())
                record["duration_ms"] = round((time.monotonic() - started_at) * 1000, 3)
                try:
                    _append_trace_record(record)
                except Exception:
                    pass
                raise

            result_text = str(result)
            result_text, ref_id, parsed_result = _inject_ref_id_into_result(result_text)
            if ref_id:
                try:
                    write_tool_ref(
                        ref_id,
                        _build_tool_ref_payload(
                            ref_id=ref_id,
                            run_id=run_id,
                            role=role,
                            task_id=task_id,
                            tool_name=tool_name,
                            call_args=call_args,
                            result_text=result_text,
                            parsed_result=parsed_result,
                        ),
                    )
                    result = result_text
                    record["ref_id"] = ref_id
                    _remember_context_ref(
                        state_obj,
                        ref_id=ref_id,
                        role=role,
                        tool_name=tool_name,
                        call_args=call_args,
                        parsed_result=parsed_result,
                    )
                except Exception:
                    logger.exception("Failed to persist tool ref: %s", ref_id)
            record["result"] = _scrub_trace_text(result_text)
            try:
                parsed = json.loads(result_text)
                if isinstance(parsed, dict) and isinstance(parsed.get("ok"), bool):
                    record["ok"] = parsed["ok"]
                else:
                    record["ok"] = True
            except Exception:
                record["ok"] = True
            record["duration_ms"] = round((time.monotonic() - started_at) * 1000, 3)
            try:
                _append_trace_record(record)
            except Exception:
                pass
            return result_text

        wrapped = _wrapped_async
    else:

        @wraps(tool_func)
        def _wrapped(*args: Any, **kwargs: Any) -> Any:
            started_at = time.monotonic()
            call_args = dict(kwargs)
            call_args.pop("state", None)
            state_obj = _extract_state(args, kwargs)
            record = _build_trace_record_base(
                run_id=run_id, role=role, task_id=task_id, tool_name=tool_name
            )
            record["args"] = _safe_jsonable(call_args)
            try:
                result = tool_func(*args, **kwargs)
            except Exception as exc:
                record["ok"] = False
                record["error"] = str(exc)
                record["traceback"] = _scrub_trace_text(traceback.format_exc())
                record["duration_ms"] = round((time.monotonic() - started_at) * 1000, 3)
                try:
                    _append_trace_record(record)
                except Exception:
                    pass
                raise

            result_text = str(result)
            result_text, ref_id, parsed_result = _inject_ref_id_into_result(result_text)
            if ref_id:
                try:
                    write_tool_ref(
                        ref_id,
                        _build_tool_ref_payload(
                            ref_id=ref_id,
                            run_id=run_id,
                            role=role,
                            task_id=task_id,
                            tool_name=tool_name,
                            call_args=call_args,
                            result_text=result_text,
                            parsed_result=parsed_result,
                        ),
                    )
                    result = result_text
                    record["ref_id"] = ref_id
                    _remember_context_ref(
                        state_obj,
                        ref_id=ref_id,
                        role=role,
                        tool_name=tool_name,
                        call_args=call_args,
                        parsed_result=parsed_result,
                    )
                except Exception:
                    logger.exception("Failed to persist tool ref: %s", ref_id)
            record["result"] = _scrub_trace_text(result_text)
            try:
                parsed = json.loads(result_text)
                if isinstance(parsed, dict) and isinstance(parsed.get("ok"), bool):
                    record["ok"] = parsed["ok"]
                else:
                    record["ok"] = True
            except Exception:
                record["ok"] = True
            record["duration_ms"] = round((time.monotonic() - started_at) * 1000, 3)
            try:
                _append_trace_record(record)
            except Exception:
                pass
            return result_text

        wrapped = _wrapped

    setattr(wrapped, "_tool_name", getattr(tool_func, "_tool_name", tool_name))
    setattr(wrapped, "_tool_schema", getattr(tool_func, "_tool_schema", {}))
    return wrapped


def _build_traced_tools(
    tools: list[Callable[..., Any]],
    *,
    role: str,
    task_id: str,
    run_id: str,
) -> list[Callable[..., Any]]:
    return [
        _wrap_tool_with_trace(
            t,
            run_id=run_id,
            role=role,
            task_id=task_id.strip(),
        )
        for t in tools
    ]


def _send_event(hub_url: str, topic: str, payload: dict) -> None:
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


def _fetch_startup_payload(hub_url: str, role: str, task_id: str, agent_key: str) -> str:
    last_exc: Exception | None = None
    for attempt in range(_RETRY.max_retries + 1):
        req_id = str(uuid.uuid4())
        try:
            ws = connect(hub_url, open_timeout=_RETRY.connect_timeout)
            try:
                ws.send(
                    json.dumps({
                        "type": "request",
                        "id": req_id,
                        "topic": STARTUP_PAYLOAD_TOPIC,
                        "payload": {"role": role, "task_id": task_id, "agent_key": agent_key},
                    })
                )
                raw = ws.recv(timeout=_RETRY.request_timeout)
            finally:
                ws.close()
            msg = json.loads(raw)
            payload = msg.get("payload", {})
            if msg.get("type") != "response" or msg.get("id") != req_id:
                return ""
            if not payload.get("ok", False):
                return ""
            return str(payload.get("payload", ""))
        except Exception as exc:
            last_exc = exc
            if attempt >= _RETRY.max_retries:
                break
            time.sleep(_RETRY.backoff_delay(attempt))
    logger.error("Failed to fetch startup payload: %s", last_exc)
    return ""


def _build_main_tools(wake_mode: str = "") -> list:
    return [
        add_note,
        search_archive,
        manage_user_profile,
        manage_cron,
        manage_env,
        manage_skills,
        manage_task,
        manage_wake,
    ]


def _read_task_type(task_id: str) -> str:
    if not task_id.strip():
        return "general"
    try:
        with Store() as store:
            task = store.task_read(task_id.strip())
    except Exception:
        logger.exception("Failed to read task type for task=%s", task_id)
        return "general"
    if not task:
        return "general"
    task_type = str(task.get("task_type", "")).strip().lower()
    return task_type or "general"


def _build_task_tools(task_type: str = "general") -> list:
    normalized = task_type.strip().lower()
    common = [read_task, read_context_ref, search_conversation_history, inspect_env, inspect_cron]
    reply_tools = [
        send_reply,
    ]
    execute_tools = [
        read,
        write,
        apply_patch,
        bash_run,
        bash_start,
        bash_read,
        bash_write,
        bash_wait,
        bash_stop,
        bash_list,
    ]

    if normalized == "reply":
        return common + reply_tools
    if normalized == "execute":
        return common + execute_tools
    return common + reply_tools + execute_tools


def _collect(
    role: str,
    task_id: str,
    payload: str = "",
    *,
    path_map: RuntimePathMap | None = None,
) -> list[str] | None:
    """收集需要注入的图片。"""
    seen: set[str] = set()
    result: list[str] = []

    def _add(paths: list) -> None:
        for p in paths:
            p = str(p).strip()
            if path_map is not None and p:
                try:
                    p = path_map.locator_to_runtime(p)
                except Exception:
                    pass
            if p and p not in seen:
                seen.add(p)
                result.append(p)

    def _is_image_attachment(attachment: dict[str, Any]) -> bool:
        flag = attachment.get("is_image")
        if isinstance(flag, bool):
            return flag
        mime_type = str(attachment.get("mime_type", "")).strip().lower()
        if mime_type.startswith("image/"):
            return True
        for key in ("file_name", "file_path"):
            suffix = Path(str(attachment.get(key, "")).strip()).suffix.lower()
            if suffix in _IMAGE_ATTACHMENT_EXTENSIONS:
                return True
        return False

    try:
        with Store() as store:
            if task_id:
                task = store.task_read(task_id)
                if task:
                    _add(task.get("images", []))
            if role == "main":
                for inbox_id in _extract_inbox_ids(payload):
                    inbox = store.inbox_read(inbox_id)
                    if not inbox:
                        continue
                    for att in inbox.get("attachments") or []:
                        if not isinstance(att, dict) or not _is_image_attachment(att):
                            continue
                        file_path = str(att.get("file_path", "")).strip()
                        if file_path:
                            _add([file_path])
    except Exception:
        logger.warning("Failed to collect images from store")

    return result if result else None


def _parse_csv_field(value: str) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for raw in str(value or "").split(","):
        item = raw.strip()
        if not item or item in seen:
            continue
        seen.add(item)
        result.append(item)
    return result


def _extract_inbox_ids(payload: str) -> list[str]:
    """从 startup payload 中提取当前 run 已 claim 的 inbox ids。"""
    startup = parse_startup_payload(payload)
    ids = _parse_csv_field(startup.get("inbox_ids", ""))
    if not ids:
        ids = re.findall(r"inbox_id=(\S+)", payload)
    if not ids:
        return []
    seen: set[str] = set()
    deduped: list[str] = []
    for inbox_id in ids:
        if inbox_id in seen:
            continue
        seen.add(inbox_id)
        deduped.append(inbox_id)
    return deduped


def _filter_unprocessed_inbox_ids(inbox_ids: list[str]) -> list[str]:
    if not inbox_ids:
        return []
    result: list[str] = []
    try:
        with Store() as store:
            for inbox_id in inbox_ids:
                inbox = store.inbox_read(inbox_id)
                if not inbox or bool(inbox.get("processed", False)):
                    continue
                result.append(inbox_id)
    except Exception:
        logger.warning("Failed to filter inbox ids", exc_info=True)
        return list(inbox_ids)
    return result


def _archive_inbox_records(hub_url: str, inbox_items: list[dict[str, Any]]) -> None:
    if not hub_url or not inbox_items:
        return
    client = IndexClient(hub_url)
    try:
        for item in inbox_items:
            content = str(item.get("content", "")).strip()
            if not content:
                continue
            metadata = {
                "gateway": item.get("gateway", ""),
                "conversation_id": item.get("conversation_id", ""),
                "message_id": item.get("message_id", ""),
                "user_id": item.get("user_id", ""),
                "person_id": item.get("person_id", ""),
                "user_name": item.get("user_name", ""),
                "attachments": item.get("attachments", []),
                "created_at": item.get("created_at", ""),
            }
            client.insert_entry(
                text=content,
                label=str(item.get("message_id", "") or item.get("id", "")),
                prefix="inbox",
                source="inbox",
                content=content,
                metadata=json.dumps(metadata, ensure_ascii=False),
            )
    finally:
        client.close()


def _archive_task_result(hub_url: str, task: dict[str, Any]) -> None:
    if not hub_url:
        return
    result_text = str(task.get("result", "")).strip()
    if not result_text:
        return
    clean_result_text = strip_structured_outcome_block(result_text)
    metadata = {
        "task_id": task.get("id", ""),
        "task_type": task.get("task_type", ""),
        "topic_id": task.get("topic_id", ""),
        "conversation_id": task.get("conversation_id", ""),
        "gateway": task.get("gateway", ""),
        "person_id": task.get("person_id", ""),
        "message_id": task.get("message_id", ""),
        "outcome_status": str((task.get("outcome_json", {}) or {}).get("status", "")),
        "updated_at": task.get("updated_at", ""),
    }
    client = IndexClient(hub_url)
    try:
        client.insert_entry(
            text="\n".join(filter(None, [str(task.get("goal", "")).strip(), clean_result_text])),
            label=str(task.get("id", "")),
            prefix="task",
            source="task",
            content=result_text,
            metadata=json.dumps(metadata, ensure_ascii=False),
        )
    finally:
        client.close()


def _build_role_io(
    role: str,
    hub_url: str,
    payload: str,
    task_id: str,
    *,
    project_root: str,
    workspace_root: str,
    cwd: str,
    skills_root: str,
    soul_path: str,
    cache_root: str,
    host_project_root: str,
    host_workspace_root: str,
    host_skills_root: str,
    host_soul_path: str,
    workspace_scope: str,
    is_admin: bool,
    sandboxed: bool,
    containerized: bool,
) -> tuple[str, str]:
    inbox_ids = None
    if role == "main" and payload:
        inbox_ids = _filter_unprocessed_inbox_ids(_extract_inbox_ids(payload)) or None
    return build_role_messages(
        role=role,
        task_id=task_id,
        inbox_ids=inbox_ids or None,
        payload=payload,
        hub_url=hub_url,
        project_root=project_root,
        workspace_root=workspace_root,
        cwd=cwd,
        skills_root=skills_root,
        soul_path=soul_path,
        cache_root=cache_root,
        host_project_root=host_project_root,
        host_workspace_root=host_workspace_root,
        host_skills_root=host_skills_root,
        host_soul_path=host_soul_path,
        workspace_scope=workspace_scope,
        is_admin=is_admin,
        sandboxed=sandboxed,
        containerized=containerized,
    )


def _run_agent(args: argparse.Namespace) -> None:
    """Core agent logic, shared by direct-start and pool-worker modes."""
    run_id = f"run-{uuid.uuid4().hex[:12]}"

    chat_cfg = cfg.chat(args.role)
    if not chat_cfg:
        logger.error("Chat profile not configured for role=%s", args.role)
        sys.exit(1)
    api_key = chat_cfg["api_key"]
    if not api_key:
        logger.error("API key not set (provider=%s, profile=%s)", chat_cfg["provider"], chat_cfg["profile"])
        sys.exit(1)
    model = chat_cfg.get("model", "")
    base_url = chat_cfg.get("base_url", "")
    image_input_mode = chat_cfg.get("image_input_mode", "paths")

    payload = _fetch_startup_payload(args.hub, args.role, args.task, args.agent_key)
    startup_meta = parse_startup_payload(payload)
    conversation_id = resolve_conversation_id(args.role, payload, args.task)
    is_admin = _resolve_is_admin(args.role, payload, args.task)
    sandboxed = not is_admin
    runtime_identity = _resolve_runtime_identity(args.role, payload, args.task, conversation_id)
    scope_kind, scope_id = workspace_scope(
        person_id=str(runtime_identity.get("person_id", "")).strip(),
        conversation_id=conversation_id,
        is_multi_party=bool(runtime_identity.get("is_multi_party", False)),
    )
    roots = _resolve_agent_roots(
        role=args.role,
        conversation_id=conversation_id,
        runtime_identity=runtime_identity,
    )
    if bool(roots.get("containerized", False)):
        sandboxed = True

    if args.role == "main":
        tools = _build_main_tools(startup_meta.get("wake_mode", ""))
    else:
        if not args.task:
            logger.error("--task is required for role=task")
            sys.exit(1)
        tools = _build_task_tools(_read_task_type(args.task))
    tools = _build_traced_tools(
        tools,
        role=args.role,
        task_id=args.task,
        run_id=run_id,
    )

    current_workspace_scope = f"{scope_kind}:{scope_id}" if scope_id else scope_kind
    system_prompt, user_input = _build_role_io(
        args.role,
        args.hub,
        payload,
        args.task,
        project_root=str(roots["project_root"]),
        workspace_root=str(roots["workspace_root"]),
        cwd=str(roots["cwd"]),
        skills_root=str(roots["skills_root"]),
        soul_path=str(roots["soul_path"]),
        cache_root=str(roots["cache_root"]),
        host_project_root=str(roots["host_project_root"]),
        host_workspace_root=str(roots["host_workspace_root"]),
        host_skills_root=str(roots["host_skills_root"]),
        host_soul_path=str(roots["host_soul_path"]),
        workspace_scope=current_workspace_scope,
        is_admin=is_admin,
        sandboxed=sandboxed,
        containerized=bool(roots["containerized"]),
    )
    pending_inbox_ids = (
        _filter_unprocessed_inbox_ids(_extract_inbox_ids(payload))
        if args.role == "main" and payload
        else []
    )
    state = AgentState(
        project_root=str(roots["project_root"]),
        workspace_root=str(roots["workspace_root"]),
        cwd=str(roots["cwd"]),
        soul_path=str(roots["soul_path"]),
        host_project_root=str(roots["host_project_root"]),
        host_workspace_root=str(roots["host_workspace_root"]),
        host_skills_path=str(roots["host_skills_root"]),
        host_soul_path=str(roots["host_soul_path"]),
        runtime_project_root=str(roots["project_root"]),
        runtime_workspace_root=str(roots["workspace_root"]),
        runtime_skills_path=str(roots["skills_root"]),
        runtime_soul_path=str(roots["soul_path"]),
        runtime_cache_root=str(roots["cache_root"]),
        containerized=bool(roots["containerized"]),
        is_admin=is_admin,
        sandboxed=sandboxed,
        skills_path=str(roots["skills_root"]),
        hub_url=args.hub,
        task_id=args.task,
        current_conversation_id=conversation_id,
        current_person_id=str(runtime_identity.get("person_id", "")).strip(),
        current_workspace_scope=current_workspace_scope,
        current_is_multi_party=bool(runtime_identity.get("is_multi_party", False)),
        pending_inbox_ids=pending_inbox_ids,
        messages=[{"role": "system", "content": system_prompt}],
    )
    agent_kwargs: dict = {
        "model": model,
        "api_key": api_key,
        "tools": tools,
        "state": state,
        "image_input_mode": image_input_mode,
    }
    if base_url:
        agent_kwargs["base_url"] = base_url
    provider_name = str(chat_cfg.get("provider", "")).lower()
    provider_cls = _CHAT_PROVIDER_MAP.get(provider_name)
    if provider_cls is None:
        logger.error("Unsupported chat provider: %s", provider_name)
        sys.exit(1)
    agent = provider_cls(**agent_kwargs)

    logger.info("Agent starting (role=%s)", args.role)
    run_status = "ok"
    run_error = ""
    completed_task_ids = _parse_csv_field(startup_meta.get("completed_task_ids", ""))
    path_map = RuntimePathMap.from_state(state)
    images: list[str] | None = _collect(args.role, args.task, payload, path_map=path_map)

    _t0 = time.monotonic()
    result = ""
    parsed_outcome: dict[str, Any] | None = None
    task_final_status = ""
    should_skip_event = False
    try:
        result = asyncio.run(agent.run(user_input, images=images))
        if args.role == "task" and args.task:
            with Store() as store:
                current_task = store.task_read(args.task) or {}
            parsed_outcome = parse_task_outcome(
                result,
                task_type=str(current_task.get("task_type", "")).strip(),
            )
        logger.info("Agent finished: %s", result[:200] if result else "(empty)")
        logger.info(f"Agent Usage:{agent.usage}")
    except Exception as exc:
        run_status = "error"
        run_error = str(exc)
        logger.exception("Agent run failed")
    finally:
        # Auto-complete task: write agent output as result, set status=done.
        # Respect externally stopped tasks and do not overwrite stopped -> done.
        if args.role == "task" and args.task:
            try:
                with Store() as store:
                    existing = store.task_read(args.task)
                    if existing and existing.get("status") == "stopped":
                        task_final_status = "stopped"
                        logger.info("Task kept stopped, skip auto-complete: %s", args.task)
                    else:
                        summary = (result if result else run_error[:500]) or "(no output)"
                        updated = store.task_complete(args.task, summary, outcome=parsed_outcome)
                        task_final_status = str((updated or {}).get("status", "done"))
                        if updated:
                            conversation_id = str(updated.get("conversation_id", "")).strip()
                            if conversation_id:
                                store.conversation_state_apply(
                                    conversation_id,
                                    lambda current: {
                                        **conversation_state_apply_task_result(
                                            current,
                                            task=updated,
                                            summary=summary,
                                            outcome=(updated.get("outcome_json", {}) or parsed_outcome),
                                        ),
                                        "active_task_ids": [
                                            item
                                            for item in (current.get("active_task_ids", []) or [])
                                            if str(item).strip() != args.task
                                        ],
                                    },
                                    gateway=str(updated.get("gateway", "")).strip(),
                                    user_id=str(updated.get("user_id", "")).strip(),
                                    person_id=str(updated.get("person_id", "")).strip(),
                                )
                                store.conversation_event_append(
                                    conversation_id,
                                    "task_completed",
                                    payload={
                                        "task_id": args.task,
                                        "task_type": updated.get("task_type", ""),
                                        "result": summary[:500],
                                        "topic_id": updated.get("topic_id", ""),
                                        "outcome_status": str((updated.get("outcome_json", {}) or {}).get("status", "")),
                                    },
                                )
                                latest_state = store.conversation_state_read(conversation_id) or {}
                                target_topic_id = str((updated.get("topic_id", "") or "")).strip()
                                target_topic = next(
                                    (
                                        item
                                        for item in (latest_state.get("topics", []) or [])
                                        if str(item.get("id", "")).strip() == target_topic_id
                                    ),
                                    None,
                                ) if target_topic_id else conversation_state_active_topic(latest_state)
                                archive_topic_snapshot(
                                    args.hub,
                                    conversation_id=conversation_id,
                                    topic=target_topic,
                                )
                            _archive_task_result(args.hub, updated)
                        logger.info("Task auto-completed: %s", args.task)
            except Exception:
                logger.exception("Failed to auto-complete task %s", args.task)

        # Finalize claimed conversation work for main-agent run.
        if args.role == "main":
            try:
                with Store() as store:
                    finish_result = store.conversation_work_finish(
                        args.agent_key,
                        conversation_id,
                        mark_inbox_processed=(run_status == "ok"),
                        consumed_task_ids=completed_task_ids,
                    )
                    archived_items = list(finish_result.get("processed_inbox_items", []) or [])
                    for inbox_item in archived_items:
                        current_conversation_id = str((inbox_item or {}).get("conversation_id", "")).strip()
                        if not current_conversation_id:
                            continue
                        store.conversation_state_apply(
                            current_conversation_id,
                            lambda current: {
                                "person_id": str((inbox_item or {}).get("person_id", "")).strip(),
                                "is_multi_party": bool(
                                    ((inbox_item or {}).get("metadata", {}) or {}).get("is_multi_party", False)
                                ),
                                "last_user_message_id": str((inbox_item or {}).get("message_id", "")).strip(),
                                "active_topic_id": str((conversation_state_active_topic(current) or {}).get("id", "")).strip(),
                            },
                            gateway=str((inbox_item or {}).get("gateway", "")).strip(),
                            user_id=str((inbox_item or {}).get("user_id", "")).strip(),
                            person_id=str((inbox_item or {}).get("person_id", "")).strip(),
                            is_multi_party=bool(
                                ((inbox_item or {}).get("metadata", {}) or {}).get("is_multi_party", False)
                            ),
                        )
                        store.conversation_event_append(
                            current_conversation_id,
                            "inbox_received",
                            payload={
                                "inbox_id": str((inbox_item or {}).get("id", "")).strip(),
                                "message_id": str((inbox_item or {}).get("message_id", "")).strip(),
                            },
                        )
                    if archived_items:
                        _archive_inbox_records(args.hub, archived_items)
                        logger.info("Processed %d claimed inbox messages", len(archived_items))
            except Exception:
                logger.exception("Failed to finalize conversation work for %s", conversation_id)

        usage_record = {
            "run_id": run_id,
            "role": args.role,
            "task_id": args.task.strip(),
            "provider": chat_cfg["provider"],
            "model": model,
            "base_url": base_url,
            "hub_url": args.hub,
            "conversation_id": conversation_id,
            "wake_mode": startup_meta.get("wake_mode", ""),
            "source_topic": startup_meta.get("source_topic", ""),
            "status": run_status,
            "error": run_error,
            "total_iterations": agent.total_iterations,
            "total_tool_calls": agent.total_tool_calls,
            "total_retries": agent.total_retries,
            "usage": agent.usage,
            "elapsed_seconds": round(time.monotonic() - _t0, 3),
        }
        try:
            persist_usage_record(usage_record)
        except Exception:
            logger.exception("Failed to persist usage record")

        # Prepare event payload
        event_topic: str | None = None
        event_payload: dict[str, Any] = {}
        if args.role == "main":
            event_topic = "agent.main_done"
            event_payload = {
                "conversation_id": conversation_id,
                "agent_key": args.agent_key,
                "run_id": run_id,
            }
        else:
            # Only emit task_done for completed tasks.
            if task_final_status == "stopped":
                should_skip_event = True
            else:
                event_topic = "agent.task_done"
                event_payload = {"task_id": args.task}
        if not should_skip_event and event_topic:
            try:
                _send_event(args.hub, event_topic, event_payload)
            except Exception:
                logger.warning("Failed to send %s event", event_topic)
        if args.role == "task" and args.task.strip() and args.hub.strip():
            try:
                client = RuntimeClient(args.hub)
                try:
                    client.call(task_id=args.task.strip(), method="shutdown", params={})
                finally:
                    client.close()
            except Exception:
                logger.warning("Failed to shutdown runtime session for task=%s", args.task)


def main() -> None:
    parser = argparse.ArgumentParser(description="LocalAgent agent subprocess")
    parser.add_argument("--role", required=True, choices=["main", "task"])
    parser.add_argument("--hub", default="", help="Hub WebSocket URL")
    parser.add_argument("--task", default="", help="Task ID (role=task)")
    parser.add_argument("--agent-key", default="", help="Hub-assigned process key")
    parser.add_argument(
        "--pool-worker",
        action="store_true",
        help="Pool worker mode: signal ready, wait for assignment on stdin",
    )
    args = parser.parse_args()

    if args.pool_worker:
        # Pool worker mode: imports are done, signal ready, then wait.
        logging.basicConfig(
            level=logging.INFO,
            format=f"%(asctime)s [{args.role}] %(name)s %(levelname)s: %(message)s",
        )
        # Signal readiness — Hub reads this line to know the worker is warm.
        try:
            sys.stdout.write('{"ready":true}\n')
            sys.stdout.flush()
        except BrokenPipeError:
            _replace_stdout_with_devnull()
            sys.exit(0)
        logger.debug("Pool worker ready (role=%s, pid=%d)", args.role, os.getpid())

        # Block until Hub sends assignment JSON on stdin.
        # Catch KeyboardInterrupt: Ctrl+C sends SIGINT to the whole process
        # group before Hub's graceful shutdown can close our stdin.
        try:
            line = sys.stdin.readline()
        except KeyboardInterrupt:
            sys.exit(0)
        if not line:
            sys.exit(0)  # stdin closed, Hub no longer needs this worker
        try:
            assignment = json.loads(line)
        except json.JSONDecodeError:
            logger.error("Pool worker received invalid JSON: %s", line.strip())
            sys.exit(1)

        args.hub = assignment.get("hub", "")
        args.task = assignment.get("task", "")
        args.agent_key = assignment.get("agent_key", "")
        if not args.hub:
            logger.error("Pool worker assignment missing 'hub'")
            sys.exit(1)
        logger.info(
            "Pool worker activated (role=%s, agent_key=%s)",
            args.role,
            args.agent_key,
        )
    else:
        # Direct-start mode (backwards compatible).
        if not args.hub:
            print("--hub is required", file=sys.stderr)
            sys.exit(1)
        logging.basicConfig(
            level=logging.INFO,
            format=f"%(asctime)s [{args.role}] %(name)s %(levelname)s: %(message)s",
        )

    _run_agent(args)


if __name__ == "__main__":
    main()
