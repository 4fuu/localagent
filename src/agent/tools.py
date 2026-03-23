import base64
import json
import logging
import mimetypes
import os
import re
import shlex
import socket
import subprocess
import tempfile
import threading
import time
import uuid
from pathlib import Path
from typing import Any

from src.core.skills import skills_list

from ..core.runtime_paths import RuntimePathMap
from ..core.secrets import (
    SECRETS_FILE,
    SECRET_KEY_ENV,
    conversation_scope,
    load_all_decrypted,
    person_scope,
    scrub_text,
)
from ..index import IndexClient
from ..provider import BaseState, tool
from ..runtime import RuntimeClient
from .sandbox import ToolSandbox
from .sandbox_runner import SandboxRunner, create_sandbox_runner

from .state import AgentState

_IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp"}
logger = logging.getLogger(__name__)


def _result(payload: dict[str, Any]) -> str:
    return json.dumps(payload, ensure_ascii=False)


def _runtime_managed(state: BaseState) -> bool:
    return bool(getattr(state, "task_id", "").strip())


def _runtime_call(
    state: BaseState,
    *,
    method: str,
    params: dict[str, Any] | None = None,
) -> dict[str, Any]:
    task_id = str(getattr(state, "task_id", "")).strip()
    hub_url = str(getattr(state, "hub_url", "")).strip()
    if not task_id or not hub_url:
        raise RuntimeError("runtime call requires task_id and hub_url")
    client = RuntimeClient(hub_url)
    try:
        return client.call(task_id=task_id, method=method, params=params or {})
    finally:
        client.close()


def _sandbox(state: BaseState) -> ToolSandbox:
    return ToolSandbox.from_state(state)


def _workspace_root(state: BaseState) -> Path:
    return _sandbox(state).workspace_root


def _project_root(state: BaseState) -> Path:
    return _sandbox(state).project_root


def _workspace_restricted(state: BaseState) -> bool:
    return _sandbox(state).restricted


def _ensure_within_workspace(target: Path, state: BaseState) -> Path:
    return _sandbox(state).ensure_allowed(target, access="write")


def _effective_cwd(state: BaseState) -> Path:
    return _sandbox(state).effective_cwd(str(getattr(state, "cwd", "")))


def _resolve_path(path: str, state: BaseState, *, access: str = "read") -> Path:
    sandbox = _sandbox(state)
    return sandbox.resolve_path(path, access=access, base_dir=_effective_cwd(state))


def _truncate_text(text: str, max_chars: int) -> tuple[str, bool]:
    if len(text) <= max_chars:
        return text, False
    return text[:max_chars], True


def _workspace_tmpfile(
    state: BaseState,
    *,
    prefix: str,
    suffix: str,
) -> tuple[int, str]:
    workspace_root = _workspace_root(state)
    workspace_root.mkdir(parents=True, exist_ok=True)
    return tempfile.mkstemp(dir=str(workspace_root), prefix=prefix, suffix=suffix)


class _SecretLookupServer:
    def __init__(self, root: Path, secrets: dict[str, str]) -> None:
        self._secrets = dict(secrets)
        self._closed = threading.Event()
        # Unix domain sockets have a small path-length limit; keep both the
        # directory and file name short because workspace roots are already long.
        self._temp_dir = Path(tempfile.mkdtemp(dir=str(root), prefix=".s"))
        self._socket_path = self._temp_dir / "s"
        self._server: socket.socket | None = None
        self._thread: threading.Thread | None = None
        try:
            os.chmod(self._temp_dir, 0o700)
            self._server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            self._server.bind(str(self._socket_path))
            os.chmod(self._socket_path, 0o600)
            self._server.listen(8)
            self._server.settimeout(0.2)
            self._thread = threading.Thread(
                target=self._serve,
                name=f"secret-lookup-{uuid.uuid4().hex[:8]}",
                daemon=True,
            )
            self._thread.start()
        except Exception:
            self._cleanup_files()
            raise

    @property
    def socket_path(self) -> str:
        return str(self._socket_path)

    def close(self) -> None:
        if self._closed.is_set():
            return
        self._closed.set()
        try:
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
                client.connect(str(self._socket_path))
        except OSError:
            pass
        try:
            if self._server is not None:
                self._server.close()
        except OSError:
            pass
        if self._thread is not None:
            self._thread.join(timeout=1.0)
        self._cleanup_files()
        self._secrets.clear()

    def _cleanup_files(self) -> None:
        try:
            if self._socket_path.exists():
                self._socket_path.unlink()
        except OSError:
            pass
        try:
            if self._temp_dir.exists():
                self._temp_dir.rmdir()
        except OSError:
            pass

    def _serve(self) -> None:
        while not self._closed.is_set():
            try:
                conn, _ = self._server.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            with conn:
                try:
                    chunks: list[bytes] = []
                    total = 0
                    while total < 4096:
                        chunk = conn.recv(4096 - total)
                        if not chunk:
                            break
                        chunks.append(chunk)
                        total += len(chunk)
                    key = b"".join(chunks).decode("utf-8").strip()
                    conn.sendall(self._secrets.get(key, "").encode("utf-8"))
                except OSError:
                    continue


_CONTAINER_FILE_OP_SCRIPT = r"""
import base64
import json
import os
import sys
import tempfile
from pathlib import Path

payload = json.loads(base64.b64decode(sys.argv[1]))
op = payload.get("op", "")
path = Path(payload.get("path", ""))
result = {"ok": True}

if op == "inspect":
    if not path.exists():
        result["exists"] = False
    else:
        result["exists"] = True
        result["is_dir"] = path.is_dir()
        if path.is_file():
            result["size"] = path.stat().st_size
            if payload.get("include_bytes"):
                limit = int(payload.get("max_bytes", 0))
                with path.open("rb") as f:
                    data = f.read(limit + 1) if limit > 0 else f.read()
                truncated = False
                if limit > 0 and len(data) > limit:
                    data = data[:limit]
                    truncated = True
                result["data_b64"] = base64.b64encode(data).decode("ascii")
                result["truncated"] = truncated
elif op == "write":
    content = str(payload.get("content", ""))
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(
        dir=str(path.parent), prefix=f".{path.name}.", suffix=".tmp"
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="") as f:
            f.write(content)
        os.replace(tmp_path, path)
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
elif op == "delete":
    if path.exists() and path.is_file():
        path.unlink()
elif op == "move":
    target = Path(str(payload.get("target", "")))
    target.parent.mkdir(parents=True, exist_ok=True)
    os.replace(path, target)
else:
    raise ValueError(f"unknown file op: {op}")

print(json.dumps(result, ensure_ascii=False), end="")
""".strip()


def _run_sandbox_file_op(state: BaseState, payload: dict[str, Any]) -> dict[str, Any] | None:
    if _runtime_managed(state):
        method = str(payload.get("op", "")).strip()
        if method == "inspect":
            return _runtime_call(
                state,
                method="inspect",
                params={
                    "path": str(payload.get("path", "")).strip(),
                    "include_bytes": bool(payload.get("include_bytes", False)),
                    "max_bytes": int(payload.get("max_bytes", 0)),
                },
            )
        if method == "write":
            return _runtime_call(
                state,
                method="write",
                params={
                    "path": str(payload.get("path", "")).strip(),
                    "content": str(payload.get("content", "")),
                },
            )
        if method == "delete":
            return _runtime_call(
                state,
                method="delete",
                params={"path": str(payload.get("path", "")).strip()},
            )
        if method == "move":
            return _runtime_call(
                state,
                method="move",
                params={
                    "path": str(payload.get("path", "")).strip(),
                    "target": str(payload.get("target", "")).strip(),
                },
            )
        raise ValueError(f"unknown runtime file op: {method}")
    return None


def _inspect_path(
    target: Path,
    state: BaseState,
    *,
    include_bytes: bool = False,
    max_bytes: int = 0,
) -> dict[str, Any]:
    sandbox_result = _run_sandbox_file_op(
        state,
        {
            "op": "inspect",
            "path": str(target),
            "include_bytes": include_bytes,
            "max_bytes": max_bytes,
        },
    )
    if sandbox_result is not None:
        return sandbox_result

    result: dict[str, Any] = {"ok": True, "exists": target.exists()}
    if not result["exists"]:
        return result
    result["is_dir"] = target.is_dir()
    if target.is_file():
        result["size"] = target.stat().st_size
        if include_bytes:
            with target.open("rb") as f:
                data = f.read(max_bytes + 1) if max_bytes > 0 else f.read()
            truncated = False
            if max_bytes > 0 and len(data) > max_bytes:
                data = data[:max_bytes]
                truncated = True
            result["data_b64"] = base64.b64encode(data).decode("ascii")
            result["truncated"] = truncated
    return result


def _write_text_via_sandbox(target: Path, content: str, state: BaseState) -> None:
    sandbox_result = _run_sandbox_file_op(
        state,
        {"op": "write", "path": str(target), "content": content},
    )
    if sandbox_result is not None:
        return
    _atomic_write_text(target, content)


def _delete_file_via_sandbox(target: Path, state: BaseState) -> None:
    sandbox_result = _run_sandbox_file_op(
        state,
        {"op": "delete", "path": str(target)},
    )
    if sandbox_result is not None:
        return
    if target.exists() and target.is_file():
        target.unlink()


def _is_managed_secrets_path(path: Path) -> bool:
    try:
        return path.resolve() == SECRETS_FILE.resolve()
    except Exception:
        return False


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


def _atomic_write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(
        dir=str(path.parent), prefix=f".{path.name}.", suffix=".tmp"
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="") as f:
            f.write(content)
        os.replace(tmp_path, path)
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)


def _read_single(
    path: str,
    state: BaseState,
    start_line: int,
    end_line: int,
    max_bytes: int,
    *,
    numbered: bool,
    include_meta: bool,
) -> dict[str, Any]:
    """读取单个文件并返回结果字典。

    返回字段：
    - ok/path/content：始终返回。
    - start_line/end_line/total_lines/truncated/numbered：仅 include_meta=True 时返回。

    失败条件：路径为空、文件不存在、路径是目录、行号区间非法。
    """
    try:
        target = _resolve_path(path, state, access="read")
    except ValueError as exc:
        return {"ok": False, "error": str(exc)}
    inspect_result = _inspect_path(target, state)
    if not inspect_result.get("exists", False):
        return {"ok": False, "error": f"文件不存在: {path}"}
    if inspect_result.get("is_dir", False):
        return {"ok": False, "error": f"路径是目录: {path}"}

    # 图片文件返回可直接用于多模态消息/附件列表的结构。
    if target.suffix.lower() in _IMAGE_EXTENSIONS:
        limit = (
            max_bytes
            if max_bytes > 0
            else int(getattr(state, "max_read_bytes", 200_000))
        )
        image_info = _inspect_path(target, state, include_bytes=True, max_bytes=limit)
        file_size = int(image_info.get("size", 0))
        if limit > 0 and file_size > limit:
            return {
                "ok": False,
                "error": f"图片文件过大: {file_size} bytes > {limit} bytes",
            }
        raw_b64 = str(image_info.get("data_b64", ""))
        if not raw_b64:
            return {"ok": False, "error": f"图片编码失败: {path}"}
        mime_type = mimetypes.guess_type(str(target))[0] or "application/octet-stream"
        data_uri = f"data:{mime_type};base64,{raw_b64}"
        result: dict[str, Any] = {
            "ok": True,
            "path": str(target),
            "content": "",
            "kind": "image",
            "mime_type": mime_type,
            "file_size": file_size,
            # 与 provider/base.py 的多模态 content 格式一致，可直接拼接。
            "multimodal": [{"type": "image_url", "image_url": {"url": data_uri}}],
            # 与 gateway/base.py 的 Attachment 字段一致，可直接用于附件列表。
            "attachment": {
                "file_path": str(target),
                "file_name": target.name,
                "mime_type": mime_type,
                "file_size": file_size,
            },
        }
        if include_meta:
            result.update({
                "start_line": 1,
                "end_line": 0,
                "total_lines": 0,
                "truncated": False,
                "numbered": False,
            })
        return result

    limit = (
        max_bytes if max_bytes > 0 else int(getattr(state, "max_read_bytes", 200_000))
    )
    read_result = _inspect_path(target, state, include_bytes=True, max_bytes=limit)
    data = base64.b64decode(str(read_result.get("data_b64", "")).encode("ascii"))
    truncated = bool(read_result.get("truncated", False))

    text = data.decode("utf-8", errors="replace")
    lines = text.splitlines()
    total_lines = len(lines)

    start = max(start_line, 1)
    end = total_lines if end_line <= 0 else min(end_line, total_lines)
    if start > end and total_lines > 0:
        return {"ok": False, "error": "start_line 不能大于 end_line"}

    selected_lines = lines[start - 1 : end] if total_lines else []
    if numbered:
        content = "\n".join(
            f"{start + i}: {line}" for i, line in enumerate(selected_lines)
        )
    else:
        content = "\n".join(selected_lines)

    result: dict[str, Any] = {
        "ok": True,
        "path": str(target),
        "content": content,
    }
    if include_meta:
        result.update({
            "start_line": start,
            "end_line": end,
            "total_lines": total_lines,
            "truncated": truncated,
            "numbered": numbered,
        })
    return result


@tool
def read(
    state: AgentState,
    path: str = "",
    paths: list[str] | None = None,
    start_line: int = 1,
    end_line: int = 0,
    max_bytes: int = 0,
    numbered: bool = False,
    include_meta: bool = True,
) -> str:
    """读取文件内容。
    可以读取图片内容。

    参数：
    - path (string, 可选): 目标文件路径。与 paths 二选一，必须提供其中一个。
    - paths (array[string], 可选): 目标文件路径数组，用于批量读取。与 path 二选一，必须提供其中一个。
    - start_line (integer, 可选): 起始行号（从 1 开始，默认 1）。批量模式下对所有文件生效。
    - end_line (integer, 可选): 结束行号（含本行）。<= 0 表示读到文件末尾（默认 0）。
    - max_bytes (integer, 可选): 本次读取的最大字节数（默认 0，表示使用系统默认上限）。
    - numbered (boolean, 可选): 是否在 content 中添加行号前缀（默认 False）。
    - include_meta (boolean, 可选): 是否返回 start_line/end_line/total_lines/truncated 等元信息（默认 True）。

    返回：JSON 字符串。
    - 单文件模式：返回单个结果对象，形如 {ok, path, content, ...meta} 或 {ok:false,error}。
    - 批量模式：返回 {ok:true, results:[...]}；每个结果项独立成功/失败，允许部分失败。
    - `.localagent/secrets.json` 属于受管 secrets 存储，禁止直接读取；请使用 `manage_env` / `get_secret`。
    """
    parsed_paths, paths_err = _coerce_list_param(paths, "paths")
    if paths_err:
        return _result({"ok": False, "error": paths_err})
    has_path = bool(path and path.strip())
    has_paths = bool(parsed_paths)
    if has_path == has_paths:
        return _result({"ok": False, "error": "path 与 paths 必须且只能提供其中一个"})

    secrets = _load_secrets_for_scrub(state)

    if has_path:
        try:
            target = _resolve_path(path, state)
        except ValueError as exc:
            return _result({"ok": False, "error": str(exc)})
        if _is_managed_secrets_path(target):
            return _result({"ok": False, "error": "请使用 manage_env 管理 secrets，不要直接读取 .localagent/secrets.json"})
        return _result_with_scrub(_read_single(
            path,
            state,
            start_line,
            end_line,
            max_bytes,
            numbered=numbered,
            include_meta=include_meta,
        ), secrets)

    results = []
    for p in parsed_paths:
        try:
            target = _resolve_path(str(p), state, access="read")
        except ValueError as exc:
            results.append({"ok": False, "error": str(exc), "path": str(p)})
            continue
        if _is_managed_secrets_path(target):
            results.append({
                "ok": False,
                "error": "请使用 manage_env 管理 secrets，不要直接读取 .localagent/secrets.json",
                "path": str(p),
            })
            continue
        results.append(_read_single(
            p,
            state,
            start_line,
            end_line,
            max_bytes,
            numbered=numbered,
            include_meta=include_meta,
        ))
    return _result_with_scrub({"ok": True, "results": results}, secrets)


@tool
def write(
    state: AgentState,
    path: str,
    content: str,
    overwrite: bool = True,
) -> str:
    """写入文件内容（原子写入）。

    参数：
    - path (string): 目标文件路径。
    - content (string): 要写入的完整文本内容（UTF-8）。
    - overwrite (boolean, 可选): 是否允许覆盖已存在文件（默认 True）；False 时若文件存在则报错。

    返回：执行情况。
    - `.localagent/secrets.json` 属于受管 secrets 存储，禁止直接写入；请使用 `manage_env`。
    """
    if getattr(state, "readonly", False):
        return _result({"ok": False, "error": "当前会话为只读模式，禁止写入"})

    max_write_bytes = int(getattr(state, "max_write_bytes", 500_000))
    content_size = len(content.encode("utf-8"))
    if content_size > max_write_bytes:
        return _result({
            "ok": False,
            "error": f"写入内容过大: {content_size} bytes > {max_write_bytes} bytes",
        })

    try:
        target = _resolve_path(path, state, access="write")
    except ValueError as exc:
        return _result({"ok": False, "error": str(exc)})
    if _is_managed_secrets_path(target):
        return _result({"ok": False, "error": "请使用 manage_env 管理 secrets，不要直接写入 .localagent/secrets.json"})
    existed = bool(_inspect_path(target, state).get("exists", False))
    if existed and not overwrite:
        return _result({"ok": False, "error": f"文件已存在且 overwrite=False: {path}"})

    _write_text_via_sandbox(target, content, state)
    return _result({
        "ok": True,
        "path": str(target),
        "bytes": content_size,
        "created": not existed,
        "overwritten": existed,
    })


@tool
def edit(
    state: AgentState,
    path: str,
    old: str,
    new: str,
) -> str:
    """简单编辑工具（优先使用 apply_patch 工具进行修改）。
    按文本替换编辑文件。old 必须在文件中唯一匹配，否则请使用 apply_patch 工具。
    - 匹配 0 次：返回“未找到要替换的内容”。
    - 匹配 >1 次：返回“old 必须唯一匹配”的歧义错误。

    参数：
    - path (string): 目标文件路径。
    - old (string): 要查找的原始文本（必须非空，且在文件中唯一匹配）。
    - new (string): 替换后的文本。

    返回：执行情况。
    """
    if getattr(state, "readonly", False):
        return _result({"ok": False, "error": "当前会话为只读模式，禁止编辑"})
    if not old:
        return _result({"ok": False, "error": "old 不能为空"})

    try:
        target = _resolve_path(path, state, access="write")
    except ValueError as exc:
        return _result({"ok": False, "error": str(exc)})
    read_result = _inspect_path(target, state, include_bytes=True)
    if not read_result.get("exists", False) or read_result.get("is_dir", False):
        return _result({"ok": False, "error": f"无效文件路径: {path}"})

    raw = base64.b64decode(str(read_result.get("data_b64", "")).encode("ascii"))
    text = raw.decode("utf-8", errors="replace")
    count = text.count(old)
    if count == 0:
        return _result({"ok": False, "error": "未找到要替换的内容"})
    if count > 1:
        return _result({
            "ok": False,
            "error": f"匹配到 {count} 处，old 必须唯一匹配。请添加更多上下文使其唯一",
        })

    new_text = text.replace(old, new, 1)
    _write_text_via_sandbox(target, new_text, state)
    return _result({
        "ok": True,
        "path": str(target),
        "replacements": 1,
    })


def _line_text(line: str) -> str:
    return line.rstrip("\n")


def _parse_special_patch(patch: str) -> list[dict[str, Any]]:
    lines = patch.splitlines(keepends=True)
    if not lines:
        raise ValueError("patch 不能为空")
    if _line_text(lines[0]) != "*** Begin Patch":
        raise ValueError("patch 必须以 '*** Begin Patch' 开头")

    ops: list[dict[str, Any]] = []
    i = 1
    while i < len(lines):
        current = _line_text(lines[i])
        if current == "*** End Patch":
            if i != len(lines) - 1:
                trailing = "".join(lines[i + 1 :]).strip()
                if trailing:
                    raise ValueError("patch 结束标记后存在多余内容")
            return ops

        if current.startswith("*** Add File: "):
            path = current[len("*** Add File: ") :].strip()
            if not path:
                raise ValueError("Add File 缺少文件路径")
            i += 1
            content_lines: list[str] = []
            while i < len(lines):
                s = _line_text(lines[i])
                if s.startswith("*** ") or s == "@@":
                    break
                if not lines[i].startswith("+"):
                    raise ValueError("Add File 内容行必须以 '+' 开头")
                content_lines.append(lines[i][1:])
                i += 1
            if not content_lines:
                raise ValueError("Add File 必须包含至少一行内容")
            ops.append({"op": "add", "path": path, "content": "".join(content_lines)})
            continue

        if current.startswith("*** Delete File: "):
            path = current[len("*** Delete File: ") :].strip()
            if not path:
                raise ValueError("Delete File 缺少文件路径")
            ops.append({"op": "delete", "path": path})
            i += 1
            continue

        if current.startswith("*** Update File: "):
            path = current[len("*** Update File: ") :].strip()
            if not path:
                raise ValueError("Update File 缺少文件路径")
            i += 1
            move_to = ""
            if i < len(lines) and _line_text(lines[i]).startswith("*** Move to: "):
                move_to = _line_text(lines[i])[len("*** Move to: ") :].strip()
                if not move_to:
                    raise ValueError("Move to 缺少目标文件路径")
                i += 1

            chunks: list[list[str]] = []
            while i < len(lines):
                s = _line_text(lines[i])
                if s.startswith("*** "):
                    break
                if not s.startswith("@@"):
                    raise ValueError("Update File 变更块必须以 '@@' 开头")
                i += 1
                chunk_lines: list[str] = []
                while i < len(lines):
                    s2 = _line_text(lines[i])
                    if s2.startswith("@@") or s2.startswith("*** "):
                        break
                    if s2 == "*** End of File":
                        i += 1
                        continue
                    if not lines[i] or lines[i][0] not in {" ", "+", "-"}:
                        raise ValueError("变更行必须以空格、'+' 或 '-' 开头")
                    chunk_lines.append(lines[i])
                    i += 1
                if not chunk_lines:
                    raise ValueError("变更块不能为空")
                chunks.append(chunk_lines)

            ops.append({"op": "update", "path": path, "move_to": move_to, "chunks": chunks})
            continue

        raise ValueError(f"无法识别的 patch 指令: {current}")

    raise ValueError("patch 缺少 '*** End Patch' 结束标记")


def _find_chunk_start(
    source_lines: list[str],
    old_lines: list[str],
    start: int,
) -> int:
    if not old_lines:
        return start

    max_idx = len(source_lines) - len(old_lines)
    for i in range(max(start, 0), max_idx + 1):
        if source_lines[i : i + len(old_lines)] == old_lines:
            return i

    for i in range(0, min(start, max_idx + 1)):
        if source_lines[i : i + len(old_lines)] == old_lines:
            return i

    raise ValueError("未找到可应用的变更块上下文")


def _apply_special_chunks(source_text: str, chunks: list[list[str]]) -> tuple[str, int]:
    """按顺序应用 update chunks，返回 (新文本, 已应用 chunk 数)。

    约束：每个 chunk 的行前缀只能是空格、'+'、'-'，并依赖上下文匹配定位。
    若上下文无法定位，会抛出 ValueError。
    """
    source_lines = source_text.splitlines(keepends=True)
    cursor = 0
    applied = 0

    for lines in chunks:
        old_lines = [ln[1:] for ln in lines if ln[0] in {" ", "-"}]
        new_lines = [ln[1:] for ln in lines if ln[0] in {" ", "+"}]
        pos = _find_chunk_start(source_lines, old_lines, cursor)
        source_lines = (
            source_lines[:pos] + new_lines + source_lines[pos + len(old_lines) :]
        )
        cursor = pos + len(new_lines)
        applied += 1

    return "".join(source_lines), applied


@tool
def apply_patch(
    state: AgentState,
    content: str,
) -> str:
    """编辑文件。应用专用 freeform patch 语法到文件。

    参数：
    - content (string): 专用 patch 文本，格式如下：
      *** Begin Patch
      *** Update File: path/to/file.py
      @@
      -old line
      +new line
      *** End Patch

    语法与约束（关键）：
    - patch 必须以 "*** Begin Patch" 开头，并以 "*** End Patch" 结束。
    - 支持三类指令："*** Add File:", "*** Update File:", "*** Delete File:"。
    - Update File 必须包含至少一个 "@@" 变更块。
    - Update 块内每一行必须以前缀之一开头：空格（上下文）/ '+'（新增）/ '-'（删除）。
    - 可选 "*** Move to:" 仅用于 Update File，表示改名/移动并可同时修改内容。

    示例 1（修改文件）：
      *** Begin Patch
      *** Update File: src/agent/tools.py
      @@
      -return "old"
      +return "new"
      *** End Patch

    示例 2（新增并删除文件）：
      *** Begin Patch
      *** Add File: notes/todo.txt
      +line 1
      +line 2
      *** Delete File: notes/old.txt
      *** End Patch

    返回：执行情况与变更统计。
    """
    if getattr(state, "readonly", False):
        return _result({"ok": False, "error": "当前会话为只读模式，禁止编辑"})
    if not content.strip():
        return _result({"ok": False, "error": "patch 不能为空"})

    try:
        ops = _parse_special_patch(content)
    except ValueError as exc:
        return _result({"ok": False, "error": str(exc)})

    touched: set[str] = set()
    move_targets: set[str] = set()
    for op in ops:
        op_name = op["op"]
        src = op.get("path", "")
        key = f"{op_name}:{src}"
        if key in touched:
            return _result({"ok": False, "error": f"重复的 patch 操作: {src}"})
        touched.add(key)
        if op_name == "update":
            to = (op.get("move_to") or "").strip()
            if to:
                if to in move_targets:
                    return _result({"ok": False, "error": f"重复的 Move to 目标: {to}"})
                move_targets.add(to)

    # 先做完整校验与计算，再执行写入，尽量避免半成功状态。
    planned_add_or_update: dict[Path, str] = {}
    planned_delete: set[Path] = set()
    planned_move: list[tuple[Path, Path]] = []
    changed_files: list[str] = []
    created_files: list[str] = []
    deleted_files: list[str] = []
    moved_files: list[dict[str, str]] = []
    chunk_count = 0

    for op in ops:
        kind = op["op"]
        if kind == "add":
            target = _resolve_path(op["path"], state, access="write")
            if target in planned_add_or_update:
                return _result({"ok": False, "error": f"同一文件重复写入: {op['path']}"})
            if _inspect_path(target, state).get("exists", False):
                return _result({"ok": False, "error": f"文件已存在，无法 Add File: {op['path']}"})
            planned_add_or_update[target] = op["content"]
            changed_files.append(str(target))
            created_files.append(str(target))
            continue

        if kind == "delete":
            target = _resolve_path(op["path"], state, access="write")
            if target in planned_delete:
                return _result({"ok": False, "error": f"同一文件重复删除: {op['path']}"})
            target_info = _inspect_path(target, state)
            if not target_info.get("exists", False):
                return _result({"ok": False, "error": f"文件不存在，无法 Delete File: {op['path']}"})
            if target_info.get("is_dir", False):
                return _result({"ok": False, "error": f"路径是目录: {op['path']}"})
            planned_delete.add(target)
            changed_files.append(str(target))
            deleted_files.append(str(target))
            continue

        if kind != "update":
            return _result({"ok": False, "error": f"未知 patch 操作: {kind}"})

        src = _resolve_path(op["path"], state, access="write")
        src_info = _inspect_path(src, state, include_bytes=True)
        if not src_info.get("exists", False):
            return _result({"ok": False, "error": f"文件不存在，无法 Update File: {op['path']}"})
        if src_info.get("is_dir", False):
            return _result({"ok": False, "error": f"路径是目录: {op['path']}"})

        source_bytes = base64.b64decode(str(src_info.get("data_b64", "")).encode("ascii"))
        source_text = source_bytes.decode("utf-8", errors="replace")
        new_text = source_text
        chunks = op.get("chunks", [])
        if chunks:
            try:
                new_text, applied = _apply_special_chunks(source_text, chunks)
            except ValueError as exc:
                return _result({"ok": False, "error": f"{op['path']}: {exc}"})
            chunk_count += applied

        dst = src
        move_to = (op.get("move_to") or "").strip()
        if move_to:
            dst = _resolve_path(move_to, state, access="write")
            if _inspect_path(dst, state).get("exists", False):
                return _result({"ok": False, "error": f"Move to 目标已存在: {move_to}"})
            if dst in planned_add_or_update:
                return _result({"ok": False, "error": f"Move to 目标冲突: {move_to}"})
            planned_add_or_update[dst] = new_text
            planned_delete.add(src)
            planned_move.append((src, dst))
            moved_files.append({"from": str(src), "to": str(dst)})
            changed_files.extend([str(src), str(dst)])
            continue

        if new_text != source_text:
            planned_add_or_update[src] = new_text
            changed_files.append(str(src))

    # 执行阶段：先写入（原子覆盖），再删除被删文件。
    for dst, text in planned_add_or_update.items():
        _write_text_via_sandbox(dst, text, state)
    for target in planned_delete:
        if target in planned_add_or_update:
            continue
        _delete_file_via_sandbox(target, state)

    return _result({
        "ok": True,
        "changed": bool(changed_files),
        "files_changed": len(set(changed_files)),
        "chunks": chunk_count,
        "created": created_files,
        "deleted": deleted_files,
        "moved": moved_files,
        "moves_planned": len(planned_move),
    })


def _update_cwd_from_file(cwd_file: str, state: BaseState) -> None:
    """从临时文件读取 pwd 输出并更新 state.cwd。"""
    try:
        if os.path.exists(cwd_file):
            detected = Path(cwd_file).read_text(encoding="utf-8").strip()
            if detected:
                sandbox = _sandbox(state)
                try:
                    next_cwd = sandbox.ensure_allowed(Path(detected), access="workdir")
                except ValueError:
                    next_cwd = None
                if next_cwd is not None and next_cwd.is_dir():
                    state.cwd = str(next_cwd)  # type: ignore[attr-defined]
    finally:
        if os.path.exists(cwd_file):
            os.remove(cwd_file)


_GET_SECRET_SCRIPT = r'''
import socket, sys

with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
    client.connect(sys.argv[1])
    client.sendall(sys.argv[2].encode("utf-8"))
    client.shutdown(socket.SHUT_WR)
    chunks = []
    while True:
        data = client.recv(4096)
        if not data:
            break
        chunks.append(data)
sys.stdout.write(b"".join(chunks).decode("utf-8"))
'''.strip()


def _build_get_secret_preamble(socket_quoted: str) -> str:
    """生成注入子 shell 的 get_secret 函数定义。"""
    dev_null = "/dev" + "/null"  # avoid scrub
    return (
        f"__qb_ss={socket_quoted}\n"
        f'get_secret() {{ python3 -c \'{_GET_SECRET_SCRIPT}\' "$__qb_ss" "$1"; }}\n'
        f"export -f get_secret 2>{dev_null}"
    )


def _build_workspace_sandbox_preamble(state: BaseState) -> str:
    if not _workspace_restricted(state):
        return ""
    workspace_root = shlex.quote(str(_workspace_root(state)))
    return (
        f"__qb_ws={workspace_root}\n"
        "cd() {\n"
        "  command cd \"$@\" || return $?\n"
        "  case \"$PWD\" in\n"
        "    \"$__qb_ws\"|\"$__qb_ws\"/*) return 0 ;;\n"
        "  esac\n"
        "  printf '%s\\n' '工作目录不能离开 workspace' >&2\n"
        "  command cd \"$__qb_ws\" || return $?\n"
        "  return 1\n"
        "}\n"
    )


def _validate_command(command: str, state: BaseState) -> tuple[bool, str]:
    if not getattr(state, "bash_enabled", True):
        return False, "bash 工具已禁用"
    if not command.strip():
        return False, "command 不能为空"

    try:
        tokens = shlex.split(command, posix=True)
    except ValueError as exc:
        return False, f"命令解析失败: {exc}"
    for token in tokens:
        stripped = token.strip().rstrip(";")
        if not stripped:
            continue
        if stripped in {str(SECRETS_FILE), SECRETS_FILE.name} or stripped.endswith(f"/{SECRETS_FILE.name}"):
            return False, "请使用 manage_env 管理 secrets，不要直接通过 bash 操作 .localagent/secrets.json"

    allowed = getattr(state, "allowed_commands", None)
    if allowed is not None:
        executable = tokens[0] if tokens else ""
        if executable not in allowed:
            return False, f"命令不在白名单中: {executable}"
    if _workspace_restricted(state):
        sandbox = _sandbox(state)
        path_candidates = list(tokens)
        path_candidates.extend(
            match.group(0)
            for match in re.finditer(
                r"(?:(?<=^)|(?<=[\s;|&()]))/[^\s;|&()]+|\.\./[^\s;|&()]+|\.\.(?=[/\s]|$)",
                command,
            )
        )
        for token in path_candidates:
            if token in {"..", "../", "..\\"}:
                return False, "命令包含越界路径"
            if token.startswith("/"):
                if not sandbox.is_allowed(Path(token), access="read"):
                    return False, f"命令包含沙箱外绝对路径: {token}"
            if token.startswith("../") or "/../" in token or token.endswith("/.."):
                return False, "命令包含 workspace 越界路径"
    return True, ""


def _prepare_shell_context(
    state: AgentState,
    *,
    workdir: str,
) -> tuple[Path, dict[str, str], _SecretLookupServer | None, dict[str, str]]:
    try:
        cwd = (
            _resolve_path(workdir, state, access="workdir")
            if workdir
            else _effective_cwd(state)
        )
    except ValueError as exc:
        raise ValueError(str(exc)) from exc

    env = os.environ.copy()
    env.pop(SECRET_KEY_ENV, None)
    env["HOME"] = str(_workspace_root(state))
    pixi_bin = _project_root(state) / ".pixi" / "envs" / "localagent" / "bin"
    if pixi_bin.is_dir():
        env["PATH"] = str(pixi_bin) + os.pathsep + env.get("PATH", "")

    secret_scopes: list[str] = []
    current_person_id = str(getattr(state, "current_person_id", "")).strip()
    current_conversation_id = str(getattr(state, "current_conversation_id", "")).strip()
    if current_person_id:
        secret_scopes.append(person_scope(current_person_id))
    if current_conversation_id:
        secret_scopes.append(conversation_scope(current_conversation_id))
    try:
        secrets = load_all_decrypted(secret_scopes or None)
    except RuntimeError as exc:
        logger.warning("Secrets unavailable for shell context: %s", exc)
        secrets = {}
    # 通过进程内 lookup server 按需读取 secret，避免将解密结果写入文件或环境变量。
    secrets_lookup: _SecretLookupServer | None = None
    if secrets:
        try:
            secrets_lookup = _SecretLookupServer(_workspace_root(state), secrets)
        except Exception:
            logger.warning("Failed to start secret lookup server", exc_info=True)
            secrets_lookup = None
    return cwd, env, secrets_lookup, secrets


def _build_wrapped_command(
    command: str,
    secrets_lookup: _SecretLookupServer | None,
    state: BaseState,
) -> str:
    # 构建 shell 包装：注入 get_secret 函数
    preamble_parts: list[str] = []
    if secrets_lookup is not None:
        socket_quoted = shlex.quote(secrets_lookup.socket_path)
        preamble_parts.append(_build_get_secret_preamble(socket_quoted))
    workspace_preamble = _build_workspace_sandbox_preamble(state)
    if workspace_preamble:
        preamble_parts.append(workspace_preamble)
    preamble = "\n".join(part for part in preamble_parts if part)
    return f"{preamble}\n{command}"


def _shell_runner(state: AgentState) -> SandboxRunner:
    return create_sandbox_runner()


def _load_secrets_for_scrub(state: BaseState) -> dict[str, str]:
    """Load decrypted secrets for scrubbing tool output."""
    pid = str(getattr(state, "current_person_id", "") or "").strip()
    cid = str(getattr(state, "current_conversation_id", "") or "").strip()
    scopes: list[str] = []
    if pid:
        scopes.append(person_scope(pid))
    if cid:
        scopes.append(conversation_scope(cid))
    if not scopes:
        return {}
    try:
        return load_all_decrypted(scopes)
    except Exception:
        return {}


def _result_with_scrub(
    payload: dict[str, Any],
    secrets: dict[str, str],
) -> str:
    for key in ("stdout", "stderr", "content"):
        if key in payload and payload[key]:
            payload[key] = scrub_text(str(payload[key]), secrets)
    # Batch results (read tool)
    if "results" in payload and isinstance(payload["results"], list):
        for item in payload["results"]:
            if isinstance(item, dict) and "content" in item and item["content"]:
                item["content"] = scrub_text(str(item["content"]), secrets)
    return _result(payload)


def _has_semantic_shell_error(stdout: str, stderr: str) -> bool:
    combined = "\n".join(part for part in (stdout, stderr) if part).lower()
    if not combined:
        return False
    error_markers = (
        "traceback (most recent call last)",
        "command not found",
        "no such file or directory",
        "permission denied",
        "syntax error",
        "modulenotfounderror",
        "filenotfounderror",
        "importerror:",
        "nameerror:",
        "typeerror:",
        "valueerror:",
        "runtimeerror:",
        "oserror:",
        "fatal error",
        "segmentation fault",
    )
    return any(marker in combined for marker in error_markers)


@tool
def bash_run(
    state: AgentState,
    command: str,
    timeout_ms: int = 0,
    workdir: str = "",
) -> str:
    """执行 shell 命令并等待完成。

    参数：
    - command (string): 要执行的 shell 命令字符串。
    - timeout_ms (integer, 可选): 超时时间（毫秒，默认 0，表示使用系统默认值）。
    - workdir (string, 可选): 执行目录（默认空字符串，表示使用当前工作目录）。

    密钥读取：
    - 命令中可直接调用 `get_secret KEY_NAME` 读取加密存储中的密钥值（存储在 .localagent/secrets.json）。
    - 推荐通过命令替换使用：`export TOKEN="$(get_secret MY_TOKEN)"`。
    - `get_secret` 仅在本次 bash 执行进程内可用，不会写入全局环境。
    - 若需要新增、修改或删除持久化 secret，请使用 `manage_env`，不要直接操作 `.localagent/secrets.json`。

    返回：执行情况、输出和退出码。
    """
    if _runtime_managed(state):
        ok, err = _validate_command(command, state)
        if not ok:
            return _result({"ok": False, "error": err})
        cwd = workdir.strip() or str(getattr(state, "cwd", "") or "/workspace")
        secrets = _load_secrets_for_scrub(state)
        try:
            payload = _runtime_call(
                state,
                method="bash.run",
                params={
                    "command": command,
                    "cwd": cwd,
                    "timeout_ms": (
                        timeout_ms if timeout_ms > 0 else int(getattr(state, "bash_timeout_ms", 30_000))
                    ),
                    "max_output_chars": int(getattr(state, "max_bash_output_chars", 20_000)),
                },
            )
        except Exception as exc:
            return _result({"ok": False, "error": str(exc)})
        state.cwd = str(payload.get("cwd", cwd))
        return _result_with_scrub(payload, secrets)

    ok, err = _validate_command(command, state)
    if not ok:
        return _result({"ok": False, "error": err})
    try:
        cwd, env, secrets_lookup, secrets = _prepare_shell_context(
            state, workdir=workdir
        )
    except ValueError as exc:
        return _result({"ok": False, "error": str(exc)})
    effective_timeout = (
        timeout_ms if timeout_ms > 0 else int(getattr(state, "bash_timeout_ms", 30_000))
    )
    output_limit = int(getattr(state, "max_bash_output_chars", 20_000))
    runner = _shell_runner(state)

    # 用临时文件捕获命令执行后的最终工作目录
    fd, cwd_file = _workspace_tmpfile(
        state,
        prefix=".localagent_cwd_",
        suffix=".tmp",
    )
    os.close(fd)
    dev_null = "/dev" + "/null"
    wrapped = (
        f"{_build_wrapped_command(command, secrets_lookup, state)}\n__qb_e=$?\n"
        f"pwd > {shlex.quote(cwd_file)} 2>{dev_null}\nexit $__qb_e"
    )

    try:
        completed = runner.run(
            state=state,
            command=wrapped,
            cwd=cwd,
            env=env,
            timeout_ms=effective_timeout,
        )
        _update_cwd_from_file(cwd_file, state)

        stdout, stdout_truncated = _truncate_text(completed.stdout, output_limit)
        stderr, stderr_truncated = _truncate_text(completed.stderr, output_limit)
        final_cwd = getattr(state, "cwd", "") or str(cwd)
        semantic_error = completed.returncode == 0 and _has_semantic_shell_error(
            completed.stdout,
            completed.stderr,
        )
        return _result_with_scrub({
            "ok": completed.returncode == 0 and not semantic_error,
            "exit_code": completed.returncode,
            "stdout": stdout,
            "stderr": stderr,
            "stdout_truncated": stdout_truncated,
            "stderr_truncated": stderr_truncated,
            "timed_out": False,
            "cwd": final_cwd,
            "semantic_error": semantic_error,
        }, secrets)
    except subprocess.TimeoutExpired as exc:
        _update_cwd_from_file(cwd_file, state)
        stdout = exc.stdout if isinstance(exc.stdout, str) else ""
        stderr = exc.stderr if isinstance(exc.stderr, str) else ""
        stdout, stdout_truncated = _truncate_text(stdout, output_limit)
        stderr, stderr_truncated = _truncate_text(stderr, output_limit)
        final_cwd = getattr(state, "cwd", "") or str(cwd)
        return _result_with_scrub({
            "ok": False,
            "exit_code": -1,
            "stdout": stdout,
            "stderr": stderr,
            "stdout_truncated": stdout_truncated,
            "stderr_truncated": stderr_truncated,
            "timed_out": True,
            "cwd": final_cwd,
            "timeout_ms": effective_timeout,
        }, secrets)
    except FileNotFoundError as exc:
        return _result({
            "ok": False,
            "error": f"sandbox 运行失败，缺少命令: {exc.filename or exc}",
        })
    finally:
        if secrets_lookup is not None:
            secrets_lookup.close()


def _bash_sessions(state: AgentState) -> dict[str, dict[str, Any]]:
    sessions = getattr(state, "bash_sessions", None)
    if sessions is None:
        sessions = {}
        setattr(state, "bash_sessions", sessions)
    return sessions


def _get_session(state: AgentState, session_id: str) -> dict[str, Any] | None:
    return _bash_sessions(state).get(session_id)


def _session_result(
    state: AgentState,
    session: dict[str, Any],
    stdout: str = "",
    stderr: str = "",
    *,
    stdout_truncated: bool = False,
    stderr_truncated: bool = False,
) -> str:
    proc = session["process"]
    return _result_with_scrub({
        "ok": True,
        "session_id": session["id"],
        "running": proc.poll() is None,
        "exit_code": proc.poll(),
        "stdout": stdout,
        "stderr": stderr,
        "stdout_truncated": stdout_truncated,
        "stderr_truncated": stderr_truncated,
        "cwd": session["cwd"],
    }, session.get("secrets", {}))


def _read_stream_nonblocking(
    stream: Any,
    max_chars: int,
) -> tuple[str, bool]:
    if stream is None or max_chars <= 0:
        return "", False
    chunks: list[str] = []
    total = 0
    while total < max_chars:
        try:
            chunk = stream.read(max_chars - total)
        except BlockingIOError:
            break
        if not chunk:
            break
        chunks.append(chunk)
        total += len(chunk)
    return "".join(chunks), total >= max_chars


@tool
def bash_start(
    state: AgentState,
    command: str,
    workdir: str = "",
) -> str:
    """启动可交互 bash 会话。

    参数：
    - command (string): 要在会话中执行的 shell 命令字符串。
    - workdir (string, 可选): 执行目录（默认空字符串，表示使用当前工作目录）。

    密钥读取：
    - 会话内可使用 `get_secret KEY_NAME` 动态读取密钥。
    - 推荐先赋值后使用：`API_KEY="$(get_secret OPENAI_API_KEY)"`。
    - `get_secret` 只在当前会话有效；会话结束后失效。
    - 若需要新增、修改或删除持久化 secret，请使用 `manage_env`，不要直接操作 `.localagent/secrets.json`。

    返回：会话信息（session_id/pid/cwd）。
    """
    if _runtime_managed(state):
        ok, err = _validate_command(command, state)
        if not ok:
            return _result({"ok": False, "error": err})
        cwd = workdir.strip() or str(getattr(state, "cwd", "") or "/workspace")
        secrets = _load_secrets_for_scrub(state)
        try:
            payload = _runtime_call(
                state,
                method="bash.start",
                params={"command": command, "cwd": cwd},
            )
        except Exception as exc:
            return _result({"ok": False, "error": str(exc)})
        state.cwd = str(payload.get("cwd", cwd))
        return _result_with_scrub(payload, secrets)

    ok, err = _validate_command(command, state)
    if not ok:
        return _result({"ok": False, "error": err})
    try:
        cwd, env, secrets_lookup, secrets = _prepare_shell_context(
            state, workdir=workdir
        )
    except ValueError as exc:
        return _result({"ok": False, "error": str(exc)})

    wrapped = _build_wrapped_command(command, secrets_lookup, state)
    runner = _shell_runner(state)
    try:
        proc = runner.start(
            state=state,
            command=wrapped,
            cwd=cwd,
            env=env,
        )
    except FileNotFoundError as exc:
        if secrets_lookup is not None:
            secrets_lookup.close()
        return _result({
            "ok": False,
            "error": f"sandbox 运行失败，缺少命令: {exc.filename or exc}",
        })
    if proc.stdout is not None:
        os.set_blocking(proc.stdout.fileno(), False)
    if proc.stderr is not None:
        os.set_blocking(proc.stderr.fileno(), False)

    session_id = f"s-{uuid.uuid4().hex[:10]}"
    session = {
        "id": session_id,
        "process": proc,
        "cwd": str(cwd),
        "created_at": int(time.time()),
        "command": command,
        "secrets_lookup": secrets_lookup,
        "secrets": secrets,
    }
    _bash_sessions(state)[session_id] = session
    state.cwd = str(cwd)
    return _result({
        "ok": True,
        "session_id": session_id,
        "running": True,
        "cwd": str(cwd),
        "pid": proc.pid,
    })


@tool
def bash_read(
    state: AgentState,
    session_id: str,
    max_chars: int = 20_000,
) -> str:
    """读取会话输出（非阻塞）。

    参数：
    - session_id (string): 会话 ID（由 bash_start 返回）。
    - max_chars (integer, 可选): 本次读取 stdout/stderr 的最大字符数（默认 20000）。

    返回：会话状态与本次读取到的输出。
    """
    if _runtime_managed(state):
        secrets = _load_secrets_for_scrub(state)
        try:
            return _result_with_scrub(_runtime_call(
                state,
                method="bash.read",
                params={"session_id": session_id, "max_chars": max(1, max_chars)},
            ), secrets)
        except Exception as exc:
            return _result({"ok": False, "error": str(exc)})

    session = _get_session(state, session_id)
    if session is None:
        return _result({"ok": False, "error": f"会话不存在: {session_id}"})
    limit = max(1, max_chars)
    stdout, stdout_truncated = _read_stream_nonblocking(
        session["process"].stdout, limit
    )
    stderr, stderr_truncated = _read_stream_nonblocking(
        session["process"].stderr, limit
    )
    return _session_result(
        state,
        session,
        stdout,
        stderr,
        stdout_truncated=stdout_truncated,
        stderr_truncated=stderr_truncated,
    )


@tool
def bash_write(
    state: AgentState,
    session_id: str,
    data: str,
) -> str:
    """向会话 stdin 写入数据。

    参数：
    - session_id (string): 会话 ID（由 bash_start 返回）。
    - data (string): 要写入 stdin 的文本。

    返回：写入结果。
    """
    if _runtime_managed(state):
        try:
            return _result(_runtime_call(
                state,
                method="bash.write",
                params={"session_id": session_id, "data": data},
            ))
        except Exception as exc:
            return _result({"ok": False, "error": str(exc)})

    session = _get_session(state, session_id)
    if session is None:
        return _result({"ok": False, "error": f"会话不存在: {session_id}"})
    proc = session["process"]
    if proc.poll() is not None:
        return _result({"ok": False, "error": "会话已结束"})
    if proc.stdin is None:
        return _result({"ok": False, "error": "会话 stdin 不可用"})
    try:
        proc.stdin.write(data)
        proc.stdin.flush()
    except BrokenPipeError:
        return _result({"ok": False, "error": "stdin 已关闭"})
    return _result({
        "ok": True,
        "session_id": session_id,
        "written_chars": len(data),
    })


@tool
def bash_wait(
    state: AgentState,
    session_id: str,
    timeout_ms: int = 0,
) -> str:
    """等待会话结束并返回累计输出。

    参数：
    - session_id (string): 会话 ID（由 bash_start 返回）。
    - timeout_ms (integer, 可选): 最长等待时间（毫秒，默认 0 表示一直等待）。

    返回：会话退出状态与尾部输出；若超时则返回 timed_out=true。
    """
    if _runtime_managed(state):
        secrets = _load_secrets_for_scrub(state)
        try:
            return _result_with_scrub(_runtime_call(
                state,
                method="bash.wait",
                params={
                    "session_id": session_id,
                    "timeout_ms": timeout_ms,
                    "max_chars": int(getattr(state, "max_bash_output_chars", 20_000)),
                },
            ), secrets)
        except Exception as exc:
            return _result({"ok": False, "error": str(exc)})

    session = _get_session(state, session_id)
    if session is None:
        return _result({"ok": False, "error": f"会话不存在: {session_id}"})

    proc = session["process"]
    timeout = None if timeout_ms <= 0 else timeout_ms / 1000
    try:
        proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        return _result({
            "ok": False,
            "session_id": session_id,
            "timed_out": True,
            "running": True,
            "timeout_ms": timeout_ms,
        })

    output_limit = int(getattr(state, "max_bash_output_chars", 20_000))
    stdout, stdout_truncated = _read_stream_nonblocking(proc.stdout, output_limit)
    stderr, stderr_truncated = _read_stream_nonblocking(proc.stderr, output_limit)
    _cleanup_session_files(session)
    _bash_sessions(state).pop(session_id, None)
    return _session_result(
        state,
        session,
        stdout,
        stderr,
        stdout_truncated=stdout_truncated,
        stderr_truncated=stderr_truncated,
    )


def _cleanup_session_files(session: dict[str, Any]) -> None:
    secrets_lookup = session.get("secrets_lookup")
    if secrets_lookup is not None:
        secrets_lookup.close()


@tool
def bash_stop(
    state: AgentState,
    session_id: str,
    timeout_ms: int = 1000,
) -> str:
    """停止会话并清理资源。

    参数：
    - session_id (string): 会话 ID（由 bash_start 返回）。
    - timeout_ms (integer, 可选): 发送 terminate 后等待退出的毫秒数（默认 1000）。

    返回：停止结果与剩余输出。
    """
    if _runtime_managed(state):
        secrets = _load_secrets_for_scrub(state)
        try:
            return _result_with_scrub(_runtime_call(
                state,
                method="bash.stop",
                params={
                    "session_id": session_id,
                    "timeout_ms": timeout_ms,
                    "max_chars": int(getattr(state, "max_bash_output_chars", 20_000)),
                },
            ), secrets)
        except Exception as exc:
            return _result({"ok": False, "error": str(exc)})

    sessions = _bash_sessions(state)
    session = sessions.get(session_id)
    if session is None:
        return _result({"ok": False, "error": f"会话不存在: {session_id}"})
    proc = session["process"]
    if proc.poll() is None:
        proc.terminate()
        try:
            proc.wait(timeout=max(timeout_ms, 1) / 1000)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=1)

    output_limit = int(getattr(state, "max_bash_output_chars", 20_000))
    stdout, stdout_truncated = _read_stream_nonblocking(proc.stdout, output_limit)
    stderr, stderr_truncated = _read_stream_nonblocking(proc.stderr, output_limit)
    _cleanup_session_files(session)
    sessions.pop(session_id, None)
    return _result_with_scrub({
        "ok": True,
        "session_id": session_id,
        "running": False,
        "exit_code": proc.poll(),
        "stdout": stdout,
        "stderr": stderr,
        "stdout_truncated": stdout_truncated,
        "stderr_truncated": stderr_truncated,
    }, session.get("secrets", {}))


@tool
def bash_list(
    state: AgentState,
) -> str:
    """列出当前 bash 会话。

    返回：所有会话的状态列表（running/exit_code/pid/cwd/command 等）。
    """
    if _runtime_managed(state):
        try:
            return _result(_runtime_call(state, method="bash.list", params={}))
        except Exception as exc:
            return _result({"ok": False, "error": str(exc)})

    sessions = _bash_sessions(state)
    items = []
    for sid, s in sessions.items():
        proc = s["process"]
        items.append({
            "session_id": sid,
            "running": proc.poll() is None,
            "exit_code": proc.poll(),
            "pid": proc.pid,
            "cwd": s["cwd"],
            "command": s["command"],
            "created_at": s["created_at"],
        })
    return _result({"ok": True, "items": items})


_SKILLS_MANY_THRESHOLD = 20
_SKILLS_LINKAGE_FILE = ".links.json"
_SKILL_TREE_TOP_MAX_ENTRIES = 24


def _normalize_skill_key(value: str) -> str:
    return value.strip().lower().replace("_", "-")


def _skill_slug_from_path(path: str) -> str:
    if not path:
        return ""
    p = Path(path)
    if p.name == "SKILL.md":
        return _normalize_skill_key(p.parent.name)
    return _normalize_skill_key(p.name)


def _load_skill_linkage(skills_path: str) -> dict[str, list[dict[str, str]]]:
    cfg_path = Path(skills_path) / _SKILLS_LINKAGE_FILE
    if not cfg_path.is_file():
        return {}
    try:
        raw = json.loads(cfg_path.read_text(encoding="utf-8"))
    except Exception:
        return {}

    rules = raw.get("rules")
    if not isinstance(rules, list):
        return {}

    linkage: dict[str, list[dict[str, str]]] = {}
    for rule in rules:
        if not isinstance(rule, dict):
            continue
        sources_raw = rule.get("skills", rule.get("skill", ""))
        sources: list[str] = []
        if isinstance(sources_raw, str):
            source = _normalize_skill_key(sources_raw)
            if source:
                sources.append(source)
        elif isinstance(sources_raw, list):
            for source_item in sources_raw:
                source = _normalize_skill_key(str(source_item))
                if source:
                    sources.append(source)
        if not sources:
            continue
        also_show = rule.get("also_show")
        if not isinstance(also_show, list):
            continue
        recs: list[dict[str, str]] = []
        for rec in also_show:
            if not isinstance(rec, dict):
                continue
            rec_skill = _normalize_skill_key(str(rec.get("skill", "")))
            if not rec_skill:
                continue
            recs.append({
                "skill": rec_skill,
                "reason": str(rec.get("reason", "")).strip(),
            })
        if recs:
            for source in sources:
                existing = linkage.get(source, [])
                linkage[source] = [*existing, *recs]
    return linkage


def _read_skill_meta_by_slug(skills_path: str, slug: str) -> dict[str, Any] | None:
    if not slug:
        return None
    skill_file = Path(skills_path) / slug / "SKILL.md"
    if not skill_file.is_file():
        return None
    item: dict[str, Any] = {
        "id": str(skill_file),
        "path": str(skill_file),
    }
    try:
        from ..core.skills import _parse_frontmatter

        meta = _parse_frontmatter(skill_file.read_text(encoding="utf-8"))
        if isinstance(meta, dict):
            if "name" in meta:
                item["name"] = meta["name"]
            if "description" in meta:
                item["description"] = meta["description"]
    except Exception:
        pass
    if "name" not in item:
        item["name"] = slug
    item["folder_tree"] = _build_skill_folder_tree(str(skill_file))
    return item


def _build_skill_folder_tree(skill_md_path: str) -> str:
    skill_file = Path(skill_md_path)
    root = skill_file.parent
    if not root.is_dir():
        return ""
    try:
        top_entries = sorted(root.iterdir(), key=lambda p: p.name.lower())
    except Exception:
        return ""
    if len(top_entries) == 1 and top_entries[0].is_file() and top_entries[0].name == "SKILL.md":
        return ""

    lines: list[str] = [f"{root.name}/"]
    shown = top_entries[:_SKILL_TREE_TOP_MAX_ENTRIES]
    hidden = max(len(top_entries) - len(shown), 0)
    for i, entry in enumerate(shown):
        is_last = (i == len(shown) - 1) and hidden == 0
        connector = "`-- " if is_last else "|-- "
        if entry.is_dir():
            sub_count = 0
            try:
                sub_count = sum(1 for _ in entry.iterdir())
            except Exception:
                sub_count = -1
            if sub_count >= 0:
                lines.append(f"{connector}{entry.name}/ ({sub_count} items)")
            else:
                lines.append(f"{connector}{entry.name}/")
        else:
            lines.append(f"{connector}{entry.name}")
    if hidden > 0:
        lines.append(f"`-- ... (+{hidden} more)")
    return "\n".join(lines)


def _dedupe_skill_items(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[str] = set()
    deduped: list[dict[str, Any]] = []
    for item in items:
        key = _skill_item_key(item)
        if not key:
            continue
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)
    return deduped


def _skill_item_key(item: dict[str, Any]) -> str:
    path_key = str(item.get("path", "")).strip()
    if path_key:
        return f"path:{path_key}"

    item_id = str(item.get("id", "")).strip()
    if item_id:
        return f"id:{item_id}"

    name_key = _normalize_skill_key(str(item.get("name", "")))
    if name_key:
        return f"name:{name_key}"
    return ""


def _normalize_skill_queries(
    query: str,
    queries: list[str] | None,
) -> tuple[list[str], str]:
    normalized: list[str] = []
    seen: set[str] = set()

    raw_queries: list[Any] = []
    if query.strip():
        raw_queries.append(query)

    parsed_queries, queries_err = _coerce_list_param(queries, "queries")
    if queries_err:
        return [], queries_err
    raw_queries.extend(parsed_queries)

    for raw in raw_queries:
        text = str(raw).strip()
        if not text or text in seen:
            continue
        seen.add(text)
        normalized.append(text)
    return normalized, ""


def _build_skill_search_item(
    result: dict[str, Any],
    path_map: RuntimePathMap | None = None,
) -> dict[str, Any]:
    raw_score = result.get("score", 0.0)
    try:
        score = round(float(raw_score), 2)
    except (TypeError, ValueError):
        score = 0.0

    item: dict[str, Any] = {"score": score}
    fields = result.get("fields", {})
    skill_path = fields.get("path", "")
    if skill_path:
        item["path"] = (
            path_map.locator_to_runtime(skill_path)
            if path_map is not None
            else skill_path
        )
        item["folder_tree"] = _build_skill_folder_tree(skill_path)
        try:
            from ..core.skills import _parse_frontmatter

            meta = _parse_frontmatter(Path(skill_path).read_text(encoding="utf-8"))
            if meta:
                if "name" in meta:
                    item["name"] = meta["name"]
                if "description" in meta:
                    item["description"] = meta["description"]
        except Exception:
            pass
    return item


def _merge_skill_search_items(
    merged: dict[str, dict[str, Any]],
    item: dict[str, Any],
    matched_query: str,
) -> None:
    key = _skill_item_key(item)
    if not key:
        return

    existing = merged.get(key)
    if existing is None:
        new_item = dict(item)
        new_item["matched_queries"] = [matched_query]
        merged[key] = new_item
        return

    matched_queries = existing.setdefault("matched_queries", [])
    if matched_query not in matched_queries:
        matched_queries.append(matched_query)

    existing_score = float(existing.get("score", 0.0))
    new_score = float(item.get("score", 0.0))
    if new_score > existing_score:
        for field in ("score", "path", "folder_tree", "name", "description", "id"):
            if field in item:
                existing[field] = item[field]


def _sort_skill_items(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        items,
        key=lambda item: (
            -float(item.get("score", 0.0)),
            -len(item.get("matched_queries", [])),
            str(item.get("name", item.get("path", ""))).lower(),
        ),
    )


@tool
def search_skills(
    state: AgentState,
    query: str = "",
    queries: list[str] | None = None,
    page: int = 1,
) -> str:
    """搜索或列出可用的 skills。

    `skills`是技能，在以下情况搜索、读取它们非常有用：
    - 当你需要一些专业指导时，或是某项细节需要确认时，例如：获取前端网页的设计指导、某个框架的执行细节等
    - 当你需要一些已经存在的技能进行复用时，例如：搜索网页、读取PDF、运行代码、创建项目等

    参数：
    - query (string, 可选): 单个搜索关键词。与 queries 可同时提供；会一起参与搜索。
    - queries (array[string], 可选): 一组搜索关键词。支持一次输入多个能力关键词并合并返回结果。
    - page (integer, 可选): 页码（从 1 开始，默认 1，每页 10 条，当不提供 query 时生效）。

    返回：skills 列表或搜索结果。
    """
    try:
        skills_path = str(_sandbox(state).resolve_skills_root())
    except ValueError as exc:
        return _result({"ok": False, "error": str(exc)})
    normalized_queries, queries_err = _normalize_skill_queries(query, queries)
    if queries_err:
        return _result({"ok": False, "error": queries_err})
    path_map = RuntimePathMap.from_state(state)
    host_skills_path = path_map.locator_to_host(skills_path)

    if not normalized_queries:
        result = skills_list(host_skills_path, page=page)
        for item in result.get("items", []):
            skill_path = str(item.get("path", "")).strip()
            if skill_path:
                item["path"] = path_map.locator_to_runtime(skill_path)
                item["folder_tree"] = _build_skill_folder_tree(skill_path)
        if result["total"] > _SKILLS_MANY_THRESHOLD:
            result["hint"] = "条目较多，可提供关键词搜索"
        return _result({"ok": True, **result})

    hub = getattr(state, "hub_url", "")
    if not hub:
        return _result({"ok": False, "error": "hub_url 未配置"})

    client = IndexClient(hub)
    try:
        merged_items: dict[str, dict[str, Any]] = {}
        for current_query in normalized_queries:
            results = client.search_skills(
                current_query,
                topk=10,
                path=host_skills_path,
            )
            for result in results:
                item = _build_skill_search_item(result, path_map)
                _merge_skill_search_items(merged_items, item, current_query)

        items = _sort_skill_items(list(merged_items.values()))
        linkage = _load_skill_linkage(host_skills_path)
        if linkage:
            existing_slugs = {
                _skill_slug_from_path(str(item.get("path", "")))
                for item in items
                if item.get("path")
            }
            existing_slugs.discard("")
            source_slugs = set(existing_slugs)
            for item in items:
                source_slugs.add(_normalize_skill_key(str(item.get("name", ""))))

            linked_by_slug: dict[str, dict[str, Any]] = {}
            slug_queries: dict[str, list[str]] = {}
            for item in items:
                matched_queries = [
                    str(q).strip()
                    for q in item.get("matched_queries", [])
                    if str(q).strip()
                ]
                for slug in (
                    _skill_slug_from_path(str(item.get("path", ""))),
                    _normalize_skill_key(str(item.get("name", ""))),
                ):
                    if not slug:
                        continue
                    existing_queries = slug_queries.setdefault(slug, [])
                    for matched_query in matched_queries:
                        if matched_query not in existing_queries:
                            existing_queries.append(matched_query)

            for source_slug in source_slugs:
                if source_slug not in linkage:
                    continue
                for rec in linkage[source_slug]:
                    target_slug = rec["skill"]
                    if target_slug in existing_slugs:
                        continue
                    target_item = linked_by_slug.get(target_slug)
                    if target_item is None:
                        target_item = _read_skill_meta_by_slug(host_skills_path, target_slug)
                        if target_item is None:
                            continue
                        target_item["recommended"] = True
                        target_item["linked_from"] = []
                        target_item["matched_queries"] = list(
                            slug_queries.get(source_slug, [])
                        )
                        linked_by_slug[target_slug] = target_item
                    else:
                        matched_queries = target_item.setdefault("matched_queries", [])
                        for matched_query in slug_queries.get(source_slug, []):
                            if matched_query not in matched_queries:
                                matched_queries.append(matched_query)
                    linked_from = target_item["linked_from"]
                    marker = (source_slug, rec["reason"])
                    existing_markers = {
                        (str(x.get("skill", "")), str(x.get("reason", "")))
                        for x in linked_from
                    }
                    if marker not in existing_markers:
                        linked_from.append({
                            "skill": source_slug,
                            "reason": rec["reason"],
                        })
            if linked_by_slug:
                items.extend(linked_by_slug.values())
                items = _sort_skill_items(_dedupe_skill_items(items))
        payload: dict[str, Any] = {
            "ok": True,
            "queries": normalized_queries,
            "items": items,
        }
        if len(normalized_queries) == 1:
            payload["query"] = normalized_queries[0]
        return _result(payload)
    finally:
        client.close()
