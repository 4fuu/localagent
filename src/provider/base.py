import asyncio
import inspect
import logging
import random
from pathlib import Path
from typing import Any, Callable

from openai import AsyncOpenAI, AsyncStream
from openai.types.chat import ChatCompletionMessageToolCall
from openai.types.chat.chat_completion_chunk import ChatCompletionChunk
from openai.types.chat.chat_completion_message_tool_call import (
    Function as ToolCallFunction,
)
from msgspec import Struct, field, json

from ..core.artifacts import ArtifactStore, is_artifact_ref
from ..core.runtime_paths import RuntimePathMap
from ..retry import RetryPolicy
from .state import BaseState

logger = logging.getLogger(__name__)


class BaseOpenAI(Struct, kw_only=True):
    """基于 Chat Completions API 的 Agent 基类"""

    client: AsyncOpenAI | None = field(default=None)
    model: str
    api_key: str
    base_url: str
    state: BaseState
    tools: list[Callable] | None = field(default=None)
    extra_params: dict[str, Any] = field(default_factory=dict)
    image_input_mode: str = field(default="paths")

    max_retries: int = field(default=3)
    retry_delay: float = field(default=1.0)
    # 统计数据
    total_iterations: int = field(default=0)
    total_tool_calls: int = field(default=0)
    usage: dict[str, Any] = field(default_factory=dict)
    total_retries: int = field(default=0)
    # 终止控制
    stopped: bool = field(default=False)
    _stop_reason: str | None = field(default=None)
    _llm_retry_max_delay: float = field(default=30.0)
    _llm_retry_jitter: float = field(default=0.0)
    _tools_registry: dict[str, Callable] = field(default_factory=dict)
    _tools_schema: list[dict] = field(default_factory=list)
    _event_listeners: list[Callable] = field(default_factory=list)

    def __post_init__(self) -> None:
        self.image_input_mode = self._normalize_image_input_mode(self.image_input_mode)
        retry = RetryPolicy.for_service("llm_chat")
        if self.max_retries == 3:
            self.max_retries = retry.max_retries
        if self.retry_delay == 1.0:
            self.retry_delay = retry.base_delay
        self._llm_retry_max_delay = retry.max_delay
        self._llm_retry_jitter = retry.jitter

        if not self.client:
            self.client = AsyncOpenAI(api_key=self.api_key, base_url=self.base_url)
        # 注册工具
        if self.tools:
            for func in self.tools:
                if hasattr(func, "_tool_schema"):
                    self._tools_registry[func._tool_name] = func  # type: ignore
                    self._tools_schema.append(func._tool_schema)  # type: ignore
                else:
                    logger.warning(f"函数 {func.__name__} 没有 @tool 装饰器，已跳过")

    def _accumulate_usage(self, usage: Any) -> None:
        """递归累加 usage 对象的所有数值字段到 self.usage"""
        self._merge_usage(self.usage, usage)

    @staticmethod
    def _merge_usage(target: dict[str, Any], source: Any) -> None:
        for key, value in vars(source).items():
            if value is None:
                continue
            if isinstance(value, (int, float)):
                target[key] = target.get(key, 0) + value
            elif hasattr(value, "__dict__"):
                sub = target.setdefault(key, {})
                BaseOpenAI._merge_usage(sub, value)

    @staticmethod
    def _normalize_image_input_mode(raw: Any) -> str:
        if isinstance(raw, bool):
            return "multimodal" if raw else "disabled"
        value = str(raw or "").strip().lower()
        if value in {"", "paths", "path", "text", "fallback"}:
            return "paths"
        if value in {"multimodal", "vision", "enabled", "on", "true"}:
            return "multimodal"
        if value in {"disabled", "disable", "off", "none", "false"}:
            return "disabled"
        logger.warning("未知 image_input_mode=%r，回退到 paths", raw)
        return "paths"

    def _collect_image_paths(self, images: list[str] | None) -> list[str]:
        if not images:
            return []

        from .utils import _IMAGE_EXTENSIONS

        resolved_paths: list[str] = []
        seen: set[str] = set()
        for file_path in images:
            raw_path = str(file_path).strip()
            if not raw_path:
                continue
            if is_artifact_ref(raw_path):
                if raw_path in seen:
                    continue
                seen.add(raw_path)
                resolved_paths.append(raw_path)
                continue
            if raw_path.startswith("/") and getattr(self.state, "task_id", ""):
                try:
                    raw_path = RuntimePathMap.from_state(self.state).locator_to_host(raw_path)
                except Exception:
                    pass
            path = Path(raw_path)
            if path.suffix.lower() not in _IMAGE_EXTENSIONS:
                logger.warning("images 仅支持图片，已跳过: %s", path.name)
                continue
            resolved = str(path.resolve())
            if resolved in seen:
                continue
            seen.add(resolved)
            resolved_paths.append(resolved)
        return resolved_paths

    @staticmethod
    def _build_paths_fallback_content(
        user_input: str, image_paths: list[str]
    ) -> str:
        if not image_paths:
            return user_input

        lines = [user_input.strip()] if user_input.strip() else []
        lines.append("附带图片文件（当前模型未走多模态输入，请按文件路径按需处理）：")
        lines.extend(f"- {path}" for path in image_paths)
        return "\n".join(lines).strip()

    def stop(self, reason: str | None = None) -> None:
        """优雅终止 Agent 循环，当前迭代完成后停止"""
        self.stopped = True
        self._stop_reason = reason

    def _patch_messages_on_stop(self) -> None:
        """停止后补全 messages 中未闭合的 tool_call 并追加 assistant 消息"""
        messages = self.state.messages
        if not messages:
            return
        placeholder = "[已中止]"
        reason_appended = False
        # 从后往前找最后一个带 tool_calls 的 assistant 消息
        for i in range(len(messages) - 1, -1, -1):
            msg = messages[i]
            if msg.get("role") == "assistant" and msg.get("tool_calls"):
                expected_ids = {tc["id"] for tc in msg["tool_calls"]}
                existing_ids = {
                    messages[j]["tool_call_id"]
                    for j in range(i + 1, len(messages))
                    if messages[j].get("role") == "tool"
                }
                for tc_id in expected_ids - existing_ids:
                    messages.append({
                        "role": "tool",
                        "tool_call_id": tc_id,
                        "content": placeholder,
                    })
                break
        # 确保最后一条消息是 assistant
        if messages[-1].get("role") != "assistant":
            content = (
                f"{placeholder}\n{self._stop_reason}"
                if self._stop_reason
                else placeholder
            )
            reason_appended = True
            messages.append({"role": "assistant", "content": content})
        # 原因只追加一次
        if self._stop_reason and not reason_appended:
            last = messages[-1]
            if last.get("role") == "assistant" and self._stop_reason not in (
                last.get("content") or ""
            ):
                last["content"] = (
                    f"{last['content'] or ''}\n{self._stop_reason}".strip()
                )

    def add_event_listener(
        self, listener: Callable[[ChatCompletionChunk], Any]
    ) -> None:
        """注册 SSE 事件监听器"""
        self._event_listeners.append(listener)

    def remove_event_listener(
        self, listener: Callable[[ChatCompletionChunk], Any]
    ) -> None:
        """移除 SSE 事件监听器"""
        self._event_listeners.remove(listener)

    async def _emit_event(self, event: ChatCompletionChunk) -> None:
        """分发事件到 on_event 钩子和所有已注册的监听器"""
        await self.on_event(event)
        for listener in self._event_listeners:
            if inspect.iscoroutinefunction(listener):
                await listener(event)
            else:
                listener(event)

    async def _call_and_consume_with_retry(
        self, request_params: dict[str, Any]
    ) -> tuple[
        str | None,
        dict[int, dict[str, str]],
        list[tuple[ChatCompletionMessageToolCall, asyncio.Task[str]]],
        set[int],
    ]:
        """带重试的流式 LLM 调用 + 消费（覆盖流式中断场景）"""
        assert self.client, "self.client is None"
        last_exc: Exception | None = None
        for attempt in range(self.max_retries):
            if self.stopped:
                raise InterruptedError("Agent 已被终止")
            try:
                stream = await self.client.chat.completions.create(**request_params)
                return await self._consume_stream(stream)
            except InterruptedError:
                raise
            except Exception as e:
                last_exc = e
                self.total_retries += 1
                if attempt < self.max_retries - 1:
                    delay = self.retry_delay * (2**attempt)
                    delay = min(delay, self._llm_retry_max_delay)
                    if self._llm_retry_jitter > 0:
                        factor = random.uniform(
                            1.0 - self._llm_retry_jitter,
                            1.0 + self._llm_retry_jitter,
                        )
                        delay *= factor
                    logger.warning(
                        f"LLM 调用失败 (第{attempt + 1}次)，{delay}s 后重试：{e}"
                    )
                    await asyncio.sleep(delay)
        raise last_exc  # type: ignore

    # ========== Hook ==========

    async def on_before_call(
        self, request_params: dict[str, Any], iteration: int
    ) -> dict[str, Any]:
        """LLM 调用前的钩子，可修改整个请求参数"""
        return request_params

    async def on_event(self, event: ChatCompletionChunk) -> None:
        """流式事件回调，子类可覆写处理 SSE 事件"""
        pass

    async def on_before_tool(
        self, tool_call: ChatCompletionMessageToolCall, args: dict[str, Any]
    ) -> dict[str, Any]:
        """工具执行前的钩子，可修改参数"""
        return args

    async def on_after_tool(
        self, tool_call: ChatCompletionMessageToolCall, result: str
    ) -> str:
        """工具执行后的钩子，可修改结果"""
        return result

    async def _execute_tool(self, tool_call: ChatCompletionMessageToolCall) -> str:
        """执行单个工具调用"""
        name = tool_call.function.name
        args = json.decode(tool_call.function.arguments)

        if name not in self._tools_registry:
            return f"错误：未知工具 {name}"

        try:
            # 调用前钩子
            args = await self.on_before_tool(tool_call, args)

            func = self._tools_registry[name]
            # 如果工具函数签名中有 state 参数，自动注入
            sig = inspect.signature(func)
            if "state" in sig.parameters:
                args["state"] = self.state

            if inspect.iscoroutinefunction(func):
                result = await func(**args)
            else:
                result = func(**args)
            result = str(result)

            # 调用后钩子
            result = await self.on_after_tool(tool_call, result)
            return result

        except Exception as e:
            logger.exception(f"工具 {name} 执行失败")
            return f"工具执行错误：{e}"

    def _make_tool_call_obj(
        self, tc_data: dict[str, str]
    ) -> ChatCompletionMessageToolCall:
        """从累积的流式数据构造 ChatCompletionMessageToolCall"""
        return ChatCompletionMessageToolCall(
            id=tc_data["id"],
            type="function",
            function=ToolCallFunction(
                name=tc_data["name"], arguments=tc_data["arguments"]
            ),
        )

    def _start_pending_tools(
        self,
        tool_calls_by_index: dict[int, dict[str, str]],
        started_indices: set[int],
        pending_tasks: list[tuple[ChatCompletionMessageToolCall, asyncio.Task[str]]],
    ) -> None:
        """启动所有已完整但未执行的工具调用"""
        for idx in sorted(tool_calls_by_index):
            if idx not in started_indices:
                started_indices.add(idx)
                tc_obj = self._make_tool_call_obj(tool_calls_by_index[idx])
                self.total_tool_calls += 1
                logger.info(f"执行工具：{tc_obj.function.name}")
                task = asyncio.create_task(self._execute_tool(tc_obj))
                pending_tasks.append((tc_obj, task))

    def _accumulate_tc_delta(
        self,
        tc_delta: Any,
        tool_calls_by_index: dict[int, dict[str, str]],
        started_indices: set[int],
        pending_tasks: list[tuple[ChatCompletionMessageToolCall, asyncio.Task[str]]],
    ) -> None:
        """累积单个 tool_call delta，新工具出现时立即启动已完整的调用"""
        idx = tc_delta.index
        if idx not in tool_calls_by_index:
            if not self.stopped:
                self._start_pending_tools(
                    tool_calls_by_index, started_indices, pending_tasks
                )
            tool_calls_by_index[idx] = {
                "id": tc_delta.id or "",
                "name": (
                    tc_delta.function.name
                    if tc_delta.function and tc_delta.function.name
                    else ""
                ),
                "arguments": (
                    tc_delta.function.arguments
                    if tc_delta.function and tc_delta.function.arguments
                    else ""
                ),
            }
        elif tc_delta.function and tc_delta.function.arguments:
            tool_calls_by_index[idx]["arguments"] += tc_delta.function.arguments

    async def _consume_stream(
        self,
        stream: AsyncStream[ChatCompletionChunk],
    ) -> tuple[
        str | None,
        dict[int, dict[str, str]],
        list[tuple[ChatCompletionMessageToolCall, asyncio.Task[str]]],
        set[int],
    ]:
        """消费流式响应，返回 (content, tool_calls_by_index, pending_tasks, started_indices)"""
        content_parts: list[str] = []
        tool_calls_by_index: dict[int, dict[str, str]] = {}
        pending_tasks: list[
            tuple[ChatCompletionMessageToolCall, asyncio.Task[str]]
        ] = []
        started_indices: set[int] = set()
        final_usage: Any | None = None

        async for chunk in stream:
            if self.stopped:
                await stream.close()
                break
            await self._emit_event(chunk)

            if chunk.usage:
                # Some providers repeat cumulative usage on multiple stream chunks.
                # Keep the latest non-empty usage for this request and merge once
                # after the stream ends, otherwise totals can be massively inflated.
                final_usage = chunk.usage
            if not chunk.choices:
                continue

            delta = chunk.choices[0].delta
            if delta.content:
                content_parts.append(delta.content)
            if delta.tool_calls:
                for tc_delta in delta.tool_calls:
                    self._accumulate_tc_delta(
                        tc_delta,
                        tool_calls_by_index,
                        started_indices,
                        pending_tasks,
                    )

        if final_usage:
            self._accumulate_usage(final_usage)

        content = "".join(content_parts) or None
        return content, tool_calls_by_index, pending_tasks, started_indices

    @staticmethod
    def _build_assistant_msg(
        content: str | None,
        tool_calls_by_index: dict[int, dict[str, str]],
    ) -> dict[str, Any]:
        """从累积数据构建 assistant 消息"""
        msg: dict[str, Any] = {"role": "assistant", "content": content}
        if tool_calls_by_index:
            msg["tool_calls"] = [
                {
                    "id": tool_calls_by_index[idx]["id"],
                    "type": "function",
                    "function": {
                        "name": tool_calls_by_index[idx]["name"],
                        "arguments": tool_calls_by_index[idx]["arguments"],
                    },
                }
                for idx in sorted(tool_calls_by_index)
            ]
        return msg

    async def _cancel_tasks(
        self,
        pending_tasks: list[tuple[ChatCompletionMessageToolCall, asyncio.Task[str]]],
    ) -> None:
        """取消所有待执行的工具任务"""
        for _, task in pending_tasks:
            task.cancel()
        await asyncio.gather(*(t for _, t in pending_tasks), return_exceptions=True)

    @staticmethod
    def _collect_multimodal_blocks_from_result(result: str) -> list[dict[str, Any]]:
        """从工具 JSON 结果中提取 image_url 多模态块（支持单结果与批量 results）。"""
        try:
            payload = json.decode(result)
        except Exception:
            return []

        items: list[Any] = []
        if isinstance(payload, dict):
            items.append(payload)
            results = payload.get("results")
            if isinstance(results, list):
                items.extend(results)
        elif isinstance(payload, list):
            items.extend(payload)
        else:
            return []

        blocks: list[dict[str, Any]] = []
        seen_urls: set[str] = set()
        for item in items:
            if not isinstance(item, dict):
                continue
            multimodal = item.get("multimodal")
            if not isinstance(multimodal, list):
                continue
            for block in multimodal:
                if not isinstance(block, dict):
                    continue
                if block.get("type") != "image_url":
                    continue
                image_url = block.get("image_url")
                if not isinstance(image_url, dict):
                    continue
                url = image_url.get("url")
                if not isinstance(url, str) or not url.strip():
                    continue
                if url in seen_urls:
                    continue
                seen_urls.add(url)
                blocks.append({
                    "type": "image_url",
                    "image_url": {"url": url},
                })
        return blocks

    @staticmethod
    def _strip_multimodal_fields_in_result(result: str) -> str:
        """移除工具结果中的 multimodal 大字段，避免在 tool 消息中重复注入。"""
        try:
            payload = json.decode(result)
        except Exception:
            return result

        if not isinstance(payload, dict):
            return result

        changed = False
        if isinstance(payload.get("multimodal"), list):
            payload["multimodal"] = "[extracted-to-messages]"
            changed = True

        results = payload.get("results")
        if isinstance(results, list):
            for item in results:
                if isinstance(item, dict) and isinstance(item.get("multimodal"), list):
                    item["multimodal"] = "[extracted-to-messages]"
                    changed = True

        if not changed:
            return result
        return json.encode(payload).decode("utf-8")

    @staticmethod
    def _merge_multimodal_into_user_message(
        user_message: dict[str, Any],
        blocks: list[dict[str, Any]],
    ) -> None:
        """将提取到的多模态块合并进首条 user 消息，避免新增 user 角色打断 tool 闭合。"""
        if not blocks:
            return

        content = user_message.get("content")
        content_blocks: list[dict[str, Any]]

        if isinstance(content, str):
            content_blocks = [{"type": "text", "text": content}]
        elif isinstance(content, list):
            content_blocks = [b for b in content if isinstance(b, dict)]
            has_text = any(b.get("type") == "text" for b in content_blocks)
            if not has_text:
                content_blocks.insert(0, {"type": "text", "text": ""})
        else:
            content_blocks = [{"type": "text", "text": ""}]

        existing_urls: set[str] = set()
        for block in content_blocks:
            if block.get("type") != "image_url":
                continue
            image_url = block.get("image_url")
            if not isinstance(image_url, dict):
                continue
            url = image_url.get("url")
            if isinstance(url, str) and url:
                existing_urls.add(url)

        for block in blocks:
            image_url = block.get("image_url")
            url = image_url.get("url") if isinstance(image_url, dict) else ""
            if not isinstance(url, str) or not url or url in existing_urls:
                continue
            existing_urls.add(url)
            content_blocks.append(block)

        user_message["content"] = content_blocks

    def _build_user_content(
        self, user_input: str, images: list[str] | None = None
    ) -> str | list[dict[str, Any]]:
        """构建 user 消息的 content，支持多模态。"""
        image_paths = self._collect_image_paths(images)
        if not image_paths:
            return user_input

        from .utils import (
            IMAGE_MIME_MAP,
            compress_image_if_needed,
            encode_image_bytes_to_data_uri,
            encode_image_to_data_uri,
        )

        if self.image_input_mode == "disabled":
            logger.info(
                "模型 %s 已禁用图片输入，跳过 %d 张图片",
                self.model,
                len(image_paths),
            )
            return user_input
        if self.image_input_mode == "paths":
            return self._build_paths_fallback_content(user_input, image_paths)

        content: list[dict[str, Any]] = [{"type": "text", "text": user_input}]
        seen: set[str] = set()
        artifact_store = ArtifactStore()
        for file_path in image_paths:
            if is_artifact_ref(file_path):
                if file_path in seen:
                    continue
                seen.add(file_path)
                meta = artifact_store.stat(file_path)
                mime = str(meta.get("mime_type", "")).strip() or "image/jpeg"
                data_uri = encode_image_bytes_to_data_uri(
                    artifact_store.read_bytes(file_path),
                    mime=mime,
                )
                if not data_uri:
                    logger.warning("无法编码 artifact 图片，已跳过: %s", file_path)
                    continue
                content.append({
                    "type": "image_url",
                    "image_url": {"url": data_uri},
                })
                continue
            p = Path(file_path)
            resolved = compress_image_if_needed(str(p))
            if resolved in seen:
                continue
            seen.add(resolved)
            data_uri = encode_image_to_data_uri(resolved)
            if not data_uri:
                if p.is_file():
                    mime = IMAGE_MIME_MAP.get(p.suffix.lower(), "image/jpeg")
                    data_uri = encode_image_bytes_to_data_uri(p.read_bytes(), mime=mime)
                if not data_uri:
                    logger.warning("无法编码图片，已跳过: %s", p.name)
                    continue
            content.append({
                "type": "image_url",
                "image_url": {"url": data_uri},
            })
        if len(content) == 1:
            return content[0]["text"]
        return content

    async def run(
        self, user_input: str, *, images: list[str] | None = None
    ) -> str:
        """
        运行 Agent 循环

        流式接收过程中，tool_call 完整后立即并发执行工具，
        不必等待整个流式结束。

        Args:
            user_input: 用户输入
            images: 可选的图片文件路径列表（以多模态方式注入）

        Returns:
            Agent 的最终回复
        """
        self.stopped = False
        self._stop_reason = None
        messages = self.state.messages
        user_content = self._build_user_content(user_input, images)
        messages.append({"role": "user", "content": user_content})
        first_user_message_index = len(messages) - 1

        i = 0
        while not self.stopped:
            logger.debug(f"Agent 循环第 {i + 1} 轮")
            self.total_iterations += 1

            request_params: dict[str, Any] = {
                "model": self.model,
                "messages": messages,
                "stream": True,
                "stream_options": {"include_usage": True},
            }
            if self._tools_schema:
                request_params["tools"] = self._tools_schema
            request_params.update(self.extra_params)
            request_params["stream"] = True  # 强制流式

            request_params = await self.on_before_call(request_params, i)
            messages = request_params["messages"]
            self.state.messages = messages

            try:
                (
                    content,
                    tool_calls_by_index,
                    pending_tasks,
                    started_indices,
                ) = await self._call_and_consume_with_retry(request_params)
            except InterruptedError:
                break
            messages.append(self._build_assistant_msg(content, tool_calls_by_index))

            if self.stopped:
                await self._cancel_tasks(pending_tasks)
                break

            self._start_pending_tools(
                tool_calls_by_index, started_indices, pending_tasks
            )

            if not pending_tasks:
                return content or ""

            iteration_multimodal_blocks: list[dict[str, Any]] = []
            seen_iteration_urls: set[str] = set()
            for tc_obj, task in pending_tasks:
                result = await task
                extracted = self._collect_multimodal_blocks_from_result(result)
                for block in extracted:
                    image_url = block.get("image_url")
                    url = image_url.get("url") if isinstance(image_url, dict) else ""
                    if not isinstance(url, str) or not url or url in seen_iteration_urls:
                        continue
                    seen_iteration_urls.add(url)
                    iteration_multimodal_blocks.append(block)
                messages.append({
                    "role": "tool",
                    "tool_call_id": tc_obj.id,
                    "content": self._strip_multimodal_fields_in_result(result),
                })

            if iteration_multimodal_blocks:
                if self.image_input_mode == "multimodal":
                    first_user_msg = messages[first_user_message_index]
                    if (
                        isinstance(first_user_msg, dict)
                        and first_user_msg.get("role") == "user"
                    ):
                        self._merge_multimodal_into_user_message(
                            first_user_msg, iteration_multimodal_blocks
                        )
                else:
                    logger.info(
                        "模型 %s image_input_mode=%s，跳过 %d 个工具图片块注入",
                        self.model,
                        self.image_input_mode,
                        len(iteration_multimodal_blocks),
                    )

            i += 1

        logger.warning("Agent 被终止")
        self._patch_messages_on_stop()
        return messages[-1].get("content", "") if messages else ""
