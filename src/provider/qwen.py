from typing import Any

from msgspec import field

from .base import BaseOpenAI


class Qwen(BaseOpenAI, kw_only=True):
    """Qwen provider。图片输入行为由 BaseOpenAI.image_input_mode 控制。"""

    base_url: str = field(default="https://dashscope.aliyuncs.com/compatible-mode/v1")

    _enable_buildin_tools: bool | None = field(default=None)

    async def run(
        self,
        user_input: str,
        *,
        enable_buildin_tools: bool = False,
        images: list[str] | None = None,
    ) -> str:
        """
        运行 Agent 循环

        根据 enable_buildin_tools 和 tools 的存在情况自动选择工具模式：
        - enable_buildin_tools=True: 使用内置工具（搜索、代码解释器等）
        - tools 不为空：使用自定义注册的工具
        - 两者都不满足：不使用工具

        Args:
            user_input: 用户输入
            enable_buildin_tools: 是否启用内置工具
            images: 可选的图片文件路径列表，实际注入方式由 image_input_mode 控制

        Returns:
            Agent 的最终回复
        """
        # 保存原始的 _tools_schema，在 run 中根据情况决定是否使用
        original_tools_schema = self._tools_schema.copy()

        if enable_buildin_tools:
            # 使用内置工具时，清空自定义工具
            self._tools_schema = []
        elif original_tools_schema:
            # 使用自定义工具，保持 _tools_schema 不变
            pass
        # 否则 _tools_schema 为空，不使用任何工具

        # 临时存储 enable_buildin_tools 供 on_before_call 使用
        self._enable_buildin_tools = enable_buildin_tools

        try:
            return await super().run(user_input, images=images)
        finally:
            # 恢复原始工具列表，以便下次 run 时重新决策
            self._tools_schema = original_tools_schema
            self._enable_buildin_tools = None

    async def on_before_call(
        self, request_params: dict[str, Any], iteration: int
    ) -> dict[str, Any]:
        if getattr(self, "_enable_buildin_tools", False):
            extra_body = request_params.get("extra_body", {})
            extra_body.update({
                "enable_thinking": True,
                "enable_code_interpreter": True,
                "enable_search": True,
                "search_options": {"search_strategy": "agent_max"},
            })
            request_params["extra_body"] = extra_body

        return request_params
