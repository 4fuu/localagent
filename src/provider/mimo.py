from typing import Any

from msgspec import field

from .base import BaseOpenAI


class Mimo(BaseOpenAI, kw_only=True):
    """Mimo provider。图片输入行为由 BaseOpenAI.image_input_mode 控制。"""

    base_url: str = field(default="https://api.xiaomimimo.com/v1")

    async def on_before_call(
        self, request_params: dict[str, Any], iteration: int
    ) -> dict[str, Any]:
        extra_body = request_params.get("extra_body", {})
        extra_body.update({"enable_thinking": True})
        request_params["extra_body"] = extra_body
        return request_params
