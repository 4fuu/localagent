import inspect
from typing import Callable, get_type_hints

from .utils import _get_json_type


def tool(func: Callable) -> Callable:
    """装饰器：标记函数为工具并提取元信息"""
    name = func.__name__
    description = (func.__doc__ or "").strip()

    sig = inspect.signature(func)
    hints = get_type_hints(func) if hasattr(func, "__annotations__") else {}

    properties = {}
    required = []

    for param_name, param in sig.parameters.items():
        # state 参数由 Agent 自动注入，不暴露给 LLM
        if param_name == "state":
            continue
        param_type = hints.get(param_name, str)
        properties[param_name] = {"type": _get_json_type(param_type)}

        if param.default is inspect.Parameter.empty:
            required.append(param_name)

    # 将 schema 附加到函数上
    func._tool_schema = {  # type: ignore
        "type": "function",
        "function": {
            "name": name,
            "description": description,
            "parameters": {
                "type": "object",
                "properties": properties,
                "required": required,
            },
        },
    }
    func._tool_name = name  # type: ignore
    return func
