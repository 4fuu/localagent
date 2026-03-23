import re
from xml.dom import minidom
from collections.abc import Mapping
from xml.etree.ElementTree import Element, SubElement, tostring
from typing import Any, Dict, Optional, Callable, Iterable


_XML_TAG_RE = re.compile(r"^[A-Za-z_][\w.\-]*$")


def _is_primitive(x: Any) -> bool:
    """判断是否为原始类型（字符串、数字、布尔、None）"""
    return isinstance(x, (str, int, float, bool)) or x is None


def _is_mapping(x: Any) -> bool:
    """判断是否为映射类型（字典等）"""
    return isinstance(x, Mapping)


def _is_iterable_nonstring(x: Any) -> bool:
    """判断是否为非字符串的可迭代类型"""
    if isinstance(x, (str, bytes, bytearray)):
        return False
    return isinstance(x, Iterable)


def _pretty_xml(elem: Element, indent: str = "  ") -> str:
    """将 Element 转换为格式化的 XML 字符串"""
    rough = tostring(elem, encoding="utf-8")
    parsed = minidom.parseString(rough)
    return parsed.toprettyxml(indent=indent, encoding="utf-8").decode("utf-8")


class XMLDumpOptions:
    """
    XML 序列化配置选项。

    属性：
        root_tag: 根标签名（默认 'root'）
        dict_tag: 非法键名时字典回退标签（默认 'dict'）
        list_tag: 非法键名时列表回退标签（默认 'list'）
        item_tag: 列表项标签名（默认 'item'）
        field_tag: 非法键名时标量回退标签（默认 'field'）
        custom_type_handler: 自定义类型序列化函数
        scalar_to_text: 标量->文本的转换函数（默认 str(v)；None->""）
        attr_processor: 全局属性处理函数（可选）
    """

    def __init__(
        self,
        root_tag: str = "root",
        dict_tag: str = "dict",
        list_tag: str = "list",
        item_tag: str = "item",
        field_tag: str = "field",
        custom_type_handler: Optional[Callable[[Any, "XMLDumper"], Any]] = None,
        scalar_to_text: Optional[Callable[[Any], str]] = None,
        attr_processor: Optional[
            Callable[
                [
                    str,
                    Dict[str, str],
                    Optional[Element],
                    Optional[str],
                    Any,
                    "XMLDumper",
                ],
                Dict[str, str],
            ]
        ] = None,
    ):
        self.root_tag = root_tag
        self.dict_tag = dict_tag
        self.list_tag = list_tag
        self.item_tag = item_tag
        self.field_tag = field_tag
        self.custom_type_handler = custom_type_handler
        self.scalar_to_text = scalar_to_text or (lambda v: "" if v is None else str(v))
        self.attr_processor = attr_processor


class XMLDumper:
    """
    将任意 Python 对象（字典/可迭代/标量）序列化为语义清晰的 XML。

    合法的 XML 标签名直接使用，非法标签名根据值类型回退：
        - 值为字典 → <dict key="原名">
        - 值为列表 → <list key="原名">
        - 值为标量 → <field key="原名">
    """

    def __init__(self, options: Optional[XMLDumpOptions] = None):
        self.opt = options or XMLDumpOptions()

    def _apply_attr_handler(
        self,
        tag: str,
        attrs: Dict[str, str],
        parent: Optional[Element],
        key: Optional[str],
        value: Any,
    ) -> Dict[str, str]:
        """应用全局属性处理器"""
        if self.opt.attr_processor:
            attrs = dict(attrs)
            attrs = self.opt.attr_processor(tag, attrs, parent, key, value, self)
        return attrs

    def dump_to_element(self, obj: Any) -> Element:
        """
        将对象序列化为 XML Element。

        参数：
            obj: 要序列化的对象

        返回：
            XML Element 根节点
        """
        root_attrs: Dict[str, str] = {}
        root_attrs = self._apply_attr_handler(
            self.opt.root_tag, root_attrs, None, None, obj
        )
        root = Element(self.opt.root_tag, root_attrs)
        self._attach(root, None, obj)
        return root

    def _attach(self, parent: Element, key: Optional[str], value: Any):
        """将值附加到父元素上"""
        value = self._maybe_custom_type(value)

        if _is_mapping(value):
            self._attach_mapping(parent, key, value)
            return

        if _is_iterable_nonstring(value):
            self._attach_iterable(parent, key, value)
            return

        self._attach_scalar(parent, key, value)

    def _tag_for_key(self, key: Any, value: Any) -> tuple[str, Dict[str, str]]:
        """
        根据 key 和 value 类型生成标签名和属性。

        合法标签名直接使用，非法标签名根据值类型回退。
        """
        k = "" if key is None else str(key)
        if _XML_TAG_RE.match(k):
            return k, {}

        if _is_mapping(value):
            return self.opt.dict_tag, {"key": k}
        elif _is_iterable_nonstring(value):
            return self.opt.list_tag, {"key": k}
        else:
            return self.opt.field_tag, {"key": k}

    def _attach_mapping(self, parent: Element, key: Optional[str], mp: Mapping):
        """
        处理字典类型。

        合法键名：<键名>...</键名>
        非法键名：<dict key="原名">...</dict>
        """
        container = self._ensure_container(parent, key, mp)
        for k, v in mp.items():
            vv = self._maybe_custom_type(v)
            tag, attrs = self._tag_for_key(k, vv)
            attrs = self._apply_attr_handler(tag, attrs, container, k, v)
            node = SubElement(container, tag, attrs)

            if _is_mapping(vv):
                self._attach_mapping(node, None, vv)
            elif _is_iterable_nonstring(vv):
                self._attach_iterable(node, None, vv)
            else:
                node.text = self.opt.scalar_to_text(vv)

    def _attach_iterable(self, parent: Element, key: Optional[str], it: Iterable):
        """
        处理可迭代类型（列表、元组、集合等）。

        合法键名：<键名><item index="0">...</item></键名>
        非法键名：<list key="原名"><item index="0">...</item></list>
        """
        container = self._ensure_container(parent, key, it)
        for i, v in enumerate(it):
            item_attrs = {"index": str(i)}
            item_attrs = self._apply_attr_handler(
                self.opt.item_tag, item_attrs, container, None, v
            )
            node = SubElement(container, self.opt.item_tag, item_attrs)

            vv = self._maybe_custom_type(v)
            if _is_mapping(vv):
                self._attach_mapping(node, None, vv)
            elif _is_iterable_nonstring(vv):
                self._attach_iterable(node, None, vv)
            else:
                node.text = self.opt.scalar_to_text(vv)

    def _attach_scalar(self, parent: Element, key: Optional[str], v: Any):
        """处理标量值"""
        if key is None:
            parent.text = self.opt.scalar_to_text(v)
            return

        tag, attrs = self._tag_for_key(key, v)
        attrs = self._apply_attr_handler(tag, attrs, parent, key, v)
        node = SubElement(parent, tag, attrs)
        node.text = self.opt.scalar_to_text(v)

    def _ensure_container(
        self, parent: Element, key: Optional[str], value: Any
    ) -> Element:
        """根据 key 合法性和 value 类型确保容器元素"""
        if key is None:
            return parent
        tag, attrs = self._tag_for_key(key, value)
        attrs = self._apply_attr_handler(tag, attrs, parent, key, None)
        return SubElement(parent, tag, attrs)

    def _maybe_custom_type(self, obj: Any) -> Any:
        """处理自定义类型"""
        if _is_primitive(obj) or _is_mapping(obj) or _is_iterable_nonstring(obj):
            return obj
        if self.opt.custom_type_handler:
            out = self.opt.custom_type_handler(obj, self)
            if (
                isinstance(out, Element)
                or _is_primitive(out)
                or _is_mapping(out)
                or _is_iterable_nonstring(out)
            ):
                return out
            if out is None:
                return None
            return out
        raise TypeError(f"Unsupported custom type: {type(obj).__name__}")

    def attach_element(self, parent: Element, elem: Element):
        """在自定义处理器中直接附加 Element"""
        parent.append(elem)


async def dump_prompt_async(
    map: Dict[str, Any],
    *,
    options: Optional[XMLDumpOptions] = None,
    custom_type_handler: Optional[Callable[[Any, XMLDumper], Any]] = None,
) -> str:
    """
    异步将对象序列化为可读的 XML 字符串。

    参数：
        map: 入口对象（通常是 dict，但也可传任意对象）
        options: XMLDumpOptions 实例，细粒度定制
        custom_type_handler: 自定义类型序列化处理器（覆盖 options.custom_type_handler）

    返回：
        格式化的 XML 字符串
    """
    opts = options or XMLDumpOptions()
    if custom_type_handler:
        opts.custom_type_handler = custom_type_handler

    dumper = XMLDumper(opts)
    root_elem = dumper.dump_to_element(map)
    xml_text = _pretty_xml(root_elem, indent="  ")
    return xml_text


if __name__ == "__main__":
    import asyncio

    context = {
        "system": "你是一个专业的代码助手，帮助用户解决编程问题。",
        "conversation": [
            {
                "role": "user",
                "content": "请帮我写一个 Python 函数，计算斐波那契数列",
                "timestamp": "2024-01-15T10:30:00Z",
            },
            {
                "role": "assistant",
                "content": "好的，这是一个递归实现的斐波那契函数...",
                "tool_calls": [
                    {
                        "name": "code_executor",
                        "arguments": {"language": "python", "code": "def fib(n): ..."},
                    }
                ],
            },
            {
                "role": "user",
                "content": "能优化一下性能吗？",
            },
        ],
        "workspace": {
            "current_file": "/home/user/project/main.py",
            "open_files": ["/home/user/project/main.py", "/home/user/project/utils.py"],
            "git_status": {
                "branch": "feature/fibonacci",
                "modified": ["main.py"],
                "staged": [],
            },
        },
        "user_preferences": {
            "language": "zh-CN",
            "code_style": "PEP8",
            "max_tokens": 4096,
        },
    }
    xml_str = asyncio.run(dump_prompt_async(context))
    print(xml_str)
