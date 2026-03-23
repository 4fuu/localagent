from __future__ import annotations

from dataclasses import dataclass, field
from html import escape
import re


@dataclass(slots=True)
class InlineNode:
    kind: str
    text: str = ""
    children: list["InlineNode"] = field(default_factory=list)
    url: str = ""


@dataclass(slots=True)
class BlockNode:
    kind: str
    children: list[InlineNode] = field(default_factory=list)
    blocks: list["BlockNode"] = field(default_factory=list)
    items: list[list["BlockNode"]] = field(default_factory=list)
    text: str = ""
    language: str = ""
    level: int = 0
    ordered: bool = False


@dataclass(slots=True)
class MarkdownDocument:
    blocks: list[BlockNode] = field(default_factory=list)


@dataclass(slots=True, frozen=True)
class RenderedText:
    text: str
    parse_mode: str | None = None


@dataclass(slots=True, frozen=True)
class MarkdownCapabilities:
    bold: bool = True
    italic: bool = True
    strike: bool = False
    spoiler: bool = False
    inline_code: bool = True
    code_block: bool = True
    link: bool = True
    blockquote: bool = False
    heading: bool = False
    list: bool = False


_FENCE_RE = re.compile(r"^```([A-Za-z0-9_+-]*)\s*$")
_HEADING_RE = re.compile(r"^(#{1,6})[ \t]+(.+?)\s*$")
_ORDERED_ITEM_RE = re.compile(r"^(\d+)\.[ \t]+(.+?)\s*$")
_UNORDERED_ITEM_RE = re.compile(r"^[-+*][ \t]+(.+?)\s*$")
_INLINE_CODE_RE = re.compile(r"(?<!`)`[^`\n]+`(?!`)")
_LINK_RE = re.compile(r"!?(\[[^\]\n]+\]\([^)]+\))")
_BOLD_RE = re.compile(r"(\*\*[^*\n][\s\S]*?\*\*|__[^_\n][\s\S]*?__)")
_ITALIC_RE = re.compile(r"(?<!\*)\*[^*\n][\s\S]*?\*(?!\*)|(?<!_)_[^_\n][\s\S]*?_(?!_)")
_STRIKE_RE = re.compile(r"~~[^~\n][\s\S]*?~~")
_SPOILER_RE = re.compile(r"\|\|[^|\n][\s\S]*?\|\|")


def parse_markdown(text: str) -> MarkdownDocument:
    normalized = text.replace("\r\n", "\n").replace("\r", "\n")
    lines = normalized.split("\n")
    blocks: list[BlockNode] = []
    index = 0
    while index < len(lines):
        line = lines[index]
        if not line.strip():
            index += 1
            continue

        fence_match = _FENCE_RE.match(line.strip())
        if fence_match:
            language = fence_match.group(1).strip()
            code_lines: list[str] = []
            index += 1
            while index < len(lines) and not lines[index].strip().startswith("```"):
                code_lines.append(lines[index])
                index += 1
            if index < len(lines):
                index += 1
            blocks.append(BlockNode(
                kind="code_block",
                text="\n".join(code_lines),
                language=language,
            ))
            continue

        if _is_blockquote_line(line):
            quote_lines: list[str] = []
            while index < len(lines) and _is_blockquote_line(lines[index]):
                quote_lines.append(_strip_blockquote_marker(lines[index]))
                index += 1
            blocks.append(BlockNode(
                kind="blockquote",
                blocks=parse_markdown("\n".join(quote_lines)).blocks,
            ))
            continue

        heading_match = _HEADING_RE.match(line)
        if heading_match:
            blocks.append(BlockNode(
                kind="heading",
                level=len(heading_match.group(1)),
                children=parse_inlines(heading_match.group(2)),
            ))
            index += 1
            continue

        if _match_list_item(line):
            list_block, index = _parse_list(lines, index)
            blocks.append(list_block)
            continue

        paragraph_lines = [line]
        index += 1
        while index < len(lines):
            current = lines[index]
            if not current.strip():
                break
            if _starts_special_block(current):
                break
            paragraph_lines.append(current)
            index += 1
        blocks.append(BlockNode(
            kind="paragraph",
            children=parse_inlines("\n".join(paragraph_lines)),
        ))
    return MarkdownDocument(blocks=blocks)


def should_render_markdown(text: str, *, min_score: int = 2) -> bool:
    normalized = text.replace("\r\n", "\n").replace("\r", "\n")
    lines = normalized.split("\n")
    score = 0

    if sum(1 for line in lines if _FENCE_RE.match(line.strip())) >= 2:
        score += 2
    if sum(1 for line in lines if _HEADING_RE.match(line)) >= 1:
        score += 1
    if sum(1 for line in lines if _is_blockquote_line(line)) >= 2:
        score += 1

    unordered_items = sum(1 for line in lines if _UNORDERED_ITEM_RE.match(line))
    ordered_items = sum(1 for line in lines if _ORDERED_ITEM_RE.match(line))
    if unordered_items >= 2:
        score += 2
    if ordered_items >= 2:
        score += 2

    score += min(len(_LINK_RE.findall(normalized)), 2)
    score += min(len(_INLINE_CODE_RE.findall(normalized)), 2)
    score += min(len(_BOLD_RE.findall(normalized)), 2)
    score += min(len(_ITALIC_RE.findall(normalized)), 2)
    score += min(len(_STRIKE_RE.findall(normalized)), 2)
    score += min(len(_SPOILER_RE.findall(normalized)), 2)
    return score >= min_score


def parse_inlines(text: str) -> list[InlineNode]:
    nodes, _, _ = _parse_inline_until(text, 0, None)
    return _merge_text_nodes(nodes)


def render_plain_text(
    doc: MarkdownDocument,
    capabilities: MarkdownCapabilities | None = None,
) -> RenderedText:
    caps = capabilities or MarkdownCapabilities()
    return RenderedText(text=_render_blocks_plain(doc.blocks, caps).strip())


def render_telegram_html(doc: MarkdownDocument) -> RenderedText:
    caps = MarkdownCapabilities(
        bold=True,
        italic=True,
        strike=True,
        spoiler=True,
        inline_code=True,
        code_block=True,
        link=True,
        blockquote=True,
        heading=False,
        list=False,
    )
    return RenderedText(
        text=_render_blocks_html(doc.blocks, caps).strip(),
        parse_mode="HTML",
    )


def _parse_list(lines: list[str], index: int) -> tuple[BlockNode, int]:
    first_match = _match_list_item(lines[index])
    assert first_match is not None
    ordered = first_match[0] == "ordered"
    items: list[list[BlockNode]] = []

    while index < len(lines):
        match = _match_list_item(lines[index])
        if match is None or (match[0] == "ordered") != ordered:
            break

        item_lines = [match[1]]
        index += 1
        while index < len(lines):
            current = lines[index]
            if not current.strip():
                break
            next_match = _match_list_item(current)
            if next_match is not None and (next_match[0] == "ordered") == ordered:
                break
            if _starts_special_block(current):
                break
            item_lines.append(current.strip())
            index += 1
        items.append(parse_markdown("\n".join(item_lines)).blocks)

        while index < len(lines) and not lines[index].strip():
            index += 1
            break

    return BlockNode(kind="list", items=items, ordered=ordered), index


def _match_list_item(line: str) -> tuple[str, str] | None:
    ordered_match = _ORDERED_ITEM_RE.match(line)
    if ordered_match:
        return "ordered", ordered_match.group(2)
    unordered_match = _UNORDERED_ITEM_RE.match(line)
    if unordered_match:
        return "unordered", unordered_match.group(1)
    return None


def _starts_special_block(line: str) -> bool:
    stripped = line.strip()
    if not stripped:
        return False
    return bool(
        _FENCE_RE.match(stripped)
        or _HEADING_RE.match(line)
        or _is_blockquote_line(line)
        or _match_list_item(line)
    )


def _is_blockquote_line(line: str) -> bool:
    return bool(re.match(r"^[ \t]{0,3}>", line))


def _strip_blockquote_marker(line: str) -> str:
    return re.sub(r"^[ \t]{0,3}>[ ]?", "", line)


def _parse_inline_until(
    text: str,
    index: int,
    stop_token: str | None,
) -> tuple[list[InlineNode], int, bool]:
    nodes: list[InlineNode] = []
    buffer: list[str] = []

    while index < len(text):
        if stop_token and text.startswith(stop_token, index):
            _flush_text_node(nodes, buffer)
            return nodes, index + len(stop_token), True

        if text[index] == "\\" and index + 1 < len(text):
            buffer.append(text[index + 1])
            index += 2
            continue

        if text.startswith("![", index):
            parsed = _try_parse_link(text, index, image=True)
            if parsed is not None:
                _flush_text_node(nodes, buffer)
                node, index = parsed
                nodes.append(node)
                continue

        if text.startswith("[", index):
            parsed = _try_parse_link(text, index, image=False)
            if parsed is not None:
                _flush_text_node(nodes, buffer)
                node, index = parsed
                nodes.append(node)
                continue

        if text[index] == "`":
            closing = _find_next_unescaped(text, "`", index + 1)
            if closing != -1:
                _flush_text_node(nodes, buffer)
                nodes.append(InlineNode(kind="code", text=text[index + 1:closing]))
                index = closing + 1
                continue

        marker = _match_inline_marker(text, index)
        if marker is not None:
            kind, token = marker
            parsed_children, next_index, closed = _parse_inline_until(
                text,
                index + len(token),
                token,
            )
            if closed:
                _flush_text_node(nodes, buffer)
                nodes.append(InlineNode(kind=kind, children=_merge_text_nodes(parsed_children)))
                index = next_index
                continue

        buffer.append(text[index])
        index += 1

    _flush_text_node(nodes, buffer)
    return nodes, index, False


def _match_inline_marker(text: str, index: int) -> tuple[str, str] | None:
    for token, kind in (
        ("**", "bold"),
        ("__", "bold"),
        ("~~", "strike"),
        ("||", "spoiler"),
        ("*", "italic"),
        ("_", "italic"),
    ):
        if text.startswith(token, index):
            return kind, token
    return None


def _try_parse_link(
    text: str,
    index: int,
    *,
    image: bool,
) -> tuple[InlineNode, int] | None:
    start = index + 2 if image else index + 1
    label_end = _find_balanced(text, start, "[", "]")
    if label_end == -1 or label_end + 1 >= len(text) or text[label_end + 1] != "(":
        return None
    url_end = _find_balanced(text, label_end + 2, "(", ")")
    if url_end == -1:
        return None

    label = text[start:label_end]
    url = text[label_end + 2:url_end].strip()
    kind = "image" if image else "link"
    return (
        InlineNode(
            kind=kind,
            url=url,
            children=parse_inlines(label),
        ),
        url_end + 1,
    )


def _find_balanced(text: str, index: int, open_char: str, close_char: str) -> int:
    depth = 1
    while index < len(text):
        if text[index] == "\\":
            index += 2
            continue
        if text[index] == open_char:
            depth += 1
        elif text[index] == close_char:
            depth -= 1
            if depth == 0:
                return index
        index += 1
    return -1


def _find_next_unescaped(text: str, target: str, index: int) -> int:
    while index < len(text):
        if text[index] == "\\":
            index += 2
            continue
        if text[index] == target:
            return index
        index += 1
    return -1


def _flush_text_node(nodes: list[InlineNode], buffer: list[str]) -> None:
    if buffer:
        nodes.append(InlineNode(kind="text", text="".join(buffer)))
        buffer.clear()


def _merge_text_nodes(nodes: list[InlineNode]) -> list[InlineNode]:
    merged: list[InlineNode] = []
    for node in nodes:
        if node.kind == "text" and merged and merged[-1].kind == "text":
            merged[-1].text += node.text
            continue
        merged.append(node)
    return merged


def _render_blocks_plain(blocks: list[BlockNode], caps: MarkdownCapabilities) -> str:
    rendered: list[str] = []
    for block in blocks:
        rendered.append(_render_block_plain(block, caps))
    return "\n\n".join(part for part in rendered if part.strip())


def _render_block_plain(block: BlockNode, caps: MarkdownCapabilities) -> str:
    if block.kind == "paragraph":
        return _render_inlines_plain(block.children, caps)
    if block.kind == "heading":
        return _render_inlines_plain(block.children, caps)
    if block.kind == "code_block":
        return block.text
    if block.kind == "blockquote":
        inner = _render_blocks_plain(block.blocks, caps)
        return "\n".join(
            f"> {line}" if line else ">"
            for line in inner.splitlines()
        )
    if block.kind == "list":
        items: list[str] = []
        for item_index, item_blocks in enumerate(block.items, start=1):
            prefix = f"{item_index}. " if block.ordered else "- "
            item = _render_blocks_plain(item_blocks, caps)
            items.append(_prefix_multiline(item, prefix))
        return "\n".join(items)
    return ""


def _render_inlines_plain(nodes: list[InlineNode], caps: MarkdownCapabilities) -> str:
    out: list[str] = []
    for node in nodes:
        if node.kind == "text":
            out.append(node.text)
        elif node.kind in {"bold", "italic", "strike", "spoiler"}:
            out.append(_render_inlines_plain(node.children, caps))
        elif node.kind == "code":
            out.append(node.text)
        elif node.kind == "link":
            label = _render_inlines_plain(node.children, caps).strip() or node.url
            out.append(label if not node.url or label == node.url else f"{label} ({node.url})")
        elif node.kind == "image":
            label = _render_inlines_plain(node.children, caps).strip()
            out.append(label if not node.url else f"{label or 'image'} ({node.url})")
    return "".join(out)


def _render_blocks_html(blocks: list[BlockNode], caps: MarkdownCapabilities) -> str:
    rendered: list[str] = []
    for block in blocks:
        rendered.append(_render_block_html(block, caps))
    return "\n\n".join(part for part in rendered if part.strip())


def _render_block_html(block: BlockNode, caps: MarkdownCapabilities) -> str:
    if block.kind == "paragraph":
        return _render_inlines_html(block.children, caps)
    if block.kind == "heading":
        content = _render_inlines_html(block.children, caps)
        return f"<b>{content}</b>"
    if block.kind == "code_block":
        code = escape(block.text, quote=False)
        language = block.language.strip()
        if language:
            return (
                f'<pre><code class="language-{escape(language, quote=True)}">'
                f"{code}</code></pre>"
            )
        return f"<pre>{code}</pre>"
    if block.kind == "blockquote":
        inner = _render_blocks_html(block.blocks, caps)
        if caps.blockquote:
            return f"<blockquote>{inner}</blockquote>"
        return "\n".join(
            f"&gt; {line}" if line else "&gt;"
            for line in inner.splitlines()
        )
    if block.kind == "list":
        items: list[str] = []
        for item_index, item_blocks in enumerate(block.items, start=1):
            prefix = f"{item_index}. " if block.ordered else "- "
            item = _render_blocks_html(item_blocks, caps)
            items.append(_prefix_multiline(item, prefix))
        return "\n".join(items)
    return ""


def _render_inlines_html(nodes: list[InlineNode], caps: MarkdownCapabilities) -> str:
    out: list[str] = []
    for node in nodes:
        if node.kind == "text":
            out.append(escape(node.text, quote=False))
        elif node.kind == "bold":
            content = _render_inlines_html(node.children, caps)
            out.append(f"<b>{content}</b>" if caps.bold else content)
        elif node.kind == "italic":
            content = _render_inlines_html(node.children, caps)
            out.append(f"<i>{content}</i>" if caps.italic else content)
        elif node.kind == "strike":
            content = _render_inlines_html(node.children, caps)
            out.append(f"<s>{content}</s>" if caps.strike else content)
        elif node.kind == "spoiler":
            content = _render_inlines_html(node.children, caps)
            out.append(f"<tg-spoiler>{content}</tg-spoiler>" if caps.spoiler else content)
        elif node.kind == "code":
            code = escape(node.text, quote=False)
            out.append(f"<code>{code}</code>" if caps.inline_code else code)
        elif node.kind == "link":
            label = _render_inlines_html(node.children, caps) or escape(node.url, quote=False)
            if caps.link and node.url:
                out.append(f'<a href="{escape(node.url, quote=True)}">{label}</a>')
            elif node.url:
                plain_label = _render_inlines_plain(node.children, caps).strip() or node.url
                out.append(escape(
                    plain_label if plain_label == node.url else f"{plain_label} ({node.url})",
                    quote=False,
                ))
            else:
                out.append(label)
        elif node.kind == "image":
            label = _render_inlines_plain(node.children, caps).strip() or "image"
            text = label if not node.url else f"{label} ({node.url})"
            out.append(escape(text, quote=False))
    return "".join(out)


def _prefix_multiline(text: str, prefix: str) -> str:
    lines = text.splitlines() or [text]
    rendered: list[str] = []
    for index, line in enumerate(lines):
        current_prefix = prefix if index == 0 else " " * len(prefix)
        rendered.append(f"{current_prefix}{line}")
    return "\n".join(rendered)
