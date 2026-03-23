#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.10"
# dependencies = [
#   "exa_py",
# ]
# ///

import os
import sys
import json
from exa_py import Exa


def search(query: str, num_results: int = 10) -> None:
    """使用 Exa API 搜索网络内容并打印结果。

    Args:
        query: 搜索关键词
        num_results: 返回结果数量，默认 10
    """
    api_key = os.getenv("EXA_API_KEY")
    if not api_key:
        print("错误：未找到 EXA_API_KEY 环境变量", file=sys.stderr)
        sys.exit(1)

    exa = Exa(api_key=api_key)

    results = exa.search(
        query,
        type="auto",
        num_results=num_results,
        contents={"highlights": {"max_characters": 4000}}
    )

    for result in results.results:
        print(f"标题：{result.title}")
        print(f"链接：{result.url}")
        if hasattr(result, 'highlights') and result.highlights:
            print(f"摘要：{result.highlights[0] if isinstance(result.highlights, list) else result.highlights}")
        print("-" * 60)


def get_contents(urls: list[str], text: bool = False, summary: bool = False, highlights: dict | None = None) -> None:
    """使用 Exa API 获取指定 URL 的网页内容。

    Args:
        urls: URL 列表
        text: 是否获取完整文本内容，默认 False
        summary: 是否获取摘要，默认 False
        highlights: 高亮内容配置，如 {"max_characters": 2000}，默认 None
    """
    api_key = os.getenv("EXA_API_KEY")
    if not api_key:
        print("错误：未找到 EXA_API_KEY 环境变量", file=sys.stderr)
        sys.exit(1)

    exa = Exa(api_key=api_key)

    if not (text or summary or highlights):
        print("错误：至少需要指定 text、summary 或 highlights 中的一个参数", file=sys.stderr)
        sys.exit(1)

    kwargs = {}
    if text:
        kwargs["text"] = text
    if summary:
        kwargs["summary"] = summary
    if highlights:
        kwargs["highlights"] = highlights

    results = exa.get_contents(urls, **kwargs)

    for result in results.results:
        print(f"URL: {result.url}")
        if hasattr(result, 'title') and result.title:
            print(f"标题：{result.title}")
        if hasattr(result, 'text') and result.text:
            print(f"文本内容：{result.text[:500]}..." if len(result.text) > 500 else f"文本内容：{result.text}")
        if hasattr(result, 'summary') and result.summary:
            print(f"摘要：{result.summary}")
        if hasattr(result, 'highlights') and result.highlights:
            highlights_text = result.highlights[0] if isinstance(result.highlights, list) else result.highlights
            print(f"高亮：{highlights_text}")
        print("-" * 60)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("用法:", file=sys.stderr)
        print("  搜索模式：uv run search.py search <搜索关键词> [结果数量]", file=sys.stderr)
        print("  获取内容：uv run search.py contents <URL1> [URL2 ...] [--text] [--summary] [--highlights <config>]", file=sys.stderr)
        print("示例:", file=sys.stderr)
        print("  uv run search.py search \"React hooks best practices 2024\"", file=sys.stderr)
        print("  uv run search.py contents \"https://openai.com/research\" --text", file=sys.stderr)
        print("  uv run search.py contents \"https://stripe.com/docs/api\" --summary", file=sys.stderr)
        print("  uv run search.py contents \"https://arxiv.org/abs/2303.08774\" --highlights '{\"max_characters\": 2000}'", file=sys.stderr)
        sys.exit(1)

    mode = sys.argv[1]

    if mode == "search":
        if len(sys.argv) < 3:
            print("错误：搜索模式需要提供关键词", file=sys.stderr)
            sys.exit(1)
        query = sys.argv[2]
        num_results = int(sys.argv[3]) if len(sys.argv) > 3 else 10
        search(query, num_results)
    elif mode == "contents":
        if len(sys.argv) < 3:
            print("错误：获取内容模式需要提供至少一个 URL", file=sys.stderr)
            sys.exit(1)
        
        urls = []
        text = False
        summary = False
        highlights = None
        
        i = 2
        while i < len(sys.argv):
            arg = sys.argv[i]
            if arg == "--text":
                text = True
                i += 1
            elif arg == "--summary":
                summary = True
                i += 1
            elif arg == "--highlights":
                if i + 1 < len(sys.argv):
                    highlights = json.loads(sys.argv[i + 1])
                    i += 2
                else:
                    print("错误：--highlights 需要提供 JSON 配置", file=sys.stderr)
                    sys.exit(1)
            else:
                urls.append(arg)
                i += 1
        
        if not urls:
            print("错误：需要提供至少一个 URL", file=sys.stderr)
            sys.exit(1)
        
        get_contents(urls, text=text, summary=summary, highlights=highlights)
    else:
        print(f"错误：未知模式 '{mode}'，支持 search 和 contents", file=sys.stderr)
        sys.exit(1)
