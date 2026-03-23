#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.10"
# dependencies = [
#   "httpx",
#   "jinja2",
#   "playwright==1.52.0",
# ]
# ///

import os
import sys
import json
import re
import argparse
import uuid
from pathlib import Path
import httpx

_SKILL_DIR = Path(__file__).resolve().parent
_PROJECT_ROOT = _SKILL_DIR.parent.parent


def _default_cards_output_dir() -> Path:
    cwd = Path.cwd().resolve()
    if ".localagent" in cwd.parts and "workspaces" in cwd.parts:
        return cwd / "bocha-cards"
    return _PROJECT_ROOT / "workspace" / "bocha-cards"


def bocha_search(
    query: str,
    freshness: str = "noLimit",
    count: int = 10,
    render_cards: bool = True,
    cards_output_dir: str | None = None,
) -> None:
    """使用博查 AI 搜索 API 进行搜索并打印结果。

    Args:
        query: 搜索关键词
        freshness: 时间范围，可选值：noLimit（不限）、oneDay（一天内）、oneWeek（一周内）、oneMonth（一月内）、oneYear（一年内），默认 noLimit
        count: 返回结果数量，默认 10，最多 50
    """
    api_key = os.getenv("BOCHA_API_KEY")
    if not api_key:
        print("错误：未找到 BOCHA_API_KEY 环境变量", file=sys.stderr)
        sys.exit(1)

    api_url = "https://api.bocha.cn/v1/ai-search"

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }

    payload = {
        "query": query,
        "freshness": freshness,
        "count": count,
        "answer": False,
        "stream": False
    }

    render_model_card_to_image = None
    output_dir = Path(cards_output_dir) if cards_output_dir else _default_cards_output_dir()
    if render_cards:
        output_dir.mkdir(parents=True, exist_ok=True)
        try:
            from render_card import render_model_card_to_image
        except Exception as e:
            print(f"警告：模态卡图片渲染器加载失败，将仅输出文本（{e}）", file=sys.stderr)
            render_cards = False

    rendered_image_count = 0
    rendered_image_records: list[dict[str, object]] = []

    try:
        with httpx.Client(timeout=60.0) as client:
            response = client.post(api_url, headers=headers, json=payload)
            response.raise_for_status()
            data = response.json()

        # 博查 API 返回的是 messages 数组
        messages = data.get("messages", [])
        
        if not messages:
            print("未找到相关搜索结果")
            return

        # 遍历所有消息
        for msg in messages:
            role = msg.get("role", "")
            msg_type = msg.get("type", "")
            content_type = msg.get("content_type", "")
            content_str = msg.get("content", "")
            
            # 跳过非 assistant 或非 source 类型的消息
            if role != "assistant" or msg_type != "source":
                continue
            
            # 处理模态卡
            if content_type and content_type not in ["webpage", "image", "video"]:
                print("=" * 60)
                print(f"📌 模态卡：{content_type}")
                print("=" * 60)
                
                # 解析 content 字段（通常是 JSON 字符串，也可能是已解析对象）
                try:
                    content_obj = json.loads(content_str) if isinstance(content_str, str) else content_str
                    if isinstance(content_obj, list) and len(content_obj) > 0:
                        model_cards = []
                        for item in content_obj:
                            if isinstance(item, dict) and isinstance(item.get("modelCard"), dict):
                                model_cards.append(item["modelCard"])

                        if model_cards:
                            for i, model_card in enumerate(model_cards, 1):
                                print(f"模态卡内容 #{i}:")
                                print(json.dumps(model_card, ensure_ascii=False, indent=2))
                                if render_cards and render_model_card_to_image is not None:
                                    safe_type = re.sub(r"[^a-zA-Z0-9_-]+", "_", content_type or "card")
                                    image_path = output_dir / f"{safe_type}_{uuid.uuid4().hex[:12]}.png"
                                    render_model_card_to_image(content_type, model_card, image_path)
                                    print(f"🖼️ 模态卡图片：{image_path}（类型：{content_type}，内容 #{i}）")
                                    rendered_image_records.append(
                                        {
                                            "content_type": content_type,
                                            "card_index": i,
                                            "image_path": str(image_path),
                                        }
                                    )
                                    rendered_image_count += 1
                        else:
                            # 如果没有 modelCard，直接打印内容
                            print(json.dumps(content_obj, ensure_ascii=False, indent=2))
                    elif isinstance(content_obj, dict) and isinstance(content_obj.get("modelCard"), dict):
                        model_card = content_obj["modelCard"]
                        print("模态卡内容 #1:")
                        print(json.dumps(model_card, ensure_ascii=False, indent=2))
                        if render_cards and render_model_card_to_image is not None:
                            safe_type = re.sub(r"[^a-zA-Z0-9_-]+", "_", content_type or "card")
                            image_path = output_dir / f"{safe_type}_{uuid.uuid4().hex[:12]}.png"
                            render_model_card_to_image(content_type, model_card, image_path)
                            print(f"🖼️ 模态卡图片：{image_path}（类型：{content_type}，内容 #1）")
                            rendered_image_records.append(
                                {
                                    "content_type": content_type,
                                    "card_index": 1,
                                    "image_path": str(image_path),
                                }
                            )
                            rendered_image_count += 1
                    else:
                        print("内容：")
                        print(json.dumps(content_obj, ensure_ascii=False, indent=2))
                except json.JSONDecodeError:
                    print(f"内容：{content_str}")
                except TypeError:
                    print(f"内容类型不支持解析：{type(content_str).__name__}")
                print()
            
            # 处理网页搜索结果
            elif content_type == "webpage":
                try:
                    web_data = json.loads(content_str) if isinstance(content_str, str) else content_str
                    value = web_data.get("value", [])
                    
                    if value:
                        print(f"📄 网页搜索结果（共 {len(value)} 条）")
                        print("=" * 60)
                        for i, result in enumerate(value, 1):
                            title = result.get("name", result.get("title", "无标题"))
                            url = result.get("url", "")
                            summary = result.get("summary", result.get("snippet", ""))
                            
                            print(f"{i}. {title}")
                            print(f"   链接：{url}")
                            if summary:
                                # 截断过长的摘要
                                if len(summary) > 300:
                                    summary = summary[:300] + "..."
                                print(f"   摘要：{summary}")
                            print("-" * 60)
                except json.JSONDecodeError:
                    print(f"网页数据解析失败：{content_str}")
                except (TypeError, AttributeError):
                    print(f"网页数据解析失败：不支持的内容类型 {type(content_str).__name__}")
            
            # 处理图片结果
            elif content_type == "image":
                try:
                    img_data = json.loads(content_str) if isinstance(content_str, str) else content_str
                    value = img_data.get("value", [])
                    
                    if value:
                        print(f"🖼️ 图片结果（共 {len(value)} 张）")
                        print("=" * 60)
                        for i, img in enumerate(value, 1):
                            thumb_url = img.get("thumbnailUrl", "")
                            host_url = img.get("hostPageUrl", "")
                            print(f"{i}. 缩略图：{thumb_url}")
                            if host_url:
                                print(f"   来源：{host_url}")
                        print("=" * 60)
                        print()
                except json.JSONDecodeError:
                    pass
                except (TypeError, AttributeError):
                    pass

        if render_cards and rendered_image_count > 0:
            print("提示：以下模态卡已生成图片，模型可直接按“文件路径 -> 卡片类型”引用：")
            for record in rendered_image_records:
                print(
                    f"- {record['image_path']} -> {record['content_type']} "
                    f"（模态卡内容 #{record['card_index']}）"
                )
            print(
                f"提示：本次共渲染 {rendered_image_count} 张模态卡图片。发送完成后，请删除目录 {output_dir} 下本次生成的图片文件。"
            )

    except httpx.HTTPStatusError as e:
        print(f"HTTP 错误：{e.response.status_code}", file=sys.stderr)
        print(f"响应内容：{e.response.text[:500]}", file=sys.stderr)
        sys.exit(1)
    except httpx.RequestError as e:
        print(f"请求错误：{e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"未知错误：{e}", file=sys.stderr)
        sys.exit(1)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="使用博查 AI 搜索 API 获取网页结果和模态卡",
        formatter_class=argparse.RawTextHelpFormatter,
        epilog=(
            "示例:\n"
            '  uv run skills/bocha-search/search.py "天空为什么是蓝色的"\n'
            '  uv run skills/bocha-search/search.py "北京天气"\n'
            '  uv run skills/bocha-search/search.py "人工智能最新进展" oneWeek 5 --cards-output-dir ./out/cards\n'
            '  uv run skills/bocha-search/search.py "天空为什么是蓝色的" --no-render-cards'
        ),
    )
    parser.add_argument("query", help="搜索关键词")
    parser.add_argument(
        "freshness",
        nargs="?",
        default="noLimit",
        help="时间范围：noLimit | oneDay | oneWeek | oneMonth | oneYear（默认 noLimit）",
    )
    parser.add_argument("count", nargs="?", type=int, default=10, help="结果数量：1-50（默认 10）")
    parser.add_argument(
        "--no-render-cards",
        action="store_false",
        dest="render_cards",
        help="关闭模态卡 PNG 渲染（默认开启）",
    )
    parser.set_defaults(render_cards=True)
    parser.add_argument(
        "--cards-output-dir",
        default=None,
        help="模态卡图片输出目录（默认当前会话 workspace 下的 bocha-cards）",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    valid_freshness = ["noLimit", "oneDay", "oneWeek", "oneMonth", "oneYear"]
    if args.freshness not in valid_freshness:
        print(f"错误：时间范围必须是 {valid_freshness} 之一", file=sys.stderr)
        sys.exit(1)
    if args.count < 1 or args.count > 50:
        print("错误：结果数量必须在 1-50 之间", file=sys.stderr)
        sys.exit(1)

    bocha_search(
        args.query,
        args.freshness,
        args.count,
        render_cards=args.render_cards,
        cards_output_dir=args.cards_output_dir,
    )
