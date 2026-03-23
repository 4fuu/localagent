---
name: exa-search
description: 使用 Exa API 进行网页级检索与内容提取，擅长获取指定 URL 的正文/摘要/高亮，以及对海外网站（如 GitHub、Reddit、X/Twitter）有详尽的搜索索引，有此类需求优先使用此技能搜索。
---

# Exa 网络搜索技能

使用 Exa API 进行智能网络搜索的单文件脚本。支持搜索并获取内容摘要，以及从指定 URL 获取网页内容。脚本通过 PEP 723 内联元数据声明外部依赖，由 uv 自动管理运行环境。

## 前置条件

- 已设置环境变量 `EXA_API_KEY`
- 已安装 `uv` 工具（当前环境已经安装）

## 使用方法

这是一个单文件脚本，带有外部依赖（exa_py）。使用 `uv run` 运行，依赖通过 PEP 723 内联元数据自动管理：

### 搜索模式

```bash
uv run skills/exa-search/search.py search "搜索关键词" [结果数量]
```

### 获取内容模式

```bash
uv run skills/exa-search/search.py contents <URL1> [URL2 ...] [--text] [--summary] [--highlights <config>]
```

## 参数说明

### 搜索模式
- 第一个参数：`search`（模式标识）
- 第二个参数：搜索关键词（必需）
- 第三个参数：结果数量（可选，默认 10）

### 获取内容模式
- 第一个参数：`contents`（模式标识）
- 后续参数：URL 列表（至少一个）
- `--text`: 获取完整文本内容
- `--summary`: 获取摘要
- `--highlights <json>`: 获取高亮内容，需提供 JSON 配置，如 `'{\"max_characters\": 2000}'`

## 示例

### 搜索示例

```bash
# 基本搜索
uv run skills/exa-search/search.py search "React hooks best practices 2024"

# 指定结果数量
uv run skills/exa-search/search.py search "Python async programming" 5
```

### 获取内容示例

```bash
# 获取文本内容
uv run skills/exa-search/search.py contents "https://openai.com/research" --text

# 获取摘要
uv run skills/exa-search/search.py contents "https://stripe.com/docs/api" --summary

# 获取高亮内容
uv run skills/exa-search/search.py contents "https://arxiv.org/abs/2303.08774" --highlights '{"max_characters": 2000}'

# 同时获取多种内容
uv run skills/exa-search/search.py contents "https://example.com" --text --summary
```

## 输出格式

### 搜索结果
每条结果包含：
- 标题（title）
- 链接（url）
- 内容摘要（highlights）

### 获取内容结果
每条结果包含：
- URL
- 标题（title，如有）
- 文本内容（text，如请求）
- 摘要（summary，如请求）
- 高亮内容（highlights，如请求）

## 环境变量

- `EXA_API_KEY`: Exa API 密钥，从系统密钥存储中读取

## 依赖管理

脚本使用 PEP 723 内联元数据声明依赖，`uv run` 会自动管理：

```python
#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.10"
# dependencies = [
#   "exa_py",
# ]
# ///
```

无需手动创建虚拟环境或安装依赖，uv 会自动处理。

## API 参考

Exa API 支持的内容获取方式：

```python
# 获取文本
exa.get_contents(["https://openai.com/research"], text=True)

# 获取摘要
exa.get_contents(["https://stripe.com/docs/api"], summary=True)

# 获取高亮
exa.get_contents(["https://arxiv.org/abs/2303.08774"], highlights={"max_characters": 2000})
```
