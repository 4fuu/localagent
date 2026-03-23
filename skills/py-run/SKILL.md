---
name: py-run
description: 使用 uv 运行和管理 Python 脚本的完整指南。当用户需要运行 Python 脚本、管理脚本依赖、创建可重复的脚本、使用内联元数据声明依赖、锁定依赖版本、或使用 shebang 创建可执行脚本时使用此技能。包括无依赖运行、按需添加依赖、PEP 723 内联元数据、锁定依赖、提高可重复性等场景。
---

# py-run 技能

使用 uv 运行 Python 脚本，确保依赖管理无需手动创建虚拟环境。

## 核心概念

uv 会自动为你管理虚拟环境，推荐使用声明式方式管理依赖。

---

## 运行无依赖脚本

如果脚本没有依赖，直接用 `uv run`：

```bash
uv run example.py
```

脚本可以从 stdin 读取：

```bash
echo 'print("hello")' | uv run -
```

如果在有 `pyproject.toml` 的项目目录中运行，但脚本不依赖该项目，加 `--no-project` 标志：

```bash
uv run --no-project example.py
```

---

## 运行有依赖的脚本

### 方式一：按需添加依赖

使用 `--with` 选项临时添加依赖：

```bash
uv run --with rich example.py
uv run --with 'rich>12,<13' example.py
uv run --with requests --with rich example.py
```

### 方式二：内联元数据（推荐）

在脚本顶部添加 PEP 723 格式的内联元数据：

```python
# /// script
# dependencies = [
#   "requests<3",
#   "rich",
# ]
# ///

import requests
from rich.pretty import pprint
```

然后用 `uv add --script` 添加依赖：

```bash
uv add --script example.py 'requests<3' 'rich'
```

uv 会自动创建包含所需依赖的环境来运行脚本。

**注意**：使用内联元数据时，即使在项目中运行，也会忽略项目的依赖。`dependencies` 字段必须提供，即使为空。

---

## 指定 Python 版本

在内联元数据中指定：

```python
# /// script
# requires-python = ">=3.12"
# dependencies = []
# ///
```

或在运行时指定：

```bash
uv run --python 3.10 example.py
```

uv 会自动下载并使用所需的 Python 版本。

---

## 创建可执行脚本

添加 shebang 使脚本可直接执行：

```python
#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.12"
# dependencies = ["httpx"]
# ///

import httpx
print(httpx.get("https://example.com"))
```

然后：

```bash
chmod +x greet
./greet
```

---

## 锁定依赖

使用 `uv lock` 为脚本创建锁定文件：

```bash
uv lock --script example.py
```

这会创建 `example.py.lock` 文件。之后运行 `uv run --script` 等命令会复用锁定的依赖。

---

## 提高可重复性

在内联元数据中添加 `exclude-newer` 限制依赖版本日期：

```python
# /// script
# dependencies = ["requests"]
# [tool.uv]
# exclude-newer = "2023-10-16T00:00:00Z"
# ///
```

日期格式为 RFC 3339 时间戳。

---

## 使用替代包索引

```bash
uv add --index "https://example.com/simple" --script example.py 'requests'
```

---

## 运行 GUI 脚本

在 Windows 上，`.pyw` 扩展名的脚本会用 `pythonw` 运行：

```bash
uv run example.pyw
uv run --with PyQt5 example_pyqt.pyw
```

---

## 初始化新脚本

使用 `uv init --script` 创建带内联元数据的新脚本：

```bash
uv init --script example.py --python 3.12
```

---

## 最佳实践

1. **优先使用内联元数据**：将依赖声明写在脚本中，提高可移植性
2. **锁定依赖**：生产环境使用 `uv lock --script` 确保一致性
3. **指定 Python 版本**：避免版本兼容问题
4. **使用 shebang**：方便直接执行常用脚本
5. **按需添加依赖**：快速测试、使用时使用 `--with`，确定后写入内联元数据

---

## 常见场景

| 场景 | 命令 |
|------|------|
| 运行简单脚本 | `uv run script.py` |
| 临时添加依赖 | `uv run --with package script.py` |
| 声明依赖 | `uv add --script script.py package` |
| 锁定依赖 | `uv lock --script script.py` |
| 指定 Python 版本 | `uv run --python 3.10 script.py` |
| 创建可执行脚本 | 添加 shebang + `chmod +x` |
