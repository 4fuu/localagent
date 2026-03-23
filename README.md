# LocalAgent

**一个可以在你自己设备上运行的个人 AI 助手。**

> [English Version](README.en.md)

---

## ✨ 功能特性

- **多平台网关**：内置 CLI 和 Telegram 支持，可扩展接入其他平台
- **双 Agent 架构**：Main Agent 负责规划与记忆管理，Task Agent 负责具体任务执行
- **记忆系统**：统一归档记忆 + 向量检索，支持长期记忆管理与语义召回
- **技能系统**：可动态加载的技能模块，支持运行时扩展
- **沙箱执行**：基于 Podman 的容器隔离，Task Agent 的所有命令均在沙箱内执行
- **跨平台用户识别**：统一 `person_id` 映射，同一用户在不同平台共享 workspace 和画像
- **定时任务**：内置 Cron 调度，支持周期性自动执行
- **向量索引**：内置 zvec 向量数据库，支持语义搜索

## 🏗️ 架构概览

```
main.py  ──→  Runtime.init()
                ├── Hub（WebSocket 消息路由 + Agent 进程管理）
                ├── IndexService（SQLite manifest + Embedding 索引）
                ├── zvec server（向量数据库，py3.12 子进程）
                └── GatewayService（CLI / Telegram 网关适配）

任意进程:
  IndexClient ──┐
  VecClient  ──┐│
               ││
            ┌──▼▼──┐
            │  Hub  │   ws://127.0.0.1:9600
            └──┬┬──┘
               ││
  IndexService ┘│   index.* topics
  zvec server ──┘   vec.* topics
```

### 目录结构

| 目录 / 文件 | 职责 |
|---|---|
| `main.py` | 启动入口：初始化 Runtime，启动 Hub 和 IndexService |
| `src/hub/` | 基础设施：WebSocket Hub + 主进程生命周期 |
| `src/agent/` | Agent 系统：子进程入口、工具集、状态、提示词 |
| `src/provider/` | LLM 服务商抽象：BaseOpenAI、Qwen、tool 装饰器、Embedding |
| `src/index/` | 索引服务：IndexService、IndexClient、IndexSource |
| `src/vec/` | 向量数据库：zvec server、VecClient |
| `src/core/` | 领域模块：收件箱、记忆、任务、技能、统一用户、工作区 |
| `skills/` | 运行时：技能模块存储 |
| `.localagent/workspaces/` | 运行时：子代理操作空间（按用户/会话隔离，自动创建） |

依赖方向：`agent → provider → core/index → hub/vec`，各层单向无环。

## 🚀 快速开始

### 1. 安装 pixi

```bash
curl -fsSL https://pixi.sh/install.sh | bash
```

### 2. 安装依赖

```bash
pixi install
pixi run sandbox-build-image
```

### 3. 初始化配置

```bash
cp config.example.toml config.toml
```

然后按你的运行环境修改 `config.toml`，至少确认 `[agent]` 里选择了可用的 `chat` 和 `embedding` profile。

### 4. 设置环境变量

```bash
# 必需：LLM API Key
export DASHSCOPE_API_KEY="your-api-key"

# 可选：Env 工具加密密钥
export LOCALAGENT_SECRET_KEY="your-secret-key"
```

也可以在项目根目录创建 `.env` 文件存放敏感信息。

### 5. 启动

```bash
pixi run localagent_start
```

启动后 Hub 监听 `ws://127.0.0.1:9600`，Main Agent 在收到消息时按需拉起。

## ⚙️ 配置

LocalAgent 采用 `config.toml` + `.env` 分层配置，由 `src/config.py` 统一加载。

**优先级**：环境变量 > config.toml > 代码默认值

主要配置项：

| 配置块 | 说明 |
|---|---|
| `[hub]` | Hub 服务参数（端口、超时、进程池、维护阈值等） |
| `[provider.*]` | LLM 服务商连接信息（base_url、api_key_env） |
| `[chat.*]` | Chat 模型配置（引用 provider，可覆盖） |
| `[embedding.*]` | Embedding 模型配置 |
| `[agent]` | Agent 角色与模型映射 |
| `[identity.person.*]` | 跨平台用户账号绑定 |
| `[gateway]` | 网关配置（CLI / Telegram） |
| `[sandbox]` | 沙箱执行配置 |
| `[retry.*]` | 重试策略（全局默认 + 按服务覆盖） |

仓库内提供的是 [`config.example.toml`](config.example.toml)，请先复制为 `config.toml` 再修改。详细说明优先参考 `config.example.toml` 中的注释。

## 🧩 技能系统

LocalAgent 支持通过技能模块扩展能力。技能存放在 `skills/` 目录，运行时动态加载。

### 内置技能

| 技能 | 说明 |
|---|---|
| `bocha-search` | 博查搜索 |
| `exa-search` | Exa 搜索 |
| `gh` | GitHub 操作 |
| `py-run` | Python 代码执行 |
| `pdf` | PDF 文件处理 |
| `docx` | Word 文档处理 |
| `xlsx` | Excel 表格处理 |
| `pptx` | PowerPoint 处理 |
| `frontend-design` | 前端设计 |
| `skill-creator` | 技能创建器（用于创建新技能） |

## 🌐 网关

### CLI

内置命令行网关，启动后可通过 `cli.py` 直接对话：

```bash
pixi run --environment localagent python cli.py
```

### Telegram

配置 `[gateway.telegram]` 中的 `bot_token` 和允许的 chat ID 即可接入 Telegram Bot。支持：

- 私聊 / 群聊
- 群聊消息模式：`mention`、`command`、`all`、`mention_or_command`
- 用户黑名单 / 管理员白名单
- 静默模式（`silent`）：消息入库但不唤醒 Agent

## 🔒 沙箱

LocalAgent 使用 Podman 容器沙箱执行所有 Task Agent 的命令，提供进程隔离、只读 rootfs 和网络限制。

首次使用前需构建沙箱镜像（含 `uv`、`node`、Playwright）：

```bash
pixi run sandbox-build-image
```

可在 `config.toml` 中自定义沙箱参数：

```toml
[sandbox]
runtime = "podman"
image = "localhost/localagent-sandbox:latest"
pids_limit = 256
read_only_rootfs = true
```

## 🗺️ 路线图

- [x] 多平台网关（CLI + Telegram）
- [x] 双 Agent 架构
- [x] 统一归档记忆 + 向量检索
- [x] 技能系统
- [x] 沙箱执行
- [x] 定时任务
- [ ] 微信 ClawBot 网关支持
- [ ] 部分 OpenClaw 生态兼容
- [ ] Web 管理面板（计划中）

## 📄 许可证

[MIT](LICENSE)
