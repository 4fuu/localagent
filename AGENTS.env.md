# 环境变量与配置

LocalAgent 采用 **`config.toml` + `.env`** 分层配置，由 `src/config.py` 统一加载。

统一用户识别通过 `config.toml` 的 `[identity.person.*]` 显式账号绑定完成。`person_id` 用于跨平台共享 workspace、用户画像和默认 secrets；`conversation_id` 仍用于消息路由与会话状态。

## 用法

```python
from src.config import cfg

cfg.hub_host        # "127.0.0.1"
cfg.hub_port        # 9600
cfg.hub_reap_interval  # 5
cfg.hub_notify_delay  # 1
cfg.hub_main_inbox_batch_size  # 10
cfg.hub_main_per_conversation_limit  # 5
cfg.hub_prioritize_admin  # True
cfg.hub_pool_size_main  # 0（预热 main agent 进程池大小，0=关闭）
cfg.hub_pool_size_task  # 0（预热 task agent 进程池大小，0=关闭）
cfg.hub_startup_timeout  # 5
cfg.hub_shutdown_timeout  # 5
cfg.hub_agent_terminate_timeout  # 10
cfg.hub_pending_request_timeout  # 300
cfg.hub_db_scrub_interval  # 0 (秒，0=关闭)
cfg.hub_maintenance_enabled  # True
cfg.hub_maintenance_cooldown_seconds  # 120
cfg.hub_maintenance_silent_threshold  # 5
cfg.hub_maintenance_silent_max_wait_seconds  # 300
cfg.hub_maintenance_silent_threshold_delay_seconds  # 30

# Chat 配置（按角色解析，合并 provider）
chat = cfg.chat("main")     # main agent 的 chat 配置
chat = cfg.chat("task")     # task agent 的 chat 配置
chat["provider"]   # "qwen"
chat["profile"]    # "qwen-plus"
chat["model"]      # "qwen3.5-plus"
chat["base_url"]   # "https://dashscope.aliyuncs.com/compatible-mode/v1"
chat["api_key"]    # 从环境变量自动解析

# Embedding 配置（合并 provider）
emb = cfg.embedding()
emb["provider"]    # "qwen"
emb["profile"]     # "qwen-v4"
emb["model"]       # "text-embedding-v4"
emb["dimension"]   # 1024
emb["base_url"]    # "https://dashscope.aliyuncs.com/compatible-mode/v1"
emb["api_key"]     # 从环境变量自动解析

cfg.gateway         # {"active": ["cli"], "cli": {...}}
cfg.sandbox         # {"runtime": "podman", "image": "...", ...}
```

## 配置优先级

**环境变量 > config.toml > 代码默认值**

### Profile 字段解析顺序

chat / embedding 的每个字段按以下顺序解析：

**子配置 (chat/embedding profile) > 服务商 (provider) > 默认值**

例如 `base_url`：chat profile 中设置了则使用 profile 的值，否则继承 provider 的值。

## 仅环境变量（敏感信息）

API Key **只能** 通过环境变量或 `.env` 文件设置。每个 provider 通过 `api_key_env` 指定读取哪个环境变量，chat/embedding 子配置可覆盖。

| 环境变量 | 说明 | 默认关联 |
|---|---|---|
| `DASHSCOPE_API_KEY` | 阿里云 DashScope API Key | provider.qwen |
| `OPENAI_API_KEY` | OpenAI API Key | provider.openai |
| `LOCALAGENT_SECRET_KEY` | Env 工具加密密钥（用于加密存储用户环境变量） | Env 工具 |

自定义 provider 若未设置 `api_key_env`，默认读取 `{PROVIDER_NAME}_API_KEY`。

## config.toml 结构

```toml
[hub]
host = "127.0.0.1"          # Hub 监听地址
port = 9600                  # Hub 监听端口
reap_interval = 5            # 进程回收轮询间隔（秒）
notify_delay = 1             # 延迟通知（秒），0 = 立即；> 0 时合并等待期内的积压消息
main_inbox_batch_size = 10   # main 每轮最多处理多少条未处理 inbox
main_per_conversation_limit = 5 # 同一会话在单轮中的最大处理条数（0=不限制）
prioritize_admin = true      # 批处理时管理员消息优先
pool_size_main = 0           # main agent 预热进程池大小（0=关闭）
pool_size_task = 0           # task agent 预热进程池大小（0=关闭）
startup_timeout = 5          # Hub 启动等待超时（秒）
shutdown_timeout = 5         # Hub 关闭等待超时（秒）
agent_terminate_timeout = 10 # 停止 agent 进程时的等待超时（秒）
pending_request_timeout = 300 # Hub 内部 RPC 请求超时（秒），需 >= 客户端侧 timeout
db_scrub_interval = 0       # 内部 DB 脱敏清洗周期（秒），0 = 关闭；>0 时 Hub 后台直接执行
maintenance_enabled = true  # 启用基于阈值的 memory maintenance
maintenance_cooldown_seconds = 120 # maintenance 冷却时间（秒）
maintenance_l1_count_threshold = 8
maintenance_l2_count_threshold = 24
maintenance_l1_total_chars_threshold = 60000
maintenance_l2_total_chars_threshold = 180000
maintenance_silent_threshold = 5 # silent inbox 批处理数量阈值
maintenance_silent_max_wait_seconds = 300 # 未达到数量阈值时，最早一条 silent 最多等待多久后触发
maintenance_silent_threshold_delay_seconds = 30 # 达到数量阈值后，再额外等待多久触发

# ---- 服务商基础定义（只含连接信息）----
[provider.qwen]
base_url = "https://dashscope.aliyuncs.com/compatible-mode/v1"
api_key_env = "DASHSCOPE_API_KEY"   # 可选，默认 {NAME}_API_KEY

[provider.openai]
base_url = "https://api.openai.com/v1"
api_key_env = "OPENAI_API_KEY"

# ---- Chat 配置（引用 provider，可覆盖所有 provider 字段）----
[chat.qwen-plus]
provider = "qwen"
model = "qwen3.5-plus"

[chat.qwen-turbo]
provider = "qwen"
model = "qwen-turbo"

[chat.gpt4o]
provider = "openai"
model = "gpt-4o"
# base_url = "https://custom.endpoint/v1"  # 可覆盖 provider 的 base_url
# api_key_env = "CUSTOM_API_KEY"           # 可覆盖 provider 的 api_key_env

# ---- Embedding 配置（引用 provider，可覆盖所有 provider 字段）----
[embedding.qwen-v4]
provider = "qwen"
model = "text-embedding-v4"
dimension = 1024

[embedding.openai-small]
provider = "openai"
model = "text-embedding-3-small"
dimension = 1536

# ---- Agent 角色 → 配置映射 ----
[agent]
main_chat = "qwen-plus"         # main agent 使用的 chat 配置
task_chat = "qwen-plus"         # task agent 使用的 chat 配置（可与 main 不同）
embedding = "qwen-v4"           # 共享的 embedding 配置

# ---- Identity（跨平台统一用户）----
[identity.person.alice]
accounts = ["telegram:123456789", "cli:alice"]

# ---- Gateway ----
[gateway]
active = ["cli"]             # 启用的网关列表

[gateway.cli]
enabled = true
mailbox_dir = ""             # 空 = 使用默认路径 .localagent/gateway_cli

[gateway.telegram]
enabled = true
debug = false                # true 时跳过鉴权（允许所有 chat/user）
bot_token = ""                   # Set via env var LOCALAGENT_GATEWAY_TELEGRAM_BOT_TOKEN or here
poll_timeout = 25
drop_pending_updates = true
blocked_user_ids = []        # 全局用户黑名单
admin_user_ids = []          # 管理员用户 ID 列表
group_message_mode = "mention"  # all | mention | command | mention_or_command（群聊默认）
command_prefixes = ["/localagent"]

# 以 chat ID 为键，列出即允许接入
[gateway.telegram.chat."123456"]
# silent = true              # 静默模式：消息入库但不唤醒 agent
# blocked_user_ids = ["789"] # 本 chat 追加黑名单（默认 append）
# blocked_user_ids_mode = "override"  # 设为 override 时替换全局黑名单
# group_message_mode = "all" # 覆盖本 chat 的群聊消息模式

# ---- Sandbox（Task Agent 命令在容器内执行，需要 Podman）----
[sandbox]
runtime = "podman"
command = "podman"
# 如需在沙箱内运行 `uv` / `node` / `playwright`：
#   podman build -t localhost/localagent-sandbox:latest -f docker/sandbox.Dockerfile .
image = "localhost/localagent-sandbox:latest"
network = "slirp4netns"
pull = "missing"
read_only_rootfs = true
tmpfs = ["/tmp", "/var/tmp"]
pids_limit = 256
user_writable_skills = false  # true 时允许普通用户修改 skills 和 SOUL.md；默认仅管理员可写
```

## Identity 统一用户

- `identity.person.<person_id>.accounts` 使用 `"<gateway>:<external_user_id>"` 格式显式列出账号绑定。
- 未出现在 `accounts` 里的账号会自动回退为 `person_id = "<gateway>:<user_id>"`，不会和其他账号自动合并。
- 私聊/单用户会话默认复用 `person_id` 级 workspace；群聊/多人会话继续使用 `conversation_id` 级 workspace，避免多人互相污染。
- secrets 默认写到 `person` 作用域；如需只覆盖当前会话，可在 `manage_env(scope="conversation")` 使用会话级作用域。

当前没有为 identity 绑定提供环境变量覆盖，建议统一在 `config.toml` 中维护。

## config.toml 与环境变量覆盖对应关系

| config.toml 路径 | 对应环境变量 | 默认值 |
|---|---|---|
| `hub.host` | `LOCALAGENT_HUB_HOST` | `127.0.0.1` |
| `hub.port` | `LOCALAGENT_HUB_PORT` | `9600` |
| `hub.reap_interval` | `LOCALAGENT_HUB_REAP_INTERVAL` | `5` |
| `hub.notify_delay` | `LOCALAGENT_HUB_NOTIFY_DELAY` | `1` |
| `hub.main_inbox_batch_size` | `LOCALAGENT_HUB_MAIN_INBOX_BATCH_SIZE` | `10` |
| `hub.main_per_conversation_limit` | `LOCALAGENT_HUB_MAIN_PER_CONVERSATION_LIMIT` | `5` |
| `hub.prioritize_admin` | `LOCALAGENT_HUB_PRIORITIZE_ADMIN` | `true` |
| `hub.pool_size_main` | `LOCALAGENT_HUB_POOL_SIZE_MAIN` | `0`（关闭） |
| `hub.pool_size_task` | `LOCALAGENT_HUB_POOL_SIZE_TASK` | `0`（关闭） |
| `hub.startup_timeout` | `LOCALAGENT_HUB_STARTUP_TIMEOUT` | `5` |
| `hub.shutdown_timeout` | `LOCALAGENT_HUB_SHUTDOWN_TIMEOUT` | `5` |
| `hub.agent_terminate_timeout` | `LOCALAGENT_HUB_AGENT_TERMINATE_TIMEOUT` | `10` |
| `hub.pending_request_timeout` | `LOCALAGENT_HUB_PENDING_REQUEST_TIMEOUT` | `300` |
| `hub.db_scrub_interval` | `LOCALAGENT_HUB_DB_SCRUB_INTERVAL` | `0`（关闭） |
| `hub.maintenance_enabled` | `LOCALAGENT_HUB_MAINTENANCE_ENABLED` | `true` |
| `hub.maintenance_cooldown_seconds` | `LOCALAGENT_HUB_MAINTENANCE_COOLDOWN_SECONDS` | `120` |
| `hub.maintenance_l1_count_threshold` | `LOCALAGENT_HUB_MAINTENANCE_L1_COUNT_THRESHOLD` | `8` |
| `hub.maintenance_l2_count_threshold` | `LOCALAGENT_HUB_MAINTENANCE_L2_COUNT_THRESHOLD` | `24` |
| `hub.maintenance_l1_total_chars_threshold` | `LOCALAGENT_HUB_MAINTENANCE_L1_TOTAL_CHARS_THRESHOLD` | `60000` |
| `hub.maintenance_l2_total_chars_threshold` | `LOCALAGENT_HUB_MAINTENANCE_L2_TOTAL_CHARS_THRESHOLD` | `180000` |
| `hub.maintenance_silent_threshold` | `LOCALAGENT_HUB_MAINTENANCE_SILENT_THRESHOLD` | `5` |
| `hub.maintenance_silent_max_wait_seconds` | `LOCALAGENT_HUB_MAINTENANCE_SILENT_MAX_WAIT_SECONDS` | `300` |
| `hub.maintenance_silent_threshold_delay_seconds` | `LOCALAGENT_HUB_MAINTENANCE_SILENT_THRESHOLD_DELAY_SECONDS` | `30` |
| `agent.main_chat` | `LOCALAGENT_MAIN_CHAT` | 空 |
| `agent.task_chat` | `LOCALAGENT_TASK_CHAT` | 空 |
| `agent.embedding` | `LOCALAGENT_EMBEDDING` | 空 |
| `gateway.active` | `LOCALAGENT_GATEWAYS` | `cli` |
| `gateway.cli.enabled` | `LOCALAGENT_GATEWAY_CLI_ENABLED` | `false` |
| `gateway.cli.mailbox_dir` | `LOCALAGENT_GATEWAY_CLI_DIR` | `.localagent/gateway_cli` |
| `gateway.telegram.debug` | `LOCALAGENT_GATEWAY_TELEGRAM_DEBUG` | `false` |
| `sandbox.runtime` | `LOCALAGENT_SANDBOX_RUNTIME` | `podman` |
| `sandbox.command` | `LOCALAGENT_SANDBOX_COMMAND` | `podman` |
| `sandbox.image` | `LOCALAGENT_SANDBOX_IMAGE` | `localhost/localagent-sandbox:latest` |
| `sandbox.network` | `LOCALAGENT_SANDBOX_NETWORK` | `slirp4netns` |
| `sandbox.pull` | `LOCALAGENT_SANDBOX_PULL` | `missing` |
| `sandbox.read_only_rootfs` | `LOCALAGENT_SANDBOX_READ_ONLY_ROOTFS` | `true` |
| `sandbox.tmpfs` | `LOCALAGENT_SANDBOX_TMPFS` | `/tmp,/var/tmp` |
| `sandbox.pids_limit` | `LOCALAGENT_SANDBOX_PIDS_LIMIT` | `256` |
| `sandbox.user_writable_skills` | `LOCALAGENT_SANDBOX_USER_WRITABLE_SKILLS` | `false` |

---

```bash
podman build -t localhost/localagent-sandbox:latest -f docker/sandbox.Dockerfile .
```

这个镜像基于官方 Playwright Python 镜像，内置：
- `uv`
- `node` / `npm`
- Playwright Python 包与 Chromium 运行依赖

说明：
- `sandbox.image` 代码默认值仍是 `docker.io/library/python:3.12-bookworm`，方便最小化场景；若要跑上述工具，请显式改成 `localhost/localagent-sandbox:latest` 或你自己的等价镜像。
- `skills/bocha-search/render_card.py` 现在会优先使用项目根 `.playwright/`；如果该目录不存在，会自动回退到镜像内置浏览器。

## CLI 专用环境变量

| 环境变量 | 说明 | 默认值 |
|---|---|---|
| `LOCALAGENT_HUB_URL` | `cli.py` 连接的 Hub WebSocket 地址 | 从 `hub.host`/`hub.port` 拼接 |
| `USER` | `cli.py` 默认用户名 | `cli-user` |
