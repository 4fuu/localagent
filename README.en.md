# LocalAgent

> A personal AI assistant you can run on your own device.

*[中文版 README](README.md)*

## Features

- **Dual-agent architecture** — Main Agent handles memory and task dispatch; Task Agent executes and replies
- **Unified memory** — Archive-based memory with vector retrieval via zvec
- **Dynamic skills** — Plug-in skill modules loaded at runtime
- **Pluggable gateways** — CLI, Telegram, and extensible adapters
- **Cross-platform identity** — Unified user mapping across gateways
- **Sandbox isolation** — Podman-based container isolation; all Task Agent commands run inside the sandbox
- **WebSocket RPC hub** — Lightweight inter-process communication backbone

## Architecture

```
┌──────────────┐
│   Gateway    │  CLI / Telegram / ...
└──────┬───────┘
       │ inbound messages
┌──────▼───────┐
│     Hub      │  WebSocket RPC (ws://127.0.0.1:9600)
├──────────────┤
│  Main Agent  │  Memory management, task dispatch
│  Task Agent  │  Execution, replies, skill invocation
├──────────────┤
│ IndexService │  SQLite manifest + embedding
│  zvec Server │  Vector DB (py3.12 subprocess)
└──────────────┘
```

All singleton services register with the Hub and communicate via WebSocket RPC. Agents run as separate processes, spawned on demand.

## Quick Start

```bash
# 1. Install pixi (if not already installed)
curl -fsSL https://pixi.sh/install.sh | bash

# 2. Install dependencies
pixi install
pixi run sandbox-build-image

# 3. Initialize config
cp config.example.toml config.toml

# 4. Set required environment variables
export DASHSCOPE_API_KEY="your-dashscope-api-key"

# 5. Run
pixi run localagent_start
```

Then edit `config.toml` for your setup. At minimum, make sure `[agent]` points to valid `chat` and `embedding` profiles.

## Configuration

LocalAgent uses `config.toml` + `.env` for configuration, loaded by `src/config.py`.

**Priority:** environment variables > `config.toml` > defaults

Key sections in `config.toml`:

| Section | Purpose |
|---------|---------|
| `[hub]` | Hub host, port, agent limits, timers |
| `[provider.*]` | LLM provider connection info |
| `[chat.*]` | Chat model profiles (referenced by agents) |
| `[embedding.*]` | Embedding model profiles |
| `[agent]` | Maps agent roles to chat/embedding profiles |
| `[identity.person.*]` | Cross-platform account bindings |
| `[gateway]` | Active gateways and per-gateway settings |
| `[sandbox]` | Container isolation settings |

API keys are set exclusively via environment variables (e.g. `DASHSCOPE_API_KEY`, `OPENAI_API_KEY`).

The repository ships [`config.example.toml`](config.example.toml); copy it to `config.toml` first, then customize it. Use `config.example.toml` as the primary reference.

## Default Skills

| Skill | Description |
|-------|-------------|
| `bocha-search` | Web search with rich card rendering |
| `docx` | Word document handling |
| `pptx` | PowerPoint document handling |
| `xlsx` | Excel spreadsheet handling |
| `pdf` | PDF document parsing |
| `exa-search` | Exa semantic search |
| `frontend-design` | Frontend design assistance |
| `gh` | GitHub integration |
| `py-run` | Python code execution |
| `skill-creator` | Create new skills at runtime |

Skills are stored in `skills/` and loaded dynamically.

## Gateways

### CLI

Interactive terminal interface. Connects to the Hub via WebSocket.

```bash
pixi run --environment localagent python cli.py
```

### Telegram

Long-polling Telegram bot adapter. Configure in `config.toml`:

```toml
[gateway.telegram]
enabled = true
bot_token = "YOUR_BOT_TOKEN"
admin_user_ids = ["YOUR_USER_ID"]
```

Add allowed chats under `[gateway.telegram.chat.*]`.

## Sandbox

All Task Agent commands run inside a Podman container with restricted PIDs, read-only rootfs, and network isolation.

Build the sandbox image before first use:

```bash
pixi run sandbox-build-image
```

Customize sandbox settings in `config.toml`:

```toml
[sandbox]
runtime = "podman"
image = "localhost/localagent-sandbox:latest"
pids_limit = 256
```

## Roadmap

- **Web admin panel** — Dashboard, usage stats, config editor, and runtime control via browser (planned)

## License

[MIT](LICENSE)
