# Development Guidelines

## Package Management

本项目使用 **pixi** 管理依赖和环境，具体查看配置文件[`pixi.toml`](./pixi.toml)。

### 环境配置

```bash
# 安装 pixi（如果尚未安装）
curl -fsSL https://pixi.sh/install.sh | bash

# 安装项目依赖
pixi run install-all
```

### 添加依赖

```bash
# 添加 Python 依赖（通过 PyPI）
pixi add --feature <feature-name> <package-name> --pypi

# 示例
pixi add --feature py314 pyee --pypi
```

### 运行命令

```bash
# 在指定环境中运行 Python 脚本
pixi run --environment localagent <command>

# 示例
pixi run --environment localagent python main.py
```

## Code Style

- 遵循 Python PEP 8 代码风格
- 保持代码简洁、可读性强
- 添加必要的类型注解
- 使用 `ruff` 进行代码检查：`pixi run --environment localagent ruff check src/`

## Architecture

### 目录结构

| 目录 / 文件       | 职责                                                         |
|-------------------|--------------------------------------------------------------|
| `main.py`         | **启动入口**：初始化 Runtime，启动 Hub、IndexService、GatewayService、RuntimeBrokerService |
| `src/hub/`        | 基础设施：WebSocket Hub (`hub.py`) + 主进程生命周期 (`runtime.py`) |
| `src/agent/`      | Agent 系统：子进程入口 (`sub.py`)、Main Agent 工具 (`main_tools.py`)、回复工具 (`reply_tools.py`)、Task 上下文工具 (`task_tools.py`)、通用工具 (`tools.py`)、状态 (`state.py`)、提示词 (`prompts.py`) |
| `src/provider/`   | LLM 服务商抽象：BaseOpenAI (`base.py`)、Qwen (`qwen.py`)、tool 装饰器 (`tool_decorator.py`)、Embedding (`embedding.py`) |
| `src/index/`      | 索引服务：IndexService (`service.py`)、IndexClient (`client.py`)、IndexSource (`source.py`) |
| `src/vec/`        | 向量数据库：zvec server (`server.py`)、VecClient (`client.py`)、启动脚本 (`launch.py`) |
| `src/core/`       | 领域模块：收件箱 (`inbox.py`)、记忆 (`memory.py`)、任务 (`task.py`)、技能 (`skills.py`)、统一用户 (`identity.py`)、工作区 (`workspace.py`) |
| `skills/`         | 运行时：技能模块存储                                         |
| `.localagent/workspaces/` | 运行时：子代理操作空间（按 person/conversation 隔离，自动创建） |

**依赖方向**：`agent → provider → core/index → hub/vec`，各层单向无环。

### 启动流程

```bash
# 1. 确保设置 API Key
export DASHSCOPE_API_KEY="your-api-key"

# 2. 启动主进程
pixi run install-all
pixi run localagent_start
```

主进程启动后：
1. `hub.runtime.init()` 启动 Hub（WebSocket 服务器，端口 9600）
2. 并行启动 IndexService（管理 SQLite manifest + embedding）、GatewayService（加载各 gateway 适配器）、RuntimeBrokerService（task 容器运行时）
3. IndexService 内部启动/连接 zvec server（py3.12 子进程，向量数据库）
4. Hub 启动后会先释放孤儿 claim、同步 inbox backlog，并尝试调度可运行的 Main Agent
5. Main Agent 不在启动阶段固定拉起；由 Hub 根据 `agent.wake` / `agent.task_done` / backlog / cron 等来源形成的 `conversation_work_queue` 按需认领和启动

### Hub + Service 模式

所有只能运行一份的服务都注册到 Hub，通过 WebSocket RPC 访问：

```
任意进程:
  IndexClient ──┐
  VecClient  ──┐│
               ││
            ┌──▼▼──┐
            │  Hub  │   ws://127.0.0.1:9600
            └──┬┬──┘
               ││
  IndexService ┘│   index.* topics (主进程线程)
  zvec server ──┤   vec.* topics   (py3.12 子进程)
  RuntimeBroker ┘   runtime.call   (task 容器运行时)
```

- **Hub**（`src/hub/hub.py`）：WebSocket 消息路由 + Agent 进程管理
- **IndexService**（`src/index/service.py`）：向量索引服务，管理 sqlite manifest + embedding + VecClient
- **IndexClient**（`src/index/client.py`）：轻量 RPC 客户端，任意进程可创建多个实例
- **zvec server**（`src/vec/server.py`）：向量存储，py3.12 子进程
- **VecClient**（`src/vec/client.py`）：向量存储 RPC 客户端，IndexService 内部使用
- **RuntimeBrokerService**（`src/runtime/broker.py`）：task runtime 容器代理，处理 `runtime.call`

### Agent 系统

**进程模型**：
- **Main Agent**：主助手，处理用户输入、记忆管理、任务调度
- **Task Agent**：子任务助手，执行特定任务，完成后通知 Main Agent

**启动方式（手动启动仅用于测试）**：
```bash
# Main Agent
python -m src.agent.sub --role=main --hub=ws://127.0.0.1:9600

# Task Agent
python -m src.agent.sub --role=task --hub=ws://127.0.0.1:9600 --task=t-abc12345
```

**工具系统**：
- `@tool` 装饰器（`src/provider/tool_decorator.py`）标记工具函数
- 工具函数签名中可注入 `state: AgentState` 参数
- Main Agent 工具：`Archive`, `Recall`, `Query`, `List`, `Task`, `Cron`, `Env`
- 通用工具：`Read`, `Write`, `Edit`, `Bash`, `SearchSkills`

### Core 模块

| 模块 | 职责 |
|------|------|
| `inbox.py` | InboxSource 索引源，解析 Markdown frontmatter |
| `memory.py` | 归档管道 `archive()`，将文件存入向量库 |
| `task.py` | TaskSource 索引源，任务创建/停止 |
| `skills.py` | SkillsSource 索引源，技能模块加载 |
| `identity.py` | 外部账号到内部 `person_id` 的统一映射 |
| `workspace.py` | 子代理工作区管理（私聊优先按 `person_id`，多人会话按 `conversation_id`） |

### 添加新服务

遵循 zvec / IndexService 的模式：

1. 创建 service 类，在后台线程中连接 Hub 并注册 topics
2. 创建 client 类，通过 Hub 发送 RPC 请求
3. 在 `hub/runtime.py` 的 `init()` 中启动 service，`shutdown()` 中关闭

### 添加新工具

```python
# src/agent/tools.py 或 src/agent/main_tools.py
from src.provider import tool
from .state import AgentState

@tool
def MyTool(state: AgentState, arg1: str, arg2: int = 10) -> str:
    """工具描述。"""
    # 通过 state.hub_url 访问 Hub
    # 通过 state.messages 访问对话历史
    return "结果"
```

### 主进程 vs 子进程

| | 主进程 | 子进程 |
|--|--------|--------|
| **启动** | `hub.runtime.init()` | 接收 `hub_url` 参数 |
| **访问索引** | `IndexClient(runtime.hub_url())` | `IndexClient(hub_url)` |
| **访问向量** | 通过 IndexService 内部 VecClient | 通过 IndexClient |
| **禁止** | — | 导入 `hub.runtime`、启动 Hub/IndexService |

## Runtime Data

### 环境变量与配置

详见 [`AGENTS.env.md`](./AGENTS.env.md)。配置通过 `src/config.py` 统一加载（`from src.config import cfg`），优先级：**环境变量 > config.toml > 代码默认值**。

### 数据存储

| 类型 | 位置 | 格式 |
|------|------|------|
| 向量库 | `.zvec/` | zvec Collection |
| Manifest | `.localagent/manifest.db` | SQLite |
| 收件箱 | `inbox/*.md` | Markdown + YAML frontmatter |
| 任务 | `task/*.md` | Markdown + YAML frontmatter |
| 记忆归档 | 向量库 + Manifest | SQLite + zvec |
| 技能 | `skills/` | 模块 |

### 统一用户与作用域

- `conversation_id` 继续承担消息路由、reply、conversation state、work queue。
- `person_id` 用于跨平台共享的长期资源：workspace、user profile、默认 secrets。
- 群聊/多人会话默认不复用 `person_id` workspace，仍按 `conversation_id` 隔离，避免多人共享一个执行目录。


## 时序图
```
---
config:
  theme: redux
  look: neo
  layout: elk
---
sequenceDiagram
    autonumber

    participant Boot as main.py
    participant Runtime as runtime.init()
    participant Hub as Hub
    participant GatewaySvc as GatewayService
    participant IndexSvc as IndexService
    participant Broker as RuntimeBrokerService
    participant Vec as zvec
    participant Ext as 外部平台/Console
    participant Store as Store(SQLite)
    participant Main as Main Agent(sub.py --role=main)
    participant Task as Task Agent(sub.py --role=task)
    participant Provider as Provider/BaseOpenAI
    participant IndexCli as IndexClient
    participant Ctr as Task Runtime Container

    rect rgb(245,245,245)
        Note over Boot,Vec: 1. 启动阶段
        Boot->>Runtime: init()
        Runtime->>Hub: start()
        par 独立服务并行启动
            Runtime->>IndexSvc: start()
            and
            Runtime->>GatewaySvc: start()
            and
            Runtime->>Broker: start()
        end
        IndexSvc->>Vec: connect/start
        Hub->>Hub: release_orphaned_main_claims()
        Hub->>Hub: sync_inbox_backlog()
        Hub->>Hub: schedule_main_agents()
        Hub->>Hub: 启动后台循环（expire/reap/cron/db_scrub/trace_cleanup）
    end

    loop Hub 周期 tick
        Hub->>Hub: expire pending requests
        Hub->>Hub: reap exited processes / release orphaned claims
        Hub->>Hub: drain queued task spawns / replenish pools
        Hub->>Hub: check cron jobs
        Hub->>Hub: maybe DB scrub / trace cleanup
    end

    rect rgb(235,245,255)
        Note over Ext,Store: 2. 外部消息进入系统
        Ext->>GatewaySvc: inbound message
        GatewaySvc->>Store: inbox_create(...)
        alt silent = false
            GatewaySvc->>Hub: event agent.wake(message)
            Hub->>Store: conversation_work_touch(conversation_id, ...)
            alt notify_delay > 0
                Hub->>Hub: arm notify timer
            else
                Hub->>Hub: _schedule_main_agents()
            end
        else silent = true
            GatewaySvc-->>GatewaySvc: 只入 inbox，不发 wake
        end
    end

    rect rgb(255,250,235)
        Note over Hub,Main: 3. Hub 认领 conversation work 并拉起 main
        Hub->>Store: conversation_work_claim(agent_key, lease, inbox_limit)
        Store-->>Hub: claim{conversation_id,inbox_ids,pending_task_ids,wake_mode}
        alt 认领成功且该会话没有运行中的 main
            Hub->>Hub: _build_startup_payload(wake_mode, source_topic, inbox_ids...)
            Hub->>Main: spawn/activate process(main)
        else 无可认领工作
            Hub-->>Hub: 等待下一次 wake / task_done / cron / reap
        end

        Main->>Hub: request hub.agent_startup_payload(role=main)
        Hub-->>Main: startup payload
        Main->>Store: 读取 prompt 所需状态
        Note over Main,Store: main 注入内容:\n- inbox_messages\n- pending_tasks\n- completed_tasks(仅 task_done / task_done_batch)\n- task_done_context(仅 task_done / task_done_batch)\n- conversation_state / recent_window\n- recall_items / user_profiles / conversation_env\n- runtime_paths / wake_context
        Main->>Provider: run(system_prompt, user_input_xml, images?)
    end

    rect rgb(245,255,245)
        Note over Main,Store: 4. main 运行：处理当前会话并决定是否派 task
        Main->>Store: 读取/更新 conversation_state、memory、user_profile、env
        alt 需要召回归档
            Main->>IndexCli: query/search archive
            IndexCli->>IndexSvc: query_entries/search
            IndexSvc-->>IndexCli: archive result
        end
        alt 需要派发任务
            Main->>Store: task_create(goal, task_type, memory_id, then, then_task_types, images)
            alt 绑定 context refs
                Main->>Store: 写 task_context_refs 文件
            end
            Main->>Hub: event agent.spawn(task_id)
        else 不需要 task
            Main-->>Main: 结束当前会话轮次
        end

        Main->>Store: conversation_work_finish(mark_inbox_processed=run_status==ok, consumed_task_ids=completed_task_ids)
        Main->>Store: upsert runtime_runs row
        Main->>Hub: event agent.main_done
    end

    rect rgb(255,245,245)
        Note over Hub,Task: 5. Hub 拉起 task
        Hub->>Hub: _handle_spawn(task_id)
        Hub->>Hub: _build_startup_payload(wake_mode=spawn, task_id)
        alt 当前有 task capacity
            Hub->>Task: spawn/activate process(task)
        else
            Hub->>Hub: queue pending task spawn
        end

        Task->>Hub: request hub.agent_startup_payload(role=task, task_id)
        Hub-->>Task: startup payload
        Task->>Store: task_read(task_id)
        Note over Task,Store: task 注入内容:\n- current task + context_ref_ids\n- conversation_state / recent_window\n- recall_items\n- runtime_paths\n- SOUL.md(user customization)\n- wake_context
        Task->>Provider: run(system_prompt, user_input_xml, images?)
    end

    rect rgb(250,240,255)
        Note over Task,Store: 6. task 执行
        alt task_type = reply
            Note over Task: 工具集:\nread_task / read_context_ref / send_reply
            Task->>Hub: request gateway.send (GatewayClient)
            Hub->>GatewaySvc: request gateway.send
            GatewaySvc->>Ext: outbound reply
        else task_type = execute
            Note over Task: 工具集:\nread_task / read_context_ref / read/write/apply_patch/bash/search_skills
        else task_type = general
            Note over Task: 工具集为 reply + execute 的并集
        end

        alt 需要读取大上下文
            Task->>Store: read runtime_task_context_refs binding
            Task->>Store: 读取 runtime_tool_refs(read_context_ref)
        end

        alt 需要执行文件/命令
            Task->>Hub: request runtime.call(read/write/bash/...)
            Hub->>Broker: route runtime.call
            Broker->>Ctr: 在容器内执行（/workspace, /skills, /config, /cache）
            Ctr-->>Broker: tool result
            Broker-->>Task: tool result
            Note over Task: 仅 task 做具体执行，main 不直接做
        end

        Task->>Store: task_complete(result)
        Note over Task,Store: task_complete 后还会更新 conversation_state / event / archive snapshot
        Task->>Store: upsert runtime_runs row
        alt 任务未被 stop
            Task->>Hub: event agent.task_done(task_id)
        else 已 stop
            Task-->>Hub: 不发 task_done
        end
    end

    rect rgb(240,255,255)
        Note over Hub,Main: 7. task_done 后续：链式任务或重新唤醒 main
        Hub->>Store: task_read(prev_task_id)
        alt task.then_chain 非空
            Hub->>Store: task_create(next_goal + 前置结果摘要, next_task_type, same memory_id/conversation_id)
            alt 有 task capacity
                Hub->>Task: 直接 spawn 下一步 task
            else
                Hub->>Hub: queue pending task spawn
            end
            Note over Hub,Task: then_chain 是 Hub 自动推进\n不必等 main 再决策
        else 无 then_chain
            Hub->>Store: conversation_work_touch(completed_task_id=task_id)
            Hub->>Hub: _schedule_main_agents()
        end
    end

    rect rgb(248,248,255)
        Note over Hub,Task: 8. cron 主线
        Hub->>Hub: 后台循环检查 cron.json
        alt cron 到期
            Hub->>Hub: _fire_cron()
            Hub->>Store: 创建 task（cron 任务）
            alt 有 task capacity
                Hub->>Task: spawn process(task, wake_mode=cron)
            else
                Hub->>Hub: queue pending task spawn
            end
        end
    end
```
