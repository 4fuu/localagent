"""Prompt construction for main/task agents."""
import json
import logging
from pathlib import Path
from typing import Any

from ..config import cfg
from ..core.identity import infer_person_id
from ..core.runtime_paths import RuntimePathMap
from ..core.secrets import conversation_scope, list_secrets, person_scope
from ..core.skills import skills_catalog
from ..core.store import Store, conversation_state_active_topic, conversation_state_topic_summaries
from ..index import IndexClient
from .context_refs import read_task_context_refs
from .dump_xml import XMLDumper, XMLDumpOptions, _pretty_xml

PROJECT_ROOT = Path(__file__).resolve().parents[2]
logger = logging.getLogger(__name__)
_TASK_TYPES = {"general", "reply", "execute"}


def _normalize_task_type(value: str) -> str:
    normalized = str(value or "").strip().lower()
    return normalized if normalized in _TASK_TYPES else ""

MAIN_SYSTEM = "你是 main agent，只负责调度，不直接对外回复，也不直接承担执行型工作。"


def _priority_item(
    field: str,
    priority: str,
    role: str,
    note: str = "",
) -> dict[str, str]:
    entry: dict[str, str] = {}
    entry["field"] = field
    entry["priority"] = priority
    entry["role"] = role
    if note:
        entry["note"] = note
    return entry


MAIN_ROLE_CONTRACT = {
    "objective": "只负责调度，不直接对外回复，也不直接承担执行型工作，有可能需要执行某些管理操作（manage_*开头的工具）或是需要厘清记忆再调度。",
    "completion_signal": "只输出一个 `[finished]` 即可，不输出其他内容。",
    "rules": [
        "所有回复、执行、澄清都通过 `manage_task` 分发；不要自己产出面向用户的回复正文。",
        "一次只围绕一个 focus 推进一个动作；单对象单动作，不得擅自扩题、并题、比较相邻对象。",
        "信息不足、指代不清、对象未确认、候选实体冲突时，优先分发澄清任务，不要代用户补全。",
        "稳定用户事实使用 `manage_user_profile`；临时推理结论才用 `add_note`；不要把会话状态写进 user profile。",
        "非管理员请求不得安排高风险副作用任务、系统级配置修改、越权读取或修改 bot 自身行为。",
        "涉及 API key、token、cookie、密码等敏感值时，直接调用 `manage_env`；默认写统一用户作用域，只有需要当前会话单独覆盖时才传 `scope=\"conversation\"`。",
    ],
}

MAIN_INPUT_MODEL = {
    "reading_rule": "读取 XML 输入时，优先按结构化字段理解，不要把整份 prompt 当普通长文本扫读。",
    "authoritative_sources": [
        _priority_item("inbox_messages", "highest", "当前轮用户输入"),
        _priority_item("completed_tasks", "high", "task_done / task_done_batch 下的最新任务结果；执行细节（facts/artifacts/constraints）的唯一来源"),
        _priority_item("task_done_context", "high", "判断是否已闭环交付、默认该结束还是继续", "仅在 task_done / task_done_batch 下提供"),
        _priority_item("conversation_state.current_topic", "high", "当前会话连续性的真源；只含 goal/status/replied，不含执行细节"),
        _priority_item("pending_tasks", "medium", "判断是否已有等价动作在进行中"),
        _priority_item("recent_window", "medium", "恢复最近线程状态；task_done 时已自动排除 completed_tasks 中的条目"),
        _priority_item("recall_items", "low", "补充候选证据，不负责决定当前 focus"),
        _priority_item("user_profiles", "low", "稳定、跨轮次可复用的用户事实"),
        _priority_item("conversation_env", "low", "只表示已保存的 key 名，不包含 value"),
        _priority_item("wake_context", "lowest", "辅助说明"),
    ],
    "topic_model": [
        "`current_topic` 只记录用户意图和进度指示器，不累积执行细节。",
        "字段：`id`, `goal`（创建后不可变）, `status`（active/blocked/done）, `replied`（bool，用户是否已收到回复）, `source_message_id`, `last_task_id`, `updated_at`。",
        "`status` 转换规则：execute/general 任务 blocked → topic blocked；active topic 在 reply/general 成功交付后自动收口为 done；reply 不负责写入执行细节。",
        "`replied=true` 表示用户已收到至少一次回复，但不代表目标已达成；是否闭环以 `status` 是否收口为 `done` 为准。",
        "执行过程中产出的 facts/artifacts/constraints 只存在于 `completed_tasks[*].outcome_json` 中，不会出现在 topic 里。",
    ],
    "evidence_rules": [
        "`conversation_state.current_topic` 是当前会话连续性的真源，不靠模型猜、不靠历史语义检索猜。",
        "任务执行细节（facts、artifacts、constraints）从 `completed_tasks[*].outcome_json` 读取，不要期望在 `current_topic` 中找到它们。",
        "`recent_window` 是最近几轮消息、任务、事件的确定性窗口。",
        "`recall_items` 只是补充候选证据，命中内容不代表已确认事实，不负责决定当前 focus。",
        "图片识别、网页卡片、搜索结果、转发/引用里的实体名都只是候选线索；除非用户明确确认，否则不得把它们写成已确认事实，也不得直接写死进 focus 或 task goal。",
        "不要把 recall、附件、metadata 或搜索命中的对象直接升级成 confirmed entity 或新 focus。",
    ],
    "conflict_resolution": [
        "当前轮 `inbox_messages` 与旧记录冲突时，优先当前轮。",
        "`wake_context` 与结构字段冲突时，优先结构字段。",
    ],
}

MAIN_DISPATCH_POLICY = {
    "manage_task_start": [
        "调用 `manage_task(start)` 时必须显式指定 `task_type` 与 `notify_main_on_finish`；若使用 `then`，也必须显式提供 `then_task_types`。",
        "`reply` 任务只能回复/确认/澄清/交付；`execute` 任务只能执行；`general` 只用于 goal 明确要求“执行后回复”的单步闭环，不能当默认兜底。",
        "goal 必须短、单步、可验收，不替 task 代写具体回复全文，也不要把整条工作流塞进一个 goal。",
        "仅当步骤稳定、顺序确定、每步输入输出可预期时才使用 `then`；存在关键不确定性时必须改为单步调度，等 task_done 后再决定下一步。",
        "`notify_main_on_finish=false` 只抑制成功完成后的回流（简单任务、无需后续的任务就选false）；若任务受阻、等待用户或被外部中止，系统仍会强制通知 main。",
        "优先检查 `pending_tasks` 和当前 state，避免重复创建等价任务。",
        "当前轮次 inbox 中的图片附件会自动注入到新 task；若需传递非当前轮次（如历史消息、之前 task）的图片，必须在 `images` 参数中显式传入 artifact 引用。非图片附件在 goal 中写明所需处理的运行时文件引用。",
        "遇到工具报错时：分发一个回复类任务告知用户报错信息（仅一次），不尝试修复，直接结束。",
        "“发送文件/图片给用户”本质是回复动作，必须分发 `reply` 或 `general` 任务（task 内使用 `send_reply`），不得分发 `execute` 任务。",
    ],
    "task_done_handling": [
        "先读取最新 task result、`completed_tasks`、`task_done_context` 和 `conversation_state`，再决定下一步。",
        "你的职责是验收结果，而不是重复上一轮调度。只在以下几类动作中选一个：结果已足够交付时分发 `reply` 或 `general`；结果表明还需下一步时分发新的单步任务；结果暴露关键信息缺失或对象未确认时分发澄清任务；结果证明当前 focus 不成立时回到 main 重新选择 focus，但不要擅自替换成候选对象。",
        "判断“用户是否已收到回复”时，以 `current_topic.replied` 和 `task_done_context.items[*].reply_sent` 为准，不要靠猜。",
        "若 `current_topic.replied=true` 或已有等价回复在 pending_tasks 中，禁止重复回复。",
        "不要因为 task result 里提到新的候选实体、链接或仓库，就自动把它升级成新的 focus 或 confirmed entity。",
        "若 task 明确失败、受阻或无结果（topic.status=blocked），必须把“失败/阻塞”当作当前状态的一部分处理；不要假装任务已完成。",
        "重点读取 `task_done_context.items[*]`；它会明确标注本轮是否有新的用户消息、刚完成的任务是否已经完成用户可见交付、系统建议的默认动作。",
        "若 `task_done_context.has_new_inbox_messages=false`，且所有完成任务的 `default_action` 都是 `finish`，默认直接结束；不要主动分发“再次发送”“补充说明”“确认是否收到”“提醒往上翻看”等后续任务，除非用户刚刚明确提出该要求，或 task result 明确显示发送失败。",
        "成功交付后的 active topic 会自动收口为 done；若 topic 已 blocked，则发送失败说明不会把它改写成 done。",
    ],
}

TASK_SYSTEM_TEMPLATE = "你是 task agent，当前 task_id={task_id}。"

TASK_ROLE_CONTRACT = {
    "objective": "只完成当前 task，不做主调度。",
    "task_focus": "执行时以 `task.goal` 和当前 `task` 字段为准；若 `conversation_state.current_topic` 与当前 task 语义不完全一致，不要被会话后续漂移带偏。",
    "rules": [
        "候选线索不能自动升级为确认事实；如果 goal 依赖的对象并未被明确确认，应输出阻塞点而不是擅自选择。",
        "只有关键动作真实完成后，才能写“已完成/已发送/已修改/已通知”；否则必须明确写失败、未完成或无法完成。",
        "遇到权限不足、对象不存在、连续报错、超时后仍无进展等情况，要明确记录阻塞点并结束，不得伪造成功。",
    ],
    "hints": [
        "需要运行 Python 时，优先考虑使用 `uv run` 而不是直接调用 `python` 或 `python3`。",
        "若不确定如何运行 Python 脚本、声明依赖或选择 `uv` 用法，可先查看 `skills/py-run/SKILL.md`。",
    ],
}

TASK_INPUT_MODEL = {
    "reading_rule": "读取 task XML 时按结构字段理解输入。",
    "authoritative_sources": [
        _priority_item("task.goal", "highest", "唯一任务边界"),
        _priority_item("task.task_type", "high", "决定可用工具和输出契约"),
        _priority_item("task.context_ref_ids", "high", "允许通过 `read_context_ref` 精确读取的附加上下文"),
        _priority_item("conversation_state.current_topic", "medium", "当前会话焦点（仅含 goal/status/replied），仅作执行参考"),
        _priority_item("recent_window", "medium", "最近几轮上下文，仅用于补证据"),
        _priority_item("recall_items", "low", "补充候选证据，不代表要重启旧话题"),
        _priority_item("user_customization", "low", "只影响回复风格"),
        _priority_item("wake_context", "lowest", "辅助说明，不是主任务描述"),
    ],
    "task_boundary": [
        "`task.goal` 是唯一任务边界。",
        "未写进 goal 的对象、文件、链接、指标、收件人不得自动加入执行范围。",
        "`task.topic_id` 存在时，它比 `conversation_state.current_topic` 更接近当前任务归属；后者仅作补充参考。",
        "`conversation_state.current_topic`（仅含 goal/status/replied，不含执行细节）、`recent_window`、`context_ref`、`recall_items` 都只是执行证据，不是额外需求。",
        "若某对象只出现在 recall、metadata 或搜索结果中而未在 goal 中确认，不得擅自把它当成已确认执行对象。",
        "`context_ref_ids` 非空时，优先 `read_context_ref` 按需读取。",
        "不要把 `wake_context` 当成主任务描述；主任务描述永远以 `task.goal` 为准。",
    ],
    "history_access_rules": [
        "如需补历史，只能通过 `search_conversation_history` 或 `read_context_ref` 读取；不要把它们当作开放式发散搜索入口。",
    ],
    "runtime_paths_usage": [
        "`runtime_paths.workspace_root` 是当前输出文件的默认落点；生成文件、脚本、草稿时优先放在这里。",
        "`runtime_paths.cwd` 是当前命令默认工作目录；相对路径默认相对于这里解析。",
        "`runtime_paths.readable_roots` 与 `runtime_paths.writable_roots` 是实际权限边界；不要自行推断管理员例外规则。",
        "`runtime_paths.soul_path` 存在时，它就是当前可编辑的人设/定制文件路径。",
    ],
}

MAIN_MODE_GUIDES: dict[str, list[str]] = {
    "wake": [
        "优先处理 `inbox_messages`，没有 live inbox 时不要凭 `wake_context` 重新推进旧消息。",
        "若请求只是保存或更新会话密钥，直接 `manage_env`，然后按需分发一个 `reply` 任务确认。",
        "若同一 `conversation_id` 下同时出现多条相邻 inbox，默认把它们视为同一个用户请求，合并理解。",
        "若 inbox metadata 中存在 `reply_to_message` 或 `forwarded_message`，必须一并读取。",
    ],
    "task_done": [
        "先验收 `completed_tasks` 与 `conversation_state`，再决定是否继续。",
        "优先参考 `task_done_context.items[*].default_action`；默认动作是 `finish` 时，不要硬造下一步。",
        "若 `current_topic.replied=true` 或已有等价 reply pending，禁止重复派发。",
    ],
    "task_done_batch": [
        "逐个验收 `completed_tasks`，不要把多个结果混成一个新 focus。",
        "优先参考 `task_done_context.items[*].default_action`。",
        "只对仍需推进的结果分发下一步。",
    ],
}

TASK_MODE_GUIDES: dict[str, list[str]] = {
    "spawn": [
        "只完成当前 task，不回头补做 main 的调度判断。",
    ],
    "wake": [
        "这是被直接唤起执行的 task；不要把 wake 文本当成额外需求。",
    ],
    "cron": [
        "优先完成当前定时目标，不扩展成新的会话任务链。",
    ],
}

TASK_TYPE_CONTRACTS: dict[str, dict[str, Any]] = {
    "reply": {
        "purpose": "只做回复、确认、澄清或交付。",
        "rules": [
            "用户可见动作只能通过 `send_reply` 完成。",
            "回复风格遵守 `user_customization` 中的人设指示。",
            "长内容拆分多次 `send_reply` 发送。",
            "群聊默认引用回复（传 `message_id`），私聊默认不引用。",
            "若 `context_ref` 已显示主流程或其他任务已经完成写入/保存，你只负责如实确认结果，不要把它误报成仍需主调度处理。",
            "若发现完成任务需要执行工具操作，应停止扩展，输出阻塞原因交给 main 重新分发。",
            "若 goal 涉及会话 secret 的存在性确认，可使用 `inspect_env` 读取键名。",
            "若 goal 涉及查看当前已登记的定时任务，可使用 `inspect_cron` 只读查询。",
        ],
        "result_requirements": [
            "必须说明：回复了什么；若未回复，为什么未回复。",
            "只有 `send_reply` 成功后，才能写“已回复”或“已发送”；不要把准备中的草稿写成已发送内容。",
            "若回复正文里带有“有问题再说”“没收到再发一次”之类的礼貌兜底，result 里只记录“已回复并请用户按需反馈”，不要把条件句写成后续待办。",
            "积极发送由前置步骤产生的各类不涉及隐私的展示性文件、图片。",
        ],
    },
    "execute": {
        "purpose": "只做执行动作，不对外回复。",
        "rules": [
            "可以使用文件或命令类工具完成 goal，但这些工具只用于执行 goal 本身。",
            "严禁使用 `bash_run`、`curl`、`python` 或 HTTP 请求直接调用 Telegram、Slack、Discord 等 bot 或 gateway API 来伪造对外回复或发送文件。",
            "若执行结果需要交付给用户，应在结果中清楚说明“已产出什么、位于哪里”，由 main 再分发 `reply` 或 `general` 任务完成对外发送。",
            "若 goal 本质是“发消息、发图片、发文件给用户”，应输出阻塞点，交给 main 改派。",
            "若 goal 涉及会话 secret 管理，应输出阻塞点交给 main 使用 `manage_env` 处理；不要手写 secret 文件。",
            "若 goal 涉及查看当前已登记的定时任务，可使用 `inspect_cron` 只读查询。",
        ],
        "result_requirements": [
            "必须说明：做了什么、依据了什么输入、用了哪些关键命令或步骤、产出了什么、结果在哪里。",
            "只能把真实完成的动作写成“已完成”；不得把“已生成文件”“已获得结果”写成“已通知用户”或“已发给用户”。",
            "若失败，说明失败点、已确认的现状、阻塞条件，以及限制或风险。",
        ],
    },
    "general": {
        "purpose": "只有 goal 明确要求时才同时执行和回复。",
        "rules": [
            "若 goal 涉及会话 secret 管理，应停止扩展，输出阻塞原因交给 main 使用 `manage_env` 处理；不要把密钥写进普通文件或回复。",
            "文件或命令类工具只用于执行 goal 本身，不得超出 goal 边界。",
            "执行完成后，用户可见交付必须走 `send_reply`。",
            "回复风格遵守 `user_customization` 中的人设指示。",
            "长内容拆分多次 `send_reply` 发送。",
            "群聊默认引用回复（传 `message_id`），私聊默认不引用。",
            "严禁使用 `bash_run`、`curl`、`python` 或 HTTP 请求直接调用 Telegram、Slack、Discord 等 bot 或 gateway API 来代替 `send_reply` 发送消息或文件。",
            "若 `context_ref` 已显示主流程或其他任务已经完成写入/保存，只负责如实确认结果，不要误报成仍需主调度处理。",
            "若 goal 涉及会话 secret 的存在性确认，可使用 `inspect_env` 读取键名。",
            "若 goal 涉及查看当前已登记的定时任务，可使用 `inspect_cron` 只读查询。",
        ],
        "result_requirements": [
            "必须同时说明：执行结果，以及实际发送给用户的内容。",
            "若执行失败或部分失败，回复内容也必须如实反映，不能伪装成功。",
            "只有 `send_reply` 成功后，才能写“已回复”或“已发送”；不要把草稿写成已发送内容。",
            "至少交代：做了什么、依据了什么输入、产出了什么结果、有哪些限制或风险。",
            "若发给用户的话里带有条件式兜底，如“没收到我再发一次”，result 里不要把它写成系统下一步计划。",
            "积极发送由前置步骤产生的各类不涉及隐私的展示性文件、图片。",
        ],
    },
}

TASK_OUTCOME_SCHEMA = {
    "required_tail_block": [
        "输出一句话结果后，必须追加一个单独的 ```json 代码块，作为机器可读的 `task outcome`。",
        "不要在该 JSON 代码块后再输出其他文本。",
    ],
    "json_schema": json.dumps(
        {
            "status": "completed | blocked | waiting_user | delivered | done",
            "summary": "一句话结果摘要",
            "facts": ["新增确认事实"],
            "constraints": ["新增约束"],
            "unknowns_open": ["仍未解决的问题"],
            "unknowns_resolved": ["本次已解决的问题"],
            "artifacts": ["文件路径、URL、产物位置、已发送内容摘要"],
            "next_action": "建议 main 的唯一下一步；没有则留空字符串",
            "delivery": "若已对用户交付，填写交付摘要；否则空字符串",
            "user_visible_delivery": False,
        },
        ensure_ascii=False,
        indent=2,
    ),
}


def _build_user_customization(soul_path: str = "", host_soul_path: str = "") -> str:
    source = host_soul_path.strip() or soul_path.strip()
    resolved = Path(source).resolve() if source else (PROJECT_ROOT / "SOUL.md")
    if not resolved.is_file():
        return ""
    try:
        return resolved.read_text(encoding="utf-8").strip()
    except Exception:
        logger.warning("Failed to read SOUL.md", exc_info=True)
        return ""


def _parse_startup_payload(payload: str) -> dict[str, str]:
    startup: dict[str, str] = {}
    in_startup = False
    for raw_line in payload.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line == "[startup]":
            in_startup = True
            continue
        if line.startswith("[") and line.endswith("]"):
            if in_startup:
                break
        if not in_startup or "=" not in line:
            continue
        key, value = line.split("=", 1)
        startup[key.strip()] = value.strip()
    return startup


def _parse_csv_startup_field(startup: dict[str, str], key: str) -> list[str]:
    value = startup.get(key, "").strip()
    if not value:
        return []
    seen: set[str] = set()
    result: list[str] = []
    for item in value.split(","):
        normalized = item.strip()
        if normalized and normalized not in seen:
            seen.add(normalized)
            result.append(normalized)
    return result


def _merge_prompt_sections(
    *sections: tuple[str, Any] | None,
) -> dict[str, Any]:
    merged: dict[str, Any] = {}
    for section in sections:
        if not section:
            continue
        key, value = section
        if value in ("", None, [], {}):
            continue
        merged[key] = value
    return merged


def _build_main_instruction_data(startup: dict[str, str]) -> dict[str, Any]:
    wake_mode = startup.get("wake_mode", "").strip()
    mode_rules = MAIN_MODE_GUIDES.get(wake_mode)
    wake_mode_policy = None
    if wake_mode or mode_rules:
        wake_mode_policy = {
            "wake_mode": wake_mode,
            "rules": mode_rules or [],
        }
    return _merge_prompt_sections(
        ("role_contract", MAIN_ROLE_CONTRACT),
        ("input_model", MAIN_INPUT_MODEL),
        ("dispatch_policy", MAIN_DISPATCH_POLICY),
        ("wake_mode_policy", wake_mode_policy),
    )


def _build_task_instruction_data(
    startup: dict[str, str],
    *,
    task_id: str,
    task_type: str,
) -> dict[str, Any]:
    wake_mode = startup.get("wake_mode", "").strip()
    mode_rules = TASK_MODE_GUIDES.get(wake_mode)
    wake_mode_policy = None
    if wake_mode or mode_rules:
        wake_mode_policy = {
            "wake_mode": wake_mode,
            "rules": mode_rules or [],
        }
    task_type_contract = None
    if task_type:
        task_type_contract = {
            "task_type": task_type,
            **TASK_TYPE_CONTRACTS.get(task_type, {}),
        }
    return _merge_prompt_sections(
        ("task_id", task_id or "unknown"),
        ("role_contract", TASK_ROLE_CONTRACT),
        ("input_model", TASK_INPUT_MODEL),
        ("task_outcome_schema", TASK_OUTCOME_SCHEMA),
        ("wake_mode_policy", wake_mode_policy),
        ("task_type_contract", task_type_contract),
    )


def _build_instruction_data(
    role: str,
    *,
    startup: dict[str, str],
    task_id: str = "",
) -> dict[str, Any]:
    if role == "main":
        return _build_main_instruction_data(startup)
    task_type = ""
    if task_id:
        with Store() as store:
            task = store.task_read(task_id)
        task_type = _normalize_task_type(str((task or {}).get("task_type", "")))
    return _build_task_instruction_data(
        startup,
        task_id=task_id,
        task_type=task_type,
    )


def _display_path(path_map: RuntimePathMap | None, value: str) -> str:
    raw = str(value or "").strip()
    if not raw or path_map is None:
        return raw
    try:
        return path_map.locator_to_runtime(raw)
    except Exception:
        return raw


def _display_attachments(
    attachments: list[dict[str, Any]],
    path_map: RuntimePathMap | None,
) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for item in attachments:
        if not isinstance(item, dict):
            continue
        entry = dict(item)
        if entry.get("file_path"):
            entry["file_path"] = _display_path(path_map, str(entry.get("file_path", "")))
        result.append(entry)
    return result


def _display_images(
    images: list[Any],
    path_map: RuntimePathMap | None,
) -> list[str]:
    return [
        converted
        for item in images
        if (converted := _display_path(path_map, str(item).strip()))
    ]


def _format_inbox(item: dict[str, Any], path_map: RuntimePathMap | None = None) -> dict[str, Any]:
    entry: dict[str, Any] = {}
    entry["id"] = item.get("id", "")
    entry["gateway"] = item.get("gateway", "")
    entry["conversation_id"] = item.get("conversation_id", "")
    entry["message_id"] = item.get("message_id", "")
    entry["user_id"] = item.get("user_id", "")
    entry["user_name"] = item.get("user_name", "")
    entry["is_admin"] = item.get("is_admin", False)
    entry["content"] = item.get("content", "")
    if item.get("attachments"):
        entry["attachments"] = _display_attachments(item.get("attachments", []), path_map)
    if item.get("metadata"):
        entry["metadata"] = item.get("metadata")
    entry["created_at"] = item.get("created_at", "")
    return entry


def _format_task(item: dict[str, Any], path_map: RuntimePathMap | None = None) -> dict[str, Any]:
    entry: dict[str, Any] = {}
    for key in (
        "id",
        "status",
        "task_type",
        "notify_main_on_finish",
        "goal",
        "topic_id",
        "gateway",
        "conversation_id",
        "user_id",
        "message_id",
        "reply_to_message_id",
        "parent_task_id",
        "memory_id",
        "result",
        "created_at",
        "updated_at",
    ):
        entry[key] = item.get(key, "")
    if item.get("images"):
        entry["images"] = _display_images(item.get("images", []), path_map)
    if item.get("outcome_json"):
        entry["outcome_json"] = item.get("outcome_json")
    if item.get("context_ref_ids"):
        entry["context_ref_ids"] = item.get("context_ref_ids")
    if item.get("then_chain"):
        entry["then_chain"] = item.get("then_chain")
    if item.get("then_task_types"):
        entry["then_task_types"] = item.get("then_task_types")
    return entry


def _format_state(item: dict[str, Any] | None) -> dict[str, Any]:
    source = item or {}
    current_topic = conversation_state_active_topic(source)
    entry: dict[str, Any] = {}
    entry["conversation_id"] = source.get("conversation_id", "")
    entry["version"] = source.get("version", 0)
    entry["gateway"] = source.get("gateway", "")
    entry["user_id"] = source.get("user_id", "")
    entry["active_topic_id"] = source.get("active_topic_id", "")
    entry["current_topic"] = current_topic or {}
    topic_summaries = conversation_state_topic_summaries(source)
    if topic_summaries:
        entry["topic_summaries"] = topic_summaries
    if source.get("session_constraints"):
        entry["session_constraints"] = source.get("session_constraints", [])
    if source.get("session_facts"):
        entry["session_facts"] = source.get("session_facts", [])
    entry["active_task_ids"] = source.get("active_task_ids", [])
    entry["last_user_message_id"] = source.get("last_user_message_id", "")
    entry["last_bot_message_id"] = source.get("last_bot_message_id", "")
    entry["recent_memory_ids"] = source.get("recent_memory_ids", [])
    entry["updated_at"] = source.get("updated_at", "")
    return entry


def _format_env_keys(keys: list[str]) -> dict[str, Any]:
    entry: dict[str, Any] = {}
    entry["keys"] = list(keys)
    entry["count"] = len(keys)
    return entry


def _format_runtime_paths(
    *,
    project_root: str,
    workspace_root: str,
    cwd: str,
    skills_root: str,
    soul_path: str,
    cache_root: str,
    workspace_scope: str,
    is_admin: bool,
    sandboxed: bool,
    containerized: bool,
) -> dict[str, Any]:
    entry: dict[str, Any] = {}
    entry["project_root"] = project_root
    entry["workspace_root"] = workspace_root
    entry["cwd"] = cwd
    entry["skills_root"] = skills_root
    entry["soul_path"] = soul_path
    if cache_root:
        entry["cache_root"] = cache_root
    entry["workspace_scope"] = workspace_scope
    entry["is_admin"] = is_admin
    entry["sandboxed"] = sandboxed
    entry["containerized"] = containerized
    if containerized:
        entry["readable_roots"] = [project_root, workspace_root, skills_root, soul_path]
        skills_writable = is_admin or bool(cfg.sandbox.get("user_writable_skills", False))
        writable = [workspace_root]
        if skills_writable:
            writable.extend([skills_root, soul_path])
        entry["writable_roots"] = writable
    elif not sandboxed:
        entry["readable_roots"] = [project_root]
        entry["writable_roots"] = [project_root]
    else:
        entry["readable_roots"] = [workspace_root, skills_root]
        entry["writable_roots"] = [workspace_root]
    return entry


def _format_enabled_skills(
    enabled_skill_slugs: list[str],
    *,
    role: str,
    host_skills_root: str,
    path_map: RuntimePathMap,
) -> list[dict[str, Any]]:
    normalized = {
        str(item).strip().lower().replace("_", "-")
        for item in enabled_skill_slugs
        if str(item).strip()
    }
    if not normalized:
        return []
    entries: list[dict[str, Any]] = []
    for item in skills_catalog(host_skills_root):
        slug = str(item.get("skill", "")).strip().lower().replace("_", "-")
        if not slug or slug not in normalized:
            continue
        entry: dict[str, Any] = {
            "name": str(item.get("name", "")).strip() or slug,
            "description": str(item.get("description", "")).strip(),
        }
        if role == "task":
            skill_path = str(item.get("path", "")).strip()
            entry["skill"] = slug
            if skill_path:
                entry["path"] = path_map.locator_to_runtime(skill_path)
        entries.append(entry)
    return entries


def _format_event(item: dict[str, Any]) -> dict[str, Any]:
    entry: dict[str, Any] = {}
    entry["id"] = item.get("id", "")
    entry["event_type"] = item.get("event_type", "")
    entry["payload"] = item.get("payload", {})
    entry["created_at"] = item.get("created_at", "")
    return entry


def _format_recall_item(item: dict[str, Any]) -> dict[str, Any]:
    entry: dict[str, Any] = {}
    entry["id"] = item.get("id", "")
    entry["score"] = item.get("score", 0.0)
    entry["source"] = item.get("source", "")
    entry["label"] = item.get("label", "")
    entry["text"] = item.get("text", "")
    if item.get("metadata"):
        entry["metadata"] = item.get("metadata")
    return entry


def _build_task_done_context(
    *,
    store: Store,
    conversation_id: str,
    completed_task_ids: list[str],
    inbox_items: list[dict[str, Any]],
) -> dict[str, Any] | None:
    normalized_conversation_id = conversation_id.strip()
    normalized_task_ids = [str(item).strip() for item in completed_task_ids if str(item).strip()]
    if not normalized_conversation_id or not normalized_task_ids:
        return None

    events = store.conversation_event_list(normalized_conversation_id, limit=20)
    context: dict[str, Any] = {}
    context["has_new_inbox_messages"] = bool(inbox_items)
    context["completed_task_count"] = len(normalized_task_ids)
    items: list[dict[str, Any]] = []

    for task_id in normalized_task_ids:
        task = store.task_read(task_id)
        if not task:
            continue
        task_type = str(task.get("task_type", "")).strip().lower()
        reply_events = [
            event
            for event in events
            if str(event.get("event_type", "")).strip() == "reply_sent"
            and str(((event.get("payload", {}) or {}).get("task_id", ""))).strip() == task_id
        ]
        reply_message_ids = [
            str((event.get("payload", {}) or {}).get("message_id", "")).strip()
            for event in reply_events
            if str((event.get("payload", {}) or {}).get("message_id", "")).strip()
        ]
        delivery_completed = (
            str(task.get("status", "")).strip().lower() == "done"
            and task_type in {"reply", "general"}
            and bool(reply_events)
            and not list(task.get("then_chain", []) or [])
        )

        item: dict[str, Any] = {}
        item["task_id"] = task_id
        item["task_type"] = task_type
        item["status"] = task.get("status", "")
        item["reply_sent"] = bool(reply_events)
        if reply_message_ids:
            item["reply_message_ids"] = reply_message_ids
        item["user_visible_delivery_completed"] = delivery_completed
        item["default_action"] = (
            "finish"
            if delivery_completed and not inbox_items
            else "inspect"
        )
        items.append(item)

    if not items:
        return None
    context["items"] = items
    return context


def _compact_wake_context(payload: str) -> str:
    lines = [line.rstrip() for line in payload.splitlines() if line.strip()]
    if not lines:
        return ""
    return "\n".join(lines[:40])


def _build_recall_query(
    *,
    inbox_messages: list[dict[str, Any]],
    conversation_state: dict[str, Any] | None,
    task_data: dict[str, Any] | None,
) -> str:
    parts: list[str] = []
    if task_data:
        parts.append(str(task_data.get("goal", "")).strip())
    current_topic = conversation_state_active_topic(conversation_state)
    focus = str((current_topic or {}).get("goal", "")).strip()
    if focus:
        parts.append(focus)
    for inbox in inbox_messages[:2]:
        content = str(inbox.get("content", "")).strip()
        if content:
            parts.append(content[:300])
    return "\n".join(part for part in parts if part).strip()


def _task_recall_limit(task_data: dict[str, Any] | None) -> int:
    if not task_data:
        return 0
    context_ref_ids = task_data.get("context_ref_ids", []) or []
    if context_ref_ids:
        return 0
    task_type = _normalize_task_type(str(task_data.get("task_type", "")).strip())
    if task_type == "reply":
        return 0
    if task_type in {"execute", "general"}:
        return 2
    return 0


def _recall_archive_items(
    *,
    hub_url: str,
    query: str,
    conversation_id: str,
    topic_id: str = "",
    limit: int = 6,
) -> list[dict[str, Any]]:
    if not hub_url or not query.strip() or not conversation_id.strip():
        return []
    client = IndexClient(hub_url)
    try:
        results = client.search(query.strip(), topk=max(1, min(int(limit), 10)))
    except Exception:
        logger.warning("Archive recall failed", exc_info=True)
        return []
    finally:
        client.close()

    exact_items: list[dict[str, Any]] = []
    fallback_items: list[dict[str, Any]] = []
    normalized_topic_id = topic_id.strip()
    for result in results:
        fields = result.get("fields", {}) or {}
        metadata = fields.get("metadata", {})
        if isinstance(metadata, str):
            try:
                metadata = json.loads(metadata)
            except Exception:
                metadata = {"raw": metadata}
        candidate_conversation_id = str(metadata.get("conversation_id", "")).strip()
        if candidate_conversation_id != conversation_id:
            continue
        candidate_topic_id = str(metadata.get("topic_id", "")).strip()
        formatted = _format_recall_item({
            "id": result.get("id", ""),
            "score": result.get("score", 0.0),
            "source": fields.get("source", ""),
            "label": fields.get("label", ""),
            "text": str(fields.get("text", ""))[:500],
            "metadata": metadata,
        })
        if normalized_topic_id and candidate_topic_id == normalized_topic_id:
            exact_items.append(formatted)
        elif not normalized_topic_id:
            exact_items.append(formatted)
        else:
            fallback_items.append(formatted)
    return exact_items or fallback_items


def _extract_conversation_id(
    *,
    startup: dict[str, str],
    inbox_messages: list[dict[str, Any]],
    task_data: dict[str, Any] | None,
) -> str:
    explicit = startup.get("conversation_id", "").strip()
    if explicit:
        return explicit
    if task_data:
        task_conversation_id = str(task_data.get("conversation_id", "")).strip()
        if task_conversation_id:
            return task_conversation_id
    unique_conversation_ids = {
        str(inbox.get("conversation_id", "")).strip()
        for inbox in inbox_messages
        if str(inbox.get("conversation_id", "")).strip()
    }
    if len(unique_conversation_ids) == 1:
        return next(iter(unique_conversation_ids))
    return ""


def _build_prompt_data(
    role: str,
    *,
    hub_url: str = "",
    task_id: str = "",
    inbox_ids: list[str] | None = None,
    payload: str = "",
    project_root: str = "",
    workspace_root: str = "",
    cwd: str = "",
    skills_root: str = "",
    soul_path: str = "",
    cache_root: str = "",
    host_project_root: str = "",
    host_workspace_root: str = "",
    host_skills_root: str = "",
    host_soul_path: str = "",
    workspace_scope: str = "",
    is_admin: bool = False,
    sandboxed: bool = True,
    containerized: bool = False,
) -> dict[str, object]:
    startup = _parse_startup_payload(payload)
    inbox_items: list[dict[str, Any]] = []
    path_map = RuntimePathMap.from_values(
        host_project_root=host_project_root or project_root,
        host_workspace_root=host_workspace_root or workspace_root,
        host_skills_root=host_skills_root or skills_root,
        host_soul_path=host_soul_path or soul_path or str(PROJECT_ROOT / "SOUL.md"),
        runtime_project_root=project_root,
        runtime_workspace_root=workspace_root,
        runtime_skills_root=skills_root,
        runtime_soul_path=soul_path or str(PROJECT_ROOT / "SOUL.md"),
        runtime_cache_root=cache_root,
    )

    with Store() as store:
        for inbox_id in inbox_ids or []:
            inbox = store.inbox_read(inbox_id)
            if inbox is not None:
                inbox_items.append(inbox)

        task_data = store.task_read(task_id) if task_id else None
        if task_data and role == "task":
            task_data = dict(task_data)
            task_data["context_ref_ids"] = read_task_context_refs(task_id)

        conversation_id = _extract_conversation_id(
            startup=startup,
            inbox_messages=inbox_items,
            task_data=task_data,
        )
        conversation_state = store.conversation_state_read(conversation_id) if conversation_id else None
        recent_window = store.conversation_recent_window(conversation_id) if conversation_id else {
            "inbox_messages": [],
            "tasks": [],
            "events": [],
        }
        pending_tasks = []
        completed_tasks = []
        task_done_context = None
        if role == "main" and conversation_id:
            pending_tasks = [
                _format_task(task, path_map)
                for task in store.conversation_recent_tasks(conversation_id, limit=10)
                if str(task.get("status", "")).strip() == "pending"
            ]
            completed_task_ids = _parse_csv_startup_field(startup, "completed_task_ids")
            for completed_task_id in completed_task_ids:
                completed_task = store.task_read(completed_task_id)
                if completed_task is not None:
                    completed_tasks.append(_format_task(completed_task, path_map))
            task_done_context = _build_task_done_context(
                store=store,
                conversation_id=conversation_id,
                completed_task_ids=completed_task_ids,
                inbox_items=inbox_items,
            )

        formatted_task = _format_task(task_data, path_map) if task_data else None
        if role == "task" and formatted_task is not None:
            formatted_task.pop("then_chain", None)
            formatted_task.pop("then_task_types", None)

        recall_query = _build_recall_query(
            inbox_messages=inbox_items,
            conversation_state=conversation_state,
            task_data=task_data,
        )
        current_topic_id = ""
        if task_data:
            current_topic_id = str(task_data.get("topic_id", "")).strip()
        if not current_topic_id:
            current_topic_id = str((conversation_state_active_topic(conversation_state) or {}).get("id", "")).strip()
        recall_items: list[dict[str, Any]] = []
        recall_limit = 6 if role == "main" else _task_recall_limit(task_data)
        if recall_limit > 0:
            recall_items = _recall_archive_items(
                hub_url=hub_url,
                query=recall_query,
                conversation_id=conversation_id,
                topic_id=current_topic_id,
                limit=recall_limit,
            )

        target_persons: list[tuple[str, str, str, str]] = []
        if inbox_items:
            seen_pairs: set[tuple[str, str, str, str]] = set()
            for inbox in inbox_items:
                metadata = inbox.get("metadata", {}) if isinstance(inbox.get("metadata", {}), dict) else {}
                gateway = str(inbox.get("gateway", "")).strip()
                user_id = str(inbox.get("user_id", "")).strip()
                conversation_scope_id = str(inbox.get("conversation_id", "")).strip()
                person_id = (
                    str(inbox.get("person_id", "")).strip()
                    or str(metadata.get("person_id", "")).strip()
                    or infer_person_id(gateway=gateway, user_id=user_id)
                )
                pair = (person_id, gateway, user_id, conversation_scope_id)
                if person_id and pair not in seen_pairs:
                    seen_pairs.add(pair)
                    target_persons.append(pair)
        elif conversation_state:
            gateway = str(conversation_state.get("gateway", "")).strip()
            user_id = str(conversation_state.get("user_id", "")).strip()
            person_id = (
                str(conversation_state.get("person_id", "")).strip()
                or infer_person_id(gateway=gateway, user_id=user_id)
            )
            if person_id:
                target_persons.append((person_id, gateway, user_id, conversation_id))

        user_profiles: list[dict[str, Any]] = []
        seen_profile_ids: set[str] = set()
        for person_id, gateway, user_id, current_conversation_id in target_persons:
            if gateway and user_id:
                store.user_profile_bind_person(
                    gateway=gateway,
                    user_id=user_id,
                    person_id=person_id,
                )
            for candidate in store.user_profile_list(
                person_id=person_id,
                conversation_id="",
                limit=100,
            ):
                profile_id = str(candidate.get("id", ""))
                if profile_id and profile_id not in seen_profile_ids:
                    seen_profile_ids.add(profile_id)
                    user_profiles.append(dict(candidate))
            if current_conversation_id:
                for candidate in store.user_profile_list(
                    person_id=person_id,
                    conversation_id=current_conversation_id,
                    limit=100,
                ):
                    profile_id = str(candidate.get("id", ""))
                    if profile_id and profile_id not in seen_profile_ids:
                        seen_profile_ids.add(profile_id)
                        user_profiles.append(dict(candidate))

        conversation_env_keys: list[str] = []
        enabled_skill_slugs = store.enabled_skills_read()
        person_id = ""
        if conversation_state:
            person_id = str(conversation_state.get("person_id", "")).strip()
        if not person_id and target_persons:
            person_id = target_persons[0][0]
        if person_id or conversation_id:
            try:
                seen_env_keys: set[str] = set()
                for scope in [person_scope(person_id), conversation_scope(conversation_id)]:
                    if not scope:
                        continue
                    for item in list_secrets(scope):
                        if item in seen_env_keys:
                            continue
                        seen_env_keys.add(item)
                        conversation_env_keys.append(item)
            except Exception:
                logger.warning("Failed to read conversation env keys", exc_info=True)

    data: dict[str, object] = {}
    if inbox_items:
        data["inbox_messages"] = [_format_inbox(item, path_map) for item in inbox_items]
    if completed_tasks:
        data["completed_tasks"] = completed_tasks
    if task_done_context:
        data["task_done_context"] = task_done_context
    if formatted_task is not None:
        data["task"] = formatted_task
    data["conversation_state"] = _format_state(conversation_state)
    if pending_tasks:
        data["pending_tasks"] = pending_tasks
    completed_task_id_set = {str(t.get("id", "")).strip() for t in completed_tasks} if completed_tasks else set()
    data["recent_window"] = {
        "inbox_messages": [_format_inbox(item, path_map) for item in recent_window.get("inbox_messages", [])],
        "tasks": [
            _format_task(item, path_map)
            for item in recent_window.get("tasks", [])
            if str(item.get("id", "")).strip() not in completed_task_id_set
        ],
        "events": [
            _format_event(item)
            for item in recent_window.get("events", [])
            if not (
                str(item.get("event_type", "")).strip() == "task_completed"
                and str((item.get("payload", {}) or {}).get("task_id", "")).strip() in completed_task_id_set
            )
        ],
    }
    if recall_items:
        data["recall_items"] = recall_items
    if user_profiles:
        data["user_profiles"] = user_profiles
    if conversation_env_keys:
        data["conversation_env"] = _format_env_keys(conversation_env_keys)
    enabled_skills = _format_enabled_skills(
        enabled_skill_slugs,
        role=role,
        host_skills_root=host_skills_root or skills_root,
        path_map=path_map,
    )
    data["enabled_skills"] = enabled_skills
    data["runtime_paths"] = _format_runtime_paths(
        project_root=project_root,
        workspace_root=workspace_root,
        cwd=cwd,
        skills_root=skills_root,
        soul_path=soul_path,
        cache_root=cache_root,
        workspace_scope=workspace_scope,
        is_admin=is_admin,
        sandboxed=sandboxed,
        containerized=containerized,
    )
    if role == "task":
        user_customization = _build_user_customization(
            soul_path=soul_path,
            host_soul_path=host_soul_path,
        )
        if user_customization:
            data["user_customization"] = user_customization
    wake_context = _compact_wake_context(payload)
    if wake_context:
        data["wake_context"] = wake_context
    return data


def _build_prompt_reading_guide(role: str) -> str:
    common_lines = [
        "Prompt Reading Guide",
        "- 先读这段纯文本引导，再读后面的 XML。",
        "- XML 中的运行时字段名尽量与系统内部保持一致；像 `goal`、`task_type`、`then_task_types`、`context_ref_ids`、`reply_to_message_id`、`wake_mode` 都沿用系统原名。",
        "- `instructions.role_contract` 表示角色边界与完成信号。",
        "- `instructions.input_model` 表示字段优先级、候选证据规则，以及冲突时该信谁。",
        "- `instructions.wake_mode_policy` 表示当前 `wake_mode` 下的附加规则。",
    ]
    if role == "main":
        lines = [
            *common_lines,
            "- `instructions.dispatch_policy.manage_task_start` 对应 `manage_task(action=\"start\")` 的调度规则。",
            "- `instructions.dispatch_policy.task_done_handling` 对应 `completed_tasks` 与 `task_done_context` 的验收规则。",
            "- main 的推荐阅读顺序：`inbox_messages` -> `completed_tasks` / `task_done_context` -> `conversation_state.current_topic` -> `pending_tasks` -> `recent_window` -> `recall_items` -> `enabled_skills` -> `user_profiles` -> `conversation_env` -> `runtime_paths` -> `wake_context`。",
            "- 下面开始是结构化 XML。",
        ]
        return "\n".join(lines)
    lines = [
        *common_lines,
        "- `instructions.task_type_contract.task_type` 与 `task.task_type` 一一对应；其中给出该 task type 的能力边界和结果要求。",
        "- `instructions.task_outcome_schema` 规定 result 末尾 JSON 代码块的结构。",
        "- task 的推荐阅读顺序：`task.goal` -> `task.task_type` -> `task.context_ref_ids` -> `conversation_state.current_topic` -> `recent_window` -> `recall_items` -> `enabled_skills` -> `runtime_paths` -> `user_customization` -> `wake_context`。",
        "- 下面开始是结构化 XML。",
    ]
    return "\n".join(lines)


def build_role_prompt(
    role: str,
    *,
    hub_url: str = "",
    task_id: str = "",
    inbox_ids: list[str] | None = None,
    payload: str = "",
    project_root: str = "",
    workspace_root: str = "",
    cwd: str = "",
    skills_root: str = "",
    soul_path: str = "",
    cache_root: str = "",
    host_project_root: str = "",
    host_workspace_root: str = "",
    host_skills_root: str = "",
    host_soul_path: str = "",
    workspace_scope: str = "",
    is_admin: bool = False,
    sandboxed: bool = True,
    containerized: bool = False,
) -> str:
    startup = _parse_startup_payload(payload)
    context_data = _build_prompt_data(
        role,
        hub_url=hub_url,
        task_id=task_id,
        inbox_ids=inbox_ids,
        payload=payload,
        project_root=project_root,
        workspace_root=workspace_root,
        cwd=cwd,
        skills_root=skills_root,
        soul_path=soul_path,
        cache_root=cache_root,
        host_project_root=host_project_root,
        host_workspace_root=host_workspace_root,
        host_skills_root=host_skills_root,
        host_soul_path=host_soul_path,
        workspace_scope=workspace_scope,
        is_admin=is_admin,
        sandboxed=sandboxed,
        containerized=containerized,
    )
    data: dict[str, Any] = {}
    data["instructions"] = _build_instruction_data(
        role,
        startup=startup,
        task_id=task_id,
    )
    data.update(context_data)
    opts = XMLDumpOptions(root_tag="prompt")
    dumper = XMLDumper(opts)
    root = dumper.dump_to_element(data)
    guide = _build_prompt_reading_guide(role)
    return f"{guide}\n\n{_pretty_xml(root)}"


def build_role_messages(
    role: str,
    *,
    hub_url: str = "",
    task_id: str = "",
    inbox_ids: list[str] | None = None,
    payload: str = "",
    project_root: str = "",
    workspace_root: str = "",
    cwd: str = "",
    skills_root: str = "",
    soul_path: str = "",
    cache_root: str = "",
    host_project_root: str = "",
    host_workspace_root: str = "",
    host_skills_root: str = "",
    host_soul_path: str = "",
    workspace_scope: str = "",
    is_admin: bool = False,
    sandboxed: bool = True,
    containerized: bool = False,
) -> tuple[str, str]:
    prompt = build_role_prompt(
        role,
        hub_url=hub_url,
        task_id=task_id,
        inbox_ids=inbox_ids,
        payload=payload,
        project_root=project_root,
        workspace_root=workspace_root,
        cwd=cwd,
        skills_root=skills_root,
        soul_path=soul_path,
        cache_root=cache_root,
        host_project_root=host_project_root,
        host_workspace_root=host_workspace_root,
        host_skills_root=host_skills_root,
        host_soul_path=host_soul_path,
        workspace_scope=workspace_scope,
        is_admin=is_admin,
        sandboxed=sandboxed,
        containerized=containerized,
    )
    if role == "main":
        system_prompt = MAIN_SYSTEM
    else:
        system_prompt = TASK_SYSTEM_TEMPLATE.format(task_id=task_id or "unknown")
    return system_prompt, prompt


__all__ = ["build_role_messages", "build_role_prompt"]
