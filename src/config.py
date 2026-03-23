"""配置加载。优先级：环境变量 > config.toml > 代码默认值。

用法::

    from src.config import cfg

    cfg.hub_host        # "127.0.0.1"
    cfg.hub_port        # 9600
    cfg.chat("main")    # {"provider": "qwen", "model": "...", "base_url": "...", "api_key": "..."}
    cfg.chat("task")    # task agent 使用的配置
    cfg.embedding()     # {"provider": "qwen", "model": "...", "dimension": 1024, ...}
    cfg.resolve_person_id("telegram", "123456")  # "me" 或 "telegram:123456"
    cfg.gateway         # {"active": ["cli"], "cli": {...}}
"""

import copy
import os
import tomllib
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

load_dotenv()

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_CONFIG_PATH = _PROJECT_ROOT / "config.toml"

_DEFAULTS: dict = {
    "hub": {
        "host": "127.0.0.1",
        "port": 9600,
        "reap_interval": 5,
        "notify_delay": 1,
        "main_inbox_batch_size": 10,
        "main_per_conversation_limit": 5,
        "max_main_agents": 2,
        "max_task_agents": 16,
        "pool_size_main": 0,
        "pool_size_task": 0,
        "prioritize_admin": True,
        "startup_timeout": 5,
        "shutdown_timeout": 5,
        "agent_terminate_timeout": 10,
        "pending_request_timeout": 300,
        "db_scrub_interval": 0,
        "trace_cleanup_interval": 0,
        "trace_retention_days": 7,
    },
    "provider": {
        "qwen": {
            "base_url": "https://dashscope.aliyuncs.com/compatible-mode/v1",
            "api_key_env": "DASHSCOPE_API_KEY",
        },
        "mimo": {
            "base_url": "https://api.xiaomimimo.com/v1",
            "api_key_env": "MIMO_API_KEY",
        },
        "openai": {
            "base_url": "https://api.openai.com/v1",
            "api_key_env": "OPENAI_API_KEY",
        },
    },
    "chat": {},
    "embedding": {},
    "agent": {
        "main_chat": "",
        "task_chat": "",
        "embedding": "",
    },
    "identity": {
        "person": {},
    },
    "gateway": {
        "active": ["cli"],
        "cli": {
            "enabled": False,
            "mailbox_dir": "",
        },
        "telegram": {
            "enabled": False,
            "debug": False,
            "bot_token": "",
            "poll_timeout": 25,
            "drop_pending_updates": True,
            "allowed_chat_ids": [],
            "allowed_user_ids": [],
            "admin_user_ids": [],
            "group_message_mode": "all",
            "group_backlog_limit": 20,
            "command_prefixes": ["/localagent"],
        },
    },
    "retry": {
        "default": {
            "max_retries": 3,
            "base_delay": 0.5,
            "max_delay": 8.0,
            "jitter": 0.1,
            "connect_timeout": 5.0,
            "request_timeout": 15.0,
            "retry_after_send": False,
        },
        "services": {},
    },
    "sandbox": {
        "runtime": "podman",
        "command": "podman",
        "image": "localhost/localagent-sandbox:latest",
        "network": "slirp4netns",
        "pull": "missing",
        "read_only_rootfs": True,
        "tmpfs": ["/tmp", "/var/tmp"],
        "pids_limit": 256,
        "user_writable_skills": False,
    },
}


def _deep_merge(base: dict, override: dict) -> dict:
    for key, value in override.items():
        if key in base and isinstance(base[key], dict) and isinstance(value, dict):
            _deep_merge(base[key], value)
        else:
            base[key] = value
    return base


class Config:
    """只读配置对象。"""

    def __init__(self) -> None:
        data = copy.deepcopy(_DEFAULTS)
        if _CONFIG_PATH.exists():
            with open(_CONFIG_PATH, "rb") as f:
                _deep_merge(data, tomllib.load(f))
        self._data = data
        self._apply_env_overrides()
        self._identity_account_map = self._build_identity_account_map()

    def _apply_env_overrides(self) -> None:
        d = self._data
        if v := os.environ.get("LOCALAGENT_HUB_HOST"):
            d["hub"]["host"] = v
        if v := os.environ.get("LOCALAGENT_HUB_PORT"):
            d["hub"]["port"] = int(v)
        if v := os.environ.get("LOCALAGENT_HUB_REAP_INTERVAL"):
            d["hub"]["reap_interval"] = int(v)
        if v := os.environ.get("LOCALAGENT_HUB_NOTIFY_DELAY"):
            d["hub"]["notify_delay"] = int(v)
        if v := os.environ.get("LOCALAGENT_HUB_MAIN_INBOX_BATCH_SIZE"):
            d["hub"]["main_inbox_batch_size"] = int(v)
        if v := os.environ.get("LOCALAGENT_HUB_MAIN_PER_CONVERSATION_LIMIT"):
            d["hub"]["main_per_conversation_limit"] = int(v)
        if v := os.environ.get("LOCALAGENT_HUB_MAX_MAIN_AGENTS"):
            d["hub"]["max_main_agents"] = int(v)
        if v := os.environ.get("LOCALAGENT_HUB_MAX_TASK_AGENTS"):
            d["hub"]["max_task_agents"] = int(v)
        if v := os.environ.get("LOCALAGENT_HUB_POOL_SIZE_MAIN"):
            d["hub"]["pool_size_main"] = int(v)
        if v := os.environ.get("LOCALAGENT_HUB_POOL_SIZE_TASK"):
            d["hub"]["pool_size_task"] = int(v)
        if v := os.environ.get("LOCALAGENT_HUB_PRIORITIZE_ADMIN"):
            d["hub"]["prioritize_admin"] = (
                v.strip().lower() in {"1", "true", "yes", "on"}
            )
        if v := os.environ.get("LOCALAGENT_HUB_STARTUP_TIMEOUT"):
            d["hub"]["startup_timeout"] = int(v)
        if v := os.environ.get("LOCALAGENT_HUB_SHUTDOWN_TIMEOUT"):
            d["hub"]["shutdown_timeout"] = int(v)
        if v := os.environ.get("LOCALAGENT_HUB_AGENT_TERMINATE_TIMEOUT"):
            d["hub"]["agent_terminate_timeout"] = int(v)
        if v := os.environ.get("LOCALAGENT_HUB_PENDING_REQUEST_TIMEOUT"):
            d["hub"]["pending_request_timeout"] = int(v)
        if v := os.environ.get("LOCALAGENT_HUB_DB_SCRUB_INTERVAL"):
            d["hub"]["db_scrub_interval"] = int(v)
        if v := os.environ.get("LOCALAGENT_HUB_TRACE_CLEANUP_INTERVAL"):
            d["hub"]["trace_cleanup_interval"] = int(v)
        if v := os.environ.get("LOCALAGENT_HUB_TRACE_RETENTION_DAYS"):
            d["hub"]["trace_retention_days"] = int(v)

        if v := os.environ.get("LOCALAGENT_MAIN_CHAT"):
            d["agent"]["main_chat"] = v.strip()
        if v := os.environ.get("LOCALAGENT_TASK_CHAT"):
            d["agent"]["task_chat"] = v.strip()
        if v := os.environ.get("LOCALAGENT_EMBEDDING"):
            d["agent"]["embedding"] = v.strip()

        if v := os.environ.get("LOCALAGENT_GATEWAYS"):
            d["gateway"]["active"] = [s.strip() for s in v.split(",") if s.strip()]
        if v := os.environ.get("LOCALAGENT_GATEWAY_CLI_ENABLED"):
            d["gateway"].setdefault("cli", {})["enabled"] = (
                v.strip().lower() in {"1", "true", "yes", "on"}
            )
        if v := os.environ.get("LOCALAGENT_GATEWAY_CLI_DIR"):
            d["gateway"].setdefault("cli", {})["mailbox_dir"] = v.strip()
        if v := os.environ.get("LOCALAGENT_GATEWAY_TELEGRAM_ENABLED"):
            d["gateway"].setdefault("telegram", {})["enabled"] = (
                v.strip().lower() in {"1", "true", "yes", "on"}
            )
        if v := os.environ.get("LOCALAGENT_GATEWAY_TELEGRAM_DEBUG"):
            d["gateway"].setdefault("telegram", {})["debug"] = (
                v.strip().lower() in {"1", "true", "yes", "on"}
            )
        if v := os.environ.get("LOCALAGENT_GATEWAY_TELEGRAM_BOT_TOKEN"):
            d["gateway"].setdefault("telegram", {})["bot_token"] = v.strip()
        if v := os.environ.get("LOCALAGENT_GATEWAY_TELEGRAM_POLL_TIMEOUT"):
            d["gateway"].setdefault("telegram", {})["poll_timeout"] = int(v)
        if v := os.environ.get("LOCALAGENT_GATEWAY_TELEGRAM_DROP_PENDING_UPDATES"):
            d["gateway"].setdefault("telegram", {})["drop_pending_updates"] = (
                v.strip().lower() in {"1", "true", "yes", "on"}
            )
        if v := os.environ.get("LOCALAGENT_GATEWAY_TELEGRAM_ALLOWED_CHAT_IDS"):
            d["gateway"].setdefault("telegram", {})["allowed_chat_ids"] = [
                s.strip() for s in v.split(",") if s.strip()
            ]
        if v := os.environ.get("LOCALAGENT_GATEWAY_TELEGRAM_ALLOWED_USER_IDS"):
            d["gateway"].setdefault("telegram", {})["allowed_user_ids"] = [
                s.strip() for s in v.split(",") if s.strip()
            ]
        if v := os.environ.get("LOCALAGENT_GATEWAY_TELEGRAM_ADMIN_USER_IDS"):
            d["gateway"].setdefault("telegram", {})["admin_user_ids"] = [
                s.strip() for s in v.split(",") if s.strip()
            ]
        if v := os.environ.get("LOCALAGENT_GATEWAY_TELEGRAM_GROUP_MESSAGE_MODE"):
            d["gateway"].setdefault("telegram", {})["group_message_mode"] = v.strip()
        if v := os.environ.get("LOCALAGENT_GATEWAY_TELEGRAM_COMMAND_PREFIXES"):
            d["gateway"].setdefault("telegram", {})["command_prefixes"] = [
                s.strip() for s in v.split(",") if s.strip()
            ]
        if v := os.environ.get("LOCALAGENT_RETRY_MAX_RETRIES"):
            d["retry"].setdefault("default", {})["max_retries"] = int(v)
        if v := os.environ.get("LOCALAGENT_RETRY_BASE_DELAY"):
            d["retry"].setdefault("default", {})["base_delay"] = float(v)
        if v := os.environ.get("LOCALAGENT_RETRY_MAX_DELAY"):
            d["retry"].setdefault("default", {})["max_delay"] = float(v)
        if v := os.environ.get("LOCALAGENT_RETRY_JITTER"):
            d["retry"].setdefault("default", {})["jitter"] = float(v)
        if v := os.environ.get("LOCALAGENT_RETRY_CONNECT_TIMEOUT"):
            d["retry"].setdefault("default", {})["connect_timeout"] = float(v)
        if v := os.environ.get("LOCALAGENT_RETRY_REQUEST_TIMEOUT"):
            d["retry"].setdefault("default", {})["request_timeout"] = float(v)
        if v := os.environ.get("LOCALAGENT_RETRY_AFTER_SEND"):
            d["retry"].setdefault("default", {})["retry_after_send"] = (
                v.strip().lower() in {"1", "true", "yes", "on"}
            )
        if v := os.environ.get("LOCALAGENT_SANDBOX_RUNTIME"):
            d["sandbox"]["runtime"] = v.strip()
        if v := os.environ.get("LOCALAGENT_SANDBOX_COMMAND"):
            d["sandbox"]["command"] = v.strip()
        if v := os.environ.get("LOCALAGENT_SANDBOX_IMAGE"):
            d["sandbox"]["image"] = v.strip()
        if v := os.environ.get("LOCALAGENT_SANDBOX_NETWORK"):
            d["sandbox"]["network"] = v.strip()
        if v := os.environ.get("LOCALAGENT_SANDBOX_PULL"):
            d["sandbox"]["pull"] = v.strip()
        if v := os.environ.get("LOCALAGENT_SANDBOX_READ_ONLY_ROOTFS"):
            d["sandbox"]["read_only_rootfs"] = (
                v.strip().lower() in {"1", "true", "yes", "on"}
            )
        if v := os.environ.get("LOCALAGENT_SANDBOX_TMPFS"):
            d["sandbox"]["tmpfs"] = [s.strip() for s in v.split(",") if s.strip()]
        if v := os.environ.get("LOCALAGENT_SANDBOX_PIDS_LIMIT"):
            d["sandbox"]["pids_limit"] = int(v)
        if v := os.environ.get("LOCALAGENT_SANDBOX_USER_WRITABLE_SKILLS"):
            d["sandbox"]["user_writable_skills"] = (
                v.strip().lower() in {"1", "true", "yes", "on"}
            )

    @staticmethod
    def _normalize_retry(raw: dict) -> dict[str, int | float | bool]:
        max_retries = int(raw.get("max_retries", 0))
        base_delay = float(raw.get("base_delay", 0.0))
        max_delay = float(raw.get("max_delay", base_delay))
        jitter = float(raw.get("jitter", 0.0))
        connect_timeout = float(raw.get("connect_timeout", 5.0))
        request_timeout = float(raw.get("request_timeout", 15.0))
        retry_after_send = bool(raw.get("retry_after_send", False))
        return {
            "max_retries": max(max_retries, 0),
            "base_delay": max(base_delay, 0.0),
            "max_delay": max(max_delay, max(base_delay, 0.0)),
            "jitter": min(max(jitter, 0.0), 1.0),
            "connect_timeout": max(connect_timeout, 0.1),
            "request_timeout": max(request_timeout, 0.1),
            "retry_after_send": retry_after_send,
        }

    @staticmethod
    def _normalize_image_input_mode(raw: Any) -> str:
        if isinstance(raw, bool):
            return "multimodal" if raw else "disabled"

        value = str(raw or "").strip().lower()
        if not value:
            return "paths"

        aliases = {
            "multimodal": "multimodal",
            "vision": "multimodal",
            "enabled": "multimodal",
            "on": "multimodal",
            "true": "multimodal",
            "paths": "paths",
            "path": "paths",
            "text": "paths",
            "fallback": "paths",
            "disabled": "disabled",
            "disable": "disabled",
            "off": "disabled",
            "none": "disabled",
            "false": "disabled",
        }
        normalized = aliases.get(value)
        if normalized:
            return normalized
        raise ValueError(f"unsupported chat image_input_mode: {raw!r}")

    def _resolve_profile(self, section: str, profile_name: str) -> dict:
        """解析 chat 或 embedding 配置，合并 provider 基础配置。

        解析顺序：provider 基础配置 ← profile 覆盖配置。
        """
        profile = self._data.get(section, {}).get(profile_name)
        if profile is None:
            return {}
        provider_name = profile.get("provider", "")
        provider = self._data.get("provider", {}).get(provider_name, {})

        resolved = {**provider}
        for k, v in profile.items():
            if k != "provider":
                resolved[k] = v
        resolved["provider"] = provider_name
        resolved["profile"] = profile_name

        api_key_env = resolved.pop("api_key_env", f"{provider_name.upper()}_API_KEY")
        resolved["api_key"] = os.environ.get(api_key_env, "")
        if section == "chat":
            resolved["image_input_mode"] = self._normalize_image_input_mode(
                resolved.get("image_input_mode", "")
            )

        return resolved

    def chat(self, role: str = "main") -> dict:
        """返回指定角色的 chat 配置（合并 provider）。"""
        agent = self._data.get("agent", {})
        profile_name = agent.get(f"{role}_chat", "")
        return self._resolve_profile("chat", profile_name)

    def embedding(self) -> dict:
        """返回 embedding 配置（合并 provider）。"""
        agent = self._data.get("agent", {})
        profile_name = agent.get("embedding", "")
        return self._resolve_profile("embedding", profile_name)

    # --- Hub ---

    @property
    def hub_host(self) -> str:
        return self._data["hub"]["host"]

    @property
    def hub_port(self) -> int:
        return self._data["hub"]["port"]

    @property
    def hub_reap_interval(self) -> int:
        return int(self._data["hub"]["reap_interval"])

    @property
    def hub_notify_delay(self) -> int:
        return int(self._data["hub"]["notify_delay"])

    @property
    def hub_main_inbox_batch_size(self) -> int:
        return int(self._data["hub"].get("main_inbox_batch_size", 10))

    @property
    def hub_main_per_conversation_limit(self) -> int:
        return int(self._data["hub"].get("main_per_conversation_limit", 5))

    @property
    def hub_max_main_agents(self) -> int:
        return max(1, int(self._data["hub"].get("max_main_agents", 2)))

    @property
    def hub_max_task_agents(self) -> int:
        return max(1, int(self._data["hub"].get("max_task_agents", 16)))

    @property
    def hub_pool_size_main(self) -> int:
        return max(0, int(self._data["hub"].get("pool_size_main", 0)))

    @property
    def hub_pool_size_task(self) -> int:
        return max(0, int(self._data["hub"].get("pool_size_task", 0)))

    @property
    def hub_prioritize_admin(self) -> bool:
        return bool(self._data["hub"].get("prioritize_admin", True))

    @property
    def hub_startup_timeout(self) -> int:
        return int(self._data["hub"]["startup_timeout"])

    @property
    def hub_shutdown_timeout(self) -> int:
        return int(self._data["hub"]["shutdown_timeout"])

    @property
    def hub_agent_terminate_timeout(self) -> int:
        return int(self._data["hub"]["agent_terminate_timeout"])

    @property
    def hub_pending_request_timeout(self) -> int:
        return int(self._data["hub"]["pending_request_timeout"])

    @property
    def hub_db_scrub_interval(self) -> int:
        return int(self._data["hub"].get("db_scrub_interval", 0))

    @property
    def hub_trace_cleanup_interval(self) -> int:
        return int(self._data["hub"].get("trace_cleanup_interval", 0))

    @property
    def hub_trace_retention_days(self) -> int:
        return int(self._data["hub"].get("trace_retention_days", 7))

    # --- Gateway ---

    @property
    def gateway(self) -> dict:
        return self._data["gateway"]

    # --- Retry ---

    def retry(self, service: str = "") -> dict[str, int | float | bool]:
        retry_root = self._data.get("retry", {})
        base = self._normalize_retry(retry_root.get("default", {}))
        if not service:
            return base
        services = retry_root.get("services", {})
        override = services.get(service, {}) if isinstance(services, dict) else {}
        merged = {**base, **override}
        return self._normalize_retry(merged)

    # --- Sandbox ---

    @property
    def sandbox(self) -> dict:
        data = self._data.get("sandbox", {})
        tmpfs = data.get("tmpfs", [])
        if not isinstance(tmpfs, list):
            tmpfs = []
        return {
            "runtime": str(data.get("runtime", "podman")).strip().lower() or "podman",
            "command": str(data.get("command", "podman")).strip() or "podman",
            "image": str(data.get("image", "")).strip(),
            "network": str(data.get("network", "slirp4netns")).strip(),
            "pull": str(data.get("pull", "missing")).strip(),
            "read_only_rootfs": bool(data.get("read_only_rootfs", True)),
            "tmpfs": [str(item).strip() for item in tmpfs if str(item).strip()],
            "pids_limit": max(16, int(data.get("pids_limit", 256))),
            "user_writable_skills": bool(data.get("user_writable_skills", False)),
        }

    # --- Identity ---

    @property
    def identity(self) -> dict:
        return self._data.get("identity", {})

    @property
    def identity_account_map(self) -> dict[str, str]:
        return dict(self._identity_account_map)

    def resolve_person_id(self, gateway: str, user_id: str) -> str:
        normalized_gateway = str(gateway).strip()
        normalized_user_id = str(user_id).strip()
        if not normalized_gateway or not normalized_user_id:
            return ""
        account_key = f"{normalized_gateway}:{normalized_user_id}"
        return self._identity_account_map.get(account_key, account_key)

    def _build_identity_account_map(self) -> dict[str, str]:
        identity_root = self._data.get("identity", {})
        people = identity_root.get("person", {}) if isinstance(identity_root, dict) else {}
        if not isinstance(people, dict):
            return {}
        account_map: dict[str, str] = {}
        for person_id, raw_cfg in people.items():
            if not isinstance(raw_cfg, dict):
                continue
            normalized_person_id = str(person_id).strip()
            if not normalized_person_id:
                continue
            accounts = raw_cfg.get("accounts", [])
            if not isinstance(accounts, list):
                continue
            for raw_account in accounts:
                account = str(raw_account).strip()
                if not account:
                    continue
                existing = account_map.get(account)
                if existing and existing != normalized_person_id:
                    raise ValueError(
                        f"identity account '{account}' is mapped to multiple persons: "
                        f"'{existing}' and '{normalized_person_id}'"
                    )
                account_map[account] = normalized_person_id
        return account_map


cfg = Config()
