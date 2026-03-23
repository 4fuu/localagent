"""主进程运行时——管理 Hub、IndexService 等只能存在一份的单例资源。

规则：
- 主进程调用 init() 启动所有服务
- 子进程通过 hub_url（环境变量 / 参数）连接 Hub，使用 IndexClient
"""

import logging
import threading

from ..config import cfg
from .hub import Hub
from ..gateway import GatewayService
from ..index import IndexService
from ..provider.embedding import create_embedding
from ..runtime import RuntimeBrokerService

logger = logging.getLogger(__name__)

_hub: Hub | None = None
_index_service: IndexService | None = None
_gateway_service: GatewayService | None = None
_runtime_broker: RuntimeBrokerService | None = None


def init() -> None:
    """主进程启动时调用一次，启动 Hub 和 IndexService。"""
    global _hub, _index_service, _gateway_service, _runtime_broker
    if _hub is not None:
        return
    _hub = Hub(
        cfg.hub_host,
        cfg.hub_port,
        notify_delay=cfg.hub_notify_delay,
        main_inbox_batch_size=cfg.hub_main_inbox_batch_size,
        main_per_conversation_limit=cfg.hub_main_per_conversation_limit,
        max_main_agents=cfg.hub_max_main_agents,
        max_task_agents=cfg.hub_max_task_agents,
        prioritize_admin=cfg.hub_prioritize_admin,
        reap_interval=cfg.hub_reap_interval,
        startup_timeout=cfg.hub_startup_timeout,
        shutdown_timeout=cfg.hub_shutdown_timeout,
        agent_terminate_timeout=cfg.hub_agent_terminate_timeout,
        pending_request_timeout=cfg.hub_pending_request_timeout,
        db_scrub_interval=cfg.hub_db_scrub_interval,
        trace_cleanup_interval=cfg.hub_trace_cleanup_interval,
        trace_retention_days=cfg.hub_trace_retention_days,
        pool_size_main=cfg.hub_pool_size_main,
        pool_size_task=cfg.hub_pool_size_task,
    )
    _hub.start()

    emb_cfg = cfg.embedding()
    if not emb_cfg:
        raise RuntimeError("Embedding profile not configured")
    embedding_provider_name = emb_cfg["provider"]
    embedding_dimension = int(emb_cfg.get("dimension", 1024))
    embedding = create_embedding(
        embedding_provider_name,
        api_key=emb_cfg["api_key"],
        model=emb_cfg.get("model", ""),
        dimension=embedding_dimension,
        base_url=emb_cfg.get("base_url", ""),
    )
    _index_service = IndexService(
        hub_url=_hub.url,
        embedding=embedding,
        vector_dimension=embedding_dimension,
    )
    _gateway_service = GatewayService(hub_url=_hub.url)
    _runtime_broker = RuntimeBrokerService(hub_url=_hub.url)

    # Start all services in parallel — they are independent of each other.
    errors: list[tuple[str, Exception]] = []
    lock = threading.Lock()

    def _start_service(name: str, service: object) -> None:
        try:
            service.start()  # type: ignore[union-attr]
        except Exception as exc:
            with lock:
                errors.append((name, exc))

    threads = [
        threading.Thread(target=_start_service, args=("IndexService", _index_service)),
        threading.Thread(target=_start_service, args=("GatewayService", _gateway_service)),
        threading.Thread(target=_start_service, args=("RuntimeBrokerService", _runtime_broker)),
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    if errors:
        names = ", ".join(n for n, _ in errors)
        raise RuntimeError(
            f"Service startup failed: {names}; "
            + "; ".join(f"{n}: {e}" for n, e in errors)
        )
    logger.info(
        "Runtime initialized (hub=%s, embedding_provider=%s, dim=%s)",
        _hub.url,
        embedding_provider_name,
        embedding_dimension,
    )


def hub_url() -> str:
    """返回 Hub 的 WebSocket URL。"""
    if _hub is None:
        raise RuntimeError("runtime.init() 未调用")
    return _hub.url


def shutdown() -> None:
    """关闭所有资源（包括 agent 子进程）。"""
    global _hub, _index_service, _gateway_service, _runtime_broker
    if _runtime_broker is not None:
        _runtime_broker.stop()
        _runtime_broker = None
    if _gateway_service is not None:
        _gateway_service.stop()
        _gateway_service = None
    if _index_service is not None:
        _index_service.stop()
        _index_service = None
    if _hub is not None:
        _hub.stop()
        _hub = None
    logger.info("Runtime shut down")
