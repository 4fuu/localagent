"""LocalAgent 主入口。

启动 Hub、IndexService、zvec server，并自动唤醒 Main Agent。
按 Ctrl+C 停止。
"""

import logging
import signal
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [main] %(name)s %(levelname)s: %(message)s",
)
logger = logging.getLogger(__name__)


def main() -> None:
    from src.config import cfg

    main_chat = cfg.chat("main")
    if not main_chat:
        logger.error("Main agent chat profile 未配置，请检查 config.toml [agent] main_chat")
        sys.exit(1)
    if not main_chat.get("api_key"):
        logger.error(
            "Chat API key 未设置 (provider=%s, profile=%s)，请在 .env 或环境变量中配置",
            main_chat["provider"], main_chat["profile"],
        )
        sys.exit(1)
    emb = cfg.embedding()
    if not emb:
        logger.error("Embedding profile 未配置，请检查 config.toml [agent] embedding")
        sys.exit(1)
    if not emb.get("api_key"):
        logger.error(
            "Embedding API key 未设置 (provider=%s, profile=%s)，请在 .env 或环境变量中配置",
            emb["provider"], emb["profile"],
        )
        sys.exit(1)

    from src.hub.runtime import init, shutdown

    def signal_handler(sig, frame):
        logger.info("收到终止信号，正在关闭...")
        shutdown()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        init()
        logger.info("LocalAgent 已启动，按 Ctrl+C 停止")

        while True:
            signal.pause()

    except Exception:
        logger.exception("主进程异常")
        shutdown()
        sys.exit(1)


if __name__ == "__main__":
    main()
