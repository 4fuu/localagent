"""Interactive CLI client for LocalAgent gateway.

Usage:
    python cli.py
    python cli.py --conversation my-chat --user alice
"""

import argparse
import json
import os
import re
import socket
import threading
from pathlib import Path

from src.config import cfg
from src.gateway import GatewayClient

_PROJECT_ROOT = Path(__file__).resolve().parent
_DEFAULT_MAILBOX_DIR = _PROJECT_ROOT / ".localagent" / "gateway_cli"


def _slug(value: str) -> str:
    raw = value.strip().lower() or "conversation"
    s = re.sub(r"[^a-z0-9._-]+", "-", raw)
    s = s.strip("-._")
    if not s:
        s = "conversation"
    return s[:80]


def _default_conversation() -> str:
    host = _slug(socket.gethostname())
    return f"{host}"


def _mailbox_path(conversation_id: str) -> Path:
    raw = str(cfg.gateway.get("cli", {}).get("mailbox_dir", "")).strip()
    mailbox_dir = Path(raw).expanduser() if raw else _DEFAULT_MAILBOX_DIR
    return mailbox_dir / f"{_slug(conversation_id)}.jsonl"


def _follow_replies(stop_event: threading.Event, mailbox: Path, start_offset: int) -> None:
    offset = start_offset
    while not stop_event.is_set():
        try:
            if mailbox.exists():
                size = mailbox.stat().st_size
                if size < offset:
                    offset = 0
                if size > offset:
                    with mailbox.open("r", encoding="utf-8") as f:
                        f.seek(offset)
                        for line in f:
                            line = line.strip()
                            if not line:
                                continue
                            try:
                                msg = json.loads(line)
                            except json.JSONDecodeError:
                                print(f"\n[agent] {line}")
                                continue

                            text = str(msg.get("text", "")).strip()
                            if text:
                                print(f"\n[agent] {text}")
                    offset = size
            stop_event.wait(0.3)
        except Exception as exc:
            print(f"\n[cli] 读取回复失败: {exc}")
            stop_event.wait(1.0)


def main() -> None:
    default_hub_url = os.getenv(
        "LOCALAGENT_HUB_URL", f"ws://{cfg.hub_host}:{cfg.hub_port}"
    )

    parser = argparse.ArgumentParser(description="LocalAgent interactive CLI gateway client")
    parser.add_argument("--hub", default=default_hub_url)
    parser.add_argument("--gateway", default="cli")
    parser.add_argument("--conversation", default=_default_conversation())
    parser.add_argument("--user", default=os.getenv("USER", "cli-user"))
    args = parser.parse_args()

    mailbox = _mailbox_path(args.conversation)
    mailbox.parent.mkdir(parents=True, exist_ok=True)
    start_offset = mailbox.stat().st_size if mailbox.exists() else 0

    client = GatewayClient(args.hub)
    try:
        info = client.list_gateways()
        enabled = set(info.get("enabled", []))
        if args.gateway not in enabled:
            print(
                "[cli] 目标 gateway 未启用。请先设置环境变量后重启主进程：\n"
                "      LOCALAGENT_GATEWAY_CLI_ENABLED=1"
            )
            print(f"[cli] 当前已启用: {sorted(enabled)}")
            return

        print(f"[cli] hub={args.hub}")
        print(f"[cli] gateway={args.gateway} conversation_id={args.conversation} user={args.user}")
        print("[cli] 输入消息后回车发送，输入 /silent <内容> 静默入站，输入 /quit 退出。")

        stop_event = threading.Event()
        watcher = threading.Thread(
            target=_follow_replies,
            args=(stop_event, mailbox, start_offset),
            daemon=True,
        )
        watcher.start()

        while True:
            try:
                text = input("you> ").strip()
            except EOFError:
                text = "/quit"

            if not text:
                continue
            if text.lower() in {"/quit", "/exit"}:
                break

            silent = False
            if text.startswith("/silent "):
                text = text[len("/silent "):].strip()
                silent = True
            if not text:
                continue

            try:
                resp = client.inbound(
                    gateway=args.gateway,
                    conversation_id=args.conversation,
                    text=text,
                    user_id=args.user,
                    metadata={"source": "cli"},
                    silent=silent,
                )
                inbox_file = resp.get("inbox_file", "")
                if inbox_file:
                    if resp.get("silent"):
                        print(f"[cli] sent silently (inbox={inbox_file})")
                    else:
                        print(f"[cli] sent (inbox={inbox_file})")
                else:
                    print("[cli] sent")
            except Exception as exc:
                print(f"[cli] 发送失败: {exc}")

        stop_event.set()
        watcher.join(timeout=1)
    finally:
        client.close()


if __name__ == "__main__":
    main()
