"""Runtime broker RPC client."""

from __future__ import annotations

import json
import time
import uuid
from socket import timeout as SocketTimeout
from typing import Any

from websockets.sync.client import ClientConnection, connect

from ..retry import RetryPolicy


class RuntimeClient:
    def __init__(self, hub_url: str, request_timeout: float | None = None):
        self._hub_url = hub_url
        self._retry = RetryPolicy.for_service("runtime_client")
        self._ws: ClientConnection | None = None
        self._closed = False
        timeout = self._retry.request_timeout if request_timeout is None else request_timeout
        self._request_timeout = max(float(timeout), 0.1)

    def _reset_connection(self) -> None:
        if self._ws is not None:
            try:
                self._ws.close()
            except Exception:
                pass
            self._ws = None

    def _ensure_connection(self) -> ClientConnection:
        if self._closed:
            raise RuntimeError("RuntimeClient is closed")
        if self._ws is None:
            self._ws = connect(self._hub_url, open_timeout=self._retry.connect_timeout)
        return self._ws

    def call(self, *, task_id: str, method: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        last_exc: Exception | None = None
        for attempt in range(self._retry.max_retries + 1):
            msg_id = str(uuid.uuid4())
            sent = False
            try:
                ws = self._ensure_connection()
                ws.send(json.dumps({
                    "type": "request",
                    "id": msg_id,
                    "topic": "runtime.call",
                    "payload": {
                        "task_id": task_id,
                        "method": method,
                        "params": params or {},
                    },
                }))
                sent = True
                while True:
                    try:
                        raw = ws.recv(timeout=self._request_timeout)
                    except SocketTimeout as exc:
                        raise RuntimeError(
                            f"runtime request timed out after {self._request_timeout:.0f}s: {method}"
                        ) from exc
                    msg = json.loads(raw)
                    if msg.get("type") == "response" and msg.get("id") == msg_id:
                        return msg["payload"]
            except Exception as exc:
                last_exc = exc
                self._reset_connection()
                can_retry = (not sent) or self._retry.retry_after_send
                if attempt >= self._retry.max_retries or not can_retry:
                    break
                time.sleep(self._retry.backoff_delay(attempt))
        raise RuntimeError("runtime request failed") from last_exc

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            self._reset_connection()
