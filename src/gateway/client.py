"""Gateway RPC client through Hub."""

import json
import time
import uuid
from socket import timeout as SocketTimeout
from typing import Any

from websockets.sync.client import ClientConnection, connect

from ..retry import RetryPolicy


class GatewayClient:
    def __init__(self, hub_url: str, request_timeout: float | None = None):
        self._hub_url = hub_url
        self._retry = RetryPolicy.for_service("gateway_client")
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
            raise RuntimeError("GatewayClient is closed")
        if self._ws is None:
            self._ws = connect(
                self._hub_url,
                open_timeout=self._retry.connect_timeout,
            )
        return self._ws

    def _call(
        self,
        topic: str,
        payload: dict[str, Any],
        *,
        retry_after_send: bool | None = None,
    ) -> dict[str, Any]:
        last_exc: Exception | None = None
        allow_retry_after_send = (
            self._retry.retry_after_send if retry_after_send is None else retry_after_send
        )
        for attempt in range(self._retry.max_retries + 1):
            msg_id = str(uuid.uuid4())
            sent = False
            try:
                ws = self._ensure_connection()
                ws.send(
                    json.dumps({
                        "type": "request",
                        "id": msg_id,
                        "topic": topic,
                        "payload": payload,
                    })
                )
                sent = True
                while True:
                    try:
                        raw = ws.recv(timeout=self._request_timeout)
                    except SocketTimeout as exc:
                        raise RuntimeError(
                            f"gateway request timed out after {self._request_timeout:.0f}s: {topic}"
                        ) from exc
                    msg = json.loads(raw)
                    if msg.get("type") == "response" and msg.get("id") == msg_id:
                        return msg["payload"]
            except Exception as exc:
                last_exc = exc
                self._reset_connection()
                can_retry = (not sent) or allow_retry_after_send
                if attempt >= self._retry.max_retries or not can_retry:
                    break
                time.sleep(self._retry.backoff_delay(attempt))
        raise RuntimeError(f"gateway request failed: {topic}") from last_exc

    def send(
        self,
        *,
        gateway: str,
        conversation_id: str,
        text: str,
        user_id: str = "",
        metadata: dict[str, Any] | None = None,
        artifact_refs: list[str] | None = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "gateway": gateway,
            "conversation_id": conversation_id,
            "text": text,
        }
        if user_id:
            payload["user_id"] = user_id
        if metadata:
            payload["metadata"] = metadata
        if artifact_refs:
            payload["artifact_refs"] = artifact_refs

        # Sending chat messages is not idempotent. If the request reaches the
        # gateway but the response is lost/times out, retrying here can
        # duplicate outbound messages and attachments.
        resp = self._call("gateway.send", payload, retry_after_send=False)
        if not resp.get("ok"):
            raise RuntimeError(resp.get("error", "gateway.send failed"))
        return resp.get("result", {})

    def send_action(
        self,
        *,
        gateway: str,
        conversation_id: str,
        action: str = "typing",
    ) -> None:
        payload = {
            "gateway": gateway,
            "conversation_id": conversation_id,
            "action": action,
        }
        try:
            self._call("gateway.send_action", payload)
        except Exception:
            pass

    def list_gateways(self) -> dict[str, Any]:
        resp = self._call("gateway.list", {})
        if not resp.get("ok"):
            raise RuntimeError(resp.get("error", "gateway.list failed"))
        return resp

    def inbound(
        self,
        *,
        gateway: str,
        conversation_id: str,
        text: str,
        user_id: str = "",
        metadata: dict[str, Any] | None = None,
        silent: bool = False,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "gateway": gateway,
            "conversation_id": conversation_id,
            "text": text,
        }
        if user_id:
            payload["user_id"] = user_id
        if metadata:
            payload["metadata"] = metadata
        if silent:
            payload["silent"] = True

        # Inbound injection is also non-idempotent from the store's
        # perspective; avoid replaying the same inbound event after send.
        resp = self._call("gateway.inbound", payload, retry_after_send=False)
        if not resp.get("ok"):
            raise RuntimeError(resp.get("error", "gateway.inbound failed"))
        return resp

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            self._reset_connection()
