"""zvec client. Communicates with the zvec server through the Hub."""

import json
import time
import uuid
from socket import timeout as SocketTimeout
from typing import Any, Optional

from websockets.sync.client import ClientConnection, connect

from ..retry import RetryPolicy


class VecClient:
    """Client for the zvec server via WebSocket Hub.

    Usage:
        vc = VecClient("ws://127.0.0.1:9600")
        vc.insert([{"id": "d1", "vectors": {"embedding": [0.1, ...]}}])
        results = vc.query([0.1, ...], topk=5)
        vc.close()
    """

    def __init__(self, hub_url: str):
        self._hub_url = hub_url
        self._retry = RetryPolicy.for_service("vec_client")
        self._ws: ClientConnection | None = None
        self._closed = False
        self._request_timeout = self._retry.request_timeout

    def _reset_connection(self) -> None:
        if self._ws is not None:
            try:
                self._ws.close()
            except Exception:
                pass
            self._ws = None

    def _ensure_connection(self) -> ClientConnection:
        if self._closed:
            raise RuntimeError("VecClient is closed")
        if self._ws is None:
            self._ws = connect(
                self._hub_url,
                open_timeout=self._retry.connect_timeout,
            )
        return self._ws

    def _call(self, req: dict) -> dict:
        last_exc: Exception | None = None
        for attempt in range(self._retry.max_retries + 1):
            msg_id = str(uuid.uuid4())
            topic = f"vec.{req['cmd']}"
            sent = False
            try:
                ws = self._ensure_connection()
                ws.send(
                    json.dumps({
                        "type": "request",
                        "id": msg_id,
                        "topic": topic,
                        "payload": req,
                    })
                )
                sent = True
                while True:
                    try:
                        raw = ws.recv(timeout=self._request_timeout)
                    except SocketTimeout as exc:
                        cmd = str(req.get("cmd", "unknown"))
                        raise RuntimeError(
                            f"vec request timed out after {self._request_timeout:.0f}s: {cmd}"
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
        raise RuntimeError(f"vec request failed: {req.get('cmd', 'unknown')}") from last_exc

    def insert(self, docs: list[dict[str, Any]]) -> int:
        """Insert documents.

        Args:
            docs: List of dicts with keys:
                - id (str): document ID
                - vectors (dict): e.g. {"embedding": [0.1, 0.2, ...]}
                - fields (dict, optional): scalar metadata

        Returns:
            Number of documents inserted.
        """
        resp = self._call({"cmd": "insert", "docs": docs})
        if not resp["ok"]:
            raise RuntimeError(resp["error"])
        return resp["count"]

    def query(
        self,
        vector: list[float],
        *,
        topk: int = 10,
        field: str = "embedding",
        filter: Optional[str] = None,
    ) -> list[dict[str, Any]]:
        """Query by vector similarity.

        Returns:
            List of {"id": str, "score": float, "fields": dict | None}.
        """
        req: dict[str, Any] = {
            "cmd": "query",
            "vector": vector,
            "topk": topk,
            "field": field,
        }
        if filter is not None:
            req["filter"] = filter
        resp = self._call(req)
        if not resp["ok"]:
            raise RuntimeError(resp["error"])
        return resp["results"]

    def delete(self, ids: list[str]) -> int:
        """Delete documents by IDs."""
        resp = self._call({"cmd": "delete", "ids": ids})
        if not resp["ok"]:
            raise RuntimeError(resp["error"])
        return resp["count"]

    def hybrid_query(
        self,
        vector: list[float],
        query_text: str,
        *,
        topk: int = 10,
        filter: Optional[str] = None,
    ) -> list[dict[str, Any]]:
        """Hybrid query combining dense vector and BM25 sparse search.

        Returns:
            List of {"id": str, "score": float, "fields": dict | None}.
        """
        req: dict[str, Any] = {
            "cmd": "hybrid_query",
            "vector": vector,
            "query_text": query_text,
            "topk": topk,
        }
        if filter is not None:
            req["filter"] = filter
        resp = self._call(req)
        if not resp["ok"]:
            raise RuntimeError(resp["error"])
        return resp["results"]

    def flush(self):
        """Flush pending writes to disk."""
        resp = self._call({"cmd": "flush"})
        if not resp["ok"]:
            raise RuntimeError(resp["error"])

    def close(self):
        """Shutdown the zvec server (flushes automatically) and close connection."""
        if not self._closed:
            try:
                self._call({"cmd": "shutdown"})
            except Exception:
                pass
            self._closed = True
            self._reset_connection()
