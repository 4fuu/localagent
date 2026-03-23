"""Index client. Lightweight RPC client for index operations via Hub.

Can be created in any process — only needs a hub_url.
"""

import json
import time
import uuid
from socket import timeout as SocketTimeout
from typing import Any

from websockets.sync.client import ClientConnection, connect

from ..retry import RetryPolicy


class IndexClient:
    """向量索引客户端。

    通过 Hub 与 IndexService 通信，可在任意进程中创建多个实例。

    Usage:
        client = IndexClient("ws://127.0.0.1:9600")
        results = client.search("关键词")
        client.close()
    """

    def __init__(self, hub_url: str, request_timeout: float | None = None):
        self._hub_url = hub_url
        self._retry = RetryPolicy.for_service("index_client")
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
            raise RuntimeError("IndexClient is closed")
        if self._ws is None:
            self._ws = connect(
                self._hub_url,
                open_timeout=self._retry.connect_timeout,
            )
        return self._ws

    def _call(self, topic: str, payload: dict) -> dict:
        last_exc: Exception | None = None
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
                            f"index request timed out after {self._request_timeout:.0f}s: {topic}"
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
        raise RuntimeError(f"index request failed: {topic}") from last_exc

    def sync(self, source: str, path: str) -> None:
        """同步指定源的文件索引。"""
        resp = self._call("index.sync", {"source": source, "path": path})
        if not resp["ok"]:
            raise RuntimeError(resp["error"])

    def search(
        self,
        query: str,
        *,
        topk: int = 10,
        sources: list[dict[str, str]] | None = None,
        filter: str | None = None,
    ) -> list[dict[str, Any]]:
        """搜索。

        Args:
            query: 查询文本。
            topk: 返回数量。
            sources: 搜索前同步的源列表。
            filter: 可选的过滤表达式，传给 zvec query。

        Returns:
            搜索结果列表 [{"id": str, "score": float, "fields": dict}, ...]。
        """
        payload: dict[str, Any] = {"query": query, "topk": topk}
        if sources:
            payload["sources"] = sources
        if filter:
            payload["filter"] = filter
        resp = self._call("index.search", payload)
        if not resp["ok"]:
            raise RuntimeError(resp["error"])
        return resp["results"]

    def hybrid_search(
        self,
        query: str,
        *,
        topk: int = 10,
        sources: list[dict[str, str]] | None = None,
        filter: str | None = None,
    ) -> list[dict[str, Any]]:
        """Hybrid search combining dense vector and BM25 sparse retrieval."""
        payload: dict[str, Any] = {"query": query, "topk": topk}
        if sources:
            payload["sources"] = sources
        if filter:
            payload["filter"] = filter
        resp = self._call("index.hybrid_search", payload)
        if not resp["ok"]:
            raise RuntimeError(resp["error"])
        return resp["results"]

    def insert_entry(
        self,
        text: str,
        label: str | None = None,
        prefix: str = "l3",
        *,
        source: str = "memory",
        content: str | None = None,
        metadata: str | None = None,
    ) -> str | None:
        """插入一条非文件条目。返回 ID，内容已存在返回已有 ID。"""
        payload: dict[str, Any] = {
            "text": text,
            "prefix": prefix,
            "source": source,
        }
        if label is not None:
            payload["label"] = label
        if content is not None:
            payload["content"] = content
        if metadata is not None:
            payload["metadata"] = metadata
        resp = self._call("index.insert_entry", payload)
        if not resp["ok"]:
            raise RuntimeError(resp["error"])
        return resp["id"]

    def upsert_entry(
        self,
        entry_id: str,
        text: str,
        *,
        label: str | None = None,
        source: str = "memory",
        content: str | None = None,
        metadata: str | None = None,
    ) -> str:
        """按稳定 ID 覆盖写入一条非文件条目。"""
        payload: dict[str, Any] = {
            "id": entry_id,
            "text": text,
            "source": source,
        }
        if label is not None:
            payload["label"] = label
        if content is not None:
            payload["content"] = content
        if metadata is not None:
            payload["metadata"] = metadata
        resp = self._call("index.upsert_entry", payload)
        if not resp["ok"]:
            raise RuntimeError(resp["error"])
        return str(resp.get("id") or entry_id)

    def upsert_entries(
        self,
        entries: list[dict[str, Any]],
    ) -> int:
        """批量按稳定 ID 覆盖写入多条非文件条目。"""
        resp = self._call("index.upsert_entries", {"entries": entries})
        if not resp["ok"]:
            raise RuntimeError(resp["error"])
        return int(resp.get("count", 0))

    def insert_reply(
        self,
        text: str,
        *,
        content: str | None = None,
        metadata: str | None = None,
    ) -> str | None:
        """插入一条回复记录（仅 SQLite，不做向量索引）。返回 ID。"""
        payload: dict[str, Any] = {"text": text}
        if content is not None:
            payload["content"] = content
        if metadata is not None:
            payload["metadata"] = metadata
        resp = self._call("index.insert_reply", payload)
        if not resp["ok"]:
            raise RuntimeError(resp["error"])
        return resp["id"]

    def delete_entry(self, entry_id: str) -> None:
        """删除一条非文件条目。"""
        resp = self._call("index.delete_entry", {"id": entry_id})
        if not resp["ok"]:
            raise RuntimeError(resp["error"])

    def query_entries(
        self,
        source: str = "",
        time_after: str = "",
        time_before: str = "",
        keyword: str = "",
        page: int = 1,
        page_size: int = 20,
    ) -> dict[str, Any]:
        """结构化检索已归档条目。"""
        payload: dict[str, Any] = {"page": page, "page_size": page_size}
        if source:
            payload["source"] = source
        if time_after:
            payload["time_after"] = time_after
        if time_before:
            payload["time_before"] = time_before
        if keyword:
            payload["keyword"] = keyword
        resp = self._call("index.query_entries", payload)
        if not resp["ok"]:
            raise RuntimeError(resp["error"])
        return resp

    def list_metadata(self, source: str) -> dict[str, Any]:
        """列出指定 source 的元数据字段概览。"""
        resp = self._call("index.list_metadata", {"source": source})
        if not resp["ok"]:
            raise RuntimeError(resp["error"])
        return resp["fields"]

    def close(self) -> None:
        """关闭连接（不影响服务端）。"""
        if not self._closed:
            self._closed = True
            self._reset_connection()
