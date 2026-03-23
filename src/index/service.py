"""Index service. Registers index.* topics on Hub.

Runs in main process background thread. Manages sqlite manifest,
embedding, and vector operations through VecClient.
"""

import hashlib
import json
import logging
import re
import sqlite3
import subprocess
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from websockets.sync.client import ClientConnection, connect
from websockets.exceptions import ConnectionClosed

from .source import IndexSource
from ..provider.embedding import BaseEmbedding
from ..retry import RetryPolicy
from ..vec import VecClient, start_server

logger = logging.getLogger(__name__)

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
_LOCALAGENT_DIR = _PROJECT_ROOT / ".localagent"
_MANIFEST_DB = _LOCALAGENT_DIR / "manifest.db"

_INDEX_TOPICS = [
    "index.sync",
    "index.search",
    "index.search_skills",
    "index.hybrid_search",
    "index.insert_entry",
    "index.upsert_entry",
    "index.upsert_entries",
    "index.insert_reply",
    "index.delete_entry",
    "index.query_entries",
    "index.list_metadata",
]

_SOURCE_FACTORIES: dict[str, type[IndexSource]] | None = None


def _get_source_factories() -> dict[str, type[IndexSource]]:
    """延迟加载 source factories，避免循环导入。"""
    global _SOURCE_FACTORIES
    if _SOURCE_FACTORIES is None:
        from ..core import InboxSource, SkillsSource, TaskSource
        _SOURCE_FACTORIES = {
            "skills": SkillsSource,
            "inbox": InboxSource,
            "task": TaskSource,
        }
    return _SOURCE_FACTORIES


class IndexService:
    """向量索引服务。

    注册到 Hub 处理 index.* topics，管理 sqlite manifest、embedding、
    和 VecClient 的交互。在主进程后台线程中运行。
    """

    def __init__(
        self, hub_url: str, embedding: BaseEmbedding, vector_dimension: int
    ):
        self._hub_url = hub_url
        self._embedding = embedding
        self._vector_dimension = int(vector_dimension)
        if self._vector_dimension <= 0:
            raise ValueError(f"invalid vector_dimension: {self._vector_dimension}")
        _LOCALAGENT_DIR.mkdir(exist_ok=True)
        self._db = sqlite3.connect(str(_MANIFEST_DB), check_same_thread=False)
        self._db.execute("PRAGMA journal_mode=WAL")
        self._init_tables()
        self._vec_client: VecClient | None = None
        self._proc: subprocess.Popen | None = None
        self._thread: threading.Thread | None = None
        self._ready = threading.Event()
        self._stopping = threading.Event()
        self._ws: ClientConnection | None = None
        self._retry = RetryPolicy.for_service("index_service")
        self._vec_server_retry = RetryPolicy.for_service("vec_server")

    def _init_tables(self):
        self._db.executescript("""
            CREATE TABLE IF NOT EXISTS files (
                path         TEXT PRIMARY KEY,
                source       TEXT NOT NULL DEFAULT '',
                content_hash TEXT NOT NULL,
                size_bytes   INTEGER,
                mtime_ns     INTEGER,
                updated_at   TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS entries (
                id           TEXT PRIMARY KEY,
                source       TEXT NOT NULL DEFAULT 'memory',
                content      TEXT,
                metadata     TEXT,
                content_hash TEXT NOT NULL,
                label        TEXT,
                created_at   TEXT NOT NULL,
                archived_at  TEXT
            );
            CREATE TABLE IF NOT EXISTS settings (
                key          TEXT PRIMARY KEY,
                value        TEXT NOT NULL
            );
        """)
        # 兼容旧表：补充新增列
        cols = {
            row[1]
            for row in self._db.execute("PRAGMA table_info(entries)").fetchall()
        }
        for col, spec in [
            ("source", "TEXT NOT NULL DEFAULT 'memory'"),
            ("content", "TEXT"),
            ("metadata", "TEXT"),
            ("archived_at", "TEXT"),
        ]:
            if col not in cols:
                self._db.execute(
                    f"ALTER TABLE entries ADD COLUMN {col} {spec}"
                )

        file_cols = {
            row[1]
            for row in self._db.execute("PRAGMA table_info(files)").fetchall()
        }
        for col, spec in [
            ("source", "TEXT NOT NULL DEFAULT ''"),
            ("size_bytes", "INTEGER"),
            ("mtime_ns", "INTEGER"),
        ]:
            if col not in file_cols:
                self._db.execute(f"ALTER TABLE files ADD COLUMN {col} {spec}")

        self._db.execute(
            "UPDATE files SET source = 'legacy' WHERE source IS NULL OR source = ''"
        )
        self._db.execute(
            "CREATE INDEX IF NOT EXISTS idx_files_source ON files(source)"
        )
        self._db.commit()

    def _check_or_init_vector_dimension(self) -> None:
        row = self._db.execute(
            "SELECT value FROM settings WHERE key = 'embedding_dimension'"
        ).fetchone()
        if row is None:
            self._db.execute(
                "INSERT INTO settings (key, value) VALUES (?, ?)",
                ("embedding_dimension", str(self._vector_dimension)),
            )
            self._db.commit()
            return

        saved_dimension = int(row[0])
        if saved_dimension != self._vector_dimension:
            raise RuntimeError(
                "embedding dimension mismatch: "
                f"saved={saved_dimension}, current={self._vector_dimension}. "
                "请保持一致，或清理 .localagent/manifest.db 与 .localagent/.zvec 后重建索引。"
            )

    @staticmethod
    def _hash(content: str) -> str:
        return hashlib.sha256(content.encode()).hexdigest()

    def _ensure_vec_available(self) -> None:
        """确保 zvec 子进程存活，异常时抛出带 stderr 的错误。"""
        if self._proc is None:
            raise RuntimeError("zvec process is not started")
        code = self._proc.poll()
        if code is None:
            return
        stderr_text = ""
        if self._proc.stderr is not None:
            try:
                stderr_text = self._proc.stderr.read().strip()
            except Exception:
                stderr_text = ""
        if stderr_text:
            stderr_text = stderr_text[-1000:]
            raise RuntimeError(
                f"zvec exited unexpectedly (code={code}), stderr={stderr_text}"
            )
        raise RuntimeError(f"zvec exited unexpectedly (code={code})")

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self):
        """Start the index service in a background thread."""
        self._check_or_init_vector_dimension()
        self._proc, migrated = start_server(
            hub_url=self._hub_url,
            cwd=_LOCALAGENT_DIR,
            vector_dim=self._vector_dimension,
            connect_timeout=self._vec_server_retry.connect_timeout,
            base_delay=self._vec_server_retry.base_delay,
            max_delay=self._vec_server_retry.max_delay,
            jitter=self._vec_server_retry.jitter,
        )
        if migrated:
            self._db.execute("DELETE FROM files")
            self._db.commit()
            logger.info("zvec collection migrated, cleared file manifest for re-index")
        self._thread = threading.Thread(
            target=self._run, daemon=True, name="index-service"
        )
        self._thread.start()
        if not self._ready.wait(timeout=5):
            raise RuntimeError("IndexService failed to start within 5 seconds")
        logger.info("IndexService started")

    def _run(self):
        self._vec_client = VecClient(self._hub_url)
        try:
            reconnect_attempt = 0
            while not self._stopping.is_set():
                try:
                    self._ws = connect(
                        self._hub_url,
                        open_timeout=self._retry.connect_timeout,
                    )
                    self._ws.send(
                        json.dumps({
                            "type": "register",
                            "name": "index",
                            "topics": _INDEX_TOPICS,
                        })
                    )
                    if not self._ready.is_set():
                        self._ready.set()
                    reconnect_attempt = 0

                    for raw in self._ws:
                        msg = json.loads(raw)
                        if msg.get("type") != "request":
                            continue

                        topic = msg["topic"]
                        cmd = topic.split(".", 1)[1]

                        try:
                            handler = getattr(self, f"_handle_{cmd}", None)
                            if handler:
                                resp = handler(msg["payload"])
                            else:
                                resp = {"ok": False, "error": f"unknown command: {cmd}"}
                        except Exception as exc:
                            if self._stopping.is_set() and isinstance(
                                exc, (ConnectionClosed, RuntimeError)
                            ):
                                logger.debug(
                                    "IndexService request dropped during shutdown topic=%s: %s",
                                    topic,
                                    exc,
                                )
                            else:
                                logger.exception("IndexService error handling %s", topic)
                            resp = {"ok": False, "error": str(exc)}

                        try:
                            assert self._ws is not None
                            self._ws.send(
                                json.dumps({
                                    "type": "response",
                                    "id": msg["id"],
                                    "payload": resp,
                                })
                            )
                        except Exception as exc:
                            if self._stopping.is_set():
                                logger.debug(
                                    "IndexService response skipped during shutdown: %s", exc
                                )
                                break
                            raise
                except Exception as exc:
                    if self._stopping.is_set():
                        break
                    delay = self._retry.backoff_delay(reconnect_attempt)
                    reconnect_attempt += 1
                    logger.warning(
                        "IndexService hub connection lost, retry in %.2fs: %s",
                        delay,
                        exc,
                    )
                    time.sleep(delay)
                finally:
                    if self._ws:
                        try:
                            self._ws.close()
                        except Exception:
                            pass
                        self._ws = None
        finally:
            if self._vec_client:
                self._vec_client.close()
                self._vec_client = None

    def stop(self):
        """关闭服务。"""
        self._stopping.set()
        if self._vec_client is not None:
            try:
                # 主动请求 zvec 优雅退出，避免仅靠 ws 断开后子进程悬挂。
                self._vec_client.close()
            except Exception:
                logger.debug("VecClient close failed during stop", exc_info=True)
            finally:
                self._vec_client = None

        if self._ws:
            try:
                self._ws.close()
            except Exception:
                pass
        if self._thread:
            self._thread.join(timeout=5)
            self._thread = None
        if self._proc and self._proc.poll() is None:
            self._proc.terminate()
            try:
                self._proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self._proc.kill()
                self._proc.wait(timeout=5)
        self._proc = None
        self._db.close()
        logger.info("IndexService stopped")

    # ------------------------------------------------------------------
    # Request handlers
    # ------------------------------------------------------------------

    def _handle_sync(self, payload: dict) -> dict:
        source_name = payload["source"]
        path = payload["path"]

        factories = _get_source_factories()
        factory = factories.get(source_name)
        if not factory:
            return {"ok": False, "error": f"unknown source: {source_name}"}

        self._sync(factory(path))  # type: ignore
        return {"ok": True}

    def _handle_search(self, payload: dict) -> dict:
        self._ensure_vec_available()
        query = payload["query"]
        topk = payload.get("topk", 10)

        if "sources" in payload:
            factories = _get_source_factories()
            for src in payload["sources"]:
                if "path" not in src:
                    continue
                factory = factories.get(src["source"])
                if factory:
                    self._sync(factory(src["path"]))  # type: ignore

        assert self._vec_client is not None
        filt = payload.get("filter")
        embeddings = self._embedding.get_embeddings([query], text_type="query")
        results = self._vec_client.query(embeddings[0], topk=topk, filter=filt)
        results = self._enrich_results(results)
        return {"ok": True, "results": results}

    def _handle_search_skills(self, payload: dict) -> dict:
        self._ensure_vec_available()
        query = payload["query"]
        topk = payload.get("topk", 10)
        path = payload["path"]

        factories = _get_source_factories()
        factory = factories.get("skills")
        if factory:
            self._sync(factory(path))  # type: ignore

        assert self._vec_client is not None
        dense = self._embedding.get_embeddings([query], text_type="query")[0]
        results = self._vec_client.hybrid_query(dense, query, topk=topk)
        results = self._enrich_results(results)
        return {"ok": True, "results": results}

    def _handle_hybrid_search(self, payload: dict) -> dict:
        self._ensure_vec_available()
        query = payload["query"]
        topk = payload.get("topk", 10)

        if "sources" in payload:
            factories = _get_source_factories()
            for src in payload["sources"]:
                if "path" not in src:
                    continue
                factory = factories.get(src["source"])
                if factory:
                    self._sync(factory(src["path"]))  # type: ignore

        assert self._vec_client is not None
        filt = payload.get("filter")
        dense = self._embedding.get_embeddings([query], text_type="query")[0]
        results = self._vec_client.hybrid_query(dense, query, topk=topk, filter=filt)
        results = self._enrich_results(results)
        return {"ok": True, "results": results}

    def _enrich_results(self, results: list[dict]) -> list[dict]:
        if not results:
            return results
        ids = [str(item.get("id", "")).strip() for item in results if str(item.get("id", "")).strip()]
        if not ids:
            return results
        placeholders = ",".join("?" for _ in ids)
        rows = self._db.execute(
            f"SELECT id, source, label, content, metadata, created_at, archived_at FROM entries WHERE id IN ({placeholders})",
            ids,
        ).fetchall()
        entry_map: dict[str, dict[str, object]] = {}
        for row in rows:
            metadata = {}
            if row[4]:
                try:
                    metadata = json.loads(row[4])
                except Exception:
                    metadata = {}
            entry_map[str(row[0])] = {
                "source": row[1],
                "label": row[2] or "",
                "content": row[3] or "",
                "metadata": metadata,
                "created_at": row[5] or "",
                "archived_at": row[6] or "",
            }
        enriched: list[dict] = []
        for item in results:
            doc_id = str(item.get("id", "")).strip()
            merged = dict(item)
            fields = dict(merged.get("fields") or {})
            if doc_id in entry_map:
                extra = entry_map[doc_id]
                fields.setdefault("source", extra.get("source", ""))
                fields.setdefault("label", extra.get("label", ""))
                fields.setdefault("content", extra.get("content", ""))
                fields.setdefault("metadata", extra.get("metadata", {}))
                fields.setdefault("created_at", extra.get("created_at", ""))
                fields.setdefault("archived_at", extra.get("archived_at", ""))
            merged["fields"] = fields
            enriched.append(merged)
        return enriched

    def _handle_insert_entry(self, payload: dict) -> dict:
        self._ensure_vec_available()
        text = payload["text"]
        label = payload.get("label")
        prefix = payload.get("prefix", "l3")
        source = payload.get("source", "memory")
        content = payload.get("content")
        metadata = payload.get("metadata")

        h = self._hash(text)
        # zvec doc_id 需匹配特定 regex，且长度需 <= 64。
        # 使用 prefix+content_hash 的 sha256 作为稳定且合法的 doc_id。
        entry_id = hashlib.sha256(f"{prefix}:{h}".encode()).hexdigest()

        existing = self._db.execute(
            "SELECT id FROM entries WHERE content_hash = ?", (h,)
        ).fetchone()
        if existing:
            return {"ok": True, "id": existing[0], "exists": True}

        assert self._vec_client is not None
        embeddings = self._embedding.get_embeddings([text], text_type="document")
        self._vec_client.insert([
            {
                "id": entry_id,
                "vectors": {"embedding": embeddings[0]},
                "fields": {"text": text, "source": source},
            }
        ])
        self._vec_client.flush()

        now = datetime.now(timezone.utc).isoformat()
        self._db.execute(
            "INSERT INTO entries"
            " (id, content_hash, label, source, content, metadata,"
            "  created_at, archived_at)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (entry_id, h, label, source, content, metadata, now, now),
        )
        self._db.commit()
        return {"ok": True, "id": entry_id, "exists": False}

    def _handle_upsert_entry(self, payload: dict) -> dict:
        resp = self._handle_upsert_entries({"entries": [payload]})
        if not resp.get("ok"):
            return resp
        entry_id = str(payload.get("id", "")).strip()
        if not entry_id:
            return {"ok": False, "error": "id is required"}
        return {
            "ok": True,
            "id": entry_id,
            "count": int(resp.get("count", 0)),
        }

    def _handle_upsert_entries(self, payload: dict) -> dict:
        self._ensure_vec_available()
        raw_entries = payload.get("entries", []) or []
        normalized_entries: list[dict[str, object]] = []
        texts: list[str] = []
        delete_ids: set[str] = set()
        topic_cleanup_map: dict[str, list[str]] = {}
        if not isinstance(raw_entries, list):
            return {"ok": False, "error": "entries must be a list"}

        for raw in raw_entries:
            if not isinstance(raw, dict):
                continue
            entry_id = str(raw.get("id", "")).strip()
            text = str(raw.get("text", ""))
            if not entry_id or not text:
                continue
            label = raw.get("label")
            source = str(raw.get("source", "memory")).strip() or "memory"
            content = raw.get("content")
            metadata = raw.get("metadata")
            existing = self._db.execute(
                "SELECT created_at FROM entries WHERE id = ?",
                (entry_id,),
            ).fetchone()
            created_at = str(existing[0]) if existing and existing[0] else ""
            if existing is not None:
                delete_ids.add(entry_id)

            topic_cleanup_ids: list[str] = []
            if source == "topic":
                metadata_obj: dict[str, object] = {}
                if isinstance(metadata, str) and metadata.strip():
                    try:
                        parsed_metadata = json.loads(metadata)
                        if isinstance(parsed_metadata, dict):
                            metadata_obj = parsed_metadata
                    except Exception:
                        metadata_obj = {}
                topic_id = str(metadata_obj.get("topic_id", "")).strip()
                if topic_id:
                    legacy_rows = self._db.execute(
                        """SELECT id
                           FROM entries
                           WHERE source = 'topic'
                             AND id != ?
                             AND (label = ? OR label = ?)""",
                        (entry_id, topic_id, f"topic:{topic_id}"),
                    ).fetchall()
                    topic_cleanup_ids = [
                        str(row[0]).strip()
                        for row in legacy_rows
                        if row and str(row[0]).strip()
                    ]
                    for legacy_id in topic_cleanup_ids:
                        delete_ids.add(legacy_id)
            topic_cleanup_map[entry_id] = topic_cleanup_ids
            normalized_entries.append({
                "id": entry_id,
                "text": text,
                "label": label,
                "source": source,
                "content": content,
                "metadata": metadata,
                "created_at": created_at,
            })
            texts.append(text)

        if not normalized_entries:
            return {"ok": True, "count": 0}

        assert self._vec_client is not None
        embeddings = self._embedding.get_embeddings(texts, text_type="document")
        if delete_ids:
            self._vec_client.delete(sorted(delete_ids))
        docs = []
        for item, emb in zip(normalized_entries, embeddings):
            docs.append({
                "id": str(item["id"]),
                "vectors": {"embedding": emb},
                "fields": {"text": str(item["text"]), "source": str(item["source"])},
            })
        self._vec_client.insert(docs)
        self._vec_client.flush()

        for item in normalized_entries:
            topic_cleanup_ids = topic_cleanup_map.get(str(item["id"]), [])
            if topic_cleanup_ids:
                placeholders = ",".join("?" for _ in topic_cleanup_ids)
                self._db.execute(
                    f"DELETE FROM entries WHERE id IN ({placeholders})",
                    tuple(topic_cleanup_ids),
                )
            now = datetime.now(timezone.utc).isoformat()
            self._db.execute(
                "INSERT OR REPLACE INTO entries"
                " (id, content_hash, label, source, content, metadata,"
                "  created_at, archived_at)"
                " VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    str(item["id"]),
                    self._hash(str(item["text"])),
                    item["label"],
                    str(item["source"]),
                    item["content"],
                    item["metadata"],
                    str(item["created_at"] or now),
                    now,
                ),
            )
        self._db.commit()
        return {"ok": True, "count": len(normalized_entries)}

    def _handle_insert_reply(self, payload: dict) -> dict:
        """将回复记录仅写入 SQLite，不做 embedding / 向量存储。"""
        text = payload["text"]
        source = "reply"
        content = payload.get("content")
        metadata = payload.get("metadata")

        h = self._hash(text)
        entry_id = hashlib.sha256(f"reply:{h}".encode()).hexdigest()

        existing = self._db.execute(
            "SELECT id FROM entries WHERE id = ?", (entry_id,)
        ).fetchone()
        if existing:
            return {"ok": True, "id": existing[0], "exists": True}

        now = datetime.now(timezone.utc).isoformat()
        self._db.execute(
            "INSERT INTO entries"
            " (id, content_hash, label, source, content, metadata,"
            "  created_at, archived_at)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (entry_id, h, None, source, content, metadata, now, now),
        )
        self._db.commit()
        return {"ok": True, "id": entry_id, "exists": False}

    def _handle_delete_entry(self, payload: dict) -> dict:
        self._ensure_vec_available()
        entry_id = payload["id"]

        assert self._vec_client is not None
        self._vec_client.delete([entry_id])
        self._vec_client.flush()

        self._db.execute("DELETE FROM entries WHERE id = ?", (entry_id,))
        self._db.commit()
        return {"ok": True}

    def _handle_query_entries(self, payload: dict) -> dict:
        """结构化检索：按 source / time / keyword 过滤 entries 表。"""
        source = payload.get("source", "")
        time_after = self._resolve_time(payload.get("time_after", ""))
        time_before = self._resolve_time(payload.get("time_before", ""))
        keyword = payload.get("keyword", "")
        page = payload.get("page", 1)
        page_size = payload.get("page_size", 20)

        conditions: list[str] = []
        params: list[object] = []

        if source:
            conditions.append("source = ?")
            params.append(source)
        if time_after:
            conditions.append("archived_at >= ?")
            params.append(time_after)
        if time_before:
            conditions.append("archived_at <= ?")
            params.append(time_before)
        if keyword:
            conditions.append("(content LIKE ? OR metadata LIKE ?)")
            params.extend([f"%{keyword}%", f"%{keyword}%"])

        where = " AND ".join(conditions) if conditions else "1=1"

        total = self._db.execute(
            f"SELECT COUNT(*) FROM entries WHERE {where}", params
        ).fetchone()[0]

        offset = (page - 1) * page_size
        rows = self._db.execute(
            f"SELECT id, source, content, metadata, created_at, archived_at"
            f" FROM entries WHERE {where}"
            f" ORDER BY archived_at DESC"
            f" LIMIT ? OFFSET ?",
            params + [page_size, offset],
        ).fetchall()

        items = []
        for row in rows:
            items.append({
                "id": row[0],
                "source": row[1],
                "content": (row[2] or "")[:200],
                "metadata": json.loads(row[3]) if row[3] else {},
                "created_at": row[4],
                "archived_at": row[5],
            })

        return {
            "ok": True,
            "items": items,
            "total": total,
            "page": page,
            "page_size": page_size,
        }

    def _handle_list_metadata(self, payload: dict) -> dict:
        """列出指定 source 的所有 metadata 字段及去重值。"""
        source = payload["source"]
        rows = self._db.execute(
            "SELECT metadata FROM entries"
            " WHERE source = ? AND metadata IS NOT NULL",
            (source,),
        ).fetchall()

        fields: dict[str, set[str]] = {}
        for (meta_json,) in rows:
            try:
                meta = json.loads(meta_json)
            except (json.JSONDecodeError, TypeError):
                continue
            for key, value in meta.items():
                if key not in fields:
                    fields[key] = set()
                fields[key].add(str(value))

        return {
            "ok": True,
            "fields": {k: sorted(v) for k, v in fields.items()},
        }

    @staticmethod
    def _resolve_time(time_str: str) -> str:
        """将相对时间（如 '7d', '2w'）转为 ISO 格式。"""
        if not time_str:
            return ""
        m = re.match(r"^(\d+)([dw])$", time_str)
        if m:
            n, unit = int(m.group(1)), m.group(2)
            delta = timedelta(days=n) if unit == "d" else timedelta(weeks=n)
            return (datetime.now(timezone.utc) - delta).isoformat()
        return time_str

    # ------------------------------------------------------------------
    # Internal sync
    # ------------------------------------------------------------------

    def _sync(self, source: IndexSource):
        self._ensure_vec_available()
        assert self._vec_client is not None
        source_name = source.name
        files = source.discover()
        current_paths = {str(p) for p in files}

        rows = self._db.execute(
            "SELECT path, content_hash, size_bytes, mtime_ns FROM files WHERE source = ?",
            (source_name,),
        ).fetchall()
        tracked = {
            row[0]: {
                "content_hash": row[1],
                "size_bytes": row[2],
                "mtime_ns": row[3],
            }
            for row in rows
        }

        to_insert: list[tuple[str, str, str, int, int]] = []
        to_touch: list[tuple[int, int, str, str, str]] = []
        to_delete: list[str] = []

        for path in files:
            spath = str(path)
            stat = path.stat()
            size_bytes = int(stat.st_size)
            mtime_ns = int(stat.st_mtime_ns)
            prev = tracked.get(spath)
            if prev is not None:
                prev_size = prev.get("size_bytes")
                prev_mtime = prev.get("mtime_ns")
                if prev_size == size_bytes and prev_mtime == mtime_ns:
                    continue

            content = path.read_text(encoding="utf-8")
            h = self._hash(content)
            if prev is None:
                to_insert.append((spath, source.extract_text(path), h, size_bytes, mtime_ns))
            elif prev["content_hash"] != h:
                to_delete.append(spath)
                to_insert.append((spath, source.extract_text(path), h, size_bytes, mtime_ns))
            else:
                to_touch.append(
                    (
                        size_bytes,
                        mtime_ns,
                        datetime.now(timezone.utc).isoformat(),
                        source_name,
                        spath,
                    )
                )

        for spath in tracked:
            if spath not in current_paths:
                to_delete.append(spath)

        if to_delete:
            self._vec_client.delete(
                [hashlib.sha256(p.encode()).hexdigest() for p in to_delete]
            )
            self._db.executemany(
                "DELETE FROM files WHERE source = ? AND path = ?",
                [(source_name, p) for p in to_delete],
            )

        if to_insert:
            texts = [t[1] for t in to_insert]
            embeddings = self._embedding.get_embeddings(texts, text_type="document")
            now = datetime.now(timezone.utc).isoformat()
            docs = []
            for (spath, text, h, size_bytes, mtime_ns), emb in zip(to_insert, embeddings):
                docs.append({
                    "id": hashlib.sha256(spath.encode()).hexdigest(),
                    "vectors": {"embedding": emb},
                    "fields": {"text": text, "path": spath, "source": source_name},
                })
                self._db.execute(
                    "INSERT OR REPLACE INTO files"
                    " (path, source, content_hash, size_bytes, mtime_ns, updated_at)"
                    " VALUES (?, ?, ?, ?, ?, ?)",
                    (spath, source_name, h, size_bytes, mtime_ns, now),
                )
            self._vec_client.insert(docs)

        if to_touch:
            self._db.executemany(
                "UPDATE files SET size_bytes = ?, mtime_ns = ?, updated_at = ?"
                " WHERE source = ? AND path = ?",
                to_touch,
            )

        if to_delete or to_insert:
            self._vec_client.flush()
        if to_delete or to_insert or to_touch:
            self._db.commit()
