"""zvec server. Runs under py3.12 environment.

Connects to the Hub as a WebSocket client, registers vec.* topics,
and handles vector database commands.

Collection is stored in .zvec/ under the working directory.
"""

import json
import os
import random
import shutil
import sys
import time
import traceback
import argparse

import zvec
from websockets.sync.client import connect

COLLECTION_DIR = ".zvec"
VECTOR_FIELD = "embedding"
SPARSE_FIELD = "bm25_sparse"


def open_or_create(path: str, vector_dim: int) -> tuple[zvec.Collection, bool]:
    migrated = False
    if os.path.exists(path):
        collection = zvec.open(path)
        actual_dim = collection.schema.vector(VECTOR_FIELD).dimension
        if actual_dim != vector_dim:
            raise RuntimeError(
                "zvec dimension mismatch: "
                f"collection={actual_dim}, requested={vector_dim}"
            )
        # 旧 collection 若缺少 path/source/sparse 任一字段，直接重建。
        # 当前 zvec 版本不支持为旧 collection 在线补 STRING 列。
        if (
            collection.schema.field("path") is None
            or collection.schema.field("source") is None
            or collection.schema.vector(SPARSE_FIELD) is None
        ):
            del collection
            shutil.rmtree(path)
            migrated = True
        else:
            return collection, migrated
    schema = zvec.CollectionSchema(
        name="localagent",
        fields=[
            zvec.FieldSchema("text", zvec.DataType.STRING, nullable=True),
            zvec.FieldSchema("path", zvec.DataType.STRING, nullable=True),
            zvec.FieldSchema("source", zvec.DataType.STRING, nullable=True),
        ],
        vectors=[
            zvec.VectorSchema(VECTOR_FIELD, zvec.DataType.VECTOR_FP32, vector_dim),
            zvec.VectorSchema(SPARSE_FIELD, zvec.DataType.SPARSE_VECTOR_FP32, 0),
        ],
    )
    return zvec.create_and_open(path=path, schema=schema), migrated


def handle(
    collection: zvec.Collection,
    bm25_doc: zvec.BM25EmbeddingFunction,
    bm25_query: zvec.BM25EmbeddingFunction,
    req: dict,
) -> dict:
    cmd = req.get("cmd")

    if cmd == "insert":
        docs = []
        for d in req["docs"]:
            vectors = d.get("vectors") or {}
            fields = d.get("fields") or {}
            text = fields.get("text", "")
            if text and text.strip() and SPARSE_FIELD not in vectors:
                vectors[SPARSE_FIELD] = bm25_doc.embed(text)
            docs.append(
                zvec.Doc(id=d["id"], vectors=vectors, fields=fields)
            )
        statuses = collection.insert(docs)
        if not isinstance(statuses, list):
            statuses = [statuses]
        return {"ok": True, "count": len(statuses)}

    elif cmd == "query":
        results = collection.query(
            zvec.VectorQuery(req.get("field", VECTOR_FIELD), vector=req["vector"]),
            topk=req.get("topk", 10),
            filter=req.get("filter"),
        )
        return {
            "ok": True,
            "results": [
                {"id": doc.id, "score": doc.score, "fields": doc.fields}
                for doc in results
            ],
        }

    elif cmd == "hybrid_query":
        dense = req["vector"]
        query_text = req["query_text"]
        topk = req.get("topk", 10)
        filt = req.get("filter")
        sparse = bm25_query.embed(query_text)
        results = collection.query(
            vectors=[
                zvec.VectorQuery(VECTOR_FIELD, vector=dense),
                zvec.VectorQuery(SPARSE_FIELD, vector=sparse),
            ],
            topk=topk,
            filter=filt,
            reranker=zvec.RrfReRanker(topn=topk),
        )
        return {
            "ok": True,
            "results": [
                {"id": doc.id, "score": doc.score, "fields": doc.fields}
                for doc in results
            ],
        }

    elif cmd == "delete":
        ids = req["ids"]
        statuses = collection.delete(ids)
        if not isinstance(statuses, list):
            statuses = [statuses]
        return {"ok": True, "count": len(statuses)}

    elif cmd == "flush":
        collection.flush()
        return {"ok": True}

    elif cmd == "shutdown":
        collection.flush()
        return {"ok": True, "cmd": "shutdown"}

    else:
        return {"ok": False, "error": f"unknown command: {cmd}"}


_VEC_TOPICS = [
    "vec.insert",
    "vec.query",
    "vec.hybrid_query",
    "vec.delete",
    "vec.flush",
    "vec.shutdown",
]


def main():
    parser = argparse.ArgumentParser(description="zvec server")
    parser.add_argument("hub_url")
    parser.add_argument("vector_dim", nargs="?", default="1024")
    parser.add_argument("--connect-timeout", type=float, default=5.0)
    parser.add_argument("--base-delay", type=float, default=0.5)
    parser.add_argument("--max-delay", type=float, default=8.0)
    parser.add_argument("--jitter", type=float, default=0.1)
    args = parser.parse_args()

    hub_url = args.hub_url
    vector_dim = int(args.vector_dim)
    root = os.getcwd()
    collection, migrated = open_or_create(os.path.join(root, COLLECTION_DIR), vector_dim)

    bm25_doc = zvec.BM25EmbeddingFunction(language="zh", encoding_type="document")
    bm25_query = zvec.BM25EmbeddingFunction(language="zh", encoding_type="query")

    ready_sent = False
    reconnect_attempt = 0

    while True:
        try:
            with connect(hub_url, open_timeout=max(args.connect_timeout, 0.1)) as ws:
                ws.send(json.dumps({
                    "type": "register",
                    "name": "zvec",
                    "topics": _VEC_TOPICS,
                }))
                if not ready_sent:
                    sys.stdout.write(
                        json.dumps({
                            "ok": True,
                            "cmd": "ready",
                            "vector_dim": vector_dim,
                            "migrated": migrated,
                        })
                        + "\n"
                    )
                    sys.stdout.flush()
                    ready_sent = True
                reconnect_attempt = 0

                for raw in ws:
                    msg = json.loads(raw)
                    if msg.get("type") != "request":
                        continue

                    req = msg["payload"]
                    try:
                        resp = handle(collection, bm25_doc, bm25_query, req)
                    except Exception as exc:
                        # 任何单次请求错误都返回给调用方，不让进程整体崩溃。
                        resp = {"ok": False, "error": f"{type(exc).__name__}: {exc}"}

                    ws.send(json.dumps({
                        "type": "response",
                        "id": msg["id"],
                        "payload": resp,
                    }))

                    if resp.get("cmd") == "shutdown":
                        return
            delay = min(args.base_delay * (2**reconnect_attempt), args.max_delay)
            if args.jitter > 0:
                factor = random.uniform(1.0 - args.jitter, 1.0 + args.jitter)
                delay *= factor
            delay = max(delay, 0.0)
            reconnect_attempt += 1
            time.sleep(delay)
        except Exception:
            delay = min(args.base_delay * (2**reconnect_attempt), args.max_delay)
            if args.jitter > 0:
                factor = random.uniform(1.0 - args.jitter, 1.0 + args.jitter)
                delay *= factor
            delay = max(delay, 0.0)
            reconnect_attempt += 1
            time.sleep(delay)


if __name__ == "__main__":
    try:
        main()
    except Exception:
        traceback.print_exc()
        raise
