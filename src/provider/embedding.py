"""Embedding 服务商抽象。

不同服务商（Qwen/DashScope、OpenAI 等）各自实现 get_embeddings 方法。
text_type 参数仅部分服务商支持，不支持的实现直接忽略即可。
"""

from abc import ABC, abstractmethod
import time
from typing import Literal

from ..retry import RetryPolicy


class BaseEmbedding(ABC):
    """向量嵌入基类。"""

    @abstractmethod
    def get_embeddings(
        self,
        texts: list[str],
        text_type: Literal["query", "document"] = "document",
    ) -> list[list[float]]:
        """获取一组文本的向量表示。

        Args:
            texts: 要获取向量的文本列表
            text_type: 文本类型，'query' 用于查询，'document' 用于文档存储。
                       部分服务商可能忽略此参数。

        Returns:
            按输入顺序返回的向量列表
        """
        ...


class QwenEmbedding(BaseEmbedding):
    """基于 DashScope 的向量嵌入实现。"""

    def __init__(
        self,
        api_key: str,
        model: str = "text-embedding-v4",
        dimension: int = 1024,
        batch_size: int = 10,
    ):
        self._api_key = api_key
        self._model = model
        self._dimension = dimension
        self._batch_size = batch_size
        self._retry = RetryPolicy.for_service("embedding_qwen")

    def get_embeddings(
        self,
        texts: list[str],
        text_type: Literal["query", "document"] = "document",
    ) -> list[list[float]]:
        if not texts:
            return []

        if not self._api_key:
            raise RuntimeError("DASHSCOPE_API_KEY not set")

        import dashscope

        dashscope.api_key = self._api_key

        results: list[list[float]] = []

        for i in range(0, len(texts), self._batch_size):
            batch = texts[i : i + self._batch_size]
            last_exc: Exception | None = None
            resp = None
            for attempt in range(self._retry.max_retries + 1):
                try:
                    resp = dashscope.TextEmbedding.call(
                        model=self._model,
                        input=batch,
                        text_type=text_type,
                        dimension=self._dimension,
                        request_timeout=self._retry.request_timeout,
                    )
                    break
                except Exception as exc:
                    last_exc = exc
                    if attempt >= self._retry.max_retries:
                        break
                    time.sleep(self._retry.backoff_delay(attempt))
            if resp is None:
                raise RuntimeError("dashscope embedding request failed") from last_exc
            output = getattr(resp, "output", None) or {}
            embeddings = output.get("embeddings", []) if isinstance(output, dict) else []
            if not embeddings:
                code = getattr(resp, "code", None)
                message = getattr(resp, "message", None)
                request_id = getattr(resp, "request_id", None)
                raise RuntimeError(
                    f"dashscope embedding failed code={code} message={message} request_id={request_id}"
                )
            results.extend(item["embedding"] for item in embeddings)

        return results


class OpenAIEmbedding(BaseEmbedding):
    """基于 OpenAI Embeddings API 的向量嵌入实现。"""

    def __init__(
        self,
        api_key: str,
        model: str = "text-embedding-3-small",
        dimension: int = 1536,
        batch_size: int = 100,
        base_url: str = "https://api.openai.com/v1",
    ):
        self._api_key = api_key
        self._model = model
        self._dimension = dimension
        self._batch_size = batch_size
        self._base_url = base_url
        self._retry = RetryPolicy.for_service("embedding_openai")

    def get_embeddings(
        self,
        texts: list[str],
        text_type: Literal["query", "document"] = "document",
    ) -> list[list[float]]:
        del text_type  # OpenAI 接口无 text_type 参数
        if not texts:
            return []

        if not self._api_key:
            raise RuntimeError("OPENAI_API_KEY not set")

        from openai import OpenAI

        client = OpenAI(
            api_key=self._api_key,
            base_url=self._base_url,
            timeout=self._retry.request_timeout,
        )
        results: list[list[float]] = []

        for i in range(0, len(texts), self._batch_size):
            batch = texts[i : i + self._batch_size]
            last_exc: Exception | None = None
            resp = None
            for attempt in range(self._retry.max_retries + 1):
                try:
                    resp = client.embeddings.create(
                        model=self._model,
                        input=batch,
                        dimensions=self._dimension,
                    )
                    break
                except Exception as exc:
                    last_exc = exc
                    if attempt >= self._retry.max_retries:
                        break
                    time.sleep(self._retry.backoff_delay(attempt))
            if resp is None:
                raise RuntimeError("openai embedding request failed") from last_exc
            if not resp.data:
                raise RuntimeError("openai embedding failed: empty data")
            results.extend(item.embedding for item in resp.data)

        return results


def create_embedding(
    provider_name: str,
    *,
    api_key: str,
    model: str,
    dimension: int,
    base_url: str = "",
) -> BaseEmbedding:
    """根据 provider_name 创建 embedding 服务实例。"""
    if provider_name == "qwen":
        return QwenEmbedding(api_key=api_key, model=model, dimension=dimension)
    if provider_name == "openai":
        resolved_base_url = base_url or "https://api.openai.com/v1"
        return OpenAIEmbedding(
            api_key=api_key,
            model=model,
            dimension=dimension,
            base_url=resolved_base_url,
        )
    raise RuntimeError(f"unsupported embedding provider: {provider_name}")
