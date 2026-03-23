"""Shared retry policy helpers."""

from __future__ import annotations

import random
from dataclasses import dataclass

from .config import cfg


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    """Retry configuration for a specific service."""

    max_retries: int
    base_delay: float
    max_delay: float
    jitter: float
    connect_timeout: float
    request_timeout: float
    retry_after_send: bool

    @classmethod
    def for_service(cls, service: str) -> "RetryPolicy":
        values = cfg.retry(service)
        return cls(
            max_retries=int(values["max_retries"]),
            base_delay=float(values["base_delay"]),
            max_delay=float(values["max_delay"]),
            jitter=float(values["jitter"]),
            connect_timeout=float(values["connect_timeout"]),
            request_timeout=float(values["request_timeout"]),
            retry_after_send=bool(values["retry_after_send"]),
        )

    def backoff_delay(self, retry_index: int) -> float:
        """Return the delay (seconds) before the next retry."""
        delay = min(self.base_delay * (2**max(retry_index, 0)), self.max_delay)
        if self.jitter > 0:
            factor = random.uniform(1.0 - self.jitter, 1.0 + self.jitter)
            delay *= factor
        return max(delay, 0.0)
