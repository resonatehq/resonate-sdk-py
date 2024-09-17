from __future__ import annotations

from dataclasses import dataclass, field
from typing import final

from resonate.retry_policy import RetryPolicy, exponential


@final
@dataclass(frozen=True)
class Options:
    durable: bool = field(default=True)
    promise_id: str | None = None
    retry_policy: RetryPolicy = field(
        default=exponential(base_delay=1, factor=2, max_retries=5)
    )
