from __future__ import annotations

from concurrent.futures import Future
from typing import Any, Protocol

from resonate.models.durable_promise import DurablePromise


class Enqueueable[T](Protocol):
    def enqueue(self, item: T, /, futures: tuple[Future[DurablePromise], Future[Any]]) -> None: ...
