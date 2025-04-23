from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from resonate.stores.local import DurablePromiseRecord


class Router(Protocol):
    def route(self, promise: DurablePromiseRecord) -> Any: ...
