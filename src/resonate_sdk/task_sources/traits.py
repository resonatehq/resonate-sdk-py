from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from queue import Queue

    from resonate_sdk.store.models import TaskRecord


class ITaskSource(ABC):
    @abstractmethod
    def start(self, cq: Queue[TaskRecord | None], pid: str) -> None: ...

    @abstractmethod
    def stop(self) -> None: ...

    @abstractmethod
    def recv(self, pid: str) -> dict[str, Any]: ...
