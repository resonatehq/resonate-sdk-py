from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class ITaskSource(ABC):
    @abstractmethod
    def start(self) -> None: ...

    @abstractmethod
    def default_recv(self, pid: str) -> dict[str, Any]: ...
