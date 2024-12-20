from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from resonate.cmd_queue import CmdQ


class ITaskSource(ABC):
    @abstractmethod
    def start(self, cmd_queue: CmdQ, pid: str) -> None: ...

    @abstractmethod
    def default_recv(self, pid: str) -> dict[str, Any]: ...
