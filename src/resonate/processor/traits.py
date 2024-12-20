from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from resonate.cmd_queue import CmdQ
    from resonate.dataclasses import SQE


class IProcessor(ABC):
    @abstractmethod
    def start(self, cmd_queue: CmdQ) -> None: ...

    @abstractmethod
    def enqueue(self, sqe: SQE[Any]) -> None: ...
