from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

from resonate.traits import SubSystem

if TYPE_CHECKING:
    from resonate.dataclasses import SQE


class IProcessor(SubSystem, ABC):
    @abstractmethod
    def enqueue(self, sqe: SQE[Any]) -> None: ...
