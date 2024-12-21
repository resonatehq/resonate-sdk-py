from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from resonate.traits import SubSystem


class ITaskSource(SubSystem, ABC):
    @abstractmethod
    def default_recv(self) -> dict[str, Any]: ...
