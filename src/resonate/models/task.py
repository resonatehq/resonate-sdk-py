from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from resonate.models.message import InvokeMesg, ResumeMesg
    from resonate.models.store import Store


@dataclass
class Task:
    id: str
    counter: int

    store: Store

    def claim(self, pid: str, ttl: int) -> InvokeMesg | ResumeMesg: ...
    def complete(self) -> None: ...

    @classmethod
    def from_dict(cls, store: Store, data: dict[str, Any]) -> Task:
        return cls(
            id=data["id"],
            counter=data["counter"],
            store=store,
        )
