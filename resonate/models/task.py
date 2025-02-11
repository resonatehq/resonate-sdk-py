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

    def claim(self, pid: str, ttl: int) -> InvokeMesg | ResumeMesg:
        return self.store.tasks.claim(id=self.id, counter=self.counter, pid=pid, ttl=ttl)

    def complete(self) -> None:
        return self.store.tasks.complete(id=self.id, counter=self.counter)

    @classmethod
    def from_dict(cls, store: Store, data: dict[str, Any]) -> Task:
        return cls(
            id=data["id"],
            counter=data["counter"],
            store=store,
        )
