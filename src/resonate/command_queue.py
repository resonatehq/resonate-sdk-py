from __future__ import annotations

from dataclasses import dataclass
from queue import Queue
from typing import TYPE_CHECKING, Any, Union

from typing_extensions import TypeAlias

if TYPE_CHECKING:
    from resonate.result import Result
    from resonate.stores.record import TaskRecord


@dataclass(frozen=True)
class Invoke:
    id: str


@dataclass(frozen=True)
class Resume:
    id: str


@dataclass(frozen=True)
class Complete:
    id: str
    result: Result[Any, Exception]


@dataclass(frozen=True)
class Claim:
    record: TaskRecord


CQE: TypeAlias = Union[Invoke, Resume, Complete, Claim]


class CQ:
    def __init__(self) -> None:
        self._q = Queue[Union[CQE, None]]()

    def enqueue(self, item: CQE) -> None:
        self._q.put(item)

    def dequeue(self) -> CQE | None:
        return self._q.get()

    def stop(self) -> None:
        self._q.put(None)
