from __future__ import annotations

from concurrent.futures import Future
from typing import TYPE_CHECKING, Any, Generic, Literal, TypeVar, final

from typing_extensions import TypeAlias

if TYPE_CHECKING:
    from resonate.stores.record import DurablePromiseRecord, TaskRecord

T = TypeVar("T")

ActionCategory: TypeAlias = Literal["local", "remote"]


@final
class Promise(Generic[T]):
    def __init__(self, id: str) -> None:
        self.id = id


@final
class Handle(Generic[T]):
    def __init__(self, id: str, future: Future[T]) -> None:
        self.id = id
        self._f = future

    def result(self, timeout: float | None = None) -> T:
        return self._f.result(timeout=timeout)


@final
class Record(Generic[T]):
    def __init__(
        self,
        id: str,
        durable_promise: DurablePromiseRecord | None,
        task: TaskRecord | None,
    ) -> None:
        self.id = id
        self.f = Future[T]()
        self.promise = Promise[T](id=id)
        self.handle = Handle[T](id=self.id, future=self.f)
        self.durable_promise = durable_promise
        self.task = task
        self.children: list[Record[Any]] = []
        self.leafs: set[Record[Any]] = set()

    def done(self) -> bool:
        return self.f.done()
