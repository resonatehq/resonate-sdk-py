from __future__ import annotations

from concurrent.futures import Future
from inspect import isgenerator
from typing import TYPE_CHECKING, Any, Generic, Literal, TypeVar, final

from typing_extensions import TypeAlias

from resonate.dataclasses import ResonateCoro

if TYPE_CHECKING:
    from resonate.context import Context
    from resonate.dataclasses import FnOrCoroutine
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
        parent_id: str | None,
        func: FnOrCoroutine[T] | None,
        durable_promise: DurablePromiseRecord | None,
        task: TaskRecord | None,
    ) -> None:
        self.id = id
        self.parent_id = parent_id
        self.f = Future[T]()
        self.promise = Promise[T](id=id)
        self.handle = Handle[T](id=self.id, future=self.f)
        self.durable_promise = durable_promise
        self.task = task
        self.func = func
        self.children: list[Record[Any]] = []

    def done(self) -> bool:
        return self.f.done()

    def resonate_coro(self, ctx: Context) -> ResonateCoro[T]:
        assert self.func is not None
        assert isgenerator(self.func.exec_unit)
        return ResonateCoro[T](
            record=self,
            coro=self.func.exec_unit(ctx, *self.func.args, **self.func.kwargs),
        )
