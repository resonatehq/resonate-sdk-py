from __future__ import annotations

from concurrent.futures import Future
from typing import TYPE_CHECKING, Any, Generic, Literal, TypeVar, final

from typing_extensions import TypeAlias, assert_never

from resonate.result import Err, Ok, Result

if TYPE_CHECKING:
    from resonate.context import Context
    from resonate.dataclasses import Invocation
    from resonate.options import Options
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
    def __init__(  # noqa: PLR0913
        self,
        id: str,
        parent_id: str | None,
        durable_promise: DurablePromiseRecord | None,
        task: TaskRecord | None,
        invocation: Invocation[T] | None,
        opts: Options | None,
        ctx: Context,
    ) -> None:
        self.id = id
        self.parent_id = parent_id
        self.f = Future[T]()
        self.children: list[Record[Any]] = []
        self.promise = Promise[T](id=id)
        self.handle = Handle[T](id=self.id, future=self.f)
        self.durable_promise = durable_promise
        self.task = task
        self.invocation = invocation
        self.opts = opts
        self.ctx = ctx

    def set_result(self, result: Result[Any, Exception]) -> None:
        assert all(
            r.done() for r in self.children
        ), "All children record must be completed."
        if isinstance(result, Ok):
            self.f.set_result(result.unwrap())
        elif isinstance(result, Err):
            self.f.set_exception(result.err())
        else:
            assert_never(result)

    def done(self) -> bool:
        return self.f.done()
