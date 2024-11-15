from __future__ import annotations

from concurrent.futures import Future
from inspect import isfunction, isgeneratorfunction
from typing import TYPE_CHECKING, Any, Generic, Literal, TypeVar, final

from typing_extensions import TypeAlias

from resonate.dataclasses import FnOrCoroutine, ResonateCoro
from resonate.functools import AsyncFnWrapper, FnWrapper, wrap_fn

if TYPE_CHECKING:
    from resonate.context import Context
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
        ctx: Context,
        durable_promise: DurablePromiseRecord | None,
        task: TaskRecord | None,
        fn_or_coro: FnOrCoroutine[T] | None,
    ) -> None:
        self.id = id
        self.parent_id = parent_id
        self.f = Future[T]()
        self.promise = Promise[T](id=id)
        self.handle = Handle[T](id=self.id, future=self.f)
        self.durable_promise = durable_promise
        self.task = task
        self.ctx = ctx
        self.children: list[Record[Any]] = []
        self.fn: FnWrapper[T] | AsyncFnWrapper[T] | None = None
        self.coro: ResonateCoro[T] | None = None
        if fn_or_coro is not None:
            if isgeneratorfunction(fn_or_coro.exec_unit):
                self.coro = ResonateCoro[T](
                    record=self,
                    coro=fn_or_coro.exec_unit(
                        self.ctx, *fn_or_coro.args, **fn_or_coro.kwargs
                    ),
                )
            else:
                assert isfunction(fn_or_coro.exec_unit)
                self.fn = wrap_fn(
                    self.ctx,
                    fn_or_coro.exec_unit,
                    *fn_or_coro.args,
                    **fn_or_coro.kwargs,
                )

    def done(self) -> bool:
        return self.f.done()
