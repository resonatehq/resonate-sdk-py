from __future__ import annotations

from concurrent.futures import Future
from typing import TYPE_CHECKING, Any, Generic, TypeVar, final

from typing_extensions import assert_never

from resonate.result import Err, Ok, Result
from resonate.stores.record import DurablePromiseRecord, TaskRecord

if TYPE_CHECKING:
    from resonate.actions import LFI, RFI
    from resonate.context import Context
    from resonate.dataclasses import ResonateCoro
    from resonate.stores.record import DurablePromiseRecord, TaskRecord

T = TypeVar("T")


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
        lfi: LFI | None,
        rfi: RFI | None,
        parent: Record[Any] | None,
        ctx: Context,
    ) -> None:
        self.id = id
        self.parent = parent
        self.f = Future[T]()
        self.children: list[Record[Any]] = []
        self.lfi = lfi
        self.rfi = rfi
        self.promise = Promise[T](id=id)
        self.handle = Handle[T](id=self.id, future=self.f)
        self.durable_promise: DurablePromiseRecord | None = None
        self.task: TaskRecord | None = None
        self.ctx = ctx
        self.coro: ResonateCoro[T] | None = None
        self._num_children: int = 0

    def add_child(self, record: Record[Any]) -> None:
        self.children.append(record)

    def add_coro(self, coro: ResonateCoro[T]) -> None:
        assert self.coro is None
        self.coro = coro

    def add_durable_promise(self, durable_promise: DurablePromiseRecord) -> None:
        assert self.durable_promise is None
        self.durable_promise = durable_promise

    def add_task(self, task: TaskRecord) -> None:
        assert self.task is None
        self.task = task

    def clear_coro(self) -> None:
        assert self.coro is not None
        self.coro = None

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

    def safe_result(self) -> Result[Any, Exception]:
        assert self.done()
        try:
            return Ok(self.f.result())
        except Exception as e:  # noqa: BLE001
            return Err(e)

    def done(self) -> bool:
        return self.f.done()

    def next_child_name(self) -> str:
        return f"{self.id}.{self._num_children+1}"

    def create_child(
        self,
        id: str,
        lfi: LFI | None,
        rfi: RFI | None,
        ctx: Context,
    ) -> Record[Any]:
        child_record = Record[Any](
            id=id,
            parent=self,
            lfi=lfi,
            rfi=rfi,
            ctx=ctx,
        )
        self.children.append(child_record)
        self._num_children += 1
        return child_record
