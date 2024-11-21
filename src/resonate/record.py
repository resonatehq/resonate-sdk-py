from __future__ import annotations

from concurrent.futures import Future
from typing import TYPE_CHECKING, Any, Generic, TypeVar, final

from typing_extensions import assert_never

from resonate.actions import LFI, RFI
from resonate.logging import logger
from resonate.result import Err, Ok, Result
from resonate.stores.record import DurablePromiseRecord, TaskRecord

if TYPE_CHECKING:
    from resonate.actions import LFI
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
        invocation: LFI | RFI,
        parent: Record[Any] | None,
        ctx: Context,
    ) -> None:
        self.id: str = id
        self.parent: Record[Any] | None = parent
        self.is_root: bool = (
            True if self.parent is None else isinstance(invocation, RFI)
        )
        self._f = Future[T]()
        self.children: list[Record[Any]] = []
        self.leafs: set[Record[Any]] = set()
        self.invocation: LFI | RFI = invocation
        self.promise = Promise[T](id=id)
        self.handle = Handle[T](id=self.id, future=self._f)
        self.durable_promise: DurablePromiseRecord | None = None
        self._task: TaskRecord | None = None
        self.ctx = ctx
        self.coro: ResonateCoro[T] | None = None
        self.is_awaiting_remotely: bool = False
        self._num_children: int = 0

    def root(self) -> Record[Any]:
        maybe_is_the_root = self
        while True:
            if maybe_is_the_root.is_root:
                return maybe_is_the_root
            assert maybe_is_the_root.parent is not None
            maybe_is_the_root = maybe_is_the_root.parent

    def add_child(self, record: Record[Any]) -> None:
        self.children.append(record)
        self.leafs.add(record)
        top_root_promise = self.root().parent
        if top_root_promise:
            top_root_promise.leafs.discard(self)
            top_root_promise.leafs.add(record)

        self._num_children += 1

    def add_coro(self, coro: ResonateCoro[T]) -> None:
        assert self.coro is None
        self.coro = coro

    def add_durable_promise(self, durable_promise: DurablePromiseRecord) -> None:
        assert self.id == durable_promise.id
        assert self.durable_promise is None
        self.durable_promise = durable_promise

    def add_task(self, task: TaskRecord) -> None:
        assert not self.has_task()
        self._task = task
        logger.info("Task added to %s", self.id)

    def has_task(self) -> bool:
        return self._task is not None

    def remove_task(self) -> None:
        assert self.has_task()
        self._task = None
        logger.info("Task completed for %s", self.id)

    def get_task(self) -> TaskRecord:
        assert self._task
        return self._task

    def clear_coro(self) -> None:
        assert self.coro is not None
        self.coro = None

    def set_result(self, result: Result[Any, Exception]) -> None:
        assert all(
            r.done() for r in self.children
        ), "All children record must be completed."
        if isinstance(result, Ok):
            self._f.set_result(result.unwrap())
        elif isinstance(result, Err):
            self._f.set_exception(result.err())
        else:
            assert_never(result)

    def safe_result(self) -> Result[Any, Exception]:
        assert self.done()
        try:
            return Ok(self._f.result())
        except Exception as e:  # noqa: BLE001
            return Err(e)

    def done(self) -> bool:
        return self._f.done()

    def next_child_name(self) -> str:
        return f"{self.id}.{self._num_children+1}"

    def create_child(
        self,
        id: str,
        invocation: LFI | RFI,
    ) -> Record[Any]:
        child_record = Record[Any](
            id=id,
            parent=self,
            invocation=invocation,
            ctx=self.ctx,
        )
        self.add_child(child_record)
        return child_record
