from __future__ import annotations

import sys
from collections import deque
from threading import Event, Thread
from typing import TYPE_CHECKING, Any, TypeVar

from typing_extensions import ParamSpec

from resonate import utils
from resonate.encoders import JsonEncoder
from resonate.queue import Queue
from resonate.record import Record
from resonate.scheduler.traits import IScheduler
from resonate.stores.record import TaskRecord

if TYPE_CHECKING:
    from resonate.dependencies import Dependencies
    from resonate.record import Handle
    from resonate.result import Result
    from resonate.stores.local import LocalStore
    from resonate.stores.record import TaskRecord
    from resonate.stores.remote import RemoteStore
    from resonate.typing import DurableCoro, DurableFn

T = TypeVar("T")
P = ParamSpec("P")


class Scheduler(IScheduler):
    def __init__(
        self, deps: Dependencies, pid: str, store: LocalStore | RemoteStore
    ) -> None:
        self._pid = pid
        self._deps = deps

        self._store = store
        self._runnable: deque[tuple[str, Result[Any, Exception]]] = deque()
        self._awaiting_rfi: dict[str, list[str]] = {}
        self._awaiting_lfi: dict[str, list[str]] = {}
        self._records: dict[str, Record[Any]] = {}

        self._record_queue: Queue[Record[Any]] = Queue()
        self._task_queue: Queue[TaskRecord] = Queue()

        self._encoder = JsonEncoder()

        self._default_recv: dict[str, Any] | None = None

        self._event_loop = Event()
        self._thread = Thread(target=self._loop, daemon=True)
        self._thread.start()

    def lfi(
        self,
        id: str,
        func: DurableCoro[P, T] | DurableFn[P, T],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Handle[T]: ...

    def rfi(
        self,
        id: str,
        func: str,
        /,
        *args: Any,  # noqa: ANN401
        **kwargs: Any,  # noqa: ANN401
    ) -> Handle[Any]:
        # If there's already a record with this ID, dedup.
        record = self._records.get(id)
        if record is not None:
            return record.handle

        assert self._default_recv is not None

        # Create durable promise while claiming the task.
        durable_promise, task = self._store.promises.create_with_task(
            id=id,
            ikey=utils.string_to_ikey(id),
            strict=False,
            headers=None,
            data=self._encoder.encode({"func": func, "args": args, "kwargs": kwargs}),
            timeout=sys.maxsize,
            tags={},
            pid=self._pid,
            ttl=5 * 1_000,
            recv=self._default_recv,
        )
        record = Record[Any](
            id=id,
            parent_id=None,
            durable_promise=durable_promise,
            task=task,
        )
        self._records[record.id] = record
        if durable_promise.is_completed():
            v = durable_promise.get_value(self._encoder)
            record.set_result(v)
            self._records.pop(record.id)
        else:
            assert record.id not in self._awaiting_rfi
            self._awaiting_rfi[record.id] = []

        return record.handle

    def add_task(self, task: TaskRecord) -> None:
        self._task_queue.put_nowait(task)
        self._continue()

    def set_default_recv(self, recv: dict[str, Any]) -> None:
        assert self._default_recv is None
        self._default_recv = recv

    def _loop(self) -> None:
        while self._event_loop.wait():
            self._event_loop.clear()

            # check for cqe in cq and run associated callbacks

            # check for newly added records from application thread
            # to feed runnable.

            # check for tasks added from task_source
            # to firt claim the task, then try to resume if in memory
            # else invoke.

            # for each item in runnable advance one step in the
            # coroutine to collect server commands.

            # send server commands to the processor

    def _continue(self) -> None:
        self._event_loop.set()
