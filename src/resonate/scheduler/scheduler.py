from __future__ import annotations

import asyncio
import sys
from collections import deque
from functools import partial
from inspect import iscoroutinefunction, isfunction, isgeneratorfunction
from threading import Event, Thread
from typing import TYPE_CHECKING, Any, Callable, TypeVar

from typing_extensions import ParamSpec, assert_never

from resonate import utils
from resonate.actions import DI, LFC, LFI, RFC, RFI
from resonate.context import Context
from resonate.dataclasses import FinalValue, Invocation, ResonateCoro
from resonate.encoders import JsonEncoder
from resonate.processor import SQE, Processor
from resonate.queue import Queue
from resonate.record import Promise, Record
from resonate.result import Err, Ok
from resonate.scheduler.traits import IScheduler
from resonate.stores.record import TaskRecord

if TYPE_CHECKING:
    from collections.abc import Generator

    from resonate.collections import FunctionRegistry
    from resonate.dependencies import Dependencies
    from resonate.processor import CQE
    from resonate.record import Handle
    from resonate.result import Result
    from resonate.stores.local import LocalStore
    from resonate.stores.record import DurablePromiseRecord, TaskRecord
    from resonate.stores.remote import RemoteStore
    from resonate.typing import DurableCoro, DurableFn

T = TypeVar("T")
P = ParamSpec("P")


class Scheduler(IScheduler):
    def __init__(
        self,
        fn_registry: FunctionRegistry,
        deps: Dependencies,
        pid: str,
        store: LocalStore | RemoteStore,
    ) -> None:
        self._pid = pid
        self._deps = deps
        self._fn_registry = fn_registry

        self._store = store
        self._runnable: deque[tuple[str, Result[Any, Exception] | None]] = deque()
        self._awaiting_rfi: dict[str, list[str]] = {}
        self._awaiting_lfi: dict[str, list[str]] = {}
        self._records: dict[str, Record[Any]] = {}

        self._record_queue: Queue[Record[Any]] = Queue()
        self._task_queue: Queue[TaskRecord] = Queue()
        self._sq: Queue[SQE[Any]] = Queue()
        self._cq: Queue[CQE[Any]] = Queue()

        self._encoder = JsonEncoder()

        self._default_recv: dict[str, Any] | None = None

        self._event_loop = Event()
        self._thread = Thread(target=self._loop, daemon=True)
        self._thread.start()

        self._processor = Processor(max_workers=None, sq=self._sq, scheduler=self)

    def lfi(
        self,
        id: str,
        func: DurableCoro[P, T] | DurableFn[P, T],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Handle[T]:
        # If there's already a record with this ID, dedup.
        record = self._records.get(id)
        if record is not None:
            return record.handle

        assert self._default_recv is not None

        # Get function name from registry
        fn_name = self._fn_registry.get_from_value(func)
        assert fn_name is not None, f"Function {func.__name__} must be registered"
        func_with_options = self._fn_registry.get(fn_name)
        assert func_with_options is not None
        opts = func_with_options[-1]
        assert opts.durable, "Top level must always be durable."

        # Create durable promise while claiming the task.
        durable_promise, task = self._store.promises.create_with_task(
            id=id,
            ikey=utils.string_to_ikey(id),
            strict=False,
            headers=None,
            data=self._encoder.encode(
                {"func": fn_name, "args": args, "kwargs": kwargs}
            ),
            timeout=sys.maxsize,
            tags={},
            pid=self._pid,
            ttl=5 * 1_000,
            recv=self._default_recv,
        )
        record = Record[Any](
            id=id,
            parent=None,
            durable_promise=durable_promise,
            task=task,
            invocation=Invocation(func, *args, **kwargs),
            opts=opts,
            ctx=Context(self._deps),
        )
        if durable_promise.is_completed():
            assert (
                task is None
            ), "If the durable promise was dedup, there cannot be any task."
            v = durable_promise.get_value(self._encoder)
            record.set_result(v)
            self._records.pop(record.id)
        else:
            self._records[record.id] = record
            self._record_queue.put_nowait(record)
            self._continue()

        return record.handle

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
            parent=None,
            durable_promise=durable_promise,
            task=task,
            invocation=None,
            opts=None,
            ctx=Context(self._deps),
        )
        if durable_promise.is_completed():
            assert (
                task is None
            ), "If the durable promise was dedup, there cannot be any task."
            v = durable_promise.get_value(self._encoder)
            record.set_result(v)
            self._records.pop(record.id)
        else:
            self._records[record.id] = record
            assert record.id not in self._awaiting_rfi
            self._awaiting_rfi[record.id] = []

        return record.handle

    def add_task(self, task: TaskRecord) -> None:
        self._task_queue.put_nowait(task)
        self._continue()

    def add_cqe(self, cqe: CQE[Any]) -> None:
        self._cq.put_nowait(cqe)
        self._continue()

    def set_default_recv(self, recv: dict[str, Any]) -> None:
        assert self._default_recv is None
        self._default_recv = recv

    def _loop(self) -> None:
        while self._event_loop.wait():
            self._event_loop.clear()

            # check for cqe in cq and run associated callbacks
            for cqe in self._cq.dequeue_all():
                cqe.callback(cqe.thunk_result)

            # check for newly added records from application thread
            # to feed runnable.
            for record in self._record_queue.dequeue_all():
                self._ingest(record)

            # check for tasks added from task_source
            # to firt claim the task, then try to resume if in memory
            # else invoke.

            # for each item in runnable advance one step in the
            # coroutine to collect server commands.
            while self._runnable:
                id, next_value = self._runnable.pop()
                record = self._records[id]

                coro = self._advance_span(record, next_value)
                final_value: None
                try:
                    v = next(coro)
                    try:
                        while True:
                            coro.send(v())
                    except StopIteration as e:
                        final_value = e.value
                except StopIteration as e:
                    final_value = e.value
                assert final_value is None

            # send server commands to the processor

    def _advance_span(  # noqa: C901, PLR0912
        self,
        record: Record[Any],
        next_value: Result[Any, Exception] | None,
    ) -> Generator[Callable[[], Any], Any, None]:
        assert record.coro
        yielded_value = record.coro.advance(next_value)

        durable_promise: DurablePromiseRecord | None = None

        if isinstance(yielded_value, FinalValue):
            # if is an error check in need to be retry. resolve otherwise
            final_value = yielded_value.v
            assert record.opts
            if record.opts.durable:
                if isinstance(final_value, Ok):
                    durable_promise = yield partial(
                        self._store.promises.resolve,
                        id=record.id,
                        ikey=utils.string_to_ikey(record.id),
                        strict=False,
                        headers=None,
                        data=self._encoder.encode(final_value.unwrap()),
                    )
                elif isinstance(final_value, Err):
                    durable_promise = yield partial(
                        self._store.promises.reject,
                        id=record.id,
                        ikey=utils.string_to_ikey(record.id),
                        strict=False,
                        headers=None,
                        data=self._encoder.encode(final_value.err()),
                    )
                else:
                    assert_never(final_value)
                assert durable_promise is not None
                final_value = durable_promise.get_value(self._encoder)

            self._resolve_record(record, final_value)
            self._unblock_awaiting_locally(record.id)
            self._records.pop(record.id)

        elif isinstance(yielded_value, LFI):
            # create child record, start execution. Add current record to runnables again  # noqa: E501
            lfi = yielded_value
            child_id = (
                lfi.opts.id if lfi.opts.id is not None else record.next_child_name()
            )
            child_record = self._records.get(child_id)
            if child_record is not None:
                self._to_runnables(record.id, Ok(child_record.promise))
            else:
                if lfi.opts.durable:
                    durable_promise = yield partial(
                        self._store.promises.create,
                        id=child_id,
                        ikey=utils.string_to_ikey(child_id),
                        strict=False,
                        headers=None,
                        data=None,
                        timeout=sys.maxsize,
                        tags=None,
                    )

                assert isinstance(lfi.unit, Invocation)
                child_record = record.create_child(
                    id=child_id,
                    durable_promise=durable_promise,
                    task=None,
                    invocation=lfi.unit,
                    opts=lfi.opts,
                    ctx=record.ctx,
                )
                assert child_record.id not in self._records
                self._records[child_record.id] = child_record
                self._ingest(child_record)
                self._to_runnables(record.id, Ok(child_record.promise))
        elif isinstance(yielded_value, LFC):
            # create child record, start execution. Add current record to awaiting local  # noqa: E501
            raise NotImplementedError
        elif isinstance(yielded_value, RFI):
            # create child record, with distribution tag. Add current record to runnables again  # noqa: E501
            raise NotImplementedError
        elif isinstance(yielded_value, RFC):
            # create child record, start execution. Add current record to awaiting remote  # noqa: E501
            raise NotImplementedError
        elif isinstance(yielded_value, Promise):
            # Add current record to awaiting (local or remote).
            id = yielded_value.id
            assert id in self._records
            self._to_awaiting_locally(id, [record.id])
        elif isinstance(yielded_value, DI):
            # start execution from the top. Add current record to runnable
            raise NotImplementedError
        else:
            assert_never(yielded_value)

    def _continue(self) -> None:
        self._event_loop.set()

    def _unblock_awaiting_locally(self, id: str) -> None:
        record = self._records[id]
        assert record.done()
        for blocked_id in self._awaiting_lfi.pop(record.id, []):
            assert blocked_id in self._records
            self._to_runnables(blocked_id, next_value=record.safe_result())

    def _resolve_record(self, record: Record[T], result: Result[T, Exception]) -> None:
        """
        This function will be called from the scheduler.

        It defines what to do with the function result.
        """
        assert not record.done(), "Cannot resolve an already completed record."
        assert isinstance(result, Ok)
        record.set_result(result)

    def _to_runnables(self, id: str, next_value: Result[Any, Exception] | None) -> None:
        self._runnable.appendleft((id, next_value))

    def _to_awaiting_locally(self, id: str, blocked: list[str]) -> None:
        self._awaiting_lfi.setdefault(id, []).extend(blocked)

    def _ingest(self, record: Record[Any]) -> None:
        assert record.id in self._records

        assert record.invocation is not None
        assert record.opts is not None
        assert record.opts.durable
        if isgeneratorfunction(record.invocation.unit):
            record.add_coro(
                ResonateCoro(
                    record,
                    record.invocation.unit(
                        record.ctx,
                        *record.invocation.args,
                        **record.invocation.kwargs,
                    ),
                )
            )
            self._to_runnables(record.id, None)
            return

        def continuation(record: Record[T], result: Result[T, Exception]) -> None:
            self._resolve_record(record, result)
            self._unblock_awaiting_locally(record.id)

        if iscoroutinefunction(record.invocation.unit):
            self._processor.enqueue(
                SQE[Any](
                    thunk=partial(
                        asyncio.run,
                        record.invocation.unit(
                            record.ctx,
                            *record.invocation.args,
                            **record.invocation.kwargs,
                        ),
                    ),
                    callback=partial(continuation, record),
                )
            )

        else:
            assert isfunction(record.invocation.unit)
            self._processor.enqueue(
                SQE[Any](
                    thunk=partial(
                        record.invocation.unit,
                        record.ctx,
                        *record.invocation.args,
                        **record.invocation.kwargs,
                    ),
                    callback=partial(continuation, record),
                )
            )
