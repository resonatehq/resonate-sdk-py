from __future__ import annotations

import asyncio
import sys
from collections import deque
from functools import partial
from inspect import iscoroutinefunction, isfunction, isgeneratorfunction
from threading import Event, Thread
from typing import TYPE_CHECKING, Any, TypeVar

from typing_extensions import ParamSpec, assert_never

from resonate import utils
from resonate.actions import DI, LFC, LFI, RFC, RFI
from resonate.context import Context
from resonate.dataclasses import FinalValue, Invocation, ResonateCoro
from resonate.encoders import JsonEncoder
from resonate.logging import logger
from resonate.options import Options
from resonate.processor import SQE, Processor
from resonate.queue import Queue
from resonate.record import Promise, Record
from resonate.result import Err, Ok
from resonate.scheduler.traits import IScheduler
from resonate.stores.record import DurablePromiseRecord, TaskRecord

if TYPE_CHECKING:
    from resonate.collections import FunctionRegistry
    from resonate.dependencies import Dependencies
    from resonate.processor import CQE
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

        self._record_queue: Queue[str] = Queue()
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

        record = Record[Any](
            id=id,
            parent=None,
            invocation=LFI(
                Invocation(func, *args, **kwargs), Options(durable=True, id=id)
            ),
            ctx=Context(self._deps),
        )
        self._records[record.id] = record

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

        record.add_durable_promise(durable_promise)
        if durable_promise.is_completed():
            record.set_result(durable_promise.get_value(self._encoder))
        if task is not None:
            record.add_task(task)

        if record.done():
            assert (
                record.task is None
            ), "If the durable promise was dedup, there cannot be any task."
            self._records.pop(record.id)
        else:
            self._record_queue.put_nowait(record.id)
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

        record = Record[Any](
            id=id,
            parent=None,
            invocation=RFI(
                unit=(func, args, kwargs), opts=Options(durable=True, id=id)
            ),
            ctx=Context(self._deps),
        )
        self._records[record.id] = record

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
        record.add_durable_promise(durable_promise)
        if durable_promise.is_completed():
            record.set_result(durable_promise.get_value(self._encoder))

        if task is not None:
            record.add_task(task)

        if record.done():
            assert (
                record.task is None
            ), "If the durable promise was dedup, there cannot be any task."
            self._records.pop(record.id)
        else:
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
            for id in self._record_queue.dequeue_all():
                self._ingest(id)

            # check for tasks added from task_source
            # to firt claim the task, then try to resume if in memory
            # else invoke.

            # for each item in runnable advance one step in the
            # coroutine to collect server commands.
            while self._runnable:
                id, next_value = self._runnable.pop()
                record = self._records[id]
                self._process(record, next_value)

            # send server commands to the processor

    def _process(
        self,
        record: Record[Any],
        next_value: Result[Any, Exception] | None,
    ) -> None:
        assert record.coro
        yielded_value = record.coro.advance(next_value)

        if isinstance(yielded_value, FinalValue):
            self._process_final_value(record, yielded_value.v)
        elif isinstance(yielded_value, LFI):
            self._process_lfi(record, yielded_value)
        elif isinstance(yielded_value, LFC):
            # create child record, start execution. Add current record to awaiting local  # noqa: E501
            self._process_lfc(record, yielded_value)
        elif isinstance(yielded_value, RFI):
            # create child record, with distribution tag. Add current record to runnables again  # noqa: E501
            raise NotImplementedError
        elif isinstance(yielded_value, RFC):
            # create child record, start execution. Add current record to awaiting remote  # noqa: E501
            raise NotImplementedError
        elif isinstance(yielded_value, Promise):
            self._process_promise(record, yielded_value)
        elif isinstance(yielded_value, DI):
            # start execution from the top. Add current record to runnable
            raise NotImplementedError
        else:
            assert_never(yielded_value)

    def _continue(self) -> None:
        self._event_loop.set()

    def _process_promise(self, record: Record[Any], promise: Promise[Any]) -> None:
        promise_record = self._records[promise.id]
        if promise_record.done():
            self._unblock_awaiting_locally(promise_record.id)
            self._to_runnables(record.id, promise_record.safe_result())
        else:
            self._to_awaiting_locally(promise_record.id, [record.id])

    def _unblock_awaiting_locally(self, id: str) -> None:
        record = self._records[id]
        assert record.done()
        blocked = self._awaiting_lfi.pop(record.id, [])
        logger.debug("Unblocking %s coros. Which were blocked by %s", len(blocked), id)
        for blocked_id in blocked:
            assert blocked_id in self._records
            self._to_runnables(blocked_id, next_value=record.safe_result())

    def _to_runnables(self, id: str, next_value: Result[Any, Exception] | None) -> None:
        self._runnable.appendleft((id, next_value))

    def _to_awaiting_locally(self, id: str, blocked: list[str]) -> None:
        logger.debug("Coro %s will be blocking %s coros", id, len(blocked))
        self._awaiting_lfi.setdefault(id, []).extend(blocked)

    def _ingest(self, id: str) -> None:
        record = self._records[id]
        assert not record.done()
        assert isinstance(record.invocation.unit, Invocation)
        fn, args, kwargs = (
            record.invocation.unit.unit,
            record.invocation.unit.args,
            record.invocation.unit.kwargs,
        )
        if isgeneratorfunction(fn):
            record.add_coro(
                ResonateCoro(
                    record,
                    fn(
                        record.ctx,
                        *args,
                        **kwargs,
                    ),
                )
            )
            self._to_runnables(record.id, None)
            return

        def continuation(result: Result[Any, Exception]) -> None:
            def continuation(result: Result[DurablePromiseRecord, Exception]) -> None:
                assert isinstance(result, Ok)
                durable_promise = result.unwrap()
                assert record.durable_promise is not None
                result = durable_promise.get_value(self._encoder)
                record.set_result(result)
                self._unblock_awaiting_locally(record.id)

            if record.invocation.opts.durable:
                if isinstance(result, Ok):
                    self._processor.enqueue(
                        SQE[DurablePromiseRecord](
                            partial(
                                self._store.promises.resolve,
                                id=record.id,
                                ikey=utils.string_to_ikey(record.id),
                                strict=False,
                                headers=None,
                                data=self._encoder.encode(result.unwrap()),
                            ),
                            continuation,
                        )
                    )
                elif isinstance(result, Err):
                    self._processor.enqueue(
                        SQE[DurablePromiseRecord](
                            partial(
                                self._store.promises.reject,
                                id=record.id,
                                ikey=utils.string_to_ikey(record.id),
                                strict=False,
                                headers=None,
                                data=self._encoder.encode(result.err()),
                            ),
                            continuation,
                        )
                    )
                else:
                    assert_never(result)
            else:
                record.set_result(result)
                self._unblock_awaiting_locally(record.id)

        if iscoroutinefunction(fn):
            self._processor.enqueue(
                SQE[Any](
                    thunk=partial(
                        asyncio.run,
                        fn(
                            record.ctx,
                            *args,
                            **kwargs,
                        ),
                    ),
                    callback=continuation,
                )
            )

        else:
            assert isfunction(fn)
            self._processor.enqueue(
                SQE[Any](
                    thunk=partial(
                        fn,
                        record.ctx,
                        *args,
                        **kwargs,
                    ),
                    callback=continuation,
                )
            )

    def _process_lfc(self, record: Record[Any], lfc: LFC) -> None:
        child_id = lfc.opts.id if lfc.opts.id is not None else record.next_child_name()
        child_record = self._records.get(child_id)
        if child_record is not None:
            if child_record.done():
                self._to_runnables(record.id, child_record.safe_result())
            else:
                self._to_awaiting_locally(id=child_id, blocked=[record.id])
            return
        child_record = record.create_child(
            id=child_id, invocation=lfc.to_invocation(), ctx=record.ctx
        )
        self._records[child_id] = child_record

        if lfc.opts.durable:

            def continuation(resp: Result[DurablePromiseRecord, Exception]) -> None:
                assert child_id in self._records
                assert not record.done()
                assert isinstance(resp, Ok)
                durable_promise = resp.unwrap()
                child_record.add_durable_promise(durable_promise)
                if durable_promise.is_completed():
                    value = durable_promise.get_value(self._encoder)
                    child_record.set_result(value)
                    self._unblock_awaiting_locally(child_record.id)
                else:
                    self._ingest(child_id)
                    self._to_awaiting_locally(id=child_id, blocked=[record.id])

            self._processor.enqueue(
                SQE[DurablePromiseRecord](
                    partial(
                        self._store.promises.create,
                        id=child_id,
                        ikey=utils.string_to_ikey(child_id),
                        strict=False,
                        headers=None,
                        data=None,
                        timeout=sys.maxsize,
                        tags=None,
                    ),
                    continuation,
                )
            )
        else:
            self._ingest(child_id)
            self._to_awaiting_locally(id=child_id, blocked=[record.id])

    def _process_lfi(self, record: Record[Any], lfi: LFI) -> None:
        child_id = lfi.opts.id if lfi.opts.id is not None else record.next_child_name()
        child_record = self._records.get(child_id)
        if child_record is not None:
            self._to_runnables(record.id, Ok(child_record.promise))
            return

        child_record = record.create_child(id=child_id, invocation=lfi, ctx=record.ctx)
        self._records[child_id] = child_record

        if lfi.opts.durable:

            def continuation(resp: Result[DurablePromiseRecord, Exception]) -> None:
                assert child_id in self._records
                assert not record.done()
                durable_promise = resp.unwrap()
                child_record.add_durable_promise(durable_promise)

                if durable_promise.is_completed():
                    value = durable_promise.get_value(self._encoder)
                    child_record.set_result(value)
                    self._unblock_awaiting_locally(child_record.id)
                else:
                    self._ingest(child_id)
                    self._to_runnables(record.id, Ok(child_record.promise))

            self._processor.enqueue(
                SQE[DurablePromiseRecord](
                    partial(
                        self._store.promises.create,
                        id=child_id,
                        ikey=utils.string_to_ikey(child_id),
                        strict=False,
                        headers=None,
                        data=None,
                        timeout=sys.maxsize,
                        tags=None,
                    ),
                    continuation,
                )
            )
        else:
            self._ingest(child_id)
            self._to_runnables(record.id, Ok(child_record.promise))

    def _process_final_value(
        self, record: Record[Any], final_value: Result[Any, Exception]
    ) -> None:
        if record.invocation.opts.durable:

            def continuation(
                resp: Result[DurablePromiseRecord, Exception],
            ) -> None:
                durable_promise = resp.unwrap()
                value = durable_promise.get_value(self._encoder)
                assert not record.done()
                record.set_result(value)
                self._unblock_awaiting_locally(record.id)

            if isinstance(final_value, Ok):
                self._processor.enqueue(
                    SQE[DurablePromiseRecord](
                        partial(
                            self._store.promises.resolve,
                            id=record.id,
                            ikey=utils.string_to_ikey(record.id),
                            strict=False,
                            headers=None,
                            data=self._encoder.encode(final_value.unwrap()),
                        ),
                        continuation,
                    )
                )
            elif isinstance(final_value, Err):
                self._processor.enqueue(
                    SQE[DurablePromiseRecord](
                        partial(
                            self._store.promises.reject,
                            id=record.id,
                            ikey=utils.string_to_ikey(record.id),
                            strict=False,
                            headers=None,
                            data=self._encoder.encode(final_value.err()),
                        ),
                        continuation,
                    )
                )
            else:
                assert_never(final_value)

        else:
            record.set_result(final_value)
            self._unblock_awaiting_locally(record.id)
