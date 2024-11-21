from __future__ import annotations

import asyncio
import sys
from collections import deque
from functools import partial
from inspect import iscoroutinefunction, isfunction, isgeneratorfunction
from threading import Event, Thread
from typing import TYPE_CHECKING, Any, TypeVar, Union

from typing_extensions import ParamSpec, assert_never

from resonate import utils
from resonate.actions import DI, LFC, LFI, RFC, RFI
from resonate.commands import DurablePromise
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
from resonate.stores.record import (
    CallbackRecord,
    DurablePromiseRecord,
    Invoke,
    Resume,
    TaskRecord,
)
from resonate.stores.remote import RemoteStore

if TYPE_CHECKING:
    from resonate.collections import FunctionRegistry
    from resonate.dependencies import Dependencies
    from resonate.processor import CQE
    from resonate.record import Handle
    from resonate.result import Result
    from resonate.stores.local import LocalStore
    from resonate.stores.record import TaskRecord
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
        assert self._default_recv
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
        if task:
            record.add_task(task)

        if durable_promise.is_completed():
            assert task is None
            record.set_result(durable_promise.get_value(self._encoder))
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

        record = Record[Any](
            id=id,
            parent=None,
            invocation=RFI(
                unit=Invocation(func, args, kwargs), opts=Options(durable=True, id=id)
            ),
            ctx=Context(self._deps),
        )
        self._records[record.id] = record

        assert self._default_recv
        assert record.is_root, "You can only register callbacks from a partition root"
        durable_promise, callback = self._store.promises.create_with_callback(
            id=id,
            ikey=utils.string_to_ikey(id),
            strict=False,
            headers=None,
            data=self._encoder.encode({"func": func, "args": args, "kwargs": kwargs}),
            timeout=sys.maxsize,
            tags={"resonate:invoke": "default"},
            root_id=record.root().id,
            recv=self._default_recv,
        )
        record.add_durable_promise(durable_promise)
        if callback is None:
            assert durable_promise.is_completed()
            record.set_result(durable_promise.get_value(self._encoder))
        else:
            self._add_to_awaiting_remote(record.id, [])

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

    def _loop(self) -> None:  # noqa: C901, PLR0912
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
            for task in self._task_queue.dequeue_all():

                def continuation(
                    task: TaskRecord, resp: Result[Invoke | Resume, Exception]
                ) -> None:
                    assert isinstance(resp, Ok)
                    invoke_or_resume = resp.unwrap()
                    if isinstance(invoke_or_resume, Invoke):
                        self._process_invoke(invoke_or_resume, task)
                    elif isinstance(invoke_or_resume, Resume):
                        self._process_resume(invoke_or_resume, task)
                    else:
                        assert_never(invoke_or_resume)

                assert isinstance(self._store, RemoteStore)

                self._processor.enqueue(
                    SQE[Union[Invoke, Resume]](
                        partial(
                            self._store.tasks.claim,
                            task_id=task.task_id,
                            counter=task.counter,
                            pid=self._pid,
                            ttl=5 * 1000,
                        ),
                        partial(continuation, task),
                    )
                )

            # for each item in runnable advance one step in the
            # coroutine to collect server commands.
            while self._runnable:
                id, next_value = self._runnable.pop()
                record = self._records[id]
                assert record.coro
                yielded_value = record.coro.advance(next_value)

                if isinstance(yielded_value, LFI):
                    self._process_lfi(record, yielded_value)
                elif isinstance(yielded_value, LFC):
                    self._process_lfc(record, yielded_value)
                elif isinstance(yielded_value, RFI):
                    # create child record, with distribution tag. Add current record to runnables again  # noqa: E501
                    self._process_rfi(record, yielded_value)
                elif isinstance(yielded_value, RFC):
                    # create child record, start execution. Add current record to awaiting remote  # noqa: E501
                    raise NotImplementedError
                elif isinstance(yielded_value, Promise):
                    self._process_promise(record, yielded_value)
                elif isinstance(yielded_value, FinalValue):
                    self._process_final_value(record, yielded_value.v)
                elif isinstance(yielded_value, DI):
                    # start execution from the top. Add current record to runnable
                    raise NotImplementedError
                else:
                    assert_never(yielded_value)

    def _continue(self) -> None:
        self._event_loop.set()

    def _process_invoke(self, invoke: Invoke, task: TaskRecord) -> None:
        logger.info("Invoke message for %s received", invoke.root_promise_store.id)
        record = self._records.get(invoke.root_promise_store.id)
        invoke_info = invoke.root_promise_store.invoke_info(self._encoder)
        func_name = invoke_info["func_name"]
        registered_func = self._fn_registry.get(func_name)
        assert registered_func, f"There's no function registered under name {func_name}"
        func, opts = registered_func
        assert opts.durable

        rfi = RFI(
            Invocation(func, *invoke_info["args"], **invoke_info["kwargs"]), opts=opts
        )
        if record:
            assert record.is_root
            assert isinstance(record.invocation, RFI)
            assert record.durable_promise is not None
            record.invocation = rfi
        else:
            record = Record[Any](
                id=invoke.root_promise_store.id,
                invocation=rfi,
                parent=None,
                ctx=Context(self._deps),
            )
            self._records[record.id] = record
            record.add_durable_promise(invoke.root_promise_store)

        record.add_task(task=task)
        self._record_queue.put_nowait(record.id)
        self._continue()

    def _process_resume(self, resume: Resume, task: TaskRecord) -> None:
        logger.info("Resume message for %s received", resume.leaf_promise_store.id)
        assert isinstance(self._store, RemoteStore)
        leaf_record = self._records.get(resume.leaf_promise_store.id)
        if leaf_record is None:
            self._process_invoke(Invoke(resume.root_promise_store), task)
        else:
            assert leaf_record.is_root
            assert isinstance(leaf_record.invocation, RFI)
            leaf_record.add_task(task)
            if not leaf_record.done():
                assert leaf_record.coro, "This had to be done here."
                leaf_record.set_result(
                    resume.leaf_promise_store.get_value(self._encoder)
                )

            self._unblock_awaiting_remote(id=leaf_record.id)

    def _process_promise(self, record: Record[Any], promise: Promise[Any]) -> None:
        promise_record = self._records[promise.id]
        if promise_record.done():
            self._add_to_runnable(record.id, promise_record.safe_result())
        elif isinstance(promise_record.invocation, LFI):
            self._add_to_awaiting_local(promise_record.id, [record.id])
        elif isinstance(promise_record.invocation, RFI):
            assert isinstance(self._store, RemoteStore)
            assert (
                promise_record.is_root
            ), "Callbacks can only be registered partition roots"

            def continuation(
                resp: Result[
                    tuple[DurablePromiseRecord, CallbackRecord | None], Exception
                ],
            ) -> None:
                assert isinstance(resp, Ok)
                assert promise_record.durable_promise
                durable_promise, callback = resp.unwrap()

                if callback is None:
                    assert durable_promise.is_completed()
                    record.set_result(durable_promise.get_value(self._encoder))
                else:
                    self._add_to_awaiting_remote(promise_record.id, [record.id])
                    root = record.root()
                    if self._all_completed_leafs_are_awaiting_remote(root.id):
                        logger.info(
                            "record %s is blocked on nothing but remote", root.id
                        )
                        assert root.has_task()

                        def continuation(resp: Result[None, Exception]) -> None:
                            assert isinstance(resp, Ok)
                            record.remove_task()

                        assert isinstance(self._store, RemoteStore)
                        task = root.get_task()
                        self._processor.enqueue(
                            SQE[None](
                                partial(
                                    self._store.tasks.complete,
                                    task_id=task.task_id,
                                    counter=task.counter,
                                ),
                                continuation,
                            )
                        )

            assert self._default_recv

            self._processor.enqueue(
                SQE[tuple[DurablePromiseRecord, Union[CallbackRecord, None]]](
                    partial(
                        self._store.callbacks.create,
                        id=promise_record.id,
                        root_id=record.root().id,
                        timeout=sys.maxsize,
                        recv=self._default_recv,
                    ),
                    continuation,
                )
            )

        else:
            assert_never(promise_record.invocation)

    def _unblock_awaiting_local(self, id: str) -> None:
        record = self._records[id]
        assert record.done()
        for blocked_id in self._awaiting_lfi.pop(record.id, []):
            assert blocked_id in self._records
            self._add_to_runnable(blocked_id, next_value=record.safe_result())

    def _unblock_awaiting_remote(self, id: str) -> None:
        record = self._records[id]
        assert record.done()
        for blocked_id in self._awaiting_rfi.pop(record.id, []):
            assert blocked_id in self._records
            self._add_to_runnable(blocked_id, next_value=record.safe_result())

    def _add_to_runnable(
        self, id: str, next_value: Result[Any, Exception] | None
    ) -> None:
        self._runnable.appendleft((id, next_value))

    def _add_to_awaiting_local(self, id: str, blocked: list[str]) -> None:
        record = self._records[id]
        assert isinstance(record.invocation, LFI)
        assert not record.done()
        self._awaiting_lfi.setdefault(id, []).extend(blocked)

    def _add_to_awaiting_remote(self, id: str, blocked: list[str]) -> None:
        record = self._records[id]
        assert isinstance(record.invocation, RFI)
        assert not record.done()
        self._awaiting_rfi.setdefault(id, []).extend(blocked)

    def _all_completed_leafs_are_awaiting_remote(self, id: str) -> bool:
        root_record = self._records[id].root()
        return all(
            leaf.id in self._awaiting_rfi
            for leaf in root_record.leafs
            if not leaf.done()
        )

    def _ingest(self, id: str) -> None:
        record = self._records[id]
        assert not record.done()
        assert isinstance(record.invocation.unit, Invocation)
        assert not isinstance(record.invocation.unit.fn, str)
        fn, args, kwargs = (
            record.invocation.unit.fn,
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
            self._add_to_runnable(record.id, None)
            return

        def continuation(result: Result[Any, Exception]) -> None:
            def continuation(result: Result[DurablePromiseRecord, Exception]) -> None:
                assert isinstance(result, Ok)
                durable_promise = result.unwrap()
                assert record.durable_promise is not None
                result = durable_promise.get_value(self._encoder)
                record.set_result(result)
                logger.info("Execution for %s has completed", record.id)
                self._unblock_awaiting_local(record.id)

                if record.has_task():

                    def continuation(result: Result[None, Exception]) -> None:
                        assert isinstance(result, Ok)
                        record.remove_task()

                    assert isinstance(self._store, RemoteStore)
                    task = record.get_task()
                    self._processor.enqueue(
                        SQE[None](
                            partial(
                                self._store.tasks.complete,
                                task_id=task.task_id,
                                counter=task.counter,
                            ),
                            continuation,
                        )
                    )

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
                self._unblock_awaiting_local(record.id)

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
            record.add_child(child_record)
            if child_record.done():
                self._add_to_runnable(record.id, child_record.safe_result())
            else:
                self._add_to_awaiting_local(id=child_id, blocked=[record.id])

        else:
            child_record = record.create_child(id=child_id, invocation=lfc.to_lfi())
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
                        self._add_to_runnable(record.id, child_record.safe_result())
                    else:
                        self._ingest(child_id)
                        self._add_to_awaiting_local(id=child_id, blocked=[record.id])

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
                self._add_to_awaiting_local(id=child_id, blocked=[record.id])

    def _process_rfi(self, record: Record[Any], rfi: RFI) -> None:
        child_id = rfi.opts.id if rfi.opts.id is not None else record.next_child_name()
        child_record = self._records.get(child_id)
        if child_record is not None:
            record.add_child(child_record)
            self._add_to_runnable(record.id, Ok(child_record.promise))
        else:
            child_record = record.create_child(id=child_id, invocation=rfi)
            self._records[child_id] = child_record
            assert rfi.opts.durable

            func: str
            if isinstance(rfi.unit, DurablePromise):
                raise NotImplementedError
            if isinstance(rfi.unit, Invocation):
                if isinstance(rfi.unit.fn, str):
                    func = rfi.unit.fn
                else:
                    assert isfunction(rfi.unit.fn)
                    registered_fn_name = self._fn_registry.get_from_value(rfi.unit.fn)
                    assert registered_fn_name is not None
                    func = registered_fn_name
                data: dict[str, Any] = {
                    "func": func,
                    "args": rfi.unit.args,
                    "kwargs": rfi.unit.kwargs,
                }
            else:
                assert_never(rfi.unit)

            def continuation(resp: Result[DurablePromiseRecord, Exception]) -> None:
                assert child_id in self._records
                assert not record.done()
                durable_promise = resp.unwrap()
                child_record.add_durable_promise(durable_promise)

                if durable_promise.is_completed():
                    value = durable_promise.get_value(self._encoder)
                    child_record.set_result(value)
                self._add_to_runnable(record.id, Ok(child_record.promise))

            self._processor.enqueue(
                SQE[DurablePromiseRecord](
                    partial(
                        self._store.promises.create,
                        id=child_id,
                        ikey=utils.string_to_ikey(child_id),
                        strict=False,
                        headers=None,
                        data=self._encoder.encode(data),
                        timeout=sys.maxsize,
                        tags={"resonate:invoke": "default"},
                    ),
                    continuation,
                )
            )

    def _process_lfi(self, record: Record[Any], lfi: LFI) -> None:
        child_id = lfi.opts.id if lfi.opts.id is not None else record.next_child_name()
        child_record = self._records.get(child_id)
        if child_record is not None:
            record.add_child(child_record)
            self._add_to_runnable(record.id, Ok(child_record.promise))
        else:
            child_record = record.create_child(id=child_id, invocation=lfi)
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
                    else:
                        self._ingest(child_id)
                    self._add_to_runnable(record.id, Ok(child_record.promise))

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
                self._add_to_runnable(record.id, Ok(child_record.promise))

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
                logger.info("Execution for %s has completed", record.id)
                self._unblock_awaiting_local(record.id)
                if record.has_task():

                    def continuation(result: Result[None, Exception]) -> None:
                        assert isinstance(result, Ok), result.err()
                        record.remove_task()

                    assert isinstance(self._store, RemoteStore)
                    task = record.get_task()
                    self._processor.enqueue(
                        SQE[None](
                            partial(
                                self._store.tasks.complete,
                                task_id=task.task_id,
                                counter=task.counter,
                            ),
                            continuation,
                        )
                    )

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
            logger.info("Execution for %s has completed", record.id)
            self._unblock_awaiting_local(record.id)
