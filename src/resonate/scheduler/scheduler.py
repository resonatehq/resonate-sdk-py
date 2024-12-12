from __future__ import annotations

import asyncio
import contextlib
import sys
import time
from collections import deque
from functools import partial
from inspect import iscoroutinefunction, isfunction, isgeneratorfunction
from threading import Event, Thread
from typing import TYPE_CHECKING, Any, TypeVar

import requests
from typing_extensions import ParamSpec, assert_never

from resonate import utils
from resonate.actions import DI, LFC, LFI, RFC, RFI
from resonate.commands import DurablePromise
from resonate.context import Context
from resonate.dataclasses import FinalValue, Invocation, ResonateCoro
from resonate.encoders import JsonEncoder
from resonate.logging import logger
from resonate.processor.processor import SQE, Processor
from resonate.queue import DelayQueue, Queue
from resonate.record import Promise, Record
from resonate.result import Err, Ok
from resonate.scheduler.traits import IScheduler
from resonate.stores.record import (
    DurablePromiseRecord,
    Invoke,
    Resume,
    TaskRecord,
)
from resonate.stores.remote import RemoteStore

if TYPE_CHECKING:
    from resonate.collections import FunctionRegistry
    from resonate.dependencies import Dependencies
    from resonate.record import Handle
    from resonate.result import Result
    from resonate.stores.local import LocalStore
    from resonate.stores.record import TaskRecord
    from resonate.task_sources.traits import ITaskSource
    from resonate.typing import DurableCoro, DurableFn

T = TypeVar("T")
P = ParamSpec("P")


class Scheduler(IScheduler):
    def __init__(
        self,
        deps: Dependencies,
        pid: str,
        registry: FunctionRegistry,
        store: LocalStore | RemoteStore,
        task_source: ITaskSource,
    ) -> None:
        self._deps = deps
        self._pid = pid
        self._processor = Processor()
        self._registry = registry
        self._store = store
        self._task_source = task_source

        self._runnable: deque[tuple[str, Result[Any, Exception] | None]] = deque()
        self._awaiting_rfi: dict[str, list[str]] = {}
        self._awaiting_lfi: dict[str, list[str]] = {}

        self._event = Event()
        self._encoder = JsonEncoder()
        self._recv = self._task_source.default_recv(self._pid)

        self._records: dict[str, Record[Any]] = {}
        self._record_queue: Queue[str] = Queue()
        self._delay_queue = DelayQueue[str]()

        self._heartbeat_thread = Thread(target=self._heartbeat, daemon=True)
        self._scheduler_thread = Thread(target=self._loop, daemon=True)

    def start(self) -> None:
        if isinstance(self._store, RemoteStore):
            # start the heartbeat thread
            self._heartbeat_thread.start()

            # start the task source
            self._task_source.start(self._event, self._pid)

        # start delay queue
        self._delay_queue.start(self._event)

        # start the processor
        self._processor.start(self._event)

        # start the scheduler
        self._scheduler_thread.start()

    def run(
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
        fn_name = self._registry.get_from_value(func)
        assert fn_name is not None, f"Function {func.__name__} must be registered"
        func_with_options = self._registry.get(fn_name)
        assert func_with_options is not None
        opts = func_with_options[-1]
        assert opts.durable, "Top level must always be durable."

        record = Record[Any](
            id=id,
            parent=None,
            invocation=RFI(Invocation(func, *args, **kwargs), opts),
            ctx=Context(self._deps),
        )
        self._records[record.id] = record

        # Create durable promise while claiming the task.
        assert self._recv
        durable_promise, task = self._store.promises.create_with_task(
            id=id,
            ikey=utils.string_to_uuid(id),
            strict=False,
            headers=None,
            data=self._encoder.encode(
                {"func": fn_name, "args": args, "kwargs": kwargs}
            ),
            timeout=sys.maxsize,
            tags={},
            pid=self._pid,
            ttl=5 * 1_000,
            recv=self._recv,
        )

        record.add_durable_promise(durable_promise)
        if task:
            record.add_task(task)

        if durable_promise.is_completed():
            assert task is None
            record.set_result(durable_promise.get_value(self._encoder), deduping=True)
        else:
            self._record_queue.put_nowait(record.id)
            self._continue()

        return record.handle

    def _heartbeat(self) -> None:
        assert isinstance(self._store, RemoteStore)
        while True:
            affected: int | None = None
            with contextlib.suppress(requests.exceptions.ConnectionError):
                affected = self._store.tasks.heartbeat(pid=self._pid)
                logger.debug("Heatbeat affected %s tasks", affected)
            time.sleep(2)

    def _loop(self) -> None:
        # wait until an event occurs, either:
        # - resonate run is called
        # - a completion is enqueued by the processor
        # - a task is enqueued by the task source
        while self._event.wait():
            # immediately clear the event so the next tick waits
            # unless another event occurs in the meantime
            self._event.clear()

            # start the next tick
            self._tick()

    def _tick(self) -> None:  # noqa: C901,PLR0912
        # get record to retry
        for id in self._delay_queue.dequeue_all():
            self._ingest(id)

        # dequeue all cqes and call the callback
        for cqe in self._processor.dequeue():
            cqe.callback(cqe.result)

        # check for newly added records from application thread
        # to feed runnable.
        for id in self._record_queue.dequeue_all():
            self._ingest(id)

        # check for tasks added from task_source
        # to firt claim the task, then try to resume if in memory
        # else invoke.
        for task in self._task_source.dequeue():
            assert isinstance(self._store, RemoteStore)

            invoke_or_resume = self._store.tasks.claim(
                task_id=task.task_id,
                counter=task.counter,
                pid=self._pid,
                ttl=5 * 1000,
            )
            if isinstance(invoke_or_resume, Invoke):
                self._process_invoke(invoke_or_resume, task)
            elif isinstance(invoke_or_resume, Resume):
                self._process_resume(invoke_or_resume, task)
            else:
                assert_never(invoke_or_resume)

        # for each item in runnable advance one step in the
        # coroutine to collect server commands.
        while self._runnable:
            id, next_value = self._runnable.pop()
            record = self._records[id]
            coro = record.get_coro()
            yielded_value = coro.advance(next_value)

            if isinstance(yielded_value, LFI):
                self._process_lfi(record, yielded_value)
            elif isinstance(yielded_value, LFC):
                self._process_lfc(record, yielded_value)
            elif isinstance(yielded_value, RFI):
                self._process_rfi(record, yielded_value)
            elif isinstance(yielded_value, RFC):
                self._process_rfc(record, yielded_value)
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
        self._event.set()

    def _process_invoke(self, invoke: Invoke, task: TaskRecord) -> None:
        logger.info("Invoke message for %s received", invoke.root_durable_promise.id)

        invoke_info = invoke.root_durable_promise.invoke_info(self._encoder)
        func_name = invoke_info["func_name"]
        registered_func = self._registry.get(func_name)
        assert registered_func, f"There's no function registered under name {func_name}"
        func, opts = registered_func
        assert opts.durable

        rfi = RFI(
            Invocation(func, *invoke_info["args"], **invoke_info["kwargs"]), opts=opts
        )
        record = self._records.get(invoke.root_durable_promise.id)
        if record:
            assert record.is_root
            assert isinstance(record.invocation, RFI)
            assert record.durable_promise is not None
            record.invocation = rfi
        else:
            record = Record[Any](
                id=invoke.root_durable_promise.id,
                invocation=rfi,
                parent=None,
                ctx=Context(self._deps),
            )
            self._records[record.id] = record
            record.add_durable_promise(invoke.root_durable_promise)

        record.add_task(task=task)
        self._record_queue.put_nowait(record.id)
        self._continue()

    def _process_resume(self, resume: Resume, task: TaskRecord) -> None:
        logger.info("Resume message for %s received", resume.leaf_durable_promise.id)
        assert isinstance(self._store, RemoteStore)
        assert resume.leaf_durable_promise.is_completed()

        root_record = self._records.get(resume.root_durable_promise.id)
        leaf_record = self._records.get(resume.leaf_durable_promise.id)
        if root_record and leaf_record:
            assert root_record.is_root
            assert leaf_record.is_root
            root_record.add_task(task)
            if not leaf_record.done():
                leaf_record.set_result(
                    resume.leaf_durable_promise.get_value(self._encoder), deduping=True
                )

            self._unblock_awaiting_remote(leaf_record.id)

        else:
            self._process_invoke(Invoke(resume.root_durable_promise), task)

    def _process_promise(self, record: Record[Any], promise: Promise[Any]) -> None:
        promise_record = self._records[promise.id]
        if promise_record.done():
            self._add_to_runnable(record.id, promise_record.safe_result())
        elif isinstance(promise_record.invocation, LFI):
            self._add_to_awaiting_local(promise_record.id, record.id)
        elif isinstance(promise_record.invocation, RFI):
            assert isinstance(self._store, RemoteStore)
            assert (
                promise_record.is_root
            ), "Callbacks can only be registered partition roots"

            root = record.root()
            leaf_id = promise_record.id

            assert self._recv
            assert (
                promise_record.durable_promise
            ), f"Record {promise_record.id} not backed by a promise"
            durable_promise, callback = self._store.callbacks.create(
                id=utils.string_to_uuid(record.id),
                promise_id=leaf_id,
                root_promise_id=root.id,
                timeout=sys.maxsize,
                recv=self._recv,
            )
            if callback is None:
                assert durable_promise.is_completed()
                record.set_result(
                    durable_promise.get_value(self._encoder), deduping=True
                )
            else:
                self._add_to_awaiting_remote(promise_record.id, record.id)
                if self._blocked_only_on_remote(root.id):
                    self._complete_task(root.id)

        else:
            assert_never(promise_record.invocation)

    def _unblock_awaiting_local(self, id: str) -> None:
        record = self._records[id]
        assert record.done()
        for blocked_id in self._awaiting_lfi.pop(record.id, []):
            blocked_record = self._records[blocked_id]
            assert blocked_record.blocked_on
            assert blocked_record.blocked_on.id == id
            blocked_record.blocked_on = None
            logger.info("Unblocking %s. Who has blocked locally on %s", blocked_id, id)
            self._add_to_runnable(blocked_id, next_value=record.safe_result())

    def _unblock_awaiting_remote(self, id: str) -> None:
        record = self._records[id]
        assert record.done()
        assert isinstance(record.invocation, RFI)
        for blocked_id in self._awaiting_rfi.pop(record.id, []):
            blocked_record = self._records[blocked_id]
            assert blocked_record.blocked_on
            assert blocked_record.blocked_on.id == id
            blocked_record.blocked_on = None
            logger.info("Unblocking %s. Who has blocked remotely on %s", blocked_id, id)
            self._add_to_runnable(blocked_id, next_value=record.safe_result())

    def _add_to_runnable(
        self, id: str, next_value: Result[Any, Exception] | None
    ) -> None:
        logger.info(
            "Adding %s to runnables. Next value to advance with=%s", id, next_value
        )
        self._runnable.appendleft((id, next_value))

    def _add_to_awaiting_local(self, id: str, blocked: str) -> None:
        logger.info("Blocking %s awaiting locally on %s", blocked, id)
        record = self._records[id]
        assert isinstance(record.invocation, LFI)
        assert not record.done()
        blocked_record = self._records[blocked]
        assert not blocked_record.blocked_on
        blocked_record.blocked_on = record
        self._awaiting_lfi.setdefault(id, []).append(blocked)

    def _add_to_awaiting_remote(self, id: str, blocked: str) -> None:
        logger.info("Blocking %s awaiting remotely on %s", blocked, id)
        record = self._records[id]
        assert isinstance(record.invocation, RFI)
        assert not record.done()
        blocked_record = self._records[blocked]
        assert not blocked_record.blocked_on
        blocked_record.blocked_on = record
        self._awaiting_rfi.setdefault(id, []).append(blocked)

    def _blocked_only_on_remote(self, id: str, *, root: bool = True) -> bool:
        record = self._records[id]
        if isinstance(record.invocation, RFI) and not root:
            return True
        if not record.blocked_on:
            return False
        return all(
            self._blocked_only_on_remote(child.id, root=False)
            for child in record.children
            if not child.done()
        )

    def _ingest(self, id: str) -> None:
        logger.info("Ingesting record %s", id)
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

        continuation = partial(self._process_final_value, record)

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

    def _complete_task(self, id: str) -> None:
        record = self._records[id]
        assert record.has_task()

        assert isinstance(self._store, RemoteStore)
        task = record.get_task()
        self._store.tasks.complete(
            task_id=task.task_id,
            counter=task.counter,
        )
        record.remove_task()

    def _process_rfc(self, record: Record[Any], rfc: RFC) -> None:
        child_id: str
        next_child_name = record.next_child_name()
        if isinstance(rfc.unit, Invocation):
            child_id = rfc.opts.id if rfc.opts.id is not None else next_child_name
        elif isinstance(rfc.unit, DurablePromise):
            child_id = rfc.unit.id if rfc.unit.id is not None else next_child_name
        else:
            assert_never(rfc.unit)
        child_record = self._records.get(child_id)
        root = record.root()
        if child_record is not None:
            record.add_child(child_record)
            if child_record.done():
                self._add_to_runnable(record.id, child_record.safe_result())
            else:
                self._add_to_awaiting_remote(child_id, record.id)
                if self._blocked_only_on_remote(root.id):
                    self._complete_task(root.id)
        else:
            child_record = record.create_child(id=child_id, invocation=rfc.to_rfi())
            self._records[child_id] = child_record
            assert rfc.opts.durable

            data, headers, tags = self._get_info_from_rfi(rfc.to_rfi())

            assert self._recv

            durable_promise, callback = self._store.promises.create_with_callback(
                id=child_id,
                ikey=utils.string_to_uuid(child_id),
                strict=False,
                headers=headers,
                data=self._encoder.encode(data),
                timeout=sys.maxsize,
                tags=tags,
                callback_id=utils.string_to_uuid(record.id),
                root_promise_id=root.id,
                recv=self._recv,
            )
            assert child_id in self._records
            assert not record.done()
            child_record.add_durable_promise(durable_promise)

            if durable_promise.is_completed():
                assert callback is None
                value = durable_promise.get_value(self._encoder)
                child_record.set_result(value, deduping=True)
                self._add_to_runnable(record.id, child_record.safe_result())
            else:
                self._add_to_awaiting_remote(child_id, record.id)
                if self._blocked_only_on_remote(root.id):
                    self._complete_task(root.id)

    def _process_lfc(self, record: Record[Any], lfc: LFC) -> None:
        child_id = lfc.opts.id if lfc.opts.id is not None else record.next_child_name()
        child_record = self._records.get(child_id)
        if child_record is not None:
            record.add_child(child_record)
            if child_record.done():
                self._add_to_runnable(record.id, child_record.safe_result())
            else:
                self._add_to_awaiting_local(child_id, record.id)

        else:
            child_record = record.create_child(id=child_id, invocation=lfc.to_lfi())
            self._records[child_id] = child_record

            if lfc.opts.durable:
                durable_promise = self._store.promises.create(
                    id=child_id,
                    ikey=utils.string_to_uuid(child_id),
                    strict=False,
                    headers=None,
                    data=None,
                    timeout=sys.maxsize,
                    tags=None,
                )
                assert child_id in self._records
                assert not record.done()
                child_record.add_durable_promise(durable_promise)
                if durable_promise.is_completed():
                    value = durable_promise.get_value(self._encoder)
                    child_record.set_result(value, deduping=True)
                    self._add_to_runnable(record.id, child_record.safe_result())
                else:
                    self._ingest(child_id)
                    self._add_to_awaiting_local(child_id, record.id)

            else:
                self._ingest(child_id)
                self._add_to_awaiting_local(child_id, record.id)

    def _process_rfi(self, record: Record[Any], rfi: RFI) -> None:
        child_id: str
        next_child_name = record.next_child_name()
        if isinstance(rfi.unit, Invocation):
            child_id = rfi.opts.id if rfi.opts.id is not None else next_child_name
        elif isinstance(rfi.unit, DurablePromise):
            child_id = rfi.unit.id if rfi.unit.id is not None else next_child_name
        else:
            assert_never(rfi.unit)
        child_record = self._records.get(child_id)
        if child_record is not None:
            record.add_child(child_record)
            self._add_to_runnable(record.id, Ok(child_record.promise))
        else:
            child_record = record.create_child(id=child_id, invocation=rfi)
            self._records[child_id] = child_record
            assert rfi.opts.durable

            data, headers, tags = self._get_info_from_rfi(rfi)

            durable_promise = self._store.promises.create(
                id=child_id,
                ikey=utils.string_to_uuid(child_id),
                strict=False,
                headers=headers,
                data=self._encoder.encode(data),
                timeout=sys.maxsize,
                tags=tags,
            )
            assert child_id in self._records
            assert not record.done()
            child_record.add_durable_promise(durable_promise)

            if durable_promise.is_completed():
                value = durable_promise.get_value(self._encoder)
                child_record.set_result(value, deduping=True)
            self._add_to_runnable(record.id, Ok(child_record.promise))

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
                durable_promise = self._store.promises.create(
                    id=child_id,
                    ikey=utils.string_to_uuid(child_id),
                    strict=False,
                    headers=None,
                    data=None,
                    timeout=sys.maxsize,
                    tags=None,
                )
                assert child_id in self._records
                assert not record.done()

                child_record.add_durable_promise(durable_promise)

                if durable_promise.is_completed():
                    value = durable_promise.get_value(self._encoder)
                    child_record.set_result(value, deduping=True)
                else:
                    self._ingest(child_id)
                self._add_to_runnable(record.id, Ok(child_record.promise))
            else:
                self._ingest(child_id)
                self._add_to_runnable(record.id, Ok(child_record.promise))

    def _process_final_value(
        self, record: Record[Any], final_value: Result[Any, Exception]
    ) -> None:
        if record.should_retry(final_value):
            record.increate_attempt()
            self._delay_queue.put_nowait(record.id, record.next_retry_delay())

        elif record.invocation.opts.durable:
            durable_promise: DurablePromiseRecord
            if isinstance(final_value, Ok):
                durable_promise = self._store.promises.resolve(
                    id=record.id,
                    ikey=utils.string_to_uuid(record.id),
                    strict=False,
                    headers=None,
                    data=self._encoder.encode(final_value.unwrap()),
                )

            elif isinstance(final_value, Err):
                durable_promise = self._store.promises.reject(
                    id=record.id,
                    ikey=utils.string_to_uuid(record.id),
                    strict=False,
                    headers=None,
                    data=self._encoder.encode(final_value.err()),
                )
            else:
                assert_never(final_value)

            final_value = durable_promise.get_value(self._encoder)
            assert not record.done()

            if record.has_task():
                self._complete_task(record.id)

            record.set_result(final_value, deduping=False)
            self._unblock_awaiting_local(record.id)

            root = record.root()
            if root != record and self._blocked_only_on_remote(root.id):
                self._complete_task(root.id)

        else:
            record.set_result(final_value, deduping=False)
            self._unblock_awaiting_local(record.id)

    def _get_info_from_rfi(
        self, rfi: RFI
    ) -> tuple[dict[str, Any] | None, dict[str, str] | None, dict[str, str] | None]:
        data: dict[str, Any] | None
        tags: dict[str, str] | None
        headers: dict[str, str] | None
        if isinstance(rfi.unit, DurablePromise):
            data = rfi.unit.data
            tags = rfi.unit.tags
            headers = rfi.unit.headers
        elif isinstance(rfi.unit, Invocation):
            func: str
            if isinstance(rfi.unit.fn, str):
                func = rfi.unit.fn
            else:
                assert isfunction(rfi.unit.fn)
                registered_fn_name = self._registry.get_from_value(rfi.unit.fn)
                assert registered_fn_name is not None
                func = registered_fn_name
            data = {
                "func": func,
                "args": rfi.unit.args,
                "kwargs": rfi.unit.kwargs,
            }
            tags = {"resonate:invoke": rfi.opts.send_to or "default"}
            headers = None
        else:
            assert_never(rfi.unit)
        return (data, headers, tags)
