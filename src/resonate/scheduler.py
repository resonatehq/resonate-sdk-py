from __future__ import annotations

import contextlib
import sys
import time
from collections import deque
from threading import Event, Thread
from typing import TYPE_CHECKING, Any, TypeVar

import requests
from typing_extensions import ParamSpec, assert_never

from resonate import utils
from resonate.collections import FunctionRegistry
from resonate.context import Context
from resonate.dataclasses import FnOrCoroutine
from resonate.encoders import JsonEncoder
from resonate.options import LOptions
from resonate.processor import FnCQE, FnSQE, Processor
from resonate.record import Record
from resonate.stores.record import Invoke, Resume, TaskRecord
from resonate.stores.remote import RemoteStore

if TYPE_CHECKING:
    from resonate.dependencies import Dependencies
    from resonate.queue import Queue
    from resonate.record import Handle
    from resonate.result import Result
    from resonate.retry_policy import RetryPolicy
    from resonate.stores.local import LocalStore
    from resonate.tracing import IAdapter
    from resonate.typing import DurableCoro, DurableFn

P = ParamSpec("P")
T = TypeVar("T")


class Scheduler:
    def __init__(  # noqa: PLR0913
        self,
        pid: str,
        group: str,
        store: RemoteStore | LocalStore,
        adapter: IAdapter,
        deps: Dependencies,
        max_workers: int | None,
        distribution_tag: str,
        stg_queue: Queue[Record[Any]],
        task_queue: Queue[TaskRecord],
        completion_queue: Queue[FnCQE[Any]],
        submission_queue: Queue[FnSQE[Any]],
    ) -> None:
        self._distribution_tag = distribution_tag

        self._runnable: deque[
            tuple[Record[Any], Result[Any, Exception] | None, bool]
        ] = deque()
        self._awaiting_locally: dict[Record[Any], list[Record[Any]]] = {}
        self._awaiting_remotely: dict[Record[Any], list[Record[Any]]] = {}

        self._store = store
        self._pid = pid
        self._group = group
        self._registered_funcs = FunctionRegistry()
        self._records: dict[str, Record[Any]] = {}
        self._adapter = adapter

        self._json_encoder = JsonEncoder()
        self._heartbeating_thread: Thread | None = None
        if isinstance(self._store, RemoteStore):
            self._heartbeating_thread = Thread(target=self._heartbeat, daemon=True)
            self._heartbeating_thread.start()

        self._ctx = Context(deps=deps)
        self._stg_queue = stg_queue
        self._task_queue = task_queue
        self._completion_queue = completion_queue
        self._submission_queue = submission_queue

        self._worker_continue = Event()
        self._worker_thread = Thread(target=self._run, daemon=True)
        self._worker_thread.start()

        self._processor = Processor(
            max_workers=max_workers,
            sq=self._submission_queue,
            scheduler=self,
        )

    def _signal(self) -> None:
        self._worker_continue.set()

    def enqueue_cqe(self, cqe: FnCQE[Any]) -> None:
        self._completion_queue.put_nowait(cqe)
        self._signal()

    def enqueue_task_record(self, task_record: TaskRecord) -> None:
        self._task_queue.put_nowait(task_record)
        self._signal()

    def _run(self) -> None:
        while self._worker_continue.wait():
            self._worker_continue.clear()

            for _top_lvl in self._stg_queue.dequeue_all():
                raise NotImplementedError

            for task in self._task_queue.dequeue_all():
                assert isinstance(
                    self._store, RemoteStore
                ), "We can only receive tasks if using the remote store."
                msg = self._store.tasks.claim(
                    task_id=task.task_id,
                    counter=task.counter,
                    pid=self._pid,
                    ttl=5 * 1_000,
                )
                if isinstance(msg, (Invoke, Resume)):
                    raise NotImplementedError
                assert_never(msg)

    def _heartbeat(self) -> None:
        assert isinstance(self._store, RemoteStore)
        while True:
            with contextlib.suppress(requests.exceptions.ConnectionError):
                self._store.tasks.heartbeat(pid=self._pid)
            time.sleep(2)

    def register(
        self,
        func: DurableCoro[P, T] | DurableFn[P, T],
        name: str | None,
        retry_policy: RetryPolicy | None,
        tags: dict[str, str] | None,
    ) -> None:
        if name is None:
            name = func.__name__
        assert (
            self._registered_funcs.get(name) is None
        ), f"There's already a function registered under name={name}"
        self._registered_funcs.add(
            key=name,
            value=(
                func,
                LOptions(
                    durable=True,
                    id=None,
                    retry_policy=retry_policy,
                    tags=tags if tags is not None else {},
                ),
            ),
        )

    def rfi(
        self, id: str, func: str, args: tuple[Any, ...], *, target: str
    ) -> Handle[Any]:
        assert isinstance(
            self._store, RemoteStore
        ), "RFI are only available if using the RemoteStore."

        # If there's already a record with this ID, dedup.
        record = self._records.get(id)
        if record is not None:
            return record.handle

        # Check if function is registered.
        attach_opts = self._registered_funcs.get(func)
        assert attach_opts is not None, f"Function {func} is not registered."

        # Add distribution tag if does not exists.
        tags = attach_opts[-1].tags.copy()
        if self._distribution_tag not in tags:
            tags[self._distribution_tag] = target

        # Create durable promise while claiming the task.
        durable_promise, task = self._store.promises.create_with_task(
            id=id,
            ikey=utils.string_to_ikey(id),
            strict=False,
            headers=None,
            data=self._json_encoder.encode({"func": func, "args": args, "kwargs": {}}),
            timeout=sys.maxsize,
            tags=tags,
            pid=self._pid,
            ttl=5 * 1_000,
            recv=utils.recv_url(group=self._group, pid=self._pid),
        )

        # Create an add record to the record registry
        record = Record[Any](
            id=id,
            durable_promise=durable_promise,
            task=task,
            parent_id=None,
            ctx=self._ctx,
            fn_or_coro=None,
        )
        self._records[id] = record
        if durable_promise.is_completed():
            v = durable_promise.get_value(self._json_encoder)
            record.set_result(v)
            self._records.pop(record.id)
        else:
            # Add record to awaiting remotely, blocking nothing.
            self._awaiting_remotely[record] = []
        return record.handle

    def lfi(
        self,
        id: str,
        func: DurableCoro[P, T] | DurableFn[P, T],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Handle[T]:
        # If there's a record with the same ID, dedup
        record = self._records.get(id)
        if record is not None:
            return record.handle

        # Check passed function is already registered.
        func_name = self._registered_funcs.get_from_value(func)
        assert func_name is not None, f"Func {func.__name__} must be registered."

        attach_options = self._registered_funcs.get(func_name)
        assert attach_options is not None
        opts = attach_options[-1]
        assert opts.durable, "Top level must always be durable."
        # check there's no tag that would cause the promise to get distributed.
        assert (
            self._distribution_tag not in opts.tags
        ), "This tag is reserved for distribution only"

        durable_promise, task = self._store.promises.create_with_task(
            id=id,
            ikey=utils.string_to_ikey(id),
            strict=False,
            headers=None,
            data=self._json_encoder.encode(
                {"func": func_name, "args": args, "kwargs": kwargs}
            ),
            timeout=sys.maxsize,
            tags=opts.tags,
            pid=self._pid,
            ttl=5 * 1_000,
            recv=utils.recv_url(group=self._group, pid=self._pid),
        )

        record = Record[Any](
            id=id,
            durable_promise=durable_promise,
            task=task,
            parent_id=None,
            ctx=self._ctx,
            fn_or_coro=FnOrCoroutine[Any](func, *args, **kwargs),
        )
        self._records[record.id] = record
        if durable_promise.is_completed():
            v = durable_promise.get_value(self._json_encoder)
            record.set_result(v)
            self._records.pop(record.id)
        else:
            # Add record to the staging queue so it's processed
            # by the scheduler.
            self._stg_queue.put_nowait(record)
            self._signal()
        return record.handle
