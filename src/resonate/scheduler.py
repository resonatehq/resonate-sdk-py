from __future__ import annotations

import contextlib
import sys
import time
from collections import deque
from threading import Event, Thread
from typing import TYPE_CHECKING, Any, Callable, Generic, TypeVar

import requests
from typing_extensions import ParamSpec, assert_never

from resonate import utils
from resonate.context import Context
from resonate.dataclasses import FnOrCoroutine
from resonate.encoders import JsonEncoder
from resonate.options import LOptions
from resonate.processor import FnCQE, FnSQE, Processor
from resonate.queue import Queue
from resonate.record import Record
from resonate.stores.local import LocalStore
from resonate.stores.record import Invoke, Resume, TaskRecord
from resonate.stores.remote import RemoteStore

if TYPE_CHECKING:
    from resonate.dependencies import Dependencies
    from resonate.record import Handle
    from resonate.result import Result
    from resonate.retry_policy import RetryPolicy
    from resonate.tracing import IAdapter
    from resonate.typing import DurableCoro, DurableFn

P = ParamSpec("P")
T = TypeVar("T")
K = TypeVar("K")
V = TypeVar("V")


class FunctionRegistry(Generic[K, V]):
    def __init__(self) -> None:
        self._store: dict[K, tuple[V, LOptions]] = {}
        self._index: dict[V, K] = {}

    def add(self, key: K, value: tuple[V, LOptions]) -> None:
        assert key not in self._store
        assert value[0] not in self._index
        self._store[key] = value
        self._index[value[0]] = key

    def get(self, key: K) -> tuple[V, LOptions] | None:
        return self._store.get(key)

    def get_from_value(self, v: V) -> K | None:
        return self._index.get(v)


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
        self._registered_funcs = FunctionRegistry[str, Callable[[Any], Any]]()
        self._records: dict[str, Record[Any]] = {}
        self._adapter = adapter

        self._json_encoder = JsonEncoder()
        self._heartbeating_thread: Thread | None = None
        if isinstance(self._store, RemoteStore):
            self._heartbeating_thread = Thread(target=self._heartbeat, daemon=True)
            self._heartbeating_thread.start()

        self._ctx = Context(deps=deps)
        self._stg_queue = Queue[Record[Any]]()
        self._task_queue = Queue[TaskRecord]()
        self._completion_queue = Queue[FnCQE[Any]]()
        self._submission_queue = Queue[FnSQE[Any]]()

        self._processor = Processor(
            max_workers=max_workers, sq=self._submission_queue, scheduler=self
        )

        self._worker_continue = Event()
        self._worker_thread = Thread(target=self._run, daemon=True)
        self._worker_thread.start()

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

        record = self._records.get(id)
        if record is not None:
            return record.handle

        attach_opts = self._registered_funcs.get(func)
        assert attach_opts is not None, f"Function {func} is not registered."
        tags = attach_opts[-1].tags.copy()
        if self._distribution_tag not in tags:
            tags[self._distribution_tag] = target
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
        assert task is not None
        record = Record[Any](
            id=id,
            durable_promise=durable_promise,
            task=task,
            parent_id=None,
            ctx=self._ctx,
            fn_or_coro=None,
        )
        self._records[id] = record
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
        record = self._records.get(id)
        if record is not None:
            return record.handle

        func_name = self._registered_funcs.get_from_value(func)
        assert func_name is not None, f"Func {func.__name__} must be registered."
        attach_options = self._registered_funcs.get(func_name)
        assert attach_options is not None
        opts = attach_options[-1]
        assert opts.durable, "Top level must always be durable."
        assert (
            self._distribution_tag not in opts.tags
        ), "This tag is reserved for distribution only"

        if isinstance(self._store, RemoteStore):
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
        elif isinstance(self._store, LocalStore):
            durable_promise = self._store.promises.create(
                id=id,
                ikey=utils.string_to_ikey(id),
                strict=False,
                headers=None,
                data=self._json_encoder.encode(
                    {"func": func, "args": args, "kwargs": kwargs}
                ),
                timeout=sys.maxsize,
                tags=opts.tags,
            )
        else:
            assert_never(self._store)

        record = Record[Any](
            id=id,
            durable_promise=durable_promise,
            task=task,
            parent_id=None,
            ctx=self._ctx,
            fn_or_coro=FnOrCoroutine[Any](func, *args, **kwargs),
        )
        self._records[id] = record
        self._stg_queue.put_nowait(record)
        self._signal()
        return record.handle
