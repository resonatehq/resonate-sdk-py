from __future__ import annotations

import contextlib
import sys
import time
from threading import Thread
from typing import TYPE_CHECKING, Any, Callable, Generic, TypeVar

import requests
from typing_extensions import ParamSpec

from resonate import utils
from resonate.encoders import JsonEncoder
from resonate.options import LOptions
from resonate.record import Record
from resonate.stores.local import LocalStore
from resonate.stores.remote import RemoteStore

if TYPE_CHECKING:
    from resonate.record import Handle
    from resonate.retry_policy import RetryPolicy
    from resonate.stores.local import LocalStore
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
    def __init__(
        self, pid: str, group: str, store: RemoteStore | LocalStore, adapter: IAdapter
    ) -> None:
        self._store = store
        self._pid = pid
        self._group = group
        self._registered_funcs = FunctionRegistry[str, Callable[[Any], Any]]()
        self._record: dict[str, Record[Any]] = {}
        self._adapter = adapter

        self._json_encoder = JsonEncoder()
        self._heartbeating_thread: Thread | None = None
        self._create_and_claim_task: bool = False
        if isinstance(self._store, RemoteStore):
            self._create_and_claim_task = True
            self._heartbeating_thread = Thread(target=self._heartbeat, daemon=True)
            self._heartbeating_thread.start()

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
                LOptions(durable=True, id=None, retry_policy=retry_policy, tags={}),
            ),
        )

    def rfi(
        self, id: str, func: str, args: tuple[Any, ...], *, target: str
    ) -> Handle[Any]:
        assert isinstance(
            self._store, RemoteStore
        ), "RFI are only available if using the RemoteStore."

        record = self._record.get(id)
        if record is not None:
            return record.handle

        attach_opts = self._registered_funcs.get(func)
        assert attach_opts is not None, f"Function {func} is not registered."
        assert self._create_and_claim_task, "If using the RemoteStore all root promises must be created while claiming task"  # noqa: E501
        tags = attach_opts[-1].tags.copy()
        tags["resonate:invoke"] = target
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
        record = Record[Any](id=id, durable_promise=durable_promise, task=task)
        self._record[id] = record
        return record.handle

    def lfi(
        self,
        id: str,
        func: DurableCoro[P, T] | DurableFn[P, T],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Handle[T]:
        record = self._record.get(id)
        if record is not None:
            return record.handle
        raise NotImplementedError
