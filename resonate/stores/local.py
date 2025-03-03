from __future__ import annotations

from concurrent.futures import Future
import sys
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal, Protocol, final

from typing_extensions import Any

from resonate.encoders.base64 import Base64Encoder
from resonate.encoders.chain import ChainEncoder
from resonate.encoders.json import JsonEncoder
from resonate.errors import ResonateError
from resonate.models.callback import Callback
from resonate.models.commands import Invoke, Notify, Resume
from resonate.models.durable_promise import DurablePromise, DurablePromiseValue
from resonate.models.message import Mesg
from resonate.models.task import Task

if TYPE_CHECKING:
    from resonate.models.encoder import Encoder
    from resonate.models.enqueueable import Enqueueable
    from resonate.models.store import Store
    from resonate.registry import Registry

# Fake it till you make it
# >>>>>>>>>>>>>>>>>>>>>>>>>>>>

type Recv = str


class Router(Protocol):
    def route(self, promise: DurablePromiseRecord) -> Recv | None: ...


class TagRouter:
    def __init__(self, tag: str = "resonate:invoke") -> None:
        self.tag = tag

    def route(self, promise: DurablePromiseRecord) -> Recv | None:
        return (promise.tags or {}).get(self.tag)


class Sender(Protocol):
    def send(self, recv: Recv, mesg: Mesg) -> None: ...


class LocalSender:
    def __init__(self, q: Enqueueable[Invoke | Resume | Notify], registry: Registry, store: Store, ttl: int = sys.maxsize) -> None:
        self._q = q
        self._registry = registry
        self._store = store
        self._ttl = ttl

    def send(self, recv: Recv, mesg: Mesg) -> None:
        # translates a msg into a cmd
        match mesg:
            case {"type": "invoke", "task": task_mesg}:
                root, leaf = self._store.tasks.claim(id=task_mesg["id"], counter=task_mesg["counter"], pid=recv, ttl=self._ttl)
                assert leaf is None
                info = self._get_information(root)
                self._q.enqueue(
                    Invoke(
                        root.id,
                        info["name"],
                        info["func"],
                        info["args"],
                        info["kwargs"],
                        root,
                        Task(
                            id=task_mesg["id"],
                            counter=task_mesg["counter"],
                            store=self._store,
                        ),
                    ),
                )
            case {"type": "resume", "task": task_mesg}:
                root, leaf = self._store.tasks.claim(id=task_mesg["id"], counter=task_mesg["counter"], pid=recv, ttl=self._ttl)
                assert leaf is not None
                assert leaf.completed
                root_info = self._get_information(root)
                self._q.enqueue(
                    Resume(
                        id=leaf.id,
                        cid=root.id,
                        promise=leaf,
                        task=Task(id=task_mesg["id"], counter=task_mesg["counter"], store=self._store),
                        invoke=Invoke(
                            root.id,
                            root_info["name"],
                            root_info["func"],
                            root_info["args"],
                            root_info["kwargs"],
                            root,
                            Task(
                                id=task_mesg["id"],
                                counter=task_mesg["counter"],
                                store=self._store,
                            ),
                        ),
                    )
                )
            case {"type": "notify", "task": task_mesg}:
                raise NotImplementedError

    def _get_information(self, promise: DurablePromise) -> dict[str, Any]:
        params = promise.params
        assert isinstance(params, dict)
        assert params, "data must be in promise param"
        assert "func" in params, "func must be in promise param data"
        assert "args" in params, "args must be in promise param data"
        assert "kwargs" in params, "func must be in promise param data"

        return {"name": params["func"], "func": self._registry.get(params["func"]), "args": params["args"], "kwargs": params["kwargs"]}


# <<<<<<<<<<<<<<<<<<<<<<<<<<<<<


class LocalStore:
    def __init__(self, encoder: Encoder[Any, str | None] | None = None) -> None:
        self._encoder = encoder or ChainEncoder(
            JsonEncoder(),
            Base64Encoder(),
        )
        self._promises: dict[str, DurablePromiseRecord] = {}
        self._tasks: dict[str, TaskRecord] = {}

        self._cq: Enqueueable[Mesg] | None = None
        self._routers: list[Router] = [TagRouter()]

    @property
    def promises(self) -> LocalPromiseStore:
        return LocalPromiseStore(
            self,
            self._encoder,
            self._promises,
            self._tasks,
            self._routers,
        )

    @property
    def tasks(self) -> LocalTaskStore:
        return LocalTaskStore(
            self,
            self._encoder,
            self._promises,
            self._tasks,
        )

    def add_cq(self, cq: Enqueueable[Mesg]) -> None:
        self._cq = cq

    def add_router(self, router: Router) -> None:
        self._routers.append(router)

    def step(self) -> None:
        assert self._cq
        current_time = now()
        for task in self._tasks.values():
            match task:
                case TaskRecord(state="INIT", type="invoke"):
                    self._cq.enqueue({"type": "invoke", "task": {"id": task.id, "counter": task.counter}})
                    self.tasks.transition(id=task.id, to="ENQUEUED")
                case TaskRecord(state="INIT", type="resume"):
                    self._cq.enqueue({"type": "resume", "task": {"id": task.id, "counter": task.counter}})
                    self.tasks.transition(id=task.id, to="ENQUEUED")
                case TaskRecord(state="CLAIMED") if task.ttl and task.ttl < current_time:
                    self.tasks.transition(id=task.id, to="INIT")

        for promise in self._promises.values():
            match promise:
                case DurablePromiseRecord(state="PENDING") if promise.timeout < current_time:
                    self.promises.transition(id=promise.id, to="REJECTED_TIMEDOUT", current_time=current_time)


class LocalPromiseStore:
    def __init__(self, store: LocalStore, encoder: Encoder[Any, str | None], promises: dict[str, DurablePromiseRecord], tasks: dict[str, TaskRecord], routers: list[Router]) -> None:
        self._store = store
        self._encoder = encoder
        self._promises = promises
        self._tasks = tasks
        self._routers = routers

    def create(
        self,
        *,
        id: str,
        timeout: int,
        ikey: str | None = None,
        strict: bool = False,
        headers: dict[str, str] | None = None,
        data: Any = None,
        tags: dict[str, str] | None = None,
    ) -> DurablePromise:
        promise, task = self._create(
            id=id,
            ikey=ikey,
            strict=strict,
            headers=headers,
            data=data,
            timeout=timeout,
            tags=tags,
        )
        assert task is None
        return promise

    def create_with_task(
        self,
        *,
        id: str,
        timeout: int,
        pid: str,
        ttl: int,
        ikey: str | None = None,
        strict: bool = False,
        headers: dict[str, str] | None = None,
        data: Any = None,
        tags: dict[str, str] | None = None,
    ) -> tuple[DurablePromise, Task | None]:
        return self._create(
            id=id,
            ikey=ikey,
            strict=strict,
            headers=headers,
            data=data,
            timeout=timeout,
            tags=tags,
            pid=pid,
            ttl=ttl,
        )

    def resolve(
        self,
        *,
        id: str,
        ikey: str | None = None,
        strict: bool = False,
        headers: dict[str, str] | None = None,
        data: Any = None,
    ) -> DurablePromise:
        return self._complete(id=id, ikey=ikey, strict=strict, headers=headers, data=data, new_state="RESOLVED")

    def reject(
        self,
        *,
        id: str,
        ikey: str | None = None,
        strict: bool = False,
        headers: dict[str, str] | None = None,
        data: Any = None,
    ) -> DurablePromise:
        return self._complete(id=id, ikey=ikey, strict=strict, headers=headers, data=data, new_state="REJECTED")

    def cancel(
        self,
        *,
        id: str,
        ikey: str | None = None,
        strict: bool = False,
        headers: dict[str, str] | None = None,
        data: Any = None,
    ) -> DurablePromise:
        return self._complete(id=id, ikey=ikey, strict=strict, headers=headers, data=data, new_state="REJECTED_CANCELED")

    def callback(
        self,
        *,
        id: str,
        promise_id: str,
        root_promise_id: str,
        timeout: int,
        recv: str,
    ) -> tuple[DurablePromise, Callback | None]:
        promise = self._promises.get(promise_id)
        if promise is None:
            msg = "Task not found"
            raise ResonateError(msg, "STORE_NOT_FOUND")
        durable_promise = DurablePromise.from_dict(
            self._store,
            promise.to_dict(),
            self._encoder.decode(promise.param.data),
            self._encoder.decode(promise.value.data),
        )
        if promise.completed_on is not None:
            return durable_promise, None

        callback = CallbackRecord(id=id, type="resume", promise_id=promise_id, root_promise_id=root_promise_id, timeout=timeout, created_on=now(), recv=recv)
        promise.callbacks.append(callback)
        return durable_promise, Callback(
            id=callback.id,
            promise_id=callback.promise_id,
            timeout=callback.timeout,
            created_on=callback.created_on,
        )

    def _create(
        self,
        *,
        id: str,
        timeout: int,
        ikey: str | None = None,
        strict: bool = False,
        headers: dict[str, str] | None = None,
        data: Any = None,
        tags: dict[str, str] | None = None,
        pid: str | None = None,
        ttl: int = 0,
    ) -> tuple[DurablePromise, Task | None]:
        current_time = now()
        record, applied = self.transition(
            id=id,
            to="PENDING",
            strict=strict,
            ikey=ikey,
            headers=headers,
            data=data,
            tags=tags,
            current_time=current_time,
            timeout=timeout,
        )

        task: Task | None = None
        if applied:
            for r in self._routers:
                if recv := r.route(record):
                    task_record = self._store.tasks.transition(
                        id=str(len(self._tasks) + 1),
                        to="INIT",
                        type="invoke",
                        recv=recv,
                        current_time=current_time,
                        root_promise_id=record.id,
                        leaf_promise_id=record.id,
                    )
                    if pid is not None:
                        task_record = self._store.tasks.transition(
                            id=task_record.id,
                            to="CLAIMED",
                            pid=pid,
                            ttl=ttl,
                            counter=1,
                        )
                        task = Task.from_dict(self._store, task_record.to_dict())
                    break
        return DurablePromise.from_dict(
            self._store,
            record.to_dict(),
            self._encoder.decode(record.param.data),
            self._encoder.decode(record.value.data),
        ), task

    def _complete(
        self,
        *,
        id: str,
        new_state: Literal["REJECTED_CANCELED", "REJECTED", "RESOLVED"],
        ikey: str | None = None,
        strict: bool = False,
        headers: dict[str, str] | None = None,
        data: Any = None,
    ) -> DurablePromise:
        current_time = now()
        record, applied = self.transition(id=id, to=new_state, strict=strict, headers=headers, data=data, current_time=current_time, ikey=ikey)
        if applied:
            for callback in record.callbacks:
                task_record = self._store.tasks.transition(
                    id=str(len(self._tasks) + 1),
                    to="INIT",
                    type=callback.type,
                    recv=callback.recv,
                    current_time=current_time,
                    root_promise_id=callback.root_promise_id,
                    leaf_promise_id=callback.promise_id,
                )
            record.callbacks.clear()

        return DurablePromise.from_dict(
            self._store,
            record.to_dict(),
            self._encoder.decode(record.param.data),
            self._encoder.decode(record.value.data),
        )

    def transition(
        self,
        id: str,
        to: Literal["PENDING", "RESOLVED", "REJECTED", "REJECTED_CANCELED", "REJECTED_TIMEDOUT"],
        current_time: int,
        strict: bool | None = None,
        timeout: int | None = None,
        ikey: str | None = None,
        headers: dict[str, str] | None = None,
        data: Any = None,
        tags: dict[str, str] | None = None,
    ) -> tuple[DurablePromiseRecord, bool]:
        match record := self._promises.get(id), to, strict:
            case None, "PENDING", _:
                assert timeout is not None
                assert current_time is not None
                record = DurablePromiseRecord(
                    id=id,
                    state=to,
                    timeout=timeout,
                    param=DurablePromiseRecordValue(
                        headers=headers or {},
                        data=self._encoder.encode(data),
                    ),
                    value=DurablePromiseRecordValue(headers={}, data=None),
                    created_on=current_time,
                    completed_on=None,
                    ikey_for_create=ikey,
                    ikey_for_complete=None,
                    tags=tags,
                    callbacks=[],
                )
                self._promises[record.id] = record
                return record, True
            case DurablePromiseRecord(state="PENDING"), "PENDING", _ if current_time < record.timeout and ikey_match(record.ikey_for_create, ikey):
                return record, False
            case DurablePromiseRecord(state="PENDING"), "PENDING", False if current_time >= record.timeout and ikey_match(record.ikey_for_create, ikey):
                return self.transition(id=id, to="REJECTED_TIMEDOUT", current_time=current_time)
            case DurablePromiseRecord(state="PENDING"), "RESOLVED" | "REJECTED" | "REJECTED_CANCELED", _ if current_time < record.timeout:
                record = DurablePromiseRecord(
                    id=record.id,
                    state=to,
                    timeout=record.timeout,
                    param=record.param,
                    value=DurablePromiseRecordValue(headers=headers, data=self._encoder.encode(data)),
                    created_on=record.created_on,
                    completed_on=current_time,
                    ikey_for_create=record.ikey_for_create,
                    ikey_for_complete=ikey,
                    tags=record.tags,
                    callbacks=record.callbacks,
                )
                self._promises[record.id] = record
                return record, True
            case DurablePromiseRecord(state="PENDING"), "RESOLVED" | "REJECTED" | "REJECTED_CANCELED", False if current_time >= record.timeout:
                return self.transition(id=id, to="REJECTED_TIMEDOUT", current_time=current_time)
            case DurablePromiseRecord(state="PENDING"), "RESOLVED" | "REJECTED" | "REJECTED_CANCELED", True if current_time >= record.timeout:
                self.transition(id=id, to="REJECTED_TIMEDOUT", current_time=current_time)
                msg = "Promise has timedout"
                raise ResonateError(msg, "STORE_FORBIDDEN")
            case DurablePromiseRecord(state="PENDING"), "REJECTED_TIMEDOUT", _:
                assert current_time >= record.timeout
                record = DurablePromiseRecord(
                    id=record.id,
                    state="RESOLVED" if record.tags and record.tags.get("resonate:timeout") == "true" else to,
                    timeout=record.timeout,
                    param=record.param,
                    value=DurablePromiseRecordValue(headers={}, data=None),
                    tags=record.tags,
                    created_on=record.created_on,
                    completed_on=record.completed_on,
                    ikey_for_create=record.ikey_for_create,
                    ikey_for_complete=None,
                    callbacks=record.callbacks,
                )
                self._promises[record.id] = record
                return record, True
            case DurablePromiseRecord(state="RESOLVED" | "REJECTED" | "REJECTED_CANCELED"), "RESOLVED" | "REJECTED" | "REJECTED_CANCELED", False if ikey_match(record.ikey_for_complete, ikey):
                return record, False
            case DurablePromiseRecord(state="RESOLVED" | "REJECTED" | "REJECTED_CANCELED"), "RESOLVED" | "REJECTED" | "REJECTED_CANCELED", True if (
                ikey_match(record.ikey_for_complete, ikey) and record.state == to
            ):
                return record, False
            case DurablePromiseRecord(state="RESOLVED" | "REJECTED" | "REJECTED_CANCELED"), "PENDING", False if ikey_match(record.ikey_for_create, ikey):
                return record, False
            case None, "RESOLVED" | "REJECTED" | "REJECTED_CANCELED", _:
                raise ResonateError("Not found", "STORE_NOT_FOUND")
            case _:
                msg = "Forbidden request"
                raise ResonateError(msg, "STORE_FORBIDDEN")


class LocalTaskStore:
    def __init__(self, store: LocalStore, encoder: Encoder[Any, str | None], promises: dict[str, DurablePromiseRecord], tasks: dict[str, TaskRecord]) -> None:
        self._store = store
        self._encoder = encoder
        self._promises = promises
        self._tasks = tasks

    def claim(
        self,
        *,
        id: str,
        counter: int,
        pid: str,
        ttl: int,
    ) -> tuple[DurablePromise, DurablePromise | None]:
        task_record = self.transition(id=id, to="CLAIMED", pid=pid, ttl=ttl, counter=counter)
        match task_record.type:
            case "invoke":
                root_promise = self._promises[task_record.root_promise_id]
                return DurablePromise.from_dict(
                    self._store,
                    root_promise.to_dict(),
                    self._encoder.decode(root_promise.param.data),
                    self._encoder.decode(root_promise.value.data),
                ), None
            case "resume":
                root_promise = self._promises[task_record.root_promise_id]
                leaf_promise = self._promises[task_record.leaf_promise_id]
                return DurablePromise.from_dict(
                    self._store,
                    root_promise.to_dict(),
                    self._encoder.decode(root_promise.param.data),
                    self._encoder.decode(root_promise.value.data),
                ), DurablePromise.from_dict(
                    self._store,
                    leaf_promise.to_dict(),
                    self._encoder.decode(leaf_promise.param.data),
                    self._encoder.decode(leaf_promise.value.data),
                )
            case "notify":
                raise NotImplementedError

    def complete(self, *, id: str, counter: int) -> bool:
        self.transition(id=id, to="COMPLETED", counter=counter, current_time=now())
        return True

    def heartbeat(self, *, pid: str) -> int:
        affected_tasks = 0
        for task in self._tasks.values():
            if task.state != "CLAIMED":
                continue
            if task.pid == pid:
                affected_tasks += 1
        return affected_tasks

    def transition(
        self,
        *,
        id: str,
        to: Literal["INIT", "ENQUEUED", "CLAIMED", "COMPLETED"],
        type: Literal["invoke", "resume", "notify"] | None = None,
        recv: Recv | None = None,
        root_promise_id: str | None = None,
        leaf_promise_id: str | None = None,
        current_time: int | None = None,
        counter: int | None = None,
        pid: str | None = None,
        ttl: int | None = None,
    ) -> TaskRecord:
        match record := self._tasks.get(id), to:
            case None, "INIT":
                assert type is not None
                assert recv is not None
                assert root_promise_id is not None
                assert leaf_promise_id is not None
                assert current_time is not None
                record = TaskRecord(
                    id=id,
                    counter=1,
                    state=to,
                    type=type,
                    recv=recv,
                    root_promise_id=root_promise_id,
                    leaf_promise_id=leaf_promise_id,
                    pid=None,
                    ttl=None,
                    created_on=current_time,
                    completed_on=None,
                )
                self._tasks[record.id] = record
                return record

            case TaskRecord(state="INIT"), "ENQUEUED":
                record = TaskRecord(
                    id=record.id,
                    counter=record.counter,
                    state=to,
                    type=record.type,
                    recv=record.recv,
                    root_promise_id=record.root_promise_id,
                    leaf_promise_id=record.leaf_promise_id,
                    pid=record.pid,
                    ttl=record.ttl,
                    created_on=record.created_on,
                    completed_on=record.completed_on,
                )
                self._tasks[record.id] = record
                return record
            case TaskRecord(state="INIT"), "CLAIMED" if record.counter == counter:
                assert ttl is not None
                record = TaskRecord(
                    id=record.id,
                    counter=record.counter,
                    state=to,
                    type=record.type,
                    recv=record.recv,
                    root_promise_id=record.root_promise_id,
                    leaf_promise_id=record.leaf_promise_id,
                    pid=pid,
                    ttl=ttl,
                    created_on=record.created_on,
                    completed_on=record.completed_on,
                )
                self._tasks[record.id] = record
                return record

            case TaskRecord(state="ENQUEUED"), "CLAIMED" if record.counter == counter:
                assert ttl is not None
                assert pid is not None
                record = TaskRecord(
                    id=record.id,
                    counter=record.counter,
                    state=to,
                    type=record.type,
                    recv=record.recv,
                    root_promise_id=record.root_promise_id,
                    leaf_promise_id=record.leaf_promise_id,
                    pid=pid,
                    ttl=ttl,
                    created_on=record.created_on,
                    completed_on=record.completed_on,
                )
                self._tasks[record.id] = record
                return record
            case TaskRecord(state="CLAIMED"), "COMPLETED" if record.counter == counter and (record.ttl is not None and current_time is not None and record.ttl >= current_time):
                record = TaskRecord(
                    id=record.id,
                    counter=record.counter,
                    state=to,
                    type=record.type,
                    recv=record.recv,
                    root_promise_id=record.root_promise_id,
                    leaf_promise_id=record.leaf_promise_id,
                    pid=None,
                    ttl=None,
                    created_on=record.created_on,
                    completed_on=current_time,
                )
                self._tasks[record.id] = record
                return record
            case None, _:
                msg = "Task not found"
                raise ResonateError(msg, "STORE_NOT_FOUND")
            case _:
                msg = "Task already claimed, completed, or invalid counter"
                raise ResonateError(
                    msg,
                    "STORE_FORBIDDEN",
                )


def now() -> int:
    return int(time.time() * 1000)


@final
@dataclass(frozen=True)
class DurablePromiseRecord:
    id: str
    state: Literal["PENDING", "RESOLVED", "REJECTED", "REJECTED_CANCELED", "REJECTED_TIMEDOUT"]
    timeout: int
    ikey_for_create: str | None
    ikey_for_complete: str | None
    param: DurablePromiseRecordValue
    value: DurablePromiseRecordValue
    tags: dict[str, str] | None
    created_on: int
    completed_on: int | None
    callbacks: list[CallbackRecord]

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "state": self.state,
            "timeout": self.timeout,
            "idempotencyKeyForCreate": self.ikey_for_create,
            "idempotencyKeyForComplete": self.ikey_for_complete,
            "param": self.param.to_dict(),
            "value": self.value.to_dict(),
            "tags": self.tags or {},
            "createdOn": self.created_on,
            "completedOn": self.completed_on,
        }


@final
@dataclass(frozen=True)
class DurablePromiseRecordValue:
    headers: dict[str, str] | None
    data: str | None

    def to_dict(self) -> dict[str, Any]:
        return {"headers": self.headers, "data": self.data}


@final
@dataclass(frozen=True)
class CallbackRecord:
    id: str
    type: Literal["resume", "notify"]
    root_promise_id: str
    promise_id: str
    timeout: int
    created_on: int
    recv: str


@final
@dataclass(frozen=True)
class TaskRecord:
    id: str
    counter: int
    state: Literal["INIT", "ENQUEUED", "CLAIMED", "COMPLETED"]
    type: Literal["invoke", "resume", "notify"]
    recv: str
    root_promise_id: str
    leaf_promise_id: str
    pid: str | None
    ttl: int | None
    created_on: int
    completed_on: int | None

    def to_dict(self) -> dict[str, Any]:
        return {"id": self.id, "counter": self.counter}


def ikey_match(left: str | None, right: str | None) -> bool:
    return left is not None and right is not None and left == right
