from __future__ import annotations

from os import pardir
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal, Protocol, TypedDict, final

from typing_extensions import Any

from resonate.encoders.base64 import Base64Encoder
from resonate.encoders.chain import ChainEncoder
from resonate.encoders.json import JsonEncoder
from resonate.errors import ResonateError
from resonate.models.callback import Callback
from resonate.models.durable_promise import DurablePromise, DurablePromiseValue
from resonate.models.message import InvokeMesg, Mesg, ResumeMesg, TaskMesg
from resonate.models.task import Task

if TYPE_CHECKING:
    from resonate.models.encoder import Encoder

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


# <<<<<<<<<<<<<<<<<<<<<<<<<<<<<


def now() -> int:
    return int(time.time() * 1000)


class LocalStore:
    def __init__(self, encoder: Encoder[Any, str | None] | None = None) -> None:
        self._encoder = encoder or ChainEncoder(
            JsonEncoder(),
            Base64Encoder(),
        )
        self._promises: dict[str, DurablePromiseRecord] = {}
        self._tasks: dict[str, TaskRecord] = {}

        self._routers: list[Router] = [TagRouter()]
        self._senders: dict[str, Sender] = {}

    @property
    def promises(self) -> LocalPromiseStore:
        return LocalPromiseStore(
            self,
            self._encoder,
            self._promises,
            self._tasks,
            self._routers,
            self._senders,
        )

    @property
    def tasks(self) -> LocalTaskStore:
        return LocalTaskStore(self, self._encoder, self._promises, self._tasks)

    def add_router(self, router: Router) -> None:
        self._routers.append(router)

    def add_sender(self, recv: str, sender: Sender) -> None:
        assert recv not in self._senders
        self._senders[recv] = sender

    def rmv_sender(self, recv: str) -> None:
        assert recv in self._senders
        del self._senders[recv]

    def send_task(self, task: TaskRecord) -> bool:
        sender = self._senders.get(task.recv)
        if sender is None:
            return False
        match task.type:
            case "invoke":
                sender.send(
                    recv=task.recv,
                    mesg=InvokeMesg(type="invoke", task=TaskMesg(id=task.id, counter=task.counter)),
                )
            case "resume":
                sender.send(
                    recv=task.recv,
                    mesg=ResumeMesg(type="resume", task=TaskMesg(id=task.id, counter=task.counter)),
                )
            case "notify":
                raise NotImplementedError
        return True

    def _transition_task(self, source: TaskRecord | None, target: ToInit | ToClaimed | ToEnqueued | ToCompleted) -> TaskRecord:
        new_task: TaskRecord
        match source, target:
            case None, ToInit(type, recv, created_on, root_promise_id, leaf_promise_id):
                new_task = TaskRecord(
                    id=str(len(self._tasks) + 1),
                    counter=1,
                    state="INIT",
                    type=type,
                    recv=recv,
                    root_promise_id=root_promise_id,
                    leaf_promise_id=leaf_promise_id,
                    pid=None,
                    ttl=0,
                    created_on=created_on,
                    completed_on=None,
                )
            case TaskRecord(state="INIT"), ToClaimed(pid, ttl):
                new_task = TaskRecord(
                    id=source.id,
                    counter=source.counter,
                    state="CLAIMED",
                    type=source.type,
                    recv=source.recv,
                    root_promise_id=source.root_promise_id,
                    leaf_promise_id=source.leaf_promise_id,
                    pid=pid,
                    ttl=ttl,
                    created_on=source.created_on,
                    completed_on=source.completed_on,
                )
            case TaskRecord(state="INIT"), ToEnqueued():
                new_task = TaskRecord(
                    id=source.id,
                    counter=source.counter,
                    state="ENQUEUED",
                    type=source.type,
                    recv=source.recv,
                    root_promise_id=source.root_promise_id,
                    leaf_promise_id=source.leaf_promise_id,
                    pid=source.pid,
                    ttl=source.ttl,
                    created_on=source.created_on,
                    completed_on=source.completed_on,
                )
            case TaskRecord(state="ENQUEUED"), ToClaimed(pid, ttl):
                new_task = TaskRecord(
                    id=source.id,
                    counter=source.counter,
                    state="CLAIMED",
                    type=source.type,
                    recv=source.recv,
                    root_promise_id=source.root_promise_id,
                    leaf_promise_id=source.leaf_promise_id,
                    pid=pid,
                    ttl=ttl,
                    created_on=source.created_on,
                    completed_on=source.completed_on,
                )
            case TaskRecord(state="CLAIMED"), ToCompleted(completed_on):
                new_task = TaskRecord(
                    id=source.id,
                    counter=source.counter,
                    state="COMPLETED",
                    type=source.type,
                    recv=source.recv,
                    root_promise_id=source.root_promise_id,
                    leaf_promise_id=source.leaf_promise_id,
                    pid=None,
                    ttl=None,
                    created_on=source.created_on,
                    completed_on=completed_on,
                )
            case _:
                raise RuntimeError("Transition unknown")

        return new_task


class LocalPromiseStore:
    def __init__(
        self,
        store: LocalStore,
        encoder: Encoder[Any, str | None],
        promises: dict[str, DurablePromiseRecord],
        tasks: dict[str, TaskRecord],
        routers: list[Router],
        senders: dict[str, Sender],
    ) -> None:
        self._store = store
        self._encoder = encoder
        self._promises = promises
        self._tasks = tasks
        self._routers = routers
        self._senders = senders

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
        promise, _ = self._create(
            id=id,
            ikey=ikey,
            strict=strict,
            headers=headers,
            data=data,
            timeout=timeout,
            tags=tags,
        )

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

    def _create(
        self,
        *,
        id: str,
        ikey: str | None,
        strict: bool,
        headers: dict[str, str] | None,
        data: Any,
        timeout: int,
        tags: dict[str, str] | None,
        pid: str | None = None,
        ttl: int = 0,
    ) -> tuple[DurablePromise, Task | None]:
        created_on = now()
        was_created: bool = False
        record = self._promises.get(id)

        if record is None:
            self._promises[id] = DurablePromiseRecord(
                state="PENDING",
                id=id,
                timeout=timeout,
                param=DurablePromiseRecordValue(
                    headers=headers or {},
                    data=self._encoder.encode(data),
                ),
                value=DurablePromiseRecordValue(headers={}, data=None),
                created_on=created_on,
                completed_on=None,
                ikey_for_create=ikey,
                ikey_for_complete=None,
                tags=tags,
                callbacks=[],
            )
            was_created = True
        elif strict and record.state != "PENDING":
            msg = "Forbidden request: Durable promise previously created"
            raise ResonateError(msg, "STORE_FORBIDDEN")
        elif record.ikey_for_create is None or ikey != record.ikey_for_create:
            msg = "Forbidden request: Missing idempotency key for create"
            raise ResonateError(msg, "STORE_FORBIDDEN")

        record = self._timeout(self._promises[id])
        self._promises[id] = record

        task: Task | None = None
        if was_created:
            for r in self._routers:
                if recv := r.route(record):
                    new_task = self._store._transition_task(
                        None,
                        ToInit("invoke", recv, created_on, record.id, record.id),
                    )
                    if pid is not None:
                        new_task = self._store._transition_task(new_task, ToClaimed(pid, ttl))

                    self._tasks[new_task.id] = new_task
                    if new_task.state == "INIT":
                        assert self._store.send_task(new_task)
                        new_task = self._store._transition_task(new_task, ToEnqueued())

                    self._tasks[new_task.id] = new_task
                    task = Task.from_dict(self._store, new_task.to_dict())

                    break
        return DurablePromise.from_dict(
            self._store,
            record.to_dict(),
            self._encoder.decode(record.param.data),
            self._encoder.decode(record.value.data),
        ), task

    def _create_v1(
        self,
        *,
        id: str,
        ikey: str | None,
        strict: bool,
        headers: dict[str, str] | None,
        data: Any,
        timeout: int,
        tags: dict[str, str] | None,
        pid: str | None = None,
        ttl: int = 0,
    ) -> tuple[DurablePromise, Task | None]:
        created_on = now()
        record = self._promises.get(id)
        task: Task | None = None

        if record is None:
            record = DurablePromiseRecord(
                state="PENDING",
                id=id,
                timeout=timeout,
                param=DurablePromiseRecordValue(
                    headers=headers or {},
                    data=self._encoder.encode(data),
                ),
                value=DurablePromiseRecordValue(headers={}, data=None),
                created_on=created_on,
                completed_on=None,
                ikey_for_create=ikey,
                ikey_for_complete=None,
                tags=tags,
                callbacks=[],
            )

            for r in self._routers:
                if recv := r.route(record):
                    task_id = str(len(self._tasks))
                    state = "INIT" if not pid else "CLAIMED"
                    new_task = TaskRecord(
                        id=task_id,
                        counter=1,
                        state=state,
                        type="invoke",
                        recv=recv,
                        root_promise_id=record.id,
                        leaf_promise_id=record.id,
                        pid=pid,
                        ttl=ttl,
                        created_on=created_on,
                        completed_on=None,
                    )

                    task = Task.from_dict(self._store, new_task.to_dict())

                    self._tasks[task_id] = new_task
                    if state == "INIT":
                        assert self._store.send_task(new_task)
                        state = "ENQUEUED"

                    self._tasks[task_id] = TaskRecord(
                        id=new_task.id,
                        counter=new_task.counter,
                        state=state,
                        type=new_task.type,
                        recv=new_task.recv,
                        root_promise_id=new_task.root_promise_id,
                        leaf_promise_id=new_task.leaf_promise_id,
                        pid=new_task.pid,
                        ttl=new_task.ttl,
                        created_on=new_task.created_on,
                        completed_on=new_task.completed_on,
                    )

                    break

        elif strict and record.state != "PENDING":
            msg = "Forbidden request: Durable promise previously created"
            raise ResonateError(msg, "STORE_FORBIDDEN")
        elif record.ikey_for_create is None or ikey != record.ikey_for_create:
            msg = "Forbidden request: Missing idempotency key for create"
            raise ResonateError(msg, "STORE_FORBIDDEN")

        new_item = self._timeout(record)
        self._promises[id] = new_item
        return DurablePromise.from_dict(
            self._store,
            new_item.to_dict(),
            self._encoder.decode(new_item.param.data),
            self._encoder.decode(new_item.value.data),
        ), task

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

    def _complete(
        self, *, id: str, ikey: str | None = None, strict: bool = False, headers: dict[str, str] | None = None, data: Any = None, new_state: Literal["REJECTED_CANCELED", "REJECTED", "RESOLVED"]
    ) -> DurablePromise:
        record = self._promises.get(id)
        if record is None:
            msg = "Not Found"
            raise ResonateError(msg, "STORE_NOT_FOUND")
        if record.state == "PENDING":
            promise_completed_on = now()
            record = DurablePromiseRecord(
                state=new_state,
                id=record.id,
                timeout=record.timeout,
                param=record.param,
                value=DurablePromiseRecordValue(
                    headers=headers or {},
                    data=self._encoder.encode(data),
                ),
                created_on=record.created_on,
                completed_on=promise_completed_on,
                ikey_for_create=record.ikey_for_create,
                ikey_for_complete=ikey,
                tags=record.tags,
                callbacks=record.callbacks,
            )
            for callback in record.callbacks:
                task_id = str(len(self._tasks))
                new_task = TaskRecord(
                    id=task_id,
                    counter=1,
                    state="INIT",
                    type=callback.type,
                    recv=callback.recv,
                    root_promise_id=record.id,
                    leaf_promise_id=record.id,
                    pid=None,
                    ttl=None,
                    created_on=promise_completed_on,
                    completed_on=None,
                )
                self._tasks[task_id] = new_task
                self._store.send_task(new_task)

                self._tasks[task_id] = TaskRecord(
                    id=new_task.id,
                    counter=new_task.counter,
                    state="ENQUEUED",
                    type=new_task.type,
                    recv=new_task.recv,
                    root_promise_id=new_task.root_promise_id,
                    leaf_promise_id=new_task.leaf_promise_id,
                    pid=new_task.pid,
                    ttl=new_task.ttl,
                    created_on=new_task.created_on,
                    completed_on=new_task.completed_on,
                )

            record.callbacks.clear()
        elif (strict and record.state != new_state) or (record.state != "REJECTED_TIMEDOUT" and (record.ikey_for_complete is None or ikey != record.ikey_for_complete)):
            msg = "Forbidden request"
            raise ResonateError(msg, "STORE_FORBIDDEN")

        new_item = self._timeout(record)
        self._promises[id] = new_item
        return DurablePromise.from_dict(
            self._store,
            new_item.to_dict(),
            self._encoder.decode(new_item.param.data),
            self._encoder.decode(new_item.value.data),
        )

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

    def _timeout(self, promise: DurablePromiseRecord) -> DurablePromiseRecord:
        new_state = "REJECTED_TIMEDOUT"
        if promise.state == "PENDING" and now() >= promise.timeout:
            if promise.tags and promise.tags.get("resonate:timeout") == "true":
                new_state = "RESOLVED"
            return DurablePromiseRecord(
                state=new_state,
                id=promise.id,
                timeout=promise.timeout,
                param=promise.param,
                value=DurablePromiseRecordValue(headers={}, data=None),
                tags=promise.tags,
                created_on=promise.created_on,
                completed_on=promise.timeout,
                ikey_for_create=promise.ikey_for_create,
                ikey_for_complete=None,
                callbacks=promise.callbacks,
            )
        return promise


class LocalTaskStore:
    def __init__(
        self,
        store: LocalStore,
        encoder: Encoder[Any, str | None],
        promises: dict[str, DurablePromiseRecord],
        tasks: dict[str, TaskRecord],
    ) -> None:
        self._store = store
        self._encoder = encoder
        self._promises = promises
        self._tasks = tasks

    def claim(self, *, id: str, counter: int, pid: str, ttl: int) -> tuple[DurablePromise, DurablePromise | None]:
        if task_record := self._tasks.get(id):
            if task_record.state in ("INIT", "ENQUEUED") and counter == task_record.counter:
                task_record = self._store._transition_task(task_record, ToClaimed(pid, ttl))
                self._tasks[id] = task_record
                assert task_record.type != "notify"
                if task_record.type == "invoke":
                    promise = self._promises[task_record.root_promise_id]
                    return DurablePromise.from_dict(
                        self._store,
                        promise.to_dict(),
                        self._encoder.decode(promise.param.data),
                        self._encoder.decode(promise.value.data),
                    ), None

                if task_record.type == "resume":
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
            msg = "Task already claimed, completed, or invalid counter"
            raise ResonateError(
                msg,
                "STORE_FORBIDDEN",
            )
        msg = "Task not found"
        raise ResonateError(msg, "STORE_NOT_FOUND")

    def complete(self, *, id: str, counter: int) -> None:
        if task_record := self._tasks.get(id):
            if task_record.state == "CLAIMED" and counter == task_record.counter:
                self._tasks[id] = self._store._transition_task(task_record, ToCompleted(now()))
            else:
                msg = "Task already claimed, completed, or invalid counter"
                raise ResonateError(
                    msg,
                    "STORE_FORBIDDEN",
                )
        else:
            msg = "Task not found"
            raise ResonateError(msg, "STORE_NOT_FOUND")

    def heartbeat(self, *, pid: str) -> int:
        affected_tasks = 0
        for task in self._tasks.values():
            if task.state != "CLAIMED":
                continue
            if task.pid == pid:
                affected_tasks += 1
        return affected_tasks


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


@final
@dataclass(frozen=True)
class ToInit:
    type: Literal["invoke", "resume", "notify"]
    recv: Recv
    created_on: int
    root_promise_id: str
    leaf_promise_id: str


@final
@dataclass(frozen=True)
class ToClaimed:
    pid: str
    ttl: int


@final
@dataclass(frozen=True)
class ToEnqueued:
    pass


@final
@dataclass(frozen=True)
class ToCompleted:
    completed_on: int
