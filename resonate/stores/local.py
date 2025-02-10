from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Literal, Protocol, final

from resonate.encoders.base64 import Base64Encoder
from resonate.errors import ResonateError
from resonate.models.durable_promise import DurablePromise
from resonate.models.encoder import Encoder
from resonate.models.message import InvokeMesg, Mesg, ResumeMesg, TaskMesg
from resonate.models.task import Task

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
    def __init__(self, encoder: Encoder[str | None, str | None] | None = None) -> None:
        self.encoder = encoder or Base64Encoder()
        self._promises: dict[str, DurablePromiseRecord] = {}
        self._tasks: dict[str, TaskRecord] = {}

        self._routers: list[Router] = [TagRouter()]
        self._senders: dict[str, Sender] = {}

    @property
    def promises(self) -> LocalPromiseStore:
        return LocalPromiseStore(self, self._promises, self._tasks, self._routers, self._senders)

    @property
    def tasks(self) -> LocalTaskStore:
        return LocalTaskStore(self, self._promises, self._tasks)

    def add_router(self, router: Router) -> None:
        self._routers.append(router)

    def add_sender(self, recv: str, sender: Sender) -> None:
        assert recv not in self._senders
        self._senders[recv] = sender

    def rmv_sender(self, recv: str) -> None:
        assert recv in self._senders
        del self._senders[recv]

class LocalPromiseStore:
    def __init__(
        self,
        store: LocalStore,
        promises: dict[str, DurablePromiseRecord],
        tasks: dict[str, TaskRecord],
        routers: list[Router],
        senders: dict[str, Sender],
    ) -> None:
        self._store = store
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
        data: str | None = None,
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
        data: str | None = None,
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
        data: str | None,
        timeout: int,
        tags: dict[str, str] | None,
        pid: str | None = None,
        ttl: int = 0,
    ) -> tuple[DurablePromise, Task | None]:
        record = self._promises.get(id)
        task = None

        if record is None:
            record = DurablePromiseRecord(
                state="PENDING",
                id=id,
                timeout=timeout,
                param=DurablePromiseRecordValue(
                    headers=headers or {},
                    data=self._store.encoder.encode(data),
                ),
                value=DurablePromiseRecordValue(headers={}, data=None),
                created_on=now(),
                completed_on=None,
                ikey_for_create=ikey,
                ikey_for_complete=None,
                tags=tags,
            )

            for r in self._routers:
                if recv := r.route(record):
                    task_id = str(len(self._tasks))
                    state = "INIT" if not pid else "CLAIMED"

                    task = Task(task_id, 1, store=self._store)

                    created_on = now()
                    self._tasks[task_id] = TaskRecord(
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
                    if state == "INIT" and (sender := self._senders.get(recv)):
                        sender.send(
                            recv=recv,
                            mesg=InvokeMesg(type="invoke", task=TaskMesg(id=task_id, counter=1)),
                        )
                        state = "ENQUEUED"

                    self._tasks[task_id] = TaskRecord(
                        id=task_id,
                        counter=1,
                        state=state,
                        type="invoke",
                        recv=recv,
                        root_promise_id=record.id,
                        leaf_promise_id=record.id,
                        pid=pid,
                        ttl=ttl,
                        created_on=now(),
                        completed_on=None,
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

        return DurablePromise(store=self._store, **new_item.__dict__), task

    def resolve(
        self,
        *,
        id: str,
        ikey: str | None = None,
        strict: bool = False,
        headers: dict[str, str] | None = None,
        data: str | None = None,
    ) -> DurablePromise:
        record = self._promises.get(id)
        if record is None:
            msg = "Not found"
            raise ResonateError(msg, "STORE_NOT_FOUND")
        if record.state == "PENDING":
            record = DurablePromiseRecord(
                state="RESOLVED",
                id=record.id,
                timeout=record.timeout,
                param=record.param,
                value=DurablePromiseRecordValue(
                    headers=headers or {},
                    data=self._store.encoder.encode(data),
                ),
                created_on=record.created_on,
                completed_on=now(),
                ikey_for_create=record.ikey_for_create,
                ikey_for_complete=ikey,
                tags=record.tags,
            )
        elif (strict and record.state != "RESOLVED") or (
            record.state != "REJECTED_TIMEDOUT"
            and (record.ikey_for_complete is None or ikey != record.ikey_for_complete)
        ):
            msg = "Forbidden request"
            raise ResonateError(msg, "STORE_FORBIDDEN")

        new_item = self._timeout(record)
        self._promises[id] = new_item
        return DurablePromise(store=self._store, **new_item.__dict__)

    def reject(
        self,
        *,
        id: str,
        ikey: str | None = None,
        strict: bool = False,
        headers: dict[str, str] | None = None,
        data: str | None = None,
    ) -> DurablePromise:
        record = self._promises.get(id)
        if record is None:
            msg = "Not found"
            raise ResonateError(msg, "STORE_NOT_FOUND")
        if record.state == "PENDING":
            record = DurablePromiseRecord(
                state="REJECTED",
                id=record.id,
                timeout=record.timeout,
                param=record.param,
                value=DurablePromiseRecordValue(
                    headers=headers or {},
                    data=self._store.encoder.encode(data),
                ),
                created_on=record.created_on,
                completed_on=now(),
                ikey_for_create=record.ikey_for_create,
                ikey_for_complete=ikey,
                tags=record.tags,
            )
        elif (strict and record.state != "REJECTED") or (
            record.state != "REJECTED_TIMEDOUT"
            and (record.ikey_for_complete is None or ikey != record.ikey_for_complete)
        ):
            msg = "Forbidden request"
            raise ResonateError(msg, "STORE_FORBIDDEN")

        new_item = self._timeout(record)
        self._promises[id] = new_item
        return DurablePromise(store=self._store, **new_item.__dict__)

    def cancel(
        self,
        *,
        id: str,
        ikey: str | None = None,
        strict: bool = False,
        headers: dict[str, str] | None = None,
        data: str | None = None,
    ) -> DurablePromise:
        record = self._promises.get(id)
        if record is None:
            msg = "Not Found"
            raise ResonateError(msg, "STORE_NOT_FOUND")
        if record.state == "PENDING":
            record = DurablePromiseRecord(
                state="REJECTED_CANCELED",
                id=record.id,
                timeout=record.timeout,
                param=record.param,
                value=DurablePromiseRecordValue(
                    headers=headers or {},
                    data=self._store.encoder.encode(data),
                ),
                created_on=record.created_on,
                completed_on=now(),
                ikey_for_create=record.ikey_for_create,
                ikey_for_complete=ikey,
                tags=record.tags,
            )
        elif (strict and record.state != "REJECTED_CANCELED") or (
            record.state != "REJECTED_TIMEDOUT"
            and (record.ikey_for_complete is None or ikey != record.ikey_for_complete)
        ):
            msg = "Forbidden request"
            raise ResonateError(msg, "STORE_FORBIDDEN")

        new_item = self._timeout(record)
        self._promises[id] = new_item
        return DurablePromise(store=self._store, **new_item.__dict__)

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
            )
        return promise


class LocalTaskStore:
    def __init__(
        self,
        store: LocalStore,
        promises: dict[str, DurablePromiseRecord],
        tasks: dict[str, TaskRecord],
    ) -> None:
        self._store = store
        self._promises = promises
        self._tasks = tasks

    def claim(self, *, id: str, counter: int, pid: str, ttl: int) -> InvokeMesg | ResumeMesg:
        if task_record := self._tasks.get(id):
            if (
                task_record.state in ("INIT", "ENQUEUED")
                and counter == task_record.counter
            ):
                task_record = TaskRecord(
                    id=task_record.id,
                    counter=task_record.counter,
                    state="CLAIMED",
                    type=task_record.type,
                    recv=task_record.recv,
                    root_promise_id=task_record.root_promise_id,
                    leaf_promise_id=task_record.leaf_promise_id,
                    created_on=task_record.created_on,
                    completed_on=task_record.completed_on,
                    ttl=ttl,
                    pid=pid,
                )
                self._tasks[id] = task_record
                assert task_record.type != "notify"
                if task_record.type == "invoke":
                    return InvokeMesg(type=task_record.type, task=Task(task_record.id, task_record.counter, store=self._store))
                if task_record.type == "resume":
                    return ResumeMesg(type=task_record.type, task=Task(task_record.id, task_record.counter, store=self._store))
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
                task_record = TaskRecord(
                    id=id,
                    counter=counter,
                    state="COMPLETED",
                    type=task_record.type,
                    recv=task_record.recv,
                    root_promise_id=task_record.root_promise_id,
                    leaf_promise_id=task_record.leaf_promise_id,
                    created_on=task_record.created_on,
                    completed_on=now(),
                    pid=None,
                    ttl=None,
                )
                self._tasks[id] = task_record
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


@final
@dataclass(frozen=True)
class DurablePromiseRecordValue:
    headers: dict[str, str] | None
    data: str | None

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
