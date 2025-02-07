from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

from resonate_sdk.encoder import Base64Encoder
from resonate_sdk.errors import ResonateError
from resonate_sdk.models.durable_promise import DurablePromiseRecord, Value
from resonate_sdk.models.task import InvokeMesg, Mesg, ResumeMesg, TaskRecord
from resonate_sdk.stores.traits import IPromiseStore, IStore, ITaskStore
from resonate_sdk.time import now

if TYPE_CHECKING:
    from resonate_sdk.encoder import IEncoder
    from resonate_sdk.typing import State


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


class LocalStore(IStore):
    def __init__(self, encoder: IEncoder[str | None, str | None] | None = None) -> None:
        self._encoder = encoder or Base64Encoder()
        self._promises: dict[str, DurablePromiseRecord] = {}
        self._tasks: dict[str, TaskRecord] = {}

        self._routers: list[Router] = [TagRouter()]
        self._senders: dict[str, Sender] = {}

    @property
    def promises(self) -> IPromiseStore:
        return LocalPromiseStore(
            self._promises, self._tasks, self._encoder, self._routers, self._senders
        )

    @property
    def tasks(self) -> ITaskStore:
        return LocalTaskStore(self._promises, self._tasks, self._encoder)

    def add_router(self, router: Router) -> None:
        self._routers.append(router)

    def add_sender(self, recv: str, sender: Sender) -> None:
        assert recv not in self._senders
        self._senders[recv] = sender


class LocalPromiseStore(IPromiseStore):
    def __init__(
        self,
        promises: dict[str, DurablePromiseRecord],
        tasks: dict[str, TaskRecord],
        encoder: IEncoder[str | None, str | None],
        routers: list[Router],
        senders: dict[str, Sender],
    ) -> None:
        self._promises = promises
        self._tasks = tasks
        self._encoder = encoder
        self._routers = routers
        self._senders = senders

    def create(
        self,
        *,
        id: str,
        ikey: str | None,
        strict: bool,
        headers: dict[str, str] | None,
        data: str | None,
        timeout: int,
        tags: dict[str, str] | None,
    ) -> DurablePromiseRecord:
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
        ikey: str | None,
        strict: bool,
        headers: dict[str, str] | None,
        data: str | None,
        timeout: int,
        tags: dict[str, str] | None,
        pid: str,
        ttl: int,
    ) -> tuple[DurablePromiseRecord, TaskRecord | None]:
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
    ) -> tuple[DurablePromiseRecord, TaskRecord | None]:
        record = self._promises.get(id)
        task_record = None

        if record is None:
            record = DurablePromiseRecord(
                state="PENDING",
                id=id,
                timeout=timeout,
                param=Value(
                    headers=headers or {},
                    data=self._encoder.encode(data),
                ),
                value=Value(headers={}, data=None),
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
                        expiry=None if state == "INIT" else created_on + ttl
                    )
                    if state == "INIT" and (sender := self._senders.get(recv)):
                        sender.send(
                            recv=recv,
                            mesg=InvokeMesg(type="invoke", task=self._tasks[task_id]),
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
                        expiry=None if state in ("INIT", "ENQUEUED") else created_on + ttl
                    )

                    task_record = self._tasks[task_id]

                    break

        elif strict and record.state != "PENDING":
            msg = "Forbidden request: Durable promise previously created"
            raise ResonateError(msg, "STORE_FORBIDDEN")
        elif record.ikey_for_create is None or ikey != record.ikey_for_create:
            msg = "Forbidden request: Missing idempotency key for create"
            raise ResonateError(msg, "STORE_FORBIDDEN")

        new_item = self._timeout(record)
        self._promises[id] = new_item

        return new_item, task_record

    def reject(
        self,
        *,
        id: str,
        ikey: str | None,
        strict: bool,
        headers: dict[str, str] | None,
        data: str | None,
    ) -> DurablePromiseRecord:
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
                value=Value(
                    headers=headers or {},
                    data=self._encoder.encode(data),
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
        return new_item

    def resolve(
        self,
        *,
        id: str,
        ikey: str | None,
        strict: bool,
        headers: dict[str, str] | None,
        data: str | None,
    ) -> DurablePromiseRecord:
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
                value=Value(
                    headers=headers or {},
                    data=self._encoder.encode(data),
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
        return new_item

    def cancel(
        self,
        *,
        id: str,
        ikey: str | None,
        strict: bool,
        headers: dict[str, str] | None,
        data: str | None,
    ) -> DurablePromiseRecord:
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
                value=Value(
                    headers=headers or {},
                    data=self._encoder.encode(data),
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
        return new_item

    def _timeout(self, promise: DurablePromiseRecord) -> DurablePromiseRecord:
        new_state: State = "REJECTED_TIMEDOUT"
        if promise.state == "PENDING" and now() >= promise.timeout:
            if promise.tags and promise.tags.get("resonate:timeout") == "true":
                new_state = "RESOLVED"
            return DurablePromiseRecord(
                state=new_state,
                id=promise.id,
                timeout=promise.timeout,
                param=promise.param,
                value=Value(headers={}, data=None),
                tags=promise.tags,
                created_on=promise.created_on,
                completed_on=promise.timeout,
                ikey_for_create=promise.ikey_for_create,
                ikey_for_complete=None,
            )
        return promise


class LocalTaskStore(ITaskStore):
    def __init__(
        self,
        promises: dict[str, DurablePromiseRecord],
        tasks: dict[str, TaskRecord],
        encoder: IEncoder[str | None, str | None],
    ) -> None:
        self._promises = promises
        self._tasks = tasks
        self._encoder = encoder

    def claim(
        self, *, id: str, counter: int, pid: str, ttl: int
    ) -> InvokeMesg | ResumeMesg:
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
                    expiry=task_record.created_on + ttl,
                )
                self._tasks[id] = task_record
                assert task_record.type != "notify"
                if task_record.type == "invoke":
                    return InvokeMesg(type=task_record.type, task=task_record)
                if task_record.type == "resume":
                    return ResumeMesg(type=task_record.type, task=task_record)
            msg = "Task already claimed, completed, or invalid counter"
            raise ResonateError(
                msg,
                "STORE_FORBIDDEN",
            )
        msg = "Task not found"
        raise ResonateError(msg, "STORE_NOT_FOUND")

    def heartbeat(self, *, pid: str) -> int:
        affected_tasks = 0
        for task in self._tasks.values():
            if task.state != "CLAIMED":
                continue
            if task.pid == pid:
                self._tasks[task.id] = TaskRecord(
                    id=task.id,
                    counter=task.counter,
                    state=task.state,
                    type=task.type,
                    recv=task.recv,
                    root_promise_id=task.root_promise_id,
                    leaf_promise_id=task.leaf_promise_id,
                    created_on=task.created_on,
                    completed_on=task.completed_on,
                    pid=task.pid,
                    ttl=task.ttl,
                    expiry=now() + task.ttl,
                )
                affected_tasks += 1
        return affected_tasks

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
                    expiry=None,
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
