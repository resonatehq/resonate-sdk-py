from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal, final

from typing_extensions import Any

if TYPE_CHECKING:
    from resonate_sdk.models.durable_promise import DurablePromiseRecord
    from resonate_sdk.stores.traits import IStore


class Task:
    def __init__(self, record: TaskRecord, store: IStore) -> None:
        self.id = record.id
        self.counter = record.counter
        self.store = store

    def claim(self, pid: str, ttl: int) -> Any:
        self.store.tasks.claim(id=self.id, counter=self.counter, pid=pid, ttl=ttl)
        raise NotImplementedError

    def complete(self) -> None:
        self.store.tasks.complete(id=self.id, counter=self.counter)
        raise NotImplementedError


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
    created_on: int
    completed_on: int | None
    pid: str | None
    ttl: int | None
    expiry: int | None


type Mesg = InvokeMesg | ResumeMesg | NotifyMesg


@dataclass
class InvokeMesg:
    type: Literal["invoke"]
    task: TaskRecord


@dataclass
class ResumeMesg:
    type: Literal["resume"]
    task: TaskRecord


@dataclass
class NotifyMesg:
    type: Literal["notify"]
    promise: DurablePromiseRecord
