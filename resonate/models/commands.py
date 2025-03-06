from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Optional

from resonate.models.callback import Callback
from resonate.models.durable_promise import DurablePromise
from resonate.models.result import Result
from resonate.models.task import Task


# Commands

type Command = Invoke | Resume | Return | Receive | Listen | Notify

@dataclass
class Invoke:
    id: str
    name: str
    func: Callable[..., Any]
    args: tuple[Any, ...] = field(default_factory=tuple)
    kwargs: dict[str, Any] = field(default_factory=dict)
    promise_and_task: tuple[DurablePromise, Task] | None = None

    @property
    def cid(self) -> str:
        return self.id


@dataclass
class Resume:
    id: str
    cid: str
    promise: DurablePromise
    task: Task
    invoke: Invoke


@dataclass
class Return:
    id: str
    cid: str
    res: Result

@dataclass
class Receive:
    id: str
    cid: str
    res: tuple[DurablePromise, Task | None, Callback | None]

@dataclass
class Listen:
    id: str

    @property
    def cid(self) -> str:
        return self.id


@dataclass
class Notify:
    id: str
    promise: DurablePromise

    @property
    def cid(self) -> str:
        return self.id

# Requests

type Request = Network | Function

@dataclass
class Network:
    id: str
    cid: str
    req: CreatePromiseReq | CreatePromiseWithTaskReq | ResolvePromiseReq | RejectPromiseReq | CancelPromiseReq | CreateCallbackReq

@dataclass
class Function:
    id: str
    cid: str
    func: Callable[[], Any]

@dataclass
class CreatePromiseReq:
    id: str
    timeout: int
    ikey: str | None = None
    strict: bool = False
    headers: dict[str, str] | None = None
    data: Any = None
    tags: dict[str, str] | None = None

@dataclass
class CreatePromiseRes:
    promise: DurablePromise

@dataclass
class CreatePromiseWithTaskReq:
    id: str
    timeout: int
    pid: str
    ttl: int
    ikey: str | None = None
    strict: bool = False
    headers: dict[str, str] | None = None
    data: Any = None
    tags: dict[str, str] | None = None

@dataclass
class CreatePromiseWithTaskRes:
    promise: DurablePromise
    task: Task | None

@dataclass
class ResolvePromiseReq:
    id: str
    ikey: str | None = None
    strict: bool = False
    headers: dict[str, str] | None = None
    data: Any = None

@dataclass
class ResolvePromiseRes:
    promise: DurablePromise

@dataclass
class RejectPromiseReq:
    id: str
    ikey: str | None = None
    strict: bool = False
    headers: dict[str, str] | None = None
    data: Any = None

@dataclass
class RejectPromiseRes:
    promise: DurablePromise

@dataclass
class CancelPromiseReq:
    id: str
    ikey: str | None = None
    strict: bool = False
    headers: dict[str, str] | None = None
    data: Any = None

@dataclass
class CancelPromiseRes:
    promise: DurablePromise

@dataclass
class CreateCallbackReq:
    id: str
    promise_id: str
    root_promise_id: str
    timeout: int
    recv: str

@dataclass
class CreateCallbackRes:
    promise: DurablePromise
    callback: Callback | None
