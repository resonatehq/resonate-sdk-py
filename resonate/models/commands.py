from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from resonate.models.options import Options

if TYPE_CHECKING:
    from collections.abc import Callable

    from resonate.models.callback import Callback
    from resonate.models.durable_promise import DurablePromise
    from resonate.models.result import Result
    from resonate.models.task import Task

# Commands

type Command = Invoke | Resume | Return | Receive | Listen | Notify | Noop | Retry


@dataclass
class Invoke:
    id: str
    name: str
    func: Callable[..., Any] | None
    args: tuple[Any, ...] = field(default_factory=tuple)
    kwargs: dict[str, Any] = field(default_factory=dict)
    opts: Options = field(default_factory=Options)
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
    res: CreatePromiseRes | CreatePromiseWithTaskRes | ResolvePromiseRes | RejectPromiseRes | CancelPromiseRes | CreateCallbackRes


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


@dataclass
class Noop:
    pass


@dataclass
class Retry:
    id: str
    cid: str

# Requests

type Request = Network | Function | Delayed


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
class Delayed[T: Function | Retry]:
    item: T
    delay: float


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

@dataclass
class ClaimTaskReq:
    id: str
    counter: int
    pid: str
    ttl: int

@dataclass
class ClaimTaskRes:
    root: DurablePromise
    leaf: DurablePromise | None
    task: Task
