from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Sequence

from resonate.models.callback import Callback
from resonate.models.durable_promise import DurablePromise
from resonate.models.result import Result
from resonate.models.task import Task

type Command = Invoke | Resume | Return | Return | Listen | Notify | PromiseEffect | CallbackEffect


@dataclass
class Invoke:
    id: str
    name: str
    func: Callable[..., Any]
    args: tuple[Any, ...] = field(default_factory=tuple)
    kwargs: dict[str, Any] = field(default_factory=dict)
    # promise: DurablePromise | None = None
    # task: Task | None = None

    @property
    def cid(self) -> str:
        return self.id


@dataclass
class Resume:
    id: str
    cid: str
    result: Result
    # promise: DurablePromise
    # task: Task
    invoke: Invoke


@dataclass
class Return:
    id: str
    cid: str
    cont: Callable[[Result], None] = field(repr=False)
    result: Result

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
class PromiseEffect:
    id: str
    cid: str
    promise: DurablePromise
    task: Task | None = None


@dataclass
class CallbackEffect:
    id: str
    cid: str
    promise: DurablePromise
    callback: Callback

# Requests

type Request = Network | Function

@dataclass
class Network[T]:
    func: Callable[[], T]
    cont: Callable[[Result[T]], Sequence[Request]]

@dataclass
class Function[T]:
    func: Callable[[], T]
    cont: Callable[[Result[T]], Sequence[Request]]
