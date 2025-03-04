from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable

from resonate.models.durable_promise import DurablePromise
from resonate.models.result import Result
from resonate.models.task import Task

type Command = Invoke | Resume | Return | Listen | Notify


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
    cont: Callable[[], None] = field(repr=False)

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

type Request[T] = Network[T] | Function[T]

@dataclass
class Network[T]:
    id: str
    cid: str
    func: Callable[[], T]
    cont: Callable[[Result[T]], None]

@dataclass
class Function[T]:
    id: str
    cid: str
    func: Callable[[], T]
    cont: Callable[[Result[T]], None]
