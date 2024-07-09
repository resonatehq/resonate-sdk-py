from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Generic, TypeVar, Union

from typing_extensions import TypeAlias

from resonate.context import Call, Invoke
from resonate.scheduler.shared import Promise

if TYPE_CHECKING:
    from collections.abc import Generator

    from result import Result

T = TypeVar("T")
Yieldable: TypeAlias = Union[Call, Invoke, Promise[Any]]


@dataclass(frozen=True)
class CoroAndPromise(Generic[T]):
    coro: Generator[Yieldable, Any, T]
    prom: Promise[T]


@dataclass(frozen=True)
class Runnable(Generic[T]):
    coro_and_promise: CoroAndPromise[T]
    next_value: Result[Any, Exception] | None


WaitingForPromiseResolution: TypeAlias = dict[Promise[Any], list[CoroAndPromise[Any]]]
PendingToRun: TypeAlias = list[Runnable[Any]]
