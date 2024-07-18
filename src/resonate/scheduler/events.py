"""Execution events."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Union

from typing_extensions import TypeAlias

if TYPE_CHECKING:
    from collections.abc import Hashable


@dataclass(frozen=True)
class PromiseCreated:
    promise_id: int
    function_name: str
    args: tuple[Hashable]
    kwargs: dict[str, Hashable]
    tick: int


@dataclass(frozen=True)
class ExecutionStarted:
    promise_id: int
    function_name: str
    args: tuple[Hashable]
    kwargs: dict[str, Hashable]
    tick: int


@dataclass(frozen=True)
class PromiseResolved:
    promise_id: int
    tick: int


@dataclass(frozen=True)
class SuspendedForPromise:
    promise_id: int
    tick: int


@dataclass(frozen=True)
class AwaitedForPromise:
    promise_id: int
    tick: int


@dataclass(frozen=True)
class Resummend:
    promise_id: int
    tick: int


SchedulerEvents: TypeAlias = Union[
    PromiseCreated,
    ExecutionStarted,
    PromiseResolved,
    SuspendedForPromise,
    AwaitedForPromise,
    Resummend,
]
