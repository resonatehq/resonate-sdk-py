from __future__ import annotations

from collections import deque
from collections.abc import Coroutine, Generator
from typing import Any, Callable, Literal, TypeVar, Union

from typing_extensions import Concatenate, ParamSpec, TypeAlias

from resonate.actions import (
    LFC,
    LFI,
    RFC,
    RFI,
    All,
    AllSettled,
    DeferredInvocation,
    Race,
)
from resonate.batching import CmdBuffer
from resonate.commands import Command
from resonate.context import Context
from resonate.dataclasses import FnOrCoroutine, ResonateCoro, Runnable
from resonate.promise import Promise

T = TypeVar("T")
P = ParamSpec("P")


ExecutionUnit: TypeAlias = Union[Command, FnOrCoroutine]

Combinator: TypeAlias = Union[All, AllSettled, Race]

PromiseActions: TypeAlias = Union[Combinator, LFI, RFI]

Yieldable: TypeAlias = Union[LFC, Promise[Any], PromiseActions, DeferredInvocation, RFC]

DurableCoro: TypeAlias = Callable[Concatenate[Context, P], Generator[Yieldable, Any, T]]
DurableSyncFn: TypeAlias = Callable[Concatenate[Context, P], T]
DurableAsyncFn: TypeAlias = Callable[Concatenate[Context, P], Coroutine[Any, Any, T]]
DurableFn: TypeAlias = Union[DurableSyncFn[P, T], DurableAsyncFn[P, T]]
MockFn: TypeAlias = Callable[[], T]

Invokable: TypeAlias = Union[DurableCoro[P, Any], DurableFn[P, Any], Command]


Awaitables: TypeAlias = dict[Promise[Any], list[ResonateCoro[Any]]]
RunnableCoroutines: TypeAlias = deque[tuple[Runnable[Any], bool]]


CommandHandlers: TypeAlias = dict[
    type[Command], Callable[[list[Any]], Union[list[Any], None]]
]
CommandHandlerQueues: TypeAlias = dict[
    type[Command], CmdBuffer[tuple[Promise[Any], Command]]
]


Headers: TypeAlias = Union[dict[str, str], None]
Tags: TypeAlias = Union[dict[str, str], None]
IdempotencyKey: TypeAlias = Union[str, None]
Data: TypeAlias = Union[str, None]
State: TypeAlias = Literal[
    "PENDING", "RESOLVED", "REJECTED", "REJECTED_CANCELED", "REJECTED_TIMEDOUT"
]

EphemeralPromiseMemo: TypeAlias = dict[str, Promise[Any]]


C = TypeVar("C", bound=Command)

CmdHandlerResult: TypeAlias = Union[list[Union[T, Exception]], T, None]
CmdHandler: TypeAlias = Callable[[Context, list[C]], CmdHandlerResult[Any]]
