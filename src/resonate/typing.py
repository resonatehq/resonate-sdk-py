from __future__ import annotations

from collections.abc import Coroutine, Generator
from typing import Any, Callable, TypeVar, Union

from typing_extensions import Concatenate, ParamSpec, TypeAlias

from resonate.actions import Call, Invoke, Sleep
from resonate.batching import CmdBuffer
from resonate.context import Context
from resonate.dataclasses import Command, CoroAndPromise, FnOrCoroutine, Runnable
from resonate.promise import Promise

T = TypeVar("T")
P = ParamSpec("P")


ExecutionUnit: TypeAlias = Union[Command, FnOrCoroutine]


Yieldable: TypeAlias = Union[Call, Invoke, Promise[Any], Sleep]

DurableCoro: TypeAlias = Callable[Concatenate[Context, P], Generator[Yieldable, Any, T]]
DurableSyncFn: TypeAlias = Callable[Concatenate[Context, P], T]
DurableAsyncFn: TypeAlias = Callable[Concatenate[Context, P], Coroutine[Any, Any, T]]
DurableFn: TypeAlias = Union[DurableSyncFn[P, T], DurableAsyncFn[P, T]]
MockFn: TypeAlias = Callable[[], T]

Invokable: TypeAlias = Union[DurableCoro[P, Any], DurableFn[P, Any], Command]


Awaitables: TypeAlias = dict[Promise[Any], list[CoroAndPromise[Any]]]
RunnableCoroutines: TypeAlias = list[Runnable[Any]]


CommandHandlers: TypeAlias = dict[
    type[Command], Callable[[list[Any]], Union[list[Any], None]]
]
CommandHandlerQueues: TypeAlias = dict[
    type[Command], CmdBuffer[tuple[Promise[Any], Command]]
]
