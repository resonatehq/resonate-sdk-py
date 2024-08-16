from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from collections.abc import Coroutine, Generator
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Callable, Generic, TypeVar, Union

from typing_extensions import Concatenate, ParamSpec, TypeAlias

from resonate.batching import CmdBuffer
from resonate.context import Call, Command, Context, Invoke
from resonate.promise import Promise
from resonate.result import Err, Ok

if TYPE_CHECKING:
    from resonate.result import Result

T = TypeVar("T")
P = ParamSpec("P")


Yieldable: TypeAlias = Union[Call, Invoke, Promise[Any]]


class IAsyncCommand(ABC, Generic[T]):
    @abstractmethod
    async def run(self) -> Result[T, Exception]: ...


class ICommand(ABC, Generic[T]):
    @abstractmethod
    def run(self) -> Result[T, Exception]: ...


class FnCmd(ICommand[T]):
    def __init__(
        self,
        ctx: Context,
        fn: DurableFn[P, T],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> None:
        self.fn = fn
        self.ctx = ctx
        self.args = args
        self.kwargs = kwargs

    def run(self) -> Result[T, Exception]:
        result: Result[T, Exception]
        try:
            result = Ok(self.fn(self.ctx, *self.args, **self.kwargs))
        except Exception as e:  # noqa: BLE001
            result = Err(e)

        return result


class AsyncFnCmd(IAsyncCommand[T]):
    def __init__(
        self,
        ctx: Context,
        fn: DurableAsyncFn[P, T],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> None:
        self.fn = fn
        self.ctx = ctx
        self.args = args
        self.kwargs = kwargs

    async def run(self) -> Result[T, Exception]:
        result: Result[T, Exception]
        try:
            result = Ok(asyncio.run(self.fn(self.ctx, *self.args, **self.kwargs)))
        except Exception as e:  # noqa: BLE001
            result = Err(e)
        return result


@dataclass(frozen=True)
class CoroAndPromise(Generic[T]):
    coro: Generator[Yieldable, Any, T]
    prom: Promise[T]
    ctx: Context


@dataclass(frozen=True)
class Runnable(Generic[T]):
    coro_and_promise: CoroAndPromise[T]
    next_value: Result[Any, Exception] | None


DurableCoro: TypeAlias = Callable[Concatenate[Context, P], Generator[Yieldable, Any, T]]
DurableFn: TypeAlias = Callable[Concatenate[Context, P], T]
DurableAsyncFn: TypeAlias = Callable[Concatenate[Context, P], Coroutine[Any, Any, T]]
MockFn: TypeAlias = Callable[[], T]

Awaitables: TypeAlias = dict[Promise[Any], list[CoroAndPromise[Any]]]
RunnableCoroutines: TypeAlias = list[Runnable[Any]]
RunnableFunctions: TypeAlias = list[
    tuple[Union[FnCmd[Any], AsyncFnCmd[Any]], Promise[Any]]
]


CommandHandlers: TypeAlias = dict[
    type[Command], Callable[[list[Any]], Union[list[Any], None]]
]
CommandHandlerQueues: TypeAlias = dict[
    type[Command], CmdBuffer[tuple[Promise[Any], Command]]
]
