from __future__ import annotations

from typing import TYPE_CHECKING, Any, Callable, TypeVar, final, overload

from typing_extensions import Concatenate, ParamSpec

from resonate.actions import (
    LFC,
    LFI,
    RFC,
    RFI,
    DeferredInvocation,
)
from resonate.commands import Command, CreateDurablePromiseReq
from resonate.dataclasses import FnOrCoroutine

if TYPE_CHECKING:
    from collections.abc import Coroutine, Generator

    from resonate.dependencies import Dependencies
    from resonate.typing import (
        DurableCoro,
        DurableFn,
        Yieldable,
    )

P = ParamSpec("P")
T = TypeVar("T")


@final
class Context:
    def __init__(
        self,
        deps: Dependencies,
    ) -> None:
        self._deps = deps

    def get_dependency(self, key: str) -> Any:  # noqa: ANN401
        return self._deps.get(key)

    @overload
    def rfc(self, cmd: CreateDurablePromiseReq, /) -> RFC: ...
    @overload
    def rfc(self, func: str, /, *args: Any, **kwargs: Any) -> RFC: ...  # noqa: ANN401
    @overload
    def rfc(
        self,
        func: Callable[Concatenate[Context, P], Generator[Yieldable, Any, Any]],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> RFC: ...
    @overload
    def rfc(
        self,
        func: Callable[Concatenate[Context, P], Any | Coroutine[Any, Any, Any]],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> RFC: ...
    def rfc(
        self,
        func_or_cmd: DurableCoro[P, T]
        | DurableFn[P, T]
        | str
        | CreateDurablePromiseReq,
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> RFC:
        unit: (
            FnOrCoroutine
            | tuple[str, tuple[Any, ...], dict[str, Any]]
            | CreateDurablePromiseReq
        )
        if isinstance(func_or_cmd, str):
            unit = (func_or_cmd, args, kwargs)
        elif isinstance(func_or_cmd, CreateDurablePromiseReq):
            unit = func_or_cmd
        else:
            unit = FnOrCoroutine(func_or_cmd, *args, **kwargs)
        return RFC(unit)

    @overload
    def rfi(self, cmd: CreateDurablePromiseReq, /) -> RFI: ...
    @overload
    def rfi(self, func: str, /, *args: Any, **kwargs: Any) -> RFI: ...  # noqa: ANN401
    @overload
    def rfi(
        self,
        func: Callable[Concatenate[Context, P], Generator[Yieldable, Any, Any]],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> RFI: ...
    @overload
    def rfi(
        self,
        func: Callable[Concatenate[Context, P], Any | Coroutine[Any, Any, Any]],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> RFI: ...
    def rfi(
        self,
        func_or_cmd: DurableCoro[P, T]
        | DurableFn[P, T]
        | str
        | CreateDurablePromiseReq,
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> RFI:
        return self.rfc(func_or_cmd, *args, **kwargs).to_invocation()

    @overload
    def lfi(self, cmd: Command, /) -> LFI: ...
    @overload
    def lfi(
        self,
        func: Callable[Concatenate[Context, P], Generator[Yieldable, Any, Any]],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> LFI: ...
    @overload
    def lfi(
        self,
        func: Callable[Concatenate[Context, P], Any | Coroutine[Any, Any, Any]],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> LFI: ...
    def lfi(
        self,
        func_or_cmd: DurableCoro[P, T] | DurableFn[P, T] | Command,
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> LFI:
        """
        Local function invocation.

        Invoke and immediatelly receive a `Promise[T]` that
        represents the future result of the execution.

        The `Promise` can be yielded later in the execution to await
        for the result.
        """
        return self.lfc(func_or_cmd, *args, **kwargs).to_invocation()

    @overload
    def lfc(self, func: Command, /) -> LFC: ...
    @overload
    def lfc(
        self,
        func: Callable[Concatenate[Context, P], Generator[Yieldable, Any, Any]],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> LFC: ...
    @overload
    def lfc(
        self,
        func: Callable[Concatenate[Context, P], Any | Coroutine[Any, Any, Any]],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> LFC: ...
    def lfc(
        self,
        func_or_cmd: DurableCoro[P, T] | DurableFn[P, T] | Command,
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> LFC:
        """
        Local function call.

        LFC and await for the result of the execution. It's syntax
        sugar for `yield (yield ctx.lfi(...))`
        """
        unit: Command | FnOrCoroutine
        if isinstance(func_or_cmd, Command):
            unit = func_or_cmd
        else:
            unit = FnOrCoroutine(func_or_cmd, *args, **kwargs)
        return LFC(unit)

    @overload
    def deferred(
        self,
        id: str,
        coro: Callable[Concatenate[Context, P], Generator[Yieldable, Any, Any]],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> DeferredInvocation: ...
    @overload
    def deferred(
        self,
        id: str,
        coro: Callable[Concatenate[Context, P], Any | Coroutine[Any, Any, Any]],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> DeferredInvocation: ...
    def deferred(
        self,
        id: str,
        coro: DurableCoro[P, T] | DurableFn[P, T],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> DeferredInvocation:
        """
        Deferred invocation.

        Invoke as a root invocation. Is equivalent to do `Scheduler.run(...)`
        invoked execution will be retried and managed from the server.
        """
        return DeferredInvocation(id=id, coro=FnOrCoroutine(coro, *args, **kwargs))
