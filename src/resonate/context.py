from __future__ import annotations

from typing import TYPE_CHECKING, Any, TypeVar, final, overload

from typing_extensions import ParamSpec

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
from resonate.commands import Command
from resonate.dataclasses import FnOrCoroutine
from resonate.dependencies import Dependencies
from resonate.promise import Promise

if TYPE_CHECKING:
    from resonate.typing import (
        DurableCoro,
        DurableFn,
        Promise,
    )

P = ParamSpec("P")
T = TypeVar("T")


@final
class Context:
    def __init__(
        self,
        seed: int | None,
        deps: Dependencies | None = None,
    ) -> None:
        self.seed = seed
        self.deps = deps if deps is not None else Dependencies()

    def assert_statement(self, stmt: bool, msg: str) -> None:  # noqa: FBT001
        if self.seed is None:
            return
        assert stmt, msg

    def get_dependency(self, key: str) -> Any:  # noqa: ANN401
        return self.deps.get(key)

    @overload
    def rfc(self, func: str, /, *args: Any, **kwargs: Any) -> RFC: ...  # noqa: ANN401
    @overload
    def rfc(
        self,
        func: DurableCoro[P, Any] | DurableFn[P, Any],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> RFC: ...
    def rfc(
        self,
        func: DurableCoro[P, Any] | DurableFn[P, Any] | str,
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> RFC:
        unit: FnOrCoroutine | tuple[str, tuple[Any, ...], dict[str, Any]]
        if isinstance(func, str):
            unit = (func, args, kwargs)
        else:
            unit = FnOrCoroutine(func, *args, **kwargs)
        return RFC(unit)

    @overload
    def rfi(self, func: str, /, *args: Any, **kwargs: Any) -> RFI: ...  # noqa: ANN401
    @overload
    def rfi(
        self,
        func: DurableCoro[P, Any] | DurableFn[P, Any],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> RFI: ...
    def rfi(
        self,
        func: DurableCoro[P, Any] | DurableFn[P, Any] | str,
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> RFI:
        return self.rfc(func, *args, **kwargs).to_invocation()

    @overload
    def lfi(self, cmd: Command, /) -> LFI: ...
    @overload
    def lfi(
        self,
        func: DurableCoro[P, Any] | DurableFn[P, Any],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> LFI: ...
    def lfi(
        self,
        func_or_cmd: DurableCoro[P, Any] | DurableFn[P, Any] | Command,
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
        func: DurableCoro[P, Any] | DurableFn[P, Any],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> LFC: ...
    def lfc(
        self,
        func_or_cmd: DurableCoro[P, Any] | DurableFn[P, Any] | Command,
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

    def all(self, promises: list[Promise[Any]]) -> All:
        """Aggregates multiple promises into a single Promise that resolves when
        all of the promises in the input list have resolved.

        Args: promises (list[Promise[Any]]): An iterable of promises to be aggregated.

        Returns: All: A new Promise that resolves with a list of the resolved values
        from each promise in the input list, or rejects with the reason of the first
        promise that rejects.
        """
        return All(promises)

    def race(self, promises: list[Promise[Any]]) -> Race:
        """
        Aggregates multiple promises and returns a new Promise that resolves or rejects
        as soon as one of the promises in the input list resolves or rejects.

        Args: promises (list[Promise[Any]]): An iterable of promises to be raced.

        Returns: Race: A new Promise that resolves or rejects with the value/reason
        of the first promise in the list that resolves or rejects.
        """
        return Race(promises)

    def all_settled(self, promises: list[Promise[Any]]) -> AllSettled:
        """
        Aggregates multiple promises and returns a new Promise that resolves when all of
        the promises in the input list have either resolved or rejected.

        Args: promises (list[Promise[Any]]): An iterable of promises to be aggregated.

        Returns: AllSettled: A new Promise that resolves with a list of objects,
        each with a `status` property of either `'fulfilled'` or `'rejected'`,
        and a `value` or `reason` property depending on the outcome of the
        corresponding promise.
        """
        return AllSettled(promises)
