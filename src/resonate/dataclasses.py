from __future__ import annotations

from dataclasses import dataclass
from inspect import isgenerator
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from typing_extensions import ParamSpec

from resonate.actions import LFI
from resonate.commands import CreateDurablePromiseReq

if TYPE_CHECKING:
    from collections.abc import Generator

    from resonate.context import Context
    from resonate.promise import Promise
    from resonate.result import Result
    from resonate.typing import (
        DurableCoro,
        DurableFn,
        Yieldable,
    )

T = TypeVar("T")
P = ParamSpec("P")


class RouteInfo:
    def __init__(
        self,
        ctx: Context,
        promise: Promise[Any],
        fn_or_coroutine: FnOrCoroutine,
        retry_attempt: int,
    ) -> None:
        assert isinstance(promise.action, LFI)
        self.ctx = ctx
        self.promise = promise
        self.fn_or_coroutine = fn_or_coroutine
        self.retry_attempt = retry_attempt
        self.retry_policy = promise.action.opts.retry_policy

    def next_retry_attempt(self) -> RouteInfo:
        return RouteInfo(
            ctx=self.ctx,
            promise=self.promise,
            fn_or_coroutine=self.fn_or_coroutine,
            retry_attempt=self.retry_attempt + 1,
        )


class ResonateCoro(Generic[T]):
    def __init__(
        self,
        route_info: RouteInfo,
        coro: Generator[Yieldable, Any, T],
    ) -> None:
        assert isgenerator(coro)
        self.route_info = route_info
        self._coro = coro
        self._coro_active = True
        self._return_value: T | None = None
        self._error_to_raise: Exception | None = None

    def next(self) -> Yieldable:
        if self._coro_active:
            return next(self._coro)
        msg = "Next can only be called when coro is active."
        raise RuntimeError(msg)

    def send(self, v: T) -> Yieldable:
        if self._coro_active:
            try:
                return self._coro.send(v)
            except StopIteration as e:
                assert self._return_value is None, "return value can only be set once."
                self._return_value = e.value
                self._coro_active = False

        for child in self.route_info.promise.children_promises:
            if child.done():
                continue
            return child
        raise StopIteration(self._return_value)

    def throw(self, error: Exception) -> Yieldable:
        if self._coro_active:
            try:
                return self._coro.throw(error)
            except StopIteration as e:
                assert self._return_value is None, "return value can only be set once."
                self._return_value = e.value
                self._coro_active = False

        if self._error_to_raise is None:
            self._error_to_raise = error

        for child in self.route_info.promise.children_promises:
            if child.done():
                continue
            return child

        if self._error_to_raise is not None:
            raise error

        raise StopIteration(self._return_value)


@dataclass(frozen=True)
class Runnable(Generic[T]):
    coro: ResonateCoro[T]
    next_value: Result[Any, Exception] | None


class FnOrCoroutine:
    def __init__(
        self,
        exec_unit: DurableCoro[P, Any] | DurableFn[P, Any],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> None:
        self.exec_unit = exec_unit
        self.args = args
        self.kwargs = kwargs

    def to_req(
        self, promise_id: str | None, func_name: str, tags: dict[str, str]
    ) -> CreateDurablePromiseReq:
        return CreateDurablePromiseReq(
            promise_id=promise_id,
            data={
                "func": func_name,
                "args": self.args,
                "kwargs": self.kwargs,
            },
            tags=tags,
        )
