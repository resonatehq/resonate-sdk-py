from __future__ import annotations

import asyncio
import time
from inspect import iscoroutinefunction, isfunction
from typing import TYPE_CHECKING, Generic, TypeVar, cast, final

from typing_extensions import ParamSpec

from resonate.result import Err, Ok, Result
from resonate.retry_policy import Never, RetryPolicy

if TYPE_CHECKING:
    from resonate.context import Context
    from resonate.typing import DurableAsyncFn, DurableFn, DurableSyncFn

T = TypeVar("T")
P = ParamSpec("P")


@final
class FnWrapper(Generic[T]):
    def __init__(
        self,
        ctx: Context,
        fn: DurableSyncFn[P, T],
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


@final
class AsyncFnWrapper(Generic[T]):
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


def wrap_fn(
    ctx: Context,
    fn: DurableFn[P, T],
    *args: P.args,
    **kwargs: P.kwargs,
) -> FnWrapper[T] | AsyncFnWrapper[T]:
    cmd: FnWrapper[T] | AsyncFnWrapper[T]
    if isfunction(fn):
        cmd = FnWrapper(ctx, fn, *args, **kwargs)
    else:
        assert iscoroutinefunction(fn)
        cmd = AsyncFnWrapper(ctx, fn, *args, **kwargs)
    return cmd


def run_with_retry_policy(
    policy: RetryPolicy,
    wrapped_fn: AsyncFnWrapper[T] | FnWrapper[T],
) -> Result[T, Exception]:
    v: Result[T, Exception]

    if isinstance(policy, Never):
        v = cast(Result[T, Exception], wrapped_fn.run())
        return v

    attempt = 1
    while policy.should_retry(attempt=attempt):
        v = cast(Result[T, Exception], wrapped_fn.run())
        if isinstance(v, Ok):
            break
        time.sleep(policy.calculate_delay(attempt))

        attempt += 1
    return v
