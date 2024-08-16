from __future__ import annotations

import contextlib
import queue
from inspect import iscoroutinefunction, isfunction
from typing import TYPE_CHECKING, TypeVar

from typing_extensions import ParamSpec

from resonate.typing import AsyncFnCmd, DurableAsyncFn, DurableFn, FnCmd

if TYPE_CHECKING:
    from resonate.context import Context

T = TypeVar("T")
P = ParamSpec("P")


def wrap_fn_into_cmd(
    ctx: Context,
    fn: DurableFn[P, T] | DurableAsyncFn[P, T],
    *args: P.args,
    **kwargs: P.kwargs,
) -> FnCmd[T] | AsyncFnCmd[T]:
    cmd: FnCmd[T] | AsyncFnCmd[T]
    if isfunction(fn):
        cmd = FnCmd(ctx, fn, *args, **kwargs)
    else:
        assert iscoroutinefunction(fn)
        cmd = AsyncFnCmd(ctx, fn, *args, **kwargs)
    return cmd


def dequeue_batch(q: queue.Queue[T], batch_size: int) -> list[T]:
    elements: list[T] = []
    with contextlib.suppress(queue.Empty):
        for _ in range(batch_size):
            e = q.get_nowait()
            elements.append(e)
            q.task_done()
    return elements


def dequeue(q: queue.Queue[T], timeout: float | None = None) -> T:
    qe = q.get(timeout=timeout)
    q.task_done()
    return qe
