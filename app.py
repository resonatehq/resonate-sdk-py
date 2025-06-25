from __future__ import annotations

from threading import Event
from typing import TYPE_CHECKING, Any, Concatenate

from resonate import Context, Resonate

if TYPE_CHECKING:
    from collections.abc import Callable, Generator

    from resonate.coroutine import Yieldable


def schedule[**P, R](
    resonate: Resonate,
    id: str,
    cron: str,
    timeout: int,
    func: Callable[Concatenate[Context, P], Generator[Any, Any, R] | R] | str,
    *args: P.args,
    **kwargs: P.kwargs,
) -> None:
    if isinstance(func, str):
        name = func
        version = resonate._registry.latest(func)
    else:
        name, _, version = resonate._registry.get(func, resonate._opts.version)

    promise_headers, promise_data = resonate._opts.get_encoder().encode({"func": name, "args": args, "kwargs": kwargs, "version": version})

    resonate.schedules.create(
        id,
        cron,
        f"{id}.{{{{.timestamp}}}}",
        timeout,
        promise_data=promise_data,
        promise_headers=promise_headers,
        promise_tags={"resonate:invoke": resonate._opts.target},
    )


resonate = Resonate.local()


@resonate.register
def foo(ctx: Context, a: int, b: int) -> Generator[Yieldable, Any, None]:
    print((yield ctx.lfc(bar, a)) + b)  # noqa: T201


def bar(ctx: Context, a: int) -> int:
    return a


resonate.start()
schedule(resonate, "foo", "* * * * *", 60 * 60, foo, 1, 2)
Event().wait()
