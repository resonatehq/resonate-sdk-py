from __future__ import annotations

from collections.abc import Callable, Generator
from typing import Any, Concatenate, overload

from croniter import CroniterBadCronError, croniter

from resonate import Context, Resonate


@overload
def schedule[**P, R](
    resonate: Resonate,
    id: str,
    cron: str,
    timeout: int,
    func: str,
    *args: Any,
    **kwargs: Any,
) -> None: ...
@overload
def schedule[**P, R](
    resonate: Resonate,
    id: str,
    cron: str,
    timeout: int,
    func: Callable[Concatenate[Context, P], Generator[Any, Any, R] | R],
    *args: P.args,
    **kwargs: P.kwargs,
) -> None: ...
def schedule[**P, R](
    resonate: Resonate,
    id: str,
    cron: str,
    timeout: int,
    func: Callable[Concatenate[Context, P], Generator[Any, Any, R] | R] | str,
    *args: P.args,
    **kwargs: P.kwargs,
) -> None:
    try:
        croniter(cron)
    except CroniterBadCronError:
        msg = "cron is not valid"
        raise ValueError(msg) from None

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
        promise_tags={"resonate:invoke": resonate._opts.target, "resonate:scope": "global"},
    )


resonate = Resonate.remote()


@resonate.register
def foo(ctx: Context, a: int, b: int) -> None:
    print("hi")


schedule(resonate, "foo", "* * * * *", 60 * 60, foo, 1, 2)
