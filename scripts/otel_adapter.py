from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any

from resonate.options import Options
from resonate.scheduler import Scheduler
from resonate.storage import LocalPromiseStore
from resonate.tracing.opentelemetry import OpenTelemetryAdapter

if TYPE_CHECKING:
    from collections.abc import Generator

    from resonate.context import Context
    from resonate.promise import Promise
    from resonate.typing import Yieldable


def _sleep_func(ctx: Context, seconds: int) -> None:
    time.sleep(seconds)


def suma(ctx: Context, a: int, b: int) -> int:
    return a + b


def long_running_execution(
    ctx: Context, seconds: int
) -> Generator[Yieldable, Any, str]:
    # execution invoked
    yield ctx.call(_sleep_func, seconds=seconds)
    r = yield ctx.call(suma, a=1, b=2)
    return f"I slept for {seconds} seconds. Result {r}"


def execution(ctx: Context) -> Generator[Yieldable, Any, list[str]]:
    # execution Invoked
    p1: Promise[str] = yield ctx.invoke(long_running_execution, seconds=3)
    p2: Promise[str] = yield ctx.invoke(long_running_execution, seconds=8)

    responses: list[str] = []
    v1: str = yield p1  # executed resumed

    responses.append(v1)
    v2: str = yield p2  # executed resumed
    responses.append(v2)

    return responses  # execution terminted


def main() -> None:
    time.time_ns()
    s = Scheduler(
        durable_promise_storage=LocalPromiseStore(),
        tracing_adapter=OpenTelemetryAdapter(
            app_name="demo", endpoint="http://localhost:4318/v1/traces"
        ),
    )
    p = s.run("execution-with-concurrency-v2", Options(durable=True), execution)
    r = p.result()
    assert len(r) == 2


if __name__ == "__main__":
    main()
