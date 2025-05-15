from __future__ import annotations

from collections.abc import Generator
from typing import Any

from resonate import Context, Resonate

resonate = Resonate.remote()
# resonate = Resonate.local()


def fib(ctx: Context, n: int) -> Generator[Any, Any, int]:
    if n < 2:
        # return n
        return (yield ctx.lfc(lambda _, n: n, n))

    p1 = yield ctx.lfi(fib, n - 1).options(id=f"fib({n - 1})")
    p2 = yield ctx.lfi(fib, n - 2).options(id=f"fib({n - 2})")

    v1 = yield p1
    v2 = yield p2

    return v1 + v2


resonate.register(fib)

n = 10
handle = resonate.run(f"fib({n})", fib, n)
handle.result()
