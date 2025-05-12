from __future__ import annotations

import logging
from collections.abc import Generator
from typing import Any

from resonate import Context, Resonate, Yieldable, resonate
from resonate.retry_policies.constant import Constant

resonate = Resonate.local(log_level=logging.DEBUG)


@resonate.register(name="foo", version=1)
def foo(ctx: Context) -> Generator[Yieldable, Any, str]:
    v1 = yield ctx.lfc(bar).options(retry_policy=Constant(delay=0, max_retries=4))
    v2 = yield ctx.lfc(baz).options(retry_policy=Constant(delay=0, max_retries=4))
    return v1 + v2


def bar(ctx: Context) -> int:
    if ctx.info.attempt < 3:
        raise ValueError
    return 1


def baz(ctx: Context) -> Generator[Yieldable, Any, int]:
    if ctx.info.attempt < 3:
        raise ValueError

    return (yield ctx.lfc(bar).options(retry_policy=Constant(delay=0, max_retries=4)))


def main() -> None:
    h = foo.run("foo")
    v = h.result()
    print(v)


main()
