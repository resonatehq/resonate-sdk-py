from __future__ import annotations

import time
from typing import TYPE_CHECKING

from resonate.scheduler import Scheduler
from resonate.storage import LocalPromiseStore

if TYPE_CHECKING:
    from resonate.context import Context


def foo(ctx: Context):
    v = yield ctx.rfc(bar)
    return v + 1


def bar(ctx: Context):
    v = yield ctx.lfc(baz)
    return v + 1


def baz(ctx: Context) -> int:
    return 1


resonate = Scheduler(LocalPromiseStore())
resonate.register(foo)
resonate.register(bar)


def main() -> None:
    resonate.run("foo", foo)
    time.sleep(0.4)
    p = resonate.run("foo.1", bar)
    p.result()
    p_top = resonate.run("foo", foo)
    assert p_top.result() == 3


if __name__ == "__main__":
    main()
