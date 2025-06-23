from __future__ import annotations

from threading import Event
from typing import TYPE_CHECKING, Any

from resonate import Context, Resonate
from resonate.coroutine import Yieldable

if TYPE_CHECKING:
    from collections.abc import Generator


resonate = Resonate.remote()


@resonate.register
def foo(ctx: Context, a: int, b: int) -> Generator[Yieldable, Any, None]:
    print((yield ctx.lfc(bar, a)) + b)


def bar(ctx: Context, a: int) -> int:
    return a


resonate.start()
resonate.options(idempotency_key=lambda x: x).schedule("foo", "* * * * *", 60 * 60, foo, 1, 2)
Event().wait()
