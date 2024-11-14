from __future__ import annotations

from typing import TYPE_CHECKING

from resonate.resonate import Resonate

if TYPE_CHECKING:
    from resonate.context import Context

resonate = Resonate(url="http://localhost:8001", polling_url="http://localhost:8002")


def foo(ctx: Context, n: str) -> str:
    return n


resonate.register(foo)
p = resonate.rfi("foo", "foo", args=("hi",))
p.result()
