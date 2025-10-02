from __future__ import annotations

import time
from collections.abc import Generator
from typing import Any

from resonate import Context, Resonate
from resonate.coroutine import Yieldable

r = Resonate.local()


def hitl(ctx: Context, id: str | None) -> Generator[Yieldable, Any, int]:
    p = yield ctx.promise().options(id=id)
    v = yield p
    return v


r.register(hitl)


def main() -> None:
    handle = r.begin_run("hitl-foo", hitl, "foo")
    time.sleep(1)
    r.promises.resolve(id="foo", data="1")
    # time.sleep(1)
    assert handle.result() == 1

    handle = r.begin_run("hitl-bar", hitl, "bar")
    time.sleep(1)
    r.promises.resolve(id="bar", data="1")
    # time.sleep(1)
    assert handle.result() == 1


main()
