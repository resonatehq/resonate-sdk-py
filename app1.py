from __future__ import annotations

import sys
import time
import uuid
from collections.abc import Generator
from typing import Any

from resonate import Context, Resonate
from resonate.coroutine import Yieldable

r = Resonate.local()


def hitl(ctx: Context, id: str) -> Generator[Yieldable, Any, int]:
    p = yield ctx.promise().options(id=id, idempotency_key=id)
    v = yield p
    return v


r.register(hitl)


def main() -> None:
    uid = uuid.uuid4().hex
    p = r.promises.create("foo", sys.maxsize, ikey="foo", strict=False)
    print(p)
    handle = r.begin_run(f"hitl-{uid}", hitl, p.id)
    p_done = r.promises.resolve(id=p.id, data="1")
    time.sleep(1)
    assert handle.result() == 1

    uid = uuid.uuid4().hex
    p = r.promises.create("bar", sys.maxsize, ikey="bar", strict=False)
    print(p)
    handle = r.begin_run(f"hitl-{uid}", hitl, p.id)
    p_done = r.promises.resolve(id=p.id, data="1")
    time.sleep(1)
    assert handle.result() == 1


main()
