"""hello is a minimal example of using the Resonate SDK.

It registers a function, invokes it durably against a Resonate server, and
prints the result. Mirrors the Go SDK's ``hello`` example.

Start a Resonate server on localhost:8001 first (``resonate dev``), then::

    uv run python examples/hello
"""

from __future__ import annotations

import asyncio
import os
import random
import time

from resonate.resonate import Resonate


async def foo(context):

    p1 = context.run(bar)  # foo.1
    p2 = context.run(bar)  # foo.2
    await p1
    await p2


async def bar(context):

    p1 = context.run(baz)
    p2 = context.rpc(".")
    p3 = context.run(baz)

    await p1
    await p2
    await p3


async def baz(context):

    await asyncio.sleep(random)

    print("hello from", context.id)


print(execute_until_blocked_inner(foo))


async def main() -> None:
    url = os.environ.get("RESONATE_URL", "http://localhost:8001")
    r = Resonate(url=url)
    r.register(foo)
    r.register(baz)
    try:
        id = f"hello-{time.time_ns()}"
        handle = r.run(id, foo)
        print(await handle.result())
    finally:
        await r.stop()


if __name__ == "__main__":
    asyncio.run(main())
