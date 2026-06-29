"""nats runs the foo/bar/baz example against a resonate-on-nats server.

The NATS connection lifecycle lives outside the SDK: this example connects,
hands the connection to :class:`NatsNetwork`, and closes it itself at the end.

Start a server first (from the ``resonate-on-nats`` checkout)::

    ./resonate-on-nats dev               # embedded NATS on nats://localhost:4222

then run::

    uv run python examples/nats
"""

from __future__ import annotations

import asyncio
import os
import time
from typing import TYPE_CHECKING

import nats

from resonate.network import NatsNetwork
from resonate.resonate import Resonate

if TYPE_CHECKING:
    from resonate.context import Context


async def foo(ctx: Context, name: str) -> str:
    return await ctx.run(bar, name)


async def bar(ctx: Context, name: str) -> str:
    return await ctx.rpc("baz", name)


async def baz(ctx: Context, name: str) -> str:
    print("foo")
    return f"hello, {name}!"


async def main() -> None:
    url = os.environ.get("RESONATE_URL", "nats://localhost:4222")

    # Connection is set up out here, configured, and torn down out here.
    nc = await nats.connect(url)
    r = Resonate(network=NatsNetwork(nc))
    r.register(foo)
    r.register(baz)
    try:
        id = f"nats-{time.time_ns()}"
        handle = r.run(id, foo, "world")
        print(await handle.result())
    finally:
        await r.stop()
        await nc.drain()


if __name__ == "__main__":
    asyncio.run(main())
