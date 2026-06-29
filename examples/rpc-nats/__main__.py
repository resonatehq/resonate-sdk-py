"""rpc shows one worker dispatching to another by group.

Two ``Resonate`` instances share a server but live in different **groups**:

* ``backend`` registers ``greet`` and does the work.
* ``frontend`` registers nothing -- it only *dispatches*.

``rpc`` dispatches by **name**, not by a local function object, so the caller
need not have the target registered at all. ``with_opts(target="backend")``
routes the call to the backend group's anycast address
(``poll://any@backend``); the server hands the execute message to a worker
subscribed there, which runs ``greet`` and settles the promise. The frontend's
handle is woken by the resulting ``unblock`` and reads the value back -- the
whole round trip crossing the durability boundary, not an in-process call.

This is the building block for splitting a system into independently deployed
services: each owns its functions and group, and they invoke each other by name
+ target without sharing code.

Start a resonate-on-nats server first (``./resonate-on-nats dev``), then::

    uv run python examples/rpc-nats
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


async def greet(ctx: Context, name: str) -> str:
    # Runs on the backend worker -- the side effect lives in the leaf.
    print(f"backend: greeting {name}")
    return f"hello from backend, {name}!"


async def main() -> None:
    url = os.environ.get("RESONATE_URL", "nats://localhost:4222")
    nc = await nats.connect(url)

    # The worker: owns ``greet`` and listens on the "backend" group.
    backend = Resonate(network=NatsNetwork(nc, group="backend"))
    backend.register(greet)

    # The caller: a different group, with ``greet`` deliberately NOT registered.
    frontend = Resonate(network=NatsNetwork(nc, group="frontend"))

    try:
        id = f"rpc-{time.time_ns()}"
        # Dispatch by name + target to the backend group, then await the result.
        handle = frontend.options(target="backend").rpc(id, "greet", "world")
        result = await handle.result()
        assert result == "hello from backend, world!"
        print(f"frontend: got {result!r}")
    finally:
        await frontend.stop()
        await backend.stop()
        await nc.drain()


if __name__ == "__main__":
    asyncio.run(main())
