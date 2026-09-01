"""stdio runs a worker with no network at all, over a tunnel on its stdio.

The worker (``worker.py``) is an ordinary Resonate program with one difference:
its connector is a :class:`~resonate.connections.StdioConnection`, so every
request it makes leaves on its stdout and every response and ``execute``
message arrives on its stdin. It has no server address and needs none.

This file is the other end -- the *host*. It spawns the worker, and for each
framed line the worker writes it applies the request against a Resonate server
and writes the response back. Here that "server" is an in-process
:class:`~resonate.connections.LocalConnection`, so the example runs with
nothing installed and nothing listening; in a real deployment the host is the
one holding the credentials and the route, which is the point of the
arrangement. A sandboxed worker gets to speak the protocol without being able
to reach anything.

Note which side is which. The worker is the *client* even though it is the
child process, and the host is the *server* even though it never runs any of
the work. All the host does is carry frames.

Run it::

    uv run python examples/stdio
"""

from __future__ import annotations

import asyncio
import sys
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any

from resonate.connections import LocalConnection
from resonate.connections.stdio import frame, unframe
from resonate.resonate import Resonate

if TYPE_CHECKING:
    from collections.abc import Coroutine

WORKER = Path(__file__).with_name("worker.py")


class Tunnel:
    """Carries the protocol between a child process's stdio and ``server``.

    One direction per method, and they are asymmetric on purpose: a request
    read off stdout is *answered*, while a push message from the server is
    simply *written*. That is the protocol's own shape, not the tunnel's.
    """

    def __init__(
        self, proc: asyncio.subprocess.Process, server: LocalConnection
    ) -> None:
        self._proc = proc
        self._server = server
        self._write_lock = asyncio.Lock()
        self._tasks: set[asyncio.Task[None]] = set()

    async def pump(self) -> None:
        """Read the worker's stdout until it ends, answering what it asks.

        Requests are answered concurrently rather than one at a time: the
        worker multiplexes by ``corrId``, so a slow one must not hold up the
        heartbeat behind it.
        """
        assert self._proc.stdout is not None
        async for raw in self._proc.stdout:
            line = raw.decode("utf-8", errors="replace").rstrip("\r\n")
            payload = unframe(line)
            if payload is None:
                # The worker's own output. Everything not framed is.
                print(f"  [worker stdout] {line}")
                continue
            self._spawn(self._answer(payload))

    def push(self, message: str) -> None:
        """Forward one server push message (``execute``/``unblock``) to the worker."""
        self._spawn(self._write(message))

    def close(self) -> None:
        """Close the worker's stdin, which is how it learns to shut down."""
        assert self._proc.stdin is not None
        self._proc.stdin.close()

    async def _answer(self, request: str) -> None:
        response = await self._server.send(request)
        await self._write(response)

    async def _write(self, payload: str) -> None:
        assert self._proc.stdin is not None
        async with self._write_lock:
            self._proc.stdin.write(frame(payload).encode("utf-8"))
            await self._proc.stdin.drain()

    def _spawn(self, coro: Coroutine[Any, Any, None]) -> None:
        task = asyncio.ensure_future(coro)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)


async def main() -> None:
    # The "server": one in-process simulation, so the example needs nothing
    # running. Swap it for HttpConnection(url=...) and the worker is unchanged.
    server = LocalConnection(group="sandbox")

    proc = await asyncio.create_subprocess_exec(
        sys.executable,
        str(WORKER),
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
    )
    tunnel = Tunnel(proc, server)
    server.recv(tunnel.push)
    pumping = asyncio.ensure_future(tunnel.pump())

    # The dispatcher: it shares the server with the worker but listens for
    # nothing itself (``sources=[]``), so its handle settles by re-reading the
    # promise rather than by push -- hence the brisk refresh, which is all
    # ``subscription_refresh_secs`` is doing here. It never has ``greet``
    # registered: the worker owns that, and this side only dispatches.
    client = Resonate(
        network=server,
        sources=[],
        group="sandbox",
        subscription_refresh_secs=0.25,
        autostart=False,
    )
    client.start()

    try:
        id = f"stdio-{time.time_ns()}"
        handle = client.options(target="sandbox").rpc(id, "greet", "world")
        print(f"host: dispatched {id}")
        print(f"host: got {await handle.result()!r}")
    finally:
        tunnel.close()
        await proc.wait()
        pumping.cancel()
        await client.stop()


if __name__ == "__main__":
    asyncio.run(main())
