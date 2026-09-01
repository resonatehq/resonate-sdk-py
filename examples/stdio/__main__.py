"""stdio runs a worker with no network at all, over a tunnel on its stdio.

The worker is an ordinary Resonate program with one difference: its connector
is a :class:`~resonate.connections.StdioConnection`, so every request it makes
leaves on its stdout and everything it is told arrives on its stdin. It has no
server address and needs none.

This file is the other end -- the *host*. It applies each framed line the
worker writes against a Resonate server and writes the response back. Here that
"server" is an in-process :class:`~resonate.connections.LocalConnection`, so the
example runs with nothing installed and nothing listening; in a real deployment
the host is the one holding the credentials and the route, which is the whole
point of the arrangement. A sandboxed worker gets to speak the protocol without
being able to reach anything.

Note which side is which. The worker is the *client* even though it is the
child process, and the host is the *server* even though it never runs any of
the work. All the host does is carry frames.

There are two ways to tell such a worker what to run, and they want different
programs, so the example does both::

    uv run python examples/stdio                    # --mode push
    uv run python examples/stdio --mode sandbox

**push** starts ``worker.py`` once and forwards ``execute`` messages to it as
frames on its stdin. The connection is a source as well as a network, and the
worker runs until its stdin closes.

**sandbox** is what the Tensorlake worker (``tensorlake://``) does: no message
is sent at all. It starts ``sandbox.py`` *per task* with ``RESONATE_TASK_ID``
in its environment, and that process runs the one task and exits. This mode is
here so the deployed shape is exercised rather than described -- the host below
is a stand-in for the worker, but the contract it holds the process to is the
real one.

Run it::

    uv run python examples/stdio [--mode push|sandbox]
"""

from __future__ import annotations

import argparse
import asyncio
import json
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
SANDBOX = Path(__file__).with_name("sandbox.py")


async def spawn(
    program: Path, env: dict[str, str] | None = None
) -> asyncio.subprocess.Process:
    """Start one child with pipes on its stdio. stderr is left alone: it is the
    child's own output channel, and inheriting it is what puts its logs here.
    """
    return await asyncio.create_subprocess_exec(
        sys.executable,
        str(program),
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        env=env,
    )


class Tunnel:
    """Carries the protocol between one child process's stdio and ``server``.

    One direction per method, and they are asymmetric on purpose: a request
    read off stdout is *answered*, while a message from the server is simply
    *written*. That is the protocol's own shape, not the tunnel's.
    """

    def __init__(
        self, proc: asyncio.subprocess.Process, server: LocalConnection
    ) -> None:
        self._proc = proc
        self._server = server
        self._write_lock = asyncio.Lock()
        self._tasks: set[asyncio.Task[None]] = set()

    async def pump(self) -> None:
        """Answer what the child asks, until its stdout ends.

        Ending means the process exited, which is how the sandbox mode below
        learns its task is over.

        Requests are answered concurrently rather than one at a time: the child
        multiplexes by ``corrId``, so a slow one must not hold up the heartbeat
        behind it.
        """
        assert self._proc.stdout is not None
        async for raw in self._proc.stdout:
            line = raw.decode("utf-8", errors="replace").rstrip("\r\n")
            payload = unframe(line)
            if payload is None:
                # The child's own output. Everything not framed is.
                print(f"  [child stdout] {line}")
                continue
            self._spawn(self._answer(payload))

    def push(self, message: str) -> None:
        """Forward one server push message (``execute``/``unblock``)."""
        self._spawn(self._write(message))

    def close(self) -> None:
        """Close the child's stdin, which is how it learns to shut down."""
        assert self._proc.stdin is not None
        self._proc.stdin.close()

    async def _answer(self, request: str) -> None:
        await self._write(await self._server.send(request))

    async def _write(self, payload: str) -> None:
        assert self._proc.stdin is not None
        async with self._write_lock:
            self._proc.stdin.write(frame(payload).encode("utf-8"))
            await self._proc.stdin.drain()

    def _spawn(self, coro: Coroutine[Any, Any, None]) -> None:
        task = asyncio.ensure_future(coro)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)


class PushHost:
    """One long-lived worker, told what to run by messages on its stdin."""

    def __init__(self, server: LocalConnection) -> None:
        self._server = server
        self._proc: asyncio.subprocess.Process | None = None
        self._tunnel: Tunnel | None = None
        self._pumping: asyncio.Task[None] | None = None

    async def start(self) -> None:
        self._proc = await spawn(WORKER)
        self._tunnel = Tunnel(self._proc, self._server)
        self._pumping = asyncio.ensure_future(self._tunnel.pump())

    def deliver(self, message: str) -> None:
        assert self._tunnel is not None
        self._tunnel.push(message)

    async def stop(self) -> None:
        assert self._tunnel is not None
        assert self._proc is not None
        self._tunnel.close()
        await self._proc.wait()
        if self._pumping is not None:
            self._pumping.cancel()


class SandboxHost:
    """One process per task, told which task by its environment.

    The Tensorlake worker's shape, in miniature. Two of its rules are worth
    keeping even at this size, because a host that drops either is a host the
    deployed program would misbehave against:

    * An ``unblock`` is not forwarded. The process it concerns has already
      exited -- that is what suspending means -- so there is nothing to tell.
      The server follows an unblock with a task for the awaiting promise, and
      the ``execute`` for *that* starts a fresh process.
    * A task already running is not started again. A redispatch under a slow
      step is ordinary, and a second process on one promise would be two
      clients on one tunnel, both convinced they hold the lease.
    """

    def __init__(self, server: LocalConnection) -> None:
        self._server = server
        self._running: dict[str, asyncio.Task[None]] = {}

    async def start(self) -> None:
        """Nothing to start: a process exists only while a task is running."""

    def deliver(self, message: str) -> None:
        msg = json.loads(message)
        if msg.get("kind") != "execute":
            return
        task = msg["data"]["task"]
        if task["id"] in self._running:
            return
        self._running[task["id"]] = asyncio.ensure_future(self._run(task))

    async def _run(self, task: dict[str, Any]) -> None:
        env = {
            "PATH": "/usr/bin:/bin",
            "RESONATE_TASK_ID": task["id"],
            "RESONATE_TASK_VERSION": str(task.get("version", 0)),
            "RESONATE_PROMISE_ID": task["id"],
            "RESONATE_TRANSPORT": "stdio",
        }
        proc = await spawn(SANDBOX, env=env)
        print(f"host: started a process for {task['id']}")
        try:
            # Returns when the process exits, which is the process saying its
            # task is finished with -- settled, or suspended awaiting a child.
            await Tunnel(proc, self._server).pump()
            code = await proc.wait()
            print(f"host: process for {task['id']} exited {code}")
        finally:
            self._running.pop(task["id"], None)

    async def stop(self) -> None:
        if self._running:
            await asyncio.gather(*self._running.values(), return_exceptions=True)


async def main(mode: str) -> None:
    # The "server": one in-process simulation, so the example needs nothing
    # running. Swap it for HttpConnection(url=...) and neither child changes.
    server = LocalConnection(group="sandbox")
    host = PushHost(server) if mode == "push" else SandboxHost(server)

    await host.start()
    server.recv(host.deliver)

    # The dispatcher: it shares the server with the children but listens for
    # nothing itself (``sources=[]``), so its handle settles by re-reading the
    # promise rather than by push -- hence the brisk refresh, which is all
    # ``subscription_refresh_secs`` is doing here. It never has ``greet``
    # registered: the child owns that, and this side only dispatches.
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
        await host.stop()
        await client.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=["push", "sandbox"], default="push")
    asyncio.run(main(parser.parse_args().mode))
