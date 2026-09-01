"""The per-task shape: a process started *for one task*, told which by its env.

The difference from ``worker.py`` is where the work comes from, and that
changes which class you build on. ``worker.py`` listens -- it is handed
``execute`` messages on stdin and runs whatever they name, for as long as its
stdin stays open, which is what :class:`~resonate.resonate.Resonate` is for.
This one is *launched* per task by a worker that already knows which task it
is: the Tensorlake sandbox worker (``tensorlake://``) starts the process with
``RESONATE_TASK_ID`` and ``RESONATE_TASK_VERSION`` set and sends no message at
all. There is nothing to listen for, so this builds on
:class:`~resonate.handler.Handler` -- no source, no address, no listener, no
refresh loop, and no way to dispatch work of its own.

Exiting is the part that is easy to get wrong. A function that awaits a child
unwinds rather than blocking: nothing is settled, the promise is still pending,
and the process is done. Its sandbox is kept, suspended with its filesystem and
memory intact, and the ``execute`` for the resumption starts a *fresh* process
in it. A process that lingered would still be holding the tunnel when that
arrived, and the worker refuses to start a second process for a promise already
running -- so the promise would stall until the lease lapsed.
:meth:`~resonate.handler.Handler.run_from_env` returns ``"suspended"`` for
exactly that case, which is the cue to exit.

Children dispatched with ``ctx.rpc`` resolve through ``resolve_target`` -- by
default the server's own ``poll://`` groups, so an ordinary worker picks them
up. Pass ``Handler(resolve_target=...)`` to send them somewhere else, a sandbox
of their own included.

Run it through the host, which launches it the way the worker does::

    uv run python examples/stdio --mode sandbox
"""

from __future__ import annotations

import asyncio
import sys
from typing import TYPE_CHECKING

from resonate.connections import StdioConnection
from resonate.handler import Handler

if TYPE_CHECKING:
    from resonate.context import Context


handler = Handler(network=StdioConnection(), group="sandbox")


@handler.register
async def greet(ctx: Context, name: str) -> str:
    print(f"sandbox: greeting {name}", file=sys.stderr)
    return f"hello from the sandbox, {name}!"


async def main() -> None:
    status = await handler.run_from_env()
    print(f"sandbox: task {status}", file=sys.stderr)


if __name__ == "__main__":
    asyncio.run(main())
