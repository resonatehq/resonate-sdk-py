"""The per-task shape: a process started *for one task*, told which by its env.

The difference from ``worker.py`` is where the work comes from. That one
listens: it is handed ``execute`` messages on stdin and runs whatever they
name, for as long as its stdin stays open. This one is *launched* per task by a
worker that already knows which task it is -- the Tensorlake sandbox worker
(``tensorlake://``) starts the process with ``RESONATE_TASK_ID`` and
``RESONATE_TASK_VERSION`` set and sends no message at all -- so it names the
task itself and exits when that task is finished with.

Exiting matters, and it is the part that is easy to get wrong. A function that
awaits a child unwinds rather than blocking: nothing is settled, the promise is
still pending, and the process is done. Its sandbox is kept, suspended with its
filesystem and memory intact, and the ``execute`` for the resumption starts a
*fresh* process in it. A process that lingered instead would still be holding
the tunnel when that arrived, and the worker refuses to start a second process
for a promise that is already running -- so the promise would stall until the
lease lapsed. :meth:`~resonate.resonate.Resonate.process_task` returns
``"suspended"`` for exactly that case, which is the cue to exit.

Note there is no source (``sources=[]``): nothing pushes work here, so this
process advertises no address of its own. Children dispatched by
``ctx.rpc`` resolve through ``resolve_target`` instead -- by default the
server's own ``poll://`` groups, i.e. picked up by an ordinary worker. Pass
``Resonate(resolve_target=...)`` to send them somewhere else, a sandbox of
their own included.

Run it through the host, which launches it the way the worker does::

    uv run python examples/stdio --mode sandbox
"""

from __future__ import annotations

import asyncio
import os
import sys
from typing import TYPE_CHECKING

from resonate.connections import StdioConnection
from resonate.resonate import Resonate

if TYPE_CHECKING:
    from resonate.context import Context


async def greet(ctx: Context, name: str) -> str:
    print(f"sandbox: greeting {name}", file=sys.stderr)
    return f"hello from the sandbox, {name}!"


async def main() -> int:
    task_id = os.environ.get("RESONATE_TASK_ID")
    if task_id is None:
        print(
            "RESONATE_TASK_ID is not set: this program is started by a worker, "
            "one process per task.",
            file=sys.stderr,
        )
        return 2
    version = int(os.environ.get("RESONATE_TASK_VERSION", "0"))

    resonate = Resonate(
        network=StdioConnection(group="sandbox"),
        sources=[],
        group="sandbox",
    )
    resonate.register(greet)
    try:
        status = await resonate.process_task(task_id, version)
        print(f"sandbox: task {task_id} {status}", file=sys.stderr)
    finally:
        await resonate.stop()
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
