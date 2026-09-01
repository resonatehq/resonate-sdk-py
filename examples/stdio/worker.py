"""The minimal Resonate program that speaks over its own stdin and stdout.

There is no url, no port and no credential -- the whole configuration is
"``StdioConnection``". Requests leave on stdout behind the ``RN8:`` marker,
responses and work come back on stdin, and whoever started this process is on
the other end relaying to a real Resonate server. See ``__main__.py`` in this
directory for that other end.

``StdioConnection`` implements both connector seams, so passing it as the
network makes it the source too: this process advertises an address, receives
``execute`` messages on stdin, and runs the work they name.

Two things a program on a tunnelled stdio has to respect:

* **Application output goes to stderr.** stdout is the protocol channel. The
  marker means a stray ``print`` cannot be *mistaken* for a frame, but keeping
  the channel clean is still the habit that makes logs readable.
* **The host decides when this is over.** There is nothing to serve and no port
  to close, so the program runs until its stdin ends -- which is what
  :meth:`~resonate.connections.StdioConnection.wait_closed` waits for.

Run it through the host, which spawns it::

    uv run python examples/stdio
"""

from __future__ import annotations

import asyncio
import sys
from typing import TYPE_CHECKING

from resonate.connections import StdioConnection
from resonate.resonate import Resonate

if TYPE_CHECKING:
    from resonate.context import Context


async def greet(ctx: Context, name: str) -> str:
    print(f"worker: greeting {name}", file=sys.stderr)
    return f"hello from the tunnel, {name}!"


async def main() -> None:
    stdio = StdioConnection(group="sandbox")
    resonate = Resonate(network=stdio, group="sandbox")
    resonate.register(greet)
    try:
        await stdio.wait_closed()
    finally:
        await resonate.stop()


if __name__ == "__main__":
    asyncio.run(main())
