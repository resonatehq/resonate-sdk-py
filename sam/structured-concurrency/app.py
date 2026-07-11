r"""AWS Lambda entrypoint for the Resonate "structured-concurrency" example.

The runtime never leaks an unawaited durable child:

    foo:
        ctx.run(bar, 1)   # spawned, never awaited
        ctx.run(bar, 2)   # spawned, never awaited
        return 5          # returns without touching either future

``foo`` fires two ``ctx.run(bar)`` children and returns ``5`` immediately.
Structured concurrency guarantees ``foo``'s promise cannot settle until both
children -- each its own pushed task back to this same Lambda -- run to
completion. The children get deterministic ids ``{foo_id}.1`` / ``{foo_id}.2``;
inspect them after the run (``resonate promises get``) to see they resolved.

Invoke from a client (see the README):

    resonate invoke sc.1 --func foo \\
      --target http://127.0.0.1:3000/structured-concurrency
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from resonate.faas.aws import Resonate

if TYPE_CHECKING:
    from resonate.context import Context

resonate = Resonate()


@resonate.register
async def bar(ctx: Context, n: int) -> int:
    # Leaf: if structured concurrency holds, both of foo's never-awaited
    # children land here even though foo returned first.
    return n * 10


@resonate.register
async def foo(ctx: Context) -> int:
    # Spawn two local children and walk away -- neither future is awaited.
    ctx.run(bar, 1)
    ctx.run(bar, 2)
    return 5


lambda_handler = resonate.handler()
