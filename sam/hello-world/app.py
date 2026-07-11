"""AWS Lambda entrypoint for the Resonate hello-world example.

Unlike a plain Lambda, this function does not respond to raw HTTP requests.
The Resonate server *pushes* one ``execute`` message per invocation (an HTTP
POST carrying the task); the shim decodes it, drives the workflow to a terminal
state via :class:`resonate.core.Core`, and reports the outcome back to the
server. Child tasks (``ctx.run`` / ``ctx.rpc``) are routed back to this same
function, so the whole workflow runs serverless.

Point the shim at your Resonate server with the ``RESONATE_URL`` env var (set
in ``template.yaml``), then invoke the registered ``foo`` from a client:

    resonate dev                     # start the server
    # from a client process:
    handle = r.run("hello-1", "foo", "world")
    print(await handle.result())     # -> "hello, world!"
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from resonate.faas.aws import Resonate

if TYPE_CHECKING:
    from resonate.context import Context

resonate = Resonate()


@resonate.register
async def foo(ctx: Context, name: str) -> str:
    return await ctx.run(bar, name)


async def bar(ctx: Context, name: str) -> str:
    return await ctx.rpc("baz", name)


@resonate.register
async def baz(ctx: Context, name: str) -> str:
    return f"hello, {name}!"


lambda_handler = resonate.handler()
