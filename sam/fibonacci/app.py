r"""AWS Lambda entrypoint for the Resonate "fibonacci" example.

Three ways to compose recursive durable invocations, each registered as its
own function name:

    fib_rpc   every recursive call goes through ctx.rpc (dispatched by name,
              routed back to this same Lambda)
    fib_run   every recursive call goes through ctx.run (local child task,
              also re-invokes this Lambda)
    fib_mix   one branch via rpc, the other via run

Pick the variant when you invoke (see the README):

    resonate invoke fib.1 --func fib_rpc --arg 10 \\
      --target http://127.0.0.1:3000/fibonacci
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from resonate.faas.aws import Resonate

if TYPE_CHECKING:
    from resonate.context import Context

resonate = Resonate()


@resonate.register
async def fib_rpc(ctx: Context, n: int) -> int:
    if n < 2:
        return n
    f1 = ctx.rpc("fib_rpc", n - 1)
    f2 = ctx.rpc("fib_rpc", n - 2)
    return await f1 + await f2


@resonate.register
async def fib_run(ctx: Context, n: int) -> int:
    if n < 2:
        return n
    f1 = ctx.run(fib_run, n - 1)
    f2 = ctx.run(fib_run, n - 2)
    return await f1 + await f2


@resonate.register
async def fib_mix(ctx: Context, n: int) -> int:
    if n < 2:
        return n
    f1 = ctx.rpc("fib_mix", n - 1)
    f2 = ctx.run(fib_mix, n - 2)
    return await f1 + await f2


lambda_handler = resonate.handler()
