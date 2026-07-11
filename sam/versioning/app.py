r"""AWS Lambda entrypoint for the Resonate "versioning" example.

Several versions of one function name registered side by side. The registry is
keyed on ``(name, version)``, so the same name ``charge`` has two coexisting
implementations -- the bread and butter of a rolling deploy where in-flight
work keeps running the version it started on.

The version is explicit, never "latest": a durable promise records its version
at create time, so replay resolves the same implementation every time.

Select the version on invoke with ``--version`` (see the README):

    resonate invoke chg.1 --func charge --version 2 --arg 100 \\
      --target http://127.0.0.1:3000/versioning
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from resonate.faas.aws import Resonate

if TYPE_CHECKING:
    from resonate.context import Context

resonate = Resonate()


@resonate.register(name="charge", version=1)
async def charge_v1(ctx: Context, amount: float) -> float:
    # v1: charge the amount as-is.
    return amount


@resonate.register(name="charge", version=2)
async def charge_v2(ctx: Context, amount: float) -> float:
    # v2: the billing rules changed -- add a 3% processing fee.
    return round(amount * 1.03, 2)


lambda_handler = resonate.handler()
