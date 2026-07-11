r"""AWS Lambda entrypoint for the Resonate "detached" example.

Fire-and-forget durable invocations whose lifetime is decoupled from the
parent:

    place_order:
        1. reserve_stock        (ctx.run -- parent waits)
        2. charge_card          (ctx.run -- parent waits)
        3. write_audit_log      (ctx.detached -- parent does NOT wait)

``ctx.detached`` dispatches ``write_audit_log`` by name back to this same
Lambda; the parent only awaits the child's promise *id*, not its result, so the
audit workflow runs as its own pushed task on its own timeline. Every function
reachable via run/detached is registered so the shim can resolve it when the
server pushes each child task back.

Invoke from a client (see the README):

    resonate invoke order.1 --func place_order \\
      --arg alice --arg WIDGET-7 --arg 2 --arg 199 \\
      --target http://127.0.0.1:3000/detached
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from resonate.faas.aws import Resonate

if TYPE_CHECKING:
    from resonate.context import Context

resonate = Resonate()


# -- Order steps (leaves: each prints once, settles once) ------------------


@resonate.register
async def reserve_stock(ctx: Context, sku: str, qty: int) -> str:
    return f"STK-{sku}-{qty}"


@resonate.register
async def charge_card(ctx: Context, customer: str, amount: int) -> str:
    return f"CH-{customer}-{amount}"


# -- Audit workflow (its own lifetime, Det subtree) ------------------------


@resonate.register
async def hash_payload(ctx: Context, customer: str, sku: str, amount: int) -> str:
    return f"{customer}:{sku}:{amount}".encode().hex()


@resonate.register
async def ship_to_warehouse(ctx: Context, digest: str) -> str:
    return f"audit-{digest}"


@resonate.register
async def write_audit_log(
    ctx: Context, customer: str, sku: str, amount: int, stock_ref: str, charge_ref: str
) -> str:
    digest = await ctx.run(hash_payload, customer, sku, amount)
    return await ctx.run(ship_to_warehouse, digest)


# -- Orchestrator ----------------------------------------------------------


@resonate.register
async def place_order(
    ctx: Context, customer: str, sku: str, qty: int, amount: int
) -> tuple[str, str, str]:
    # Foreground work -- the order needs both of these to commit.
    stock_ref = await ctx.run(reserve_stock, sku, qty)
    charge_ref = await ctx.run(charge_card, customer, amount)

    # Fire-and-forget: dispatch the audit workflow by name and get back only
    # the id of its durable promise. Its body never runs in this task.
    audit_future = ctx.detached(
        "write_audit_log",
        customer=customer,
        sku=sku,
        amount=amount,
        stock_ref=stock_ref,
        charge_ref=charge_ref,
    )
    audit_id = await audit_future.id()

    return stock_ref, charge_ref, audit_id


lambda_handler = resonate.handler()
