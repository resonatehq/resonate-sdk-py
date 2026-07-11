r"""AWS Lambda entrypoint for the Resonate "pydantic" example.

Opt-in Pydantic support at the durability boundary. The codec serializes a
``pydantic.BaseModel`` (via ``model_dump``) and, when a result is decoded
against that model type, reconstructs the exact model -- at every boundary, so
an internal ``ctx.run`` stage's declared return model is rebuilt on recovery
just like the workflow's own return.

Requires the ``pydantic`` dependency (see ``requirements.txt``).

Invoke from a client (see the README); pass the items as a JSON arg:

    resonate invoke pyd.1 --func create_order \\
      --arg ord-42 \\
      --arg '[{"sku":"A1","qty":2,"unit_price":9.5},{"sku":"B2","qty":1,"unit_price":4.0}]' \\
      --target http://127.0.0.1:3000/pydantic
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pydantic

from resonate.faas.aws import Resonate

if TYPE_CHECKING:
    from resonate.context import Context

resonate = Resonate()


class LineItem(pydantic.BaseModel):
    sku: str
    qty: int = pydantic.Field(gt=0)
    unit_price: float = pydantic.Field(ge=0)


class Order(pydantic.BaseModel):
    id: str
    items: list[LineItem]
    total: float


class Pricing(pydantic.BaseModel):
    total: float


@resonate.register
async def price_items(ctx: Context, items: list[dict]) -> Pricing:
    # A leaf stage: its declared return model is rebuilt from the settled value.
    total = sum(i["qty"] * i["unit_price"] for i in items)
    return Pricing(total=total)


@resonate.register
async def create_order(ctx: Context, order_id: str, items: list[dict]) -> Order:
    priced = await ctx.run(price_items, items)
    # The return annotation (Order) is what rebuilds the model for the awaiter.
    return Order(
        id=order_id,
        items=[LineItem(**i) for i in items],
        total=priced.total,
    )


lambda_handler = resonate.handler()
