from __future__ import annotations

from typing import TYPE_CHECKING, Any

from resonate.context import Context
from resonate.dataclasses import CreateDurablePromiseReq

if TYPE_CHECKING:
    from collections.abc import Generator

    from resonate.typing import Yieldable


def foo(ctx: Context, n: int) -> Generator[Yieldable, Any, int]:
    raise NotImplementedError


def main() -> None:
    ctx = Context(seed=1)
    ctx.lfi(foo, 1)
    ctx.rfi(foo, 1)
    ctx.rfi(CreateDurablePromiseReq()).with_options(
        promise_id="custom-promise-id",
    )
