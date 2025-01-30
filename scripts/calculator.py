from __future__ import annotations

import uuid
from typing import TYPE_CHECKING, Any

from resonate import Context, Promise, Resonate

if TYPE_CHECKING:
    from collections.abc import Generator

    from resonate.typing import Yieldable

resonate = Resonate()


@resonate.register(name="+")
def add(ctx: Context, a: int, b: int) -> int:
    return a + b


@resonate.register()
def calculator(ctx: Context, a: int, b: int) -> Generator[Yieldable, Any, int]:
    p: Promise[int] = yield ctx.rfi("+", a, b)
    return (yield p)


def main() -> None:
    h = resonate.run(uuid.uuid4().hex, calculator, 1, 2)
    v = h.result()
    print(v)  # noqa: T201


if __name__ == "__main__":
    main()
