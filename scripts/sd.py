from __future__ import annotations

from typing import TYPE_CHECKING, Any

from resonate.resonate import Resonate
from resonate.retry_policy import never

if TYPE_CHECKING:
    from collections.abc import Generator

    from resonate.context import Context
    from resonate.typing import Yieldable

resonate = Resonate()


@resonate.register
def foo(ctx: Context, n: int) -> Generator[Yieldable, Any, int]:
    return (yield ctx.lfc(bar, n).options(retry_policy=never()))


def bar(ctx: Context, n: int) -> int:
    raise NotImplementedError


def main() -> None:
    foo.run("foo", 1)


if __name__ == "__main__":
    main()
