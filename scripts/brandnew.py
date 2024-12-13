from __future__ import annotations

from typing import TYPE_CHECKING

from resonate.resonate import Resonate

if TYPE_CHECKING:
    from resonate.context import Context
    from resonate.typing import Coro

resonate = Resonate()


@resonate.register
def foo(ctx: Context, n: int) -> Coro[str]:
    yield ctx.sleep(n)
    return f"slept for {n} secs"


def main() -> None:
    n = 15
    p = foo.run(f"sleep-for-{n}", n=n)
    print(p.result())  # noqa: T201


if __name__ == "__main__":
    main()
