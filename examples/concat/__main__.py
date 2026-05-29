"""concat shows divide-and-conquer recursion with the Resonate SDK.

A list of characters is concatenated by recursively splitting the range in
half, dispatching both halves as durable invocations, and joining the results
in deterministic left-then-right order. Because the join order is fixed, the
output always equals ``"".join(chars)`` no matter how the halves interleave
across replays or workers.

    --mode rpc   every recursive call goes through ctx.rpc (server-dispatched,
                 may execute on any worker in the group)
    --mode run   every recursive call goes through ctx.run (local task,
                 same worker)

Start a Resonate server on localhost:8001 first (``resonate dev``), then e.g.::

    uv run python examples/concat --mode run --n 6
"""

from __future__ import annotations

import argparse
import asyncio
import os
import string
from typing import TYPE_CHECKING, LiteralString

from resonate.resonate import Resonate

if TYPE_CHECKING:
    from resonate.context import Context


async def concat_run(
    ctx: Context, chars: list[LiteralString], left: int, right: int
) -> str:
    # Base case: a single character.
    if left == right:
        return chars[left]
    if left > right:
        return ""

    # Split the range in half and spawn both branches.
    mid = (left + right) // 2
    f1 = ctx.run(concat_run, chars, left, mid)
    f2 = ctx.run(concat_run, chars, mid + 1, right)

    # Critical: concatenate in deterministic order (left then right).
    return await f1 + await f2


async def concat_rpc(
    ctx: Context, chars: list[LiteralString], left: int, right: int
) -> str:
    if left == right:
        return chars[left]
    if left > right:
        return ""

    mid = (left + right) // 2
    f1 = ctx.rpc("concat_rpc", chars, left, mid)
    f2 = ctx.rpc("concat_rpc", chars, mid + 1, right)

    return await f1 + await f2


async def concat_mix(
    ctx: Context, chars: list[LiteralString], left: int, right: int
) -> str:
    if left == right:
        return chars[left]
    if left > right:
        return ""

    mid = (left + right) // 2
    f1 = ctx.run(concat_mix, chars, left, mid)
    f2 = ctx.rpc("concat_mix", chars, mid + 1, right)

    return await f1 + await f2


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=("rpc", "run", "mix"), default="run")
    parser.add_argument("--n", type=int, default=6, help="number of characters")
    args = parser.parse_args()

    # Build the input: a, b, c, ... wrapping past the alphabet if needed.
    chars = [string.ascii_lowercase[i % 26] for i in range(args.n)]
    expected = "".join(chars)

    url = os.environ.get("RESONATE_URL", "http://localhost:8001")
    r = Resonate(url=url)
    fns = {"rpc": concat_rpc, "run": concat_run, "mix": concat_mix}
    for fn in fns.values():
        r.register(fn)

    try:
        id = f"concat-{args.mode}-{args.n}"
        handle = r.run(id, fns[args.mode], chars, 0, len(chars) - 1)
        result = await handle.result()
        print(f"concat({args.n}) = {result!r}  [mode={args.mode}]")
        assert result == expected, f"Expected {expected!r}, got {result!r}"
        print("ordering preserved ✓")
    finally:
        await r.stop()


if __name__ == "__main__":
    asyncio.run(main())
