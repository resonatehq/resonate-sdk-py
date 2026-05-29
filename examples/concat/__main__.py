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

After the workflow finishes, the example also walks the recursion tree and
fetches each child promise via ``r.promises.get()`` to verify that the
**order of durable-promise creation matches source-call order**. Inside the
SDK (see ``Context._next_id`` in ``src/resonate/context.py``) each call to
``ctx.run`` / ``ctx.rpc`` mints the next child id as ``{parent_id}.{seq}``,
so the first call in source order gets ``.1`` and the second ``.2``. Because
our two recursive calls are always ``(left..mid)`` then ``(mid+1..right)``,
``parent.1`` *must* resolve to ``chars[left:mid+1]`` and ``parent.2`` to
``chars[mid+1:right+1]``. If the SDK ever swapped the seqs, the substrings
would land under the wrong ids and the assertion would fire.

Start a Resonate server on localhost:8001 first (``resonate dev``), then e.g.::

    uv run python examples/concat --mode run --n 6
"""

from __future__ import annotations

import argparse
import asyncio
import os
import string
from typing import TYPE_CHECKING

from resonate.resonate import Resonate

if TYPE_CHECKING:
    from resonate.context import Context


async def concat_run(ctx: Context, chars: list[str], left: int, right: int) -> str:
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


async def concat_rpc(ctx: Context, chars: list[str], left: int, right: int) -> str:
    if left == right:
        return chars[left]
    if left > right:
        return ""

    mid = (left + right) // 2
    f1 = ctx.rpc("concat_rpc", chars, left, mid)
    f2 = ctx.rpc("concat_rpc", chars, mid + 1, right)

    return await f1 + await f2


async def concat_mix(ctx: Context, chars: list[str], left: int, right: int) -> str:
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
    chars: list[str] = [string.ascii_lowercase[i % 26] for i in range(args.n)]
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

        # Build the (id -> expected substring) map by walking the recursion
        # in source order, mirroring how concat_* spawns its children.
        expected_by_id: dict[str, str] = {id: expected}
        _expected_children(id, chars, 0, len(chars) - 1, expected_by_id)

        # Fetch every promise from the server and assert it resolved to the
        # substring its position in the source-order tree predicts.
        for pid, want in expected_by_id.items():
            record = await r.promises.get(pid)
            assert record.state == "resolved", (
                f"promise {pid} is in state {record.state!r}, expected resolved"
            )
            assert record.value.data == want, (
                f"promise {pid} resolved to {record.value.data!r}, "
                f"expected {want!r} -- creation order was NOT respected"
            )
        print(
            f"durable-promise creation order respected ✓  "
            f"({len(expected_by_id)} promises checked)"
        )
    finally:
        await r.stop()


def _expected_children(
    parent_id: str,
    chars: list[str],
    left: int,
    right: int,
    out: dict[str, str],
) -> None:
    """Populate ``out`` with the substring each child promise must resolve to.

    Mirrors the recursion in ``concat_*``: the first ``ctx.run``/``ctx.rpc``
    call covers ``[left, mid]`` and is minted with seq ``1``; the second
    covers ``[mid+1, right]`` and is minted with seq ``2``. Any base case
    (``left >= right``) spawns no children, so we stop recursing.
    """
    if left >= right:
        return
    mid = (left + right) // 2
    left_id = f"{parent_id}.1"
    right_id = f"{parent_id}.2"
    out[left_id] = "".join(chars[left : mid + 1])
    out[right_id] = "".join(chars[mid + 1 : right + 1])
    _expected_children(left_id, chars, left, mid, out)
    _expected_children(right_id, chars, mid + 1, right, out)


if __name__ == "__main__":
    asyncio.run(main())
