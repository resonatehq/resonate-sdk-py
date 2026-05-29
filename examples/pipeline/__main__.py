"""pipeline shows a multi-stage DAG-shaped durable workflow:

    download -> parse -> +- transform_a -+
                         +- transform_b -+- merge -> emit

transform_a and transform_b run in parallel (both RPCs are dispatched before
either is awaited); merge depends on both and synchronizes them with await.
Every stage is a registered function backed by a durable promise, so a crash
mid-pipeline picks up at the first unsettled stage without re-doing completed
work.

Mirrors the Go SDK's ``pipeline`` example. Start a Resonate server on
localhost:8001 first (``resonate dev``), then::

    uv run python examples/pipeline

Note on replay: a durable orchestrator re-executes from the top each time it
awaits a not-yet-settled future, so any side effect (a ``print``, an external
call) belongs in a leaf stage function -- which settles once and never re-runs
-- not in ``run_pipeline`` itself. That is why every log line below lives in a
stage, never in the orchestrator.
"""

from __future__ import annotations

import asyncio
import os
import time
from typing import TYPE_CHECKING

import msgspec

from resonate.resonate import Resonate

if TYPE_CHECKING:
    from resonate.context import Context

# -- Domain types ----------------------------------------------------------


class Raw(msgspec.Struct, frozen=True):
    body: str


class Parsed(msgspec.Struct, frozen=True):
    words: list[str]


class MergeArgs(msgspec.Struct, frozen=True):
    word_count: int
    upper: str


class Merged(msgspec.Struct, frozen=True):
    word_count: int
    upper: str


# -- Stage functions (leaves: each prints once, settles once) --------------


async def download(ctx: Context, url: str) -> Raw:
    body = f"the quick brown fox jumps over {url}"
    print(f"  [download] {url} -> {len(body)} bytes")
    return Raw(body=body)


async def parse(ctx: Context, raw: Raw) -> Parsed:
    words = raw.body.split()
    print(f"  [parse] {len(words)} words")
    return Parsed(words=words)


async def transform_a(ctx: Context, p: Parsed) -> int:
    print("  [transform_a] counting words")
    return len(p.words)


async def transform_b(ctx: Context, p: Parsed) -> str:
    upper = " ".join(p.words).upper()
    print(f"  [transform_b] uppercased {len(upper)} chars")
    return upper


async def merge(ctx: Context, args: MergeArgs) -> Merged:
    print("  [merge] combining transforms")
    return Merged(word_count=args.word_count, upper=args.upper)


async def emit(ctx: Context, m: Merged) -> str:
    print(f"  [emit] words={m.word_count} upper={m.upper!r}")
    return "ok"


# -- Pipeline orchestrator -------------------------------------------------


async def run_pipeline(ctx: Context, url: str) -> str:
    raw = await ctx.rpc("download", url)
    parsed = await ctx.rpc("parse", raw)

    # fan out: transform_a and transform_b dispatched before either is awaited
    fa = ctx.rpc("transform_a", parsed)
    fb = ctx.rpc("transform_b", parsed)

    # fan in
    a = await fa
    b = await fb

    merged = await ctx.rpc("merge", MergeArgs(word_count=a, upper=b))
    return await ctx.rpc("emit", merged)


# -- main ------------------------------------------------------------------


async def main() -> None:
    url = os.environ.get("RESONATE_URL", "http://localhost:8001")
    r = Resonate(url=url)
    for fn in (run_pipeline, download, parse, transform_a, transform_b, merge, emit):
        r.register(fn)

    try:
        id = f"pipeline-{time.time_ns()}"
        print(f"[run_pipeline] starting workflow id={id}")
        handle = r.run(id, run_pipeline, "example.com/doc")
        out = await handle.result()
        print(f"[run_pipeline] OK: sent={out!r}")
    finally:
        await r.stop()


if __name__ == "__main__":
    asyncio.run(main())
