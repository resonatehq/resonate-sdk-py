r"""AWS Lambda entrypoint for the Resonate "pipeline" example.

A multi-stage DAG-shaped durable workflow:

    download -> parse -> +- transform_a -+
                         +- transform_b -+- merge -> emit

transform_a and transform_b are dispatched before either is awaited, so they
run concurrently; merge joins both. Every stage is a registered function
routed back to this same Lambda as its own pushed task, so a crash mid-pipeline
resumes at the first unsettled stage.

Invoke from a client (see the README):

    resonate invoke pipe.1 --func run_pipeline \\
      --arg example.com/doc \\
      --target http://127.0.0.1:3000/pipeline
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from resonate.faas.aws import Resonate

if TYPE_CHECKING:
    from resonate.context import Context

resonate = Resonate()


@resonate.register
async def download(ctx: Context, url: str) -> str:
    return f"the quick brown fox jumps over {url}"


@resonate.register
async def parse(ctx: Context, raw: str) -> list[str]:
    return raw.split()


@resonate.register
async def transform_a(ctx: Context, p: list[str]) -> int:
    return len(p)


@resonate.register
async def transform_b(ctx: Context, p: list[str]) -> str:
    return " ".join(p).upper()


@resonate.register
async def merge(ctx: Context, word_count: int, upper: str) -> tuple[int, str]:
    return word_count, upper


@resonate.register
async def emit(ctx: Context, word_count: int, upper: str) -> str:
    return "ok"


@resonate.register
async def run_pipeline(ctx: Context, url: str) -> str:
    raw = await ctx.run(download, url)
    parsed = await ctx.run(parse, raw)

    # fan out: transform_a and transform_b dispatched before either is awaited
    fa = ctx.run(transform_a, parsed)
    fb = ctx.run(transform_b, parsed)

    # fan in
    a = await fa
    b = await fb

    word_count, upper = await ctx.run(merge, word_count=a, upper=b)
    return await ctx.run(emit, word_count, upper)


lambda_handler = resonate.handler()
