from __future__ import annotations

import argparse
import uuid
from typing import TYPE_CHECKING, Any

from resonate import Context, Resonate, Yieldable

if TYPE_CHECKING:
    from collections.abc import Generator

resonate = Resonate.local()


@resonate.register(name="foo", version=1)
def foo1(ctx: Context) -> Generator[Yieldable, Any, str]:
    return (yield ctx.lfc(bar))


@resonate.register(name="foo", version=2)
def foo2(ctx: Context) -> Generator[Yieldable, Any, str]:
    return (yield ctx.lfc(baz))


def bar(ctx: Context) -> str:
    return "bar"


def baz(ctx: Context) -> str:
    return "baz"


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--version", type=int, choices=[1, 2], required=True)

    args = parser.parse_args()
    h = resonate.options(version=args.version).run(f"foo-{uuid.uuid4().hex}", "foo")
    v = h.result()
    assert v == "bar" if args.version == 1 else "baz"
