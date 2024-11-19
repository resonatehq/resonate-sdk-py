from __future__ import annotations

import random
from typing import TYPE_CHECKING, Any

from resonate.record import Handle, Promise
from resonate.resonate import Resonate

if TYPE_CHECKING:
    from collections.abc import Generator

    from resonate.context import Context
    from resonate.typing import Yieldable


def test_calling_only_sync_function() -> None:
    def foo_sync(ctx: Context, n: str) -> str:  # noqa: ARG001
        return n

    resonate = Resonate()
    resonate.register(foo_sync)
    p: Handle[str] = resonate.lfi("foo-sync", foo_sync, "hi")
    assert p.result() == "hi"


def test_calling_only_async_function() -> None:
    async def foo_async(ctx: Context, n: str) -> str:  # noqa: ARG001
        return n

    resonate = Resonate()
    resonate.register(foo_async)
    p: Handle[str] = resonate.lfi("foo-async", foo_async, "hi")
    assert p.result() == "hi"


def test_golden_device_lfi() -> None:
    def foo_golden_device_lfi(ctx: Context, n: str) -> Generator[Yieldable, Any, str]:
        p: Promise[str] = yield ctx.lfi(bar_golden_device_lfi, n).options(
            durable=random.choice([True, False])  # noqa: S311
        )
        assert isinstance(p, Promise)
        v: str = yield p
        assert isinstance(v, str)
        return v

    def bar_golden_device_lfi(ctx: Context, n: str) -> str:  # noqa: ARG001
        return n

    resonate = Resonate()
    resonate.register(foo_golden_device_lfi)
    p: Handle[str] = resonate.lfi("test-golden-device-lfi", foo_golden_device_lfi, "hi")
    assert isinstance(p, Handle)
    assert p.result() == "hi"


def test_factorial_lfi() -> None:
    def exec_id(n: int) -> str:
        return f"test-factorial-lfi-{n}"

    def factorial_lfi(ctx: Context, n: int) -> Generator[Yieldable, Any, int]:
        if n == 0:
            return 1
        p = yield ctx.lfi(factorial_lfi, n - 1).options(
            id=exec_id(n - 1),
            durable=random.choice([True, False]),  # noqa: S311
        )
        return n * (yield p)

    resonate = Resonate()
    resonate.register(factorial_lfi)
    n = 10
    p: Handle[int] = resonate.lfi(exec_id(n), factorial_lfi, n)
    assert p.result() == 3_628_800  # noqa: PLR2004


def test_fibonacci_preorder_lfi() -> None:
    def exec_id(n: int) -> str:
        return f"test-fib-lfi-{n}"

    def fib_lfi(ctx: Context, n: int) -> Generator[Yieldable, Any, int]:
        if n <= 1:
            return n
        p1 = yield ctx.lfi(fib_lfi, n - 1).options(
            id=exec_id(n - 1),
            durable=random.choice([True, False]),  # noqa: S311
        )
        p2 = yield ctx.lfi(fib_lfi, n - 2).options(
            id=exec_id(n - 2),
            durable=random.choice([True, False]),  # noqa: S311
        )
        n1 = yield p1
        n2 = yield p2
        return n1 + n2

    resonate = Resonate()
    resonate.register(fib_lfi)
    n = 10
    p: Handle[int] = resonate.lfi(exec_id(n), fib_lfi, n)
    assert p.result() == 55  # noqa: PLR2004
