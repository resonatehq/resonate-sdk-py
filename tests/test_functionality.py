from __future__ import annotations

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
        p: Promise[str] = yield ctx.lfi(bar_golden_device_lfi, n)
        assert isinstance(p, Promise)
        v: str = yield p
        assert isinstance(v, str)
        return v

    def bar_golden_device_lfi(ctx: Context, n: str) -> str:
        return n

    resonate = Resonate()
    resonate.register(foo_golden_device_lfi)
    p: Handle[str] = resonate.lfi("test-golden-device-lfi", foo_golden_device_lfi, "hi")
    assert isinstance(p, Handle)
    assert p.result() == "hi"
