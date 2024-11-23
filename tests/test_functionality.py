from __future__ import annotations

import os
import random
from functools import cache
from typing import TYPE_CHECKING, Any

import pytest

from resonate.record import Handle, Promise
from resonate.resonate import Resonate
from resonate.stores.local import LocalStore, MemoryStorage
from resonate.stores.remote import RemoteStore

if TYPE_CHECKING:
    from collections.abc import Generator

    from resonate.context import Context
    from resonate.typing import Yieldable


@cache
def _promise_storages() -> list[LocalStore | RemoteStore]:
    stores: list[LocalStore | RemoteStore] = [LocalStore(MemoryStorage())]
    if os.getenv("RESONATE_STORE_URL") is not None:
        stores.append(RemoteStore(url=os.environ["RESONATE_STORE_URL"]))
    return stores


@pytest.mark.parametrize("store", _promise_storages())
def test_calling_only_sync_function(store: LocalStore | RemoteStore) -> None:
    def foo_sync(ctx: Context, n: str) -> str:  # noqa: ARG001
        return n

    resonate = Resonate(store=store)
    resonate.register(foo_sync)
    p: Handle[str] = resonate.run("foo-sync", foo_sync, "hi")
    assert p.result() == "hi"


@pytest.mark.parametrize("store", _promise_storages())
def test_calling_only_async_function(store: LocalStore | RemoteStore) -> None:
    async def foo_async(ctx: Context, n: str) -> str:  # noqa: ARG001
        return n

    resonate = Resonate(store=store)
    resonate.register(foo_async)
    p: Handle[str] = resonate.run("foo-async", foo_async, "hi")
    assert p.result() == "hi"


@pytest.mark.parametrize("store", _promise_storages())
def test_golden_device_lfi(store: LocalStore | RemoteStore) -> None:
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

    resonate = Resonate(store=store)
    resonate.register(foo_golden_device_lfi)
    p: Handle[str] = resonate.run("test-golden-device-lfi", foo_golden_device_lfi, "hi")
    assert isinstance(p, Handle)
    assert p.result() == "hi"


@pytest.mark.parametrize("store", _promise_storages())
def test_factorial_lfi(store: LocalStore | RemoteStore) -> None:
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

    resonate = Resonate(store=store)
    resonate.register(factorial_lfi)
    n = 30
    p: Handle[int] = resonate.run(exec_id(n), factorial_lfi, n)
    assert p.result() == 265252859812191058636308480000000  # noqa: PLR2004


@pytest.mark.parametrize("store", _promise_storages())
def test_fibonacci_preorder_lfi(store: LocalStore | RemoteStore) -> None:
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

    resonate = Resonate(store=store)
    resonate.register(fib_lfi)
    n = 30
    p: Handle[int] = resonate.run(exec_id(n), fib_lfi, n)
    assert p.result() == 832040  # noqa: PLR2004


@pytest.mark.parametrize("store", _promise_storages())
def test_fibonacci_postorder_lfi(store: LocalStore | RemoteStore) -> None:
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
        n2 = yield p2
        n1 = yield p1
        return n1 + n2

    resonate = Resonate(store=store)
    resonate.register(fib_lfi)
    n = 30
    p: Handle[int] = resonate.run(exec_id(n), fib_lfi, n)
    assert p.result() == 832040  # noqa: PLR2004


@pytest.mark.parametrize("store", _promise_storages())
def test_golden_device_lfc(store: RemoteStore | LocalStore) -> None:
    def foo_golden_device_lfc(ctx: Context, n: str) -> Generator[Yieldable, Any, str]:
        v: str = yield ctx.lfc(bar_golden_device_lfi, n).options(
            durable=random.choice([True, False])  # noqa: S311
        )
        assert isinstance(v, str)
        return v

    def bar_golden_device_lfi(ctx: Context, n: str) -> str:  # noqa: ARG001
        return n

    resonate = Resonate(store=store)
    resonate.register(foo_golden_device_lfc)
    p: Handle[str] = resonate.run("test-golden-device-lfc", foo_golden_device_lfc, "hi")
    assert isinstance(p, Handle)
    assert p.result() == "hi"


@pytest.mark.parametrize("store", _promise_storages())
def test_factorial_lfc(store: LocalStore | RemoteStore) -> None:
    def exec_id(n: int) -> str:
        return f"test-factorial-lfc-{n}"

    def factorial_lfc(ctx: Context, n: int) -> Generator[Yieldable, Any, int]:
        if n == 0:
            return 1

        return n * (
            yield ctx.lfc(factorial_lfc, n - 1).options(
                id=exec_id(n - 1),
                durable=random.choice([True, False]),  # noqa: S311
            )
        )

    resonate = Resonate(store=store)
    resonate.register(factorial_lfc)
    n = 30
    p: Handle[int] = resonate.run(exec_id(n), factorial_lfc, n)
    assert p.result() == 265252859812191058636308480000000  # noqa: PLR2004


@pytest.mark.parametrize("store", _promise_storages())
def test_fibonacci_lfc(store: LocalStore | RemoteStore) -> None:
    def exec_id(n: int) -> str:
        return f"test-fib-lfc-{n}"

    def fib_lfc(ctx: Context, n: int) -> Generator[Yieldable, Any, int]:
        if n <= 1:
            return n
        n1 = yield ctx.lfc(fib_lfc, n - 1).options(
            id=exec_id(n - 1),
            durable=random.choice([True, False]),  # noqa: S311
        )
        n2 = yield ctx.lfc(fib_lfc, n - 2).options(
            id=exec_id(n - 2),
            durable=random.choice([True, False]),  # noqa: S311
        )
        return n1 + n2

    resonate = Resonate(store=store)
    resonate.register(fib_lfc)
    n = 30
    p: Handle[int] = resonate.run(exec_id(n), fib_lfc, n)
    assert p.result() == 832040  # noqa: PLR2004


@pytest.mark.skip
@pytest.mark.skipif(
    os.getenv("RESONATE_STORE_URL") is None, reason="env variable is not set"
)
def test_golden_device_rfi() -> None:
    def foo_golden_device_rfi(ctx: Context, n: str) -> Generator[Yieldable, Any, str]:
        p: Promise[str] = yield ctx.rfi(bar_golden_device_rfi, n).options(id="bar")
        assert isinstance(p, Promise)
        v: str = yield p
        assert isinstance(v, str)
        return v

    def bar_golden_device_rfi(ctx: Context, n: str) -> str:  # noqa: ARG001
        return n

    resonate = Resonate(store=RemoteStore(url=os.environ["RESONATE_STORE_URL"]))
    resonate.register(foo_golden_device_rfi)
    resonate.register(bar_golden_device_rfi)
    p: Handle[str] = resonate.run("foo", foo_golden_device_rfi, "hi")
    assert isinstance(p, Handle)
    assert p.result() == "hi"


@pytest.mark.skip
@pytest.mark.skipif(
    os.getenv("RESONATE_STORE_URL") is None, reason="env variable is not set"
)
def test_factorial_rfi() -> None:
    def exec_id(n: int) -> str:
        return f"test-factorial-rfi-{n}"

    def factorial_rfi(ctx: Context, n: int) -> Generator[Yieldable, Any, int]:
        if n == 0:
            return 1
        p = yield ctx.rfi(factorial_rfi, n - 1).options(
            id=exec_id(n - 1),
        )
        return n * (yield p)

    resonate = Resonate(store=RemoteStore(url=os.environ["RESONATE_STORE_URL"]))
    resonate.register(factorial_rfi)
    n = 10
    p: Handle[int] = resonate.run(exec_id(n), factorial_rfi, n)
    assert p.result() == 3628800  # noqa: PLR2004


@pytest.mark.skip
@pytest.mark.skipif(
    os.getenv("RESONATE_STORE_URL") is None, reason="env variable is not set"
)
def test_fibonacci_preorder_rfi() -> None:
    def exec_id(n: int) -> str:
        return f"test-fib-rfi-{n}"

    def fib_rfi(ctx: Context, n: int) -> Generator[Yieldable, Any, int]:
        if n <= 1:
            return n
        p1 = yield ctx.rfi(fib_rfi, n - 1).options(
            id=exec_id(n - 1),
        )
        n1 = yield p1
        p2 = yield ctx.rfi(fib_rfi, n - 2).options(id=exec_id(n - 2))
        n2 = yield p2
        return n1 + n2

    resonate = Resonate(store=RemoteStore(url=os.environ["RESONATE_STORE_URL"]))
    resonate.register(fib_rfi)
    n = 10
    p: Handle[int] = resonate.run(exec_id(n), fib_rfi, n)
    assert p.result() == 55  # noqa: PLR2004


@pytest.mark.skip
@pytest.mark.skipif(
    os.getenv("RESONATE_STORE_URL") is None, reason="env variable is not set"
)
def test_fibonacci_postorder_rfi() -> None:
    def exec_id(n: int) -> str:
        return f"test-fib-rfi-{n}"

    def fib_rfi(ctx: Context, n: int) -> Generator[Yieldable, Any, int]:
        if n <= 1:
            return n
        p1 = yield ctx.rfi(fib_rfi, n - 1).options(
            id=exec_id(n - 1),
        )
        p2 = yield ctx.rfi(fib_rfi, n - 2).options(
            id=exec_id(n - 2),
        )
        n1 = yield p1
        n2 = yield p2
        return n1 + n2

    resonate = Resonate(store=RemoteStore(url=os.environ["RESONATE_STORE_URL"]))
    resonate.register(fib_rfi)
    n = 10
    p: Handle[int] = resonate.run(exec_id(n), fib_rfi, n)
    assert p.result() == 55  # noqa: PLR2004


@pytest.mark.skip
@pytest.mark.skipif(
    os.getenv("RESONATE_STORE_URL") is None, reason="env variable is not set"
)
def test_golden_device_rfi_and_lfc() -> None:
    def foo(ctx: Context, n: str) -> Generator[Yieldable, Any, str]:
        v: str = yield ctx.lfc(bar, n).options(
            id="bar",
            durable=False,
        )
        return v

    def bar(ctx: Context, n: str) -> Generator[Yieldable, Any, str]:
        p: Promise[str] = yield ctx.rfi(baz, n).options(id="baz")
        v: str = yield p
        return v

    def baz(ctx: Context, n: str) -> str:  # noqa: ARG001
        return n

    resonate = Resonate(store=RemoteStore(url=os.environ["RESONATE_STORE_URL"]))
    resonate.register(foo)
    resonate.register(baz)
    p: Handle[str] = resonate.run("foo", foo, "hi")
    assert p.result() == "hi"


@pytest.mark.skip
@pytest.mark.skipif(
    os.getenv("RESONATE_STORE_URL") is None, reason="env variable is not set"
)
def test_fibonacci_postorder_rfi_or_lfi() -> None:
    def exec_id(n: int) -> str:
        return f"fib({n})"

    def fib(ctx: Context, n: int) -> Generator[Yieldable, Any, int]:
        if n <= 1:
            return n

        p1 = yield ctx.rfi(fib, n - 1).options(
            id=exec_id(n - 1),
        )
        p2 = yield ctx.lfi(fib, n - 2).options(
            id=exec_id(n - 2),
        )
        n1 = yield p1
        n2 = yield p2
        return n1 + n2

    resonate = Resonate(store=RemoteStore(url=os.environ["RESONATE_STORE_URL"]))
    resonate.register(fib)
    n = 30
    p: Handle[int] = resonate.run(exec_id(n), fib, n)
    assert p.result()
