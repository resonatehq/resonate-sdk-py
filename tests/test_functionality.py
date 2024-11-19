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
    p: Handle[str] = resonate.lfi("foo-sync", foo_sync, "hi")
    assert p.result() == "hi"


@pytest.mark.parametrize("store", _promise_storages())
def test_calling_only_async_function(store: LocalStore | RemoteStore) -> None:
    async def foo_async(ctx: Context, n: str) -> str:  # noqa: ARG001
        return n

    resonate = Resonate(store=store)
    resonate.register(foo_async)
    p: Handle[str] = resonate.lfi("foo-async", foo_async, "hi")
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
    p: Handle[str] = resonate.lfi("test-golden-device-lfi", foo_golden_device_lfi, "hi")
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
    p: Handle[int] = resonate.lfi(exec_id(n), factorial_lfi, n)
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
    p: Handle[int] = resonate.lfi(exec_id(n), fib_lfi, n)
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
    p: Handle[int] = resonate.lfi(exec_id(n), fib_lfi, n)
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
    p: Handle[str] = resonate.lfi("test-golden-device-lfc", foo_golden_device_lfc, "hi")
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
    p: Handle[int] = resonate.lfi(exec_id(n), factorial_lfc, n)
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
    p: Handle[int] = resonate.lfi(exec_id(n), fib_lfc, n)
    assert p.result() == 832040  # noqa: PLR2004
