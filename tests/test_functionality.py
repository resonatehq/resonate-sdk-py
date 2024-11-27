from __future__ import annotations

import os
import random
from functools import cache, lru_cache
from typing import TYPE_CHECKING, Any

import pytest

from resonate.record import Handle, Promise
from resonate.resonate import Resonate
from resonate.stores.local import LocalStore, MemoryStorage
from resonate.stores.remote import RemoteStore
from resonate.targets import poll
from resonate.task_sources.poller import Poller

if TYPE_CHECKING:
    from collections.abc import Generator

    from resonate.context import Context
    from resonate.typing import Yieldable


@lru_cache
def _fib(n: int) -> int:
    if n <= 1:
        return n
    return _fib(n - 1) + _fib(n - 2)


@lru_cache
def _factorial(n: int) -> int:
    if n == 0:
        return 1
    return n * _factorial(n - 1)


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
    assert p.result() == _factorial(n)


@pytest.mark.parametrize("store", _promise_storages())
def test_fibonacci_preorder_lfi(store: LocalStore | RemoteStore) -> None:
    def exec_id(n: int) -> str:
        return f"test-fib-preorder-lfi-{n}"

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
    assert p.result() == _fib(n)


@pytest.mark.parametrize("store", _promise_storages())
def test_fibonacci_postorder_lfi(store: LocalStore | RemoteStore) -> None:
    def exec_id(n: int) -> str:
        return f"test-fib-postorder-lfi-{n}"

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
    assert p.result() == _fib(n)


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
    assert p.result() == _factorial(n)


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
    assert p.result() == _fib(n)


@pytest.mark.skipif(
    os.getenv("RESONATE_STORE_URL") is None, reason="env variable is not set"
)
def test_golden_device_rfi() -> None:
    group = "test-golden-device-rfi"

    def foo_golden_device_rfi(ctx: Context, n: str) -> Generator[Yieldable, Any, str]:
        p: Promise[str] = yield ctx.rfi(bar_golden_device_rfi, n).options(
            id="bar", send_to=poll(group)
        )
        assert isinstance(p, Promise)
        v: str = yield p
        assert isinstance(v, str)
        return v

    def bar_golden_device_rfi(ctx: Context, n: str) -> str:  # noqa: ARG001
        return n

    resonate = Resonate(
        store=RemoteStore(url=os.environ["RESONATE_STORE_URL"]),
        task_source=Poller("http://localhost:8002", group=group),
    )
    resonate.register(foo_golden_device_rfi)
    resonate.register(bar_golden_device_rfi)
    p: Handle[str] = resonate.run("foo", foo_golden_device_rfi, "hi")
    assert isinstance(p, Handle)
    assert p.result() == "hi"


@pytest.mark.skipif(
    os.getenv("RESONATE_STORE_URL") is None, reason="env variable is not set"
)
def test_factorial_rfi() -> None:
    group = "test-factorial-rfi"

    def exec_id(n: int) -> str:
        return f"test-factorial-rfi-{n}"

    def factorial_rfi(ctx: Context, n: int) -> Generator[Yieldable, Any, int]:
        if n == 0:
            return 1
        p = yield ctx.rfi(factorial_rfi, n - 1).options(
            id=exec_id(n - 1), send_to=poll(group)
        )
        return n * (yield p)

    resonate = Resonate(
        store=RemoteStore(url=os.environ["RESONATE_STORE_URL"]),
        task_source=Poller("http://localhost:8002", group=group),
    )
    resonate.register(factorial_rfi)
    n = 10
    p: Handle[int] = resonate.run(exec_id(n), factorial_rfi, n)
    assert p.result() == _factorial(n)


@pytest.mark.skipif(
    os.getenv("RESONATE_STORE_URL") is None, reason="env variable is not set"
)
def test_fibonacci_preorder_rfi() -> None:
    group = "test-fibonacci-preorder-rfi"

    def exec_id(n: int) -> str:
        return f"test-fib-preorder-rfi-{n}"

    def fib_rfi(ctx: Context, n: int) -> Generator[Yieldable, Any, int]:
        if n <= 1:
            return n
        p1 = yield ctx.rfi(fib_rfi, n - 1).options(
            id=exec_id(n - 1), send_to=poll(group)
        )
        n1 = yield p1
        p2 = yield ctx.rfi(fib_rfi, n - 2).options(
            id=exec_id(n - 2), send_to=poll(group)
        )
        n2 = yield p2
        return n1 + n2

    resonate = Resonate(
        store=RemoteStore(url=os.environ["RESONATE_STORE_URL"]),
        task_source=Poller("http://localhost:8002", group=group),
    )
    resonate.register(fib_rfi)
    n = 10
    p: Handle[int] = resonate.run(exec_id(n), fib_rfi, n)
    assert p.result() == _fib(n)


@pytest.mark.skip
@pytest.mark.skipif(
    os.getenv("RESONATE_STORE_URL") is None, reason="env variable is not set"
)
def test_fibonacci_postorder_rfi() -> None:
    group = "test-fibonacci-postorder-rfi"

    def exec_id(n: int) -> str:
        return f"test-fib-postorder-rfi-{n}"

    def fib_rfi(ctx: Context, n: int) -> Generator[Yieldable, Any, int]:
        if n <= 1:
            return n
        p1 = yield ctx.rfi(fib_rfi, n - 1).options(
            id=exec_id(n - 1), send_to=poll(group)
        )
        p2 = yield ctx.rfi(fib_rfi, n - 2).options(
            id=exec_id(n - 2), send_to=poll(group)
        )
        n2 = yield p2
        n1 = yield p1
        return n1 + n2

    resonate = Resonate(
        store=RemoteStore(url=os.environ["RESONATE_STORE_URL"]),
        task_source=Poller("http://localhost:8002", group="default"),
    )
    resonate.register(fib_rfi)
    n = 10

    p: Handle[int] = resonate.run(exec_id(n), fib_rfi, n)
    assert p.result() == _fib(n)


@pytest.mark.skipif(
    os.getenv("RESONATE_STORE_URL") is None, reason="env variable is not set"
)
def test_golden_device_rfi_and_lfc() -> None:
    group = "test-golden-device-rfi-and-lfc"

    def foo(ctx: Context, n: str) -> Generator[Yieldable, Any, str]:
        v: str = yield ctx.lfc(bar, n).options(
            id="bar",
            durable=False,
        )
        return v

    def bar(ctx: Context, n: str) -> Generator[Yieldable, Any, str]:
        p: Promise[str] = yield ctx.rfi(baz, n).options(id="baz", send_to=poll(group))
        v: str = yield p
        return v

    def baz(ctx: Context, n: str) -> str:  # noqa: ARG001
        return n

    resonate = Resonate(
        store=RemoteStore(url=os.environ["RESONATE_STORE_URL"]),
        task_source=Poller("http://localhost:8002", group=group),
    )
    resonate.register(foo)
    resonate.register(baz)
    p: Handle[str] = resonate.run("foo", foo, "hi")
    assert p.result() == "hi"


@pytest.mark.skip
@pytest.mark.skipif(
    os.getenv("RESONATE_STORE_URL") is None, reason="env variable is not set"
)
def test_fibonacci_full_random() -> None:
    rand = random.Random(444)  # noqa: S311

    def exec_id(n: int) -> str:
        return f"fib({n})"

    def fib(ctx: Context, n: int) -> Generator[Yieldable, Any, int]:
        if n <= 1:
            return n

        if rand.choice((True, False)):
            p1 = yield ctx.rfi(fib, n - 1).options(
                id=exec_id(n - 1),
            )
        else:
            p1 = yield ctx.lfi(fib, n - 1).options(
                id=exec_id(n - 1),
                durable=rand.choice((True, False)),
            )

        if rand.choice((True, False)):
            p2 = yield ctx.rfi(fib, n - 2).options(
                id=exec_id(n - 2),
            )
        else:
            p2 = yield ctx.lfi(fib, n - 2).options(
                id=exec_id(n - 2),
                durable=rand.choice((True, False)),
            )

        promises = [p1, p2]
        total: int = 0
        while promises:
            idx = rand.randint(0, len(promises) - 1)
            p = promises.pop(idx)
            v = yield p
            total += v

        return total

    resonate = Resonate(store=RemoteStore(url=os.environ["RESONATE_STORE_URL"]))
    resonate.register(fib)
    n = 3
    p: Handle[int] = resonate.run(exec_id(n), fib, n)
    assert p.result() == _fib(n)


@pytest.mark.skipif(
    os.getenv("RESONATE_STORE_URL") is None, reason="env variable is not set"
)
def test_golden_device_rfi_and_lfc_with_decorator() -> None:
    group = "test-golden-device-rfi-and-lfc-with-decorator"

    resonate = Resonate(
        store=RemoteStore(url=os.environ["RESONATE_STORE_URL"]),
        task_source=Poller("http://localhost:8002", group=group),
    )

    @resonate.register
    def foo(ctx: Context, n: str) -> Generator[Yieldable, Any, str]:
        v: str = yield ctx.lfc(bar, n).options(
            id="bar",
            durable=False,
        )
        return v

    def bar(ctx: Context, n: str) -> Generator[Yieldable, Any, str]:
        p: Promise[str] = yield ctx.rfi(baz, n).options(id="baz", send_to=poll(group))
        v: str = yield p
        return v

    @resonate.register
    def baz(ctx: Context, n: str) -> str:  # noqa: ARG001
        return n

    p: Handle[str] = foo.run("foo", n="hi")
    assert p.result() == "hi"
