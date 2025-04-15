from __future__ import annotations

import os
import time
from typing import TYPE_CHECKING, Any

import pytest

from resonate.resonate import Resonate
from resonate.stores.local import LocalStore
from resonate.stores.remote import RemoteStore
from resonate.task_sources.poller import Poller

if TYPE_CHECKING:
    from collections.abc import Generator

    from resonate.models.context import Yieldable
    from resonate.models.message_source import MessageSource
    from resonate.models.store import Store
    from resonate.resonate import Context


def foo_lfi(ctx: Context) -> Generator:
    p = yield ctx.lfi(bar_lfi)
    v = yield p
    return v


def bar_lfi(ctx: Context) -> Generator:
    p = yield ctx.lfi(baz)
    v = yield p
    return v


def foo_rfi(ctx: Context) -> Generator:
    p = yield ctx.rfi(bar_rfi)
    v = yield p
    return v


def bar_rfi(ctx: Context) -> Generator:
    p = yield ctx.rfi(baz)
    v = yield p
    return v


def baz(ctx: Context) -> str:
    return "baz"


def fib_lfi(ctx: Context, n: int) -> Generator[Any, Any, int]:
    if n <= 1:
        return n

    p1 = yield ctx.lfi(fib_lfi, n - 1).options(id=f"fli{n - 1}")
    p2 = yield ctx.lfi(fib_lfi, n - 2).options(id=f"fli{n - 2}")

    v1 = yield p1
    v2 = yield p2

    return v1 + v2


def fib_lfc(ctx: Context, n: int) -> Generator[Any, Any, int]:
    if n <= 1:
        return n

    v1 = yield ctx.lfc(fib_lfc, n - 1).options(id=f"flc{n - 1}")
    v2 = yield ctx.lfc(fib_lfc, n - 2).options(id=f"flc{n - 2}")

    return v1 + v2


def fib_rfi(ctx: Context, n: int) -> Generator[Any, Any, int]:
    if n <= 1:
        return n

    p1 = yield ctx.rfi(fib_rfi, n - 1).options(id=f"fri{n - 1}")
    p2 = yield ctx.rfi(fib_rfi, n - 2).options(id=f"fri{n - 2}")

    v1 = yield p1
    v2 = yield p2

    return v1 + v2


def fib_rfc(ctx: Context, n: int) -> Generator[Any, Any, int]:
    if n <= 1:
        return n

    v1 = yield ctx.rfc(fib_rfc, n - 1).options(id=f"frc{n - 1}")
    v2 = yield ctx.rfc(fib_rfc, n - 2).options(id=f"frc{n - 2}")

    return v1 + v2


def sleep(ctx: Context) -> Generator[Yieldable, Any, int]:
    yield ctx.sleep(0)
    return 1


def get_stores_config() -> list[tuple[Store, MessageSource]]:
    local_store = LocalStore()
    local_message_source = local_store.as_msg_source()
    stores: list[tuple[Store, MessageSource]] = [(local_store, local_message_source)]

    if "RESONATE_STORE_URL" in os.environ and "RESONATE_MSG_SRC_URL" in os.environ:
        remote_store = RemoteStore(url=os.environ["RESONATE_STORE_URL"])
        remote_msg_source = Poller(os.environ["RESONATE_MSG_SRC_URL"])
        stores.append((remote_store, remote_msg_source))

    return stores


@pytest.fixture(params=get_stores_config(), ids=lambda s: f"{s[0]}", scope="session")
def resonate_instance(request: pytest.FixtureRequest) -> Generator[Resonate, None, None]:
    store, msg_src = request.param
    resonate = Resonate(store=store, message_source=msg_src)
    resonate.register(foo_lfi)
    resonate.register(bar_lfi)
    resonate.register(foo_rfi)
    resonate.register(bar_rfi)
    resonate.register(baz)
    resonate.register(fib_lfi)
    resonate.register(fib_lfc)
    resonate.register(fib_rfi)
    resonate.register(fib_rfc)
    resonate.register(sleep)
    resonate.start()
    yield resonate
    resonate.stop()


def test_basic_lfi_bridge(resonate_instance: Resonate) -> None:
    timestamp = int(time.time())
    handle = resonate_instance.run(f"foo-lfi-{timestamp}", foo_lfi)
    assert handle.result() == "baz"


def test_basic_rfi_bridge(resonate_instance: Resonate) -> None:
    timestamp = int(time.time())
    handle = resonate_instance.run(f"foo-rfi-{timestamp}", foo_rfi)
    assert handle.result() == "baz"


def test_fib_lfi(resonate_instance: Resonate) -> None:
    timestamp = int(time.time())
    handle = resonate_instance.run(f"fib_lfi-{timestamp}", fib_lfi, 10)
    fib_10 = 55
    assert handle.result() == fib_10


def test_fib_rfi(resonate_instance: Resonate) -> None:
    timestamp = int(time.time())
    handle = resonate_instance.run(f"fib_rfi-{timestamp}", fib_rfi, 10)
    fib_10 = 55
    assert handle.result() == fib_10


def test_fib_lfc(resonate_instance: Resonate) -> None:
    timestamp = int(time.time())
    handle = resonate_instance.run(f"fib_lfc-{timestamp}", fib_lfc, 10)
    fib_10 = 55
    assert handle.result() == fib_10


def test_fib_rfc(resonate_instance: Resonate) -> None:
    timestamp = int(time.time())
    handle = resonate_instance.run(f"fib_rfc-{timestamp}", fib_rfc, 10)
    fib_10 = 55
    assert handle.result() == fib_10


def test_sleep(resonate_instance: Resonate) -> None:
    timestamp = int(time.time())
    handle = resonate_instance.run(f"sleep-{timestamp}", sleep)
    assert handle.result() == 1
