"""Behaviour tests for :mod:`resonate.promises`.

Mirrors the ``#[cfg(test)]`` module in ``promises.rs`` (same test names, same
cases). The Rust tests drive the sub-clients through ``Resonate::local()``; the
``Resonate`` root is not yet ported, so here the :class:`Promises` /
:class:`Schedules` clients are built directly over a real :class:`Sender` +
:class:`Transport` + :class:`LocalNetwork` -- the same wiring
``Resonate::local()`` performs -- with a ``Codec(NoopEncryptor())``.
"""

from __future__ import annotations

import pytest

from resonate.codec import Codec, NoopEncryptor
from resonate.error import ServerError
from resonate.network import LocalNetwork
from resonate.promises import Promises, Schedules
from resonate.send import Sender
from resonate.transport import Transport
from resonate.types import Value

I64_MAX = 2**63 - 1


def _local() -> tuple[Promises, Schedules]:
    """Build promise/schedule clients sharing one local network, like ``Resonate::local()``."""
    net = LocalNetwork(pid="default", group="default")
    sender = Sender(Transport(net), None)
    codec = Codec(NoopEncryptor())
    return Promises(sender, codec), Schedules(sender, codec)


@pytest.mark.asyncio
async def test_promises_create_get_resolve_roundtrip() -> None:
    promises, _ = _local()

    created = await promises.create(
        "unit-p1", I64_MAX, Value.from_serializable({"x": 1}), {}
    )
    assert created.id == "unit-p1"
    assert created.state == "pending"

    fetched = await promises.get("unit-p1")
    assert fetched.id == "unit-p1"

    settled = await promises.resolve(
        "unit-p1", Value.from_serializable({"result": "ok"})
    )
    assert settled.state == "resolved"

    after = await promises.get("unit-p1")
    assert after.state == "resolved"


@pytest.mark.asyncio
async def test_promises_get_missing_returns_server_error() -> None:
    promises, _ = _local()
    with pytest.raises(ServerError):
        await promises.get("does-not-exist")


@pytest.mark.asyncio
async def test_schedules_create_get_delete_roundtrip() -> None:
    _, schedules = _local()

    created = await schedules.create(
        "unit-s1", "*/5 * * * *", "unit-s1.{{.timestamp}}", 60_000, Value()
    )
    assert created.id == "unit-s1"
    assert created.cron == "*/5 * * * *"

    fetched = await schedules.get("unit-s1")
    assert fetched.id == "unit-s1"

    await schedules.delete("unit-s1")


@pytest.mark.asyncio
async def test_schedules_delete_missing_returns_server_error() -> None:
    _, schedules = _local()
    with pytest.raises(ServerError):
        await schedules.delete("no-such-schedule")


@pytest.mark.asyncio
async def test_schedules_search_returns_record() -> None:
    _, schedules = _local()

    await schedules.create(
        "unit-s-search",
        "* * * * *",
        "unit-s-search.{{.timestamp}}",
        60_000,
        Value(),
    )

    result = await schedules.search(None, 100, None)
    assert any(s.id == "unit-s-search" for s in result.schedules)
