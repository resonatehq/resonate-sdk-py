"""Behaviour tests for :mod:`resonate.handle`.

``handle.rs`` carries no ``#[cfg(test)]`` module, so there is no Rust mirror to
keep in sync; these cases exercise the port's own responsibilities (result
decoding per promise state and the non-blocking ``done`` check) through the
public API, using a real :class:`Codec` round-trip rather than white-box calls.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from resonate.codec import Codec, NoopEncryptor
from resonate.error import ApplicationError, TimeoutError as ResonateTimeoutError
from resonate.handle import PromiseResult, ResonateHandle, Subscription


def _codec() -> Codec:
    return Codec(NoopEncryptor())


def _encoded(codec: Codec, value: Any) -> dict[str, Any]:
    """Build a raw promise ``value`` whose ``data`` is the codec-encoded ``value``."""
    return {"data": codec.encode(value).data}


def _ready(result: PromiseResult) -> Subscription:
    sub = Subscription()
    sub.settle(result)
    return sub


def _created() -> asyncio.Event:
    """Build a pre-set creation event: these handles model created promises."""
    evt = asyncio.Event()
    evt.set()
    return evt


@pytest.mark.asyncio
async def test_result_resolved_decodes_value() -> None:
    codec = _codec()
    result = PromiseResult(state="resolved", value=_encoded(codec, 42))
    handle = ResonateHandle("p1", _ready(result), codec, int, _created())
    assert await handle.result() == 42


@pytest.mark.asyncio
async def test_result_rejected_raises_application_error() -> None:
    codec = _codec()
    value = _encoded(codec, {"message": "boom"})
    result = PromiseResult(state="rejected", value=value)
    handle: ResonateHandle[int] = ResonateHandle(
        "p1", _ready(result), codec, int, _created()
    )
    with pytest.raises(ApplicationError, match="boom"):
        await handle.result()


@pytest.mark.asyncio
async def test_result_rejected_canceled_raises() -> None:
    result = PromiseResult(state="rejected_canceled", value=None)
    handle: ResonateHandle[int] = ResonateHandle(
        "p1", _ready(result), _codec(), int, _created()
    )
    with pytest.raises(ApplicationError, match="Promise canceled"):
        await handle.result()


@pytest.mark.asyncio
async def test_result_rejected_timedout_raises_timeout() -> None:
    result = PromiseResult(state="rejected_timedout", value=None)
    handle: ResonateHandle[int] = ResonateHandle(
        "p1", _ready(result), _codec(), int, _created()
    )
    with pytest.raises(ResonateTimeoutError):
        await handle.result()


@pytest.mark.asyncio
async def test_result_pending_raises() -> None:
    result = PromiseResult(state="pending", value=None)
    handle: ResonateHandle[int] = ResonateHandle(
        "p1", _ready(result), _codec(), int, _created()
    )
    with pytest.raises(ApplicationError, match="promise still pending"):
        await handle.result()


@pytest.mark.asyncio
async def test_result_channel_closed_raises() -> None:
    # A subscription woken without a settled result (the analogue of tokio's
    # "all senders dropped") surfaces as "promise channel closed".
    sub = Subscription()
    sub._done.set()
    handle: ResonateHandle[int] = ResonateHandle("p1", sub, _codec(), int, _created())
    with pytest.raises(ApplicationError, match="promise channel closed"):
        await handle.result()


@pytest.mark.asyncio
async def test_result_empty_data_decodes_to_none() -> None:
    result = PromiseResult(state="resolved", value={"data": ""})
    handle: ResonateHandle[Any] = ResonateHandle(
        "p1", _ready(result), _codec(), Any, _created()
    )
    assert await handle.result() is None


@pytest.mark.asyncio
async def test_result_non_string_data_passed_through() -> None:
    result = PromiseResult(state="resolved", value={"data": {"x": 1}})
    handle: ResonateHandle[Any] = ResonateHandle(
        "p1", _ready(result), _codec(), Any, _created()
    )
    assert await handle.result() == {"x": 1}


@pytest.mark.asyncio
async def test_result_value_without_data_key_passed_through() -> None:
    result = PromiseResult(state="resolved", value={"x": 1})
    handle: ResonateHandle[Any] = ResonateHandle(
        "p1", _ready(result), _codec(), Any, _created()
    )
    assert await handle.result() == {"x": 1}


@pytest.mark.asyncio
async def test_result_non_object_value_passed_through() -> None:
    result = PromiseResult(state="resolved", value=7)
    handle = ResonateHandle("p1", _ready(result), _codec(), int, _created())
    assert await handle.result() == 7


@pytest.mark.asyncio
async def test_done_reflects_channel_state() -> None:
    sub = Subscription()
    handle: ResonateHandle[int] = ResonateHandle("p1", sub, _codec(), int, _created())
    assert handle.done() is False
    sub.settle(PromiseResult(state="resolved", value={"data": ""}))
    assert handle.done() is True


@pytest.mark.asyncio
async def test_id_blocks_until_creation_confirmed() -> None:
    # The id is not exposed until the creation event fires: until then a caller
    # awaiting it stays blocked, mirroring ``ResonateFuture.id``.
    created = asyncio.Event()
    handle = ResonateHandle(
        "p1",
        _ready(PromiseResult(state="pending", value=None)),
        _codec(),
        int,
        created,
    )
    id_task = asyncio.create_task(handle.id())
    await asyncio.sleep(0)
    assert not id_task.done()
    created.set()
    assert await id_task == "p1"
