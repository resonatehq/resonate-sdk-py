from __future__ import annotations

import asyncio

import pytest

from resonate.error import ApplicationError, SuspendedError
from resonate.futures import DurableFuture, RemoteFuture

# -- DurableFuture ------------------------------------------------------------


@pytest.mark.asyncio
async def test_durable_future_completed_via_await() -> None:
    future: DurableFuture[int] = DurableFuture.resolved(42)
    assert await future == 42


@pytest.mark.asyncio
async def test_durable_future_failed_via_await() -> None:
    future: DurableFuture[int] = DurableFuture.rejected(ApplicationError("boom"))
    with pytest.raises(ApplicationError):
        await future


@pytest.mark.asyncio
async def test_durable_future_pending_resolves_via_await() -> None:
    receiver: asyncio.Future[str] = asyncio.get_running_loop().create_future()
    future: DurableFuture[str] = DurableFuture.pending("test-id", receiver)

    receiver.set_result("hello")
    assert await future == "hello"


@pytest.mark.asyncio
async def test_durable_future_pending_error_via_await() -> None:
    receiver: asyncio.Future[int] = asyncio.get_running_loop().create_future()
    future: DurableFuture[int] = DurableFuture.pending("test-id", receiver)

    receiver.set_exception(ApplicationError("task failed"))
    with pytest.raises(ApplicationError):
        await future


@pytest.mark.asyncio
async def test_durable_future_pending_suspended_via_await() -> None:
    receiver: asyncio.Future[int] = asyncio.get_running_loop().create_future()
    future: DurableFuture[int] = DurableFuture.pending("test-id", receiver)

    receiver.set_exception(SuspendedError())
    with pytest.raises(SuspendedError):
        await future


# -- RemoteFuture -------------------------------------------------------------


@pytest.mark.asyncio
async def test_remote_future_completed_via_await() -> None:
    future: RemoteFuture[str] = RemoteFuture.resolved("remote-value")
    assert await future == "remote-value"


@pytest.mark.asyncio
async def test_remote_future_failed_via_await() -> None:
    future: RemoteFuture[int] = RemoteFuture.rejected(ApplicationError("remote error"))
    with pytest.raises(ApplicationError):
        await future


@pytest.mark.asyncio
async def test_remote_future_pending_returns_suspended_via_await() -> None:
    future: RemoteFuture[int] = RemoteFuture.pending()
    with pytest.raises(SuspendedError):
        await future
