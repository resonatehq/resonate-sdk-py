from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Any, Self

import msgspec

from resonate.error import JoinError, ResonateError, SuspendedError

if TYPE_CHECKING:
    from collections.abc import Generator

# Mirrors Rust's `tracing::info!(target: "resonate::validation", ...)`: the
# validation harness keys off this logger name rather than the module name.
logger = logging.getLogger("resonate.validation")


# =============================================================================
# DurableFuture -- a handle to an eagerly spawned local durable task
# =============================================================================


class Pending[T](msgspec.Struct, frozen=True, kw_only=True):
    """The task is running -- await ``receiver`` for the typed result."""

    id: str
    receiver: asyncio.Future[T]


#: The inner state of a :class:`DurableFuture`. Mirrors Rust's
#: ``DurableFutureInner``: a resolved value ``T``, a cached :class:`ResonateError`
#: (rejected), or a :class:`Pending` handle still awaiting its result.
type DurableFutureInner[T] = T | ResonateError | Pending[T]


class DurableFuture[T](msgspec.Struct, frozen=True, kw_only=True):
    """A handle to an eagerly spawned local durable task.

    Created by ``ctx.run(f, args).spawn()``. Awaiting this future returns the
    result once the spawned task completes.
    """

    inner: DurableFutureInner[T]

    @classmethod
    def resolved(cls, value: T) -> Self:
        return cls(inner=value)

    @classmethod
    def rejected(cls, error: ResonateError) -> Self:
        return cls(inner=error)

    @classmethod
    def pending(cls, id: str, receiver: asyncio.Future[T]) -> Self:
        return cls(inner=Pending(id=id, receiver=receiver))

    def __await__(self) -> Generator[Any, Any, T]:
        return self._into_future().__await__()

    async def _into_future(self) -> T:
        inner = self.inner
        if isinstance(inner, ResonateError):
            raise inner
        if isinstance(inner, Pending):
            logger.info("promise_execution_await promise_id=%s", inner.id)
            try:
                return await inner.receiver
            except asyncio.CancelledError:
                msg = f"task {inner.id} was dropped"
                raise JoinError(msg) from None
        return inner


# =============================================================================
# RemoteFuture -- a handle to a remote durable task
# =============================================================================


class RemotePending(msgspec.Struct, frozen=True, kw_only=True):
    """The task is pending -- only another worker can resolve it."""


#: The inner state of a :class:`RemoteFuture`. Mirrors Rust's
#: ``RemoteFutureInner``: a resolved value ``T``, a cached :class:`ResonateError`
#: (rejected), or :class:`RemotePending` (awaiting another worker).
type RemoteFutureInner[T] = T | ResonateError | RemotePending


class RemoteFuture[T](msgspec.Struct, frozen=True, kw_only=True):
    """A handle to a remote durable task.

    Created by ``ctx.rpc("func", args).spawn()``. Awaiting this future returns
    the result once the remote worker resolves the promise.
    """

    inner: RemoteFutureInner[T]

    @classmethod
    def resolved(cls, value: T) -> Self:
        return cls(inner=value)

    @classmethod
    def rejected(cls, error: ResonateError) -> Self:
        return cls(inner=error)

    @classmethod
    def pending(cls) -> Self:
        return cls(inner=RemotePending())

    def __await__(self) -> Generator[Any, Any, T]:
        return self._into_future().__await__()

    async def _into_future(self) -> T:
        inner = self.inner
        if isinstance(inner, ResonateError):
            raise inner
        if isinstance(inner, RemotePending):
            raise SuspendedError
        return inner
