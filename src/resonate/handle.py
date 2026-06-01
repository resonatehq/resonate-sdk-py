from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

import msgspec

from resonate.error import (
    ApplicationError,
    TimeoutError as ResonateTimeoutError,
)
from resonate.types import PromiseState, Value

if TYPE_CHECKING:
    from resonate.codec import Codec


class PromiseResult(msgspec.Struct, frozen=True, kw_only=True):
    """The settled state of a durable promise, delivered over the result channel.

    Mirrors Rust's crate-internal ``PromiseResult``: a (folded) ``PromiseState``
    and the raw, still-encoded wire ``value``. Decoding the value is deferred to
    :class:`~resonate.codec.Codec` when the holder reads the result.
    """

    state: PromiseState
    value: Value


class Subscription:
    """A settle-once signal shared by every handle to one promise id.

    Mirrors Go's ``subscription`` (a ``done`` channel + a ``result`` slot) and
    supersedes the earlier ``asyncio.get_running_loop().create_future()``: an
    :class:`asyncio.Event` is the idiomatic high-level primitive for "wait until
    something external happens", needs no loop reference at construction, and a
    single ``settle`` wakes every waiter at once. The settle is driven from the
    ``unblock`` push handler (or an already-settled listener registration).
    """

    def __init__(self) -> None:
        self._done = asyncio.Event()
        self._result: PromiseResult | None = None

    def settle(self, result: PromiseResult) -> None:
        """Record the settled result and wake all waiters. Idempotent."""
        if not self._done.is_set():
            self._result = result
            self._done.set()

    def settled(self) -> bool:
        """Whether the subscription has been settled (non-blocking)."""
        return self._done.is_set()

    async def wait(self) -> PromiseResult:
        """Block until settled, then return the result.

        A woken-but-resultless subscription (the analogue of tokio's "all
        senders dropped") raises, mirroring Go's "subscription closed without a
        settled result".
        """
        await self._done.wait()
        if self._result is None:
            msg = "promise channel closed"
            raise ApplicationError(msg)
        return self._result


class ResonateHandle[T]:
    """A handle to a durable promise, returned from ``run``, ``rpc``, and ``get``.

    Allows non-blocking observation and eventual awaiting of a durable promise.
    The target type ``type_`` stands in for Rust's ``PhantomData<T>``: Python
    cannot resolve the type variable at runtime, so the decode type is passed
    explicitly at construction.

    The promise id is not exposed synchronously. For ``run``/``rpc`` the durable
    promise is created in the background, so the id is only meaningful once that
    creation round-trip has confirmed; :meth:`id` awaits ``created`` before
    handing it back, mirroring :class:`~resonate.context.ResonateFuture.id`. The
    ``get`` path passes an already-set event, since it only builds a handle after
    the promise has been confirmed to exist.
    """

    def __init__(
        self,
        id: str,
        sub: Subscription,
        codec: Codec,
        type_: type[T],
        created: asyncio.Event,
    ) -> None:
        self._id = id
        self._sub = sub
        self._codec = codec
        self._type = type_
        self._created = created

    async def id(self) -> str:
        """Return the durable promise id, once its creation is confirmed.

        Waits on the creation event so a caller never observes an id before the
        backing durable promise is known to exist. Mirrors
        :meth:`~resonate.context.ResonateFuture.id`.
        """
        await self._created.wait()
        return self._id

    async def result(self) -> T:
        """Block until the promise completes, returning the result or raising."""
        result = await self._sub.wait()
        return self._decode_result(result)

    def done(self) -> bool:
        """Check if the promise is done (non-blocking).

        Rust's ``done`` is ``async`` only to lock the receiver; the single-threaded
        asyncio port needs no lock, so this mirrors ``recv`` in being synchronous.
        """
        return self._sub.settled()

    def _decode_result(self, result: PromiseResult) -> T:
        """Decode a :class:`PromiseResult` into the final ``T`` or raise.

        The wire ``value`` crosses the durability boundary through the
        :class:`~resonate.codec.Codec` -- the sole owner of that decode. This
        method only maps the settled state onto the decoded value (coerced to
        ``T``) or the matching error. The coercion mirrors Rust's
        ``serde_json::from_value::<T>`` in ``handle.rs``: type-shaping an
        already-decoded value, distinct from the codec's serialization work.
        """
        match result.state:
            case "resolved":
                return self._codec.convert(self._codec.decode(result.value), self._type)
            case "rejected":
                raise self._codec.decode_error(result.value)
            case "rejected_canceled":
                msg = "Promise canceled"
                raise ApplicationError(msg)
            case "rejected_timedout":
                raise ResonateTimeoutError
            case "pending":
                msg = "promise still pending"
                raise ApplicationError(msg)
