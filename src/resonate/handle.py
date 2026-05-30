from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any

import msgspec

from resonate.codec import deserialize_error
from resonate.error import (
    ApplicationError,
    SerializationError,
    TimeoutError as ResonateTimeoutError,
)

if TYPE_CHECKING:
    from resonate.codec import Codec
    from resonate.types import PromiseState


class PromiseResult(msgspec.Struct, frozen=True, kw_only=True):
    """The settled state of a durable promise, delivered over the result channel.

    Mirrors Rust's crate-internal ``PromiseResult``: a (folded) ``PromiseState``
    and the raw, still-encoded ``value``.
    """

    state: PromiseState
    value: Any


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
        """Decode a :class:`PromiseResult` into the final ``T`` or raise."""
        match result.state:
            case "resolved":
                decoded = self._decode_value(result.value)
                try:
                    return msgspec.convert(decoded, self._type)
                except (TypeError, ValueError, msgspec.MsgspecError) as exc:
                    raise SerializationError(exc) from exc
            case "rejected":
                decoded = self._decode_value(result.value)
                raise deserialize_error(decoded)
            case "rejected_canceled":
                msg = "Promise canceled"
                raise ApplicationError(msg)
            case "rejected_timedout":
                raise ResonateTimeoutError
            case "pending":
                msg = "promise still pending"
                raise ApplicationError(msg)

    def _decode_value(self, value: Any) -> Any:
        """Decode a promise's ``value`` field, which may be base64-encoded."""
        # Try to decode the value.data field through the codec.
        if isinstance(value, dict) and "data" in value:
            data = value["data"]
            if isinstance(data, str):
                # Empty string is the wire-encoding for null/unit values.
                if data == "":
                    return None
                return self._codec.decode_base64_str(data, Any)
            return data
        return value
