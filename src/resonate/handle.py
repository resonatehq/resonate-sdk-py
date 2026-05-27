from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any, Literal

import msgspec

from resonate.codec import deserialize_error
from resonate.error import (
    ApplicationError,
    SerializationError,
    TimeoutError as ResonateTimeoutError,
)

if TYPE_CHECKING:
    from resonate.codec import Codec


class PromiseResult(msgspec.Struct, frozen=True, kw_only=True):
    """The settled state of a durable promise, delivered over the result channel.

    Mirrors Rust's crate-internal ``PromiseResult``: a (folded) ``PromiseState``
    and the raw, still-encoded ``value``.
    """

    state: Literal[
        "pending",
        "resolved",
        "rejected",
        "rejected_canceled",
        "rejected_timedout",
    ]
    value: Any


class ResonateHandle[T]:
    """A handle to a durable promise, returned from ``run``, ``rpc``, and ``get``.

    Allows non-blocking observation and eventual awaiting of a durable promise.
    The target type ``type_`` stands in for Rust's ``PhantomData<T>``: Python
    cannot resolve the type variable at runtime, so the decode type is passed
    explicitly at construction.
    """

    def __init__(
        self,
        id: str,
        rx: asyncio.Future[PromiseResult],
        codec: Codec,
        type_: type[T],
    ) -> None:
        self.id = id
        self._rx = rx
        self._codec = codec
        self._type = type_

    def __repr__(self) -> str:
        return f"ResonateHandle(id={self.id!r})"

    async def result(self) -> T:
        """Block until the promise completes, returning the result or raising."""
        try:
            result = await self._rx
        except asyncio.CancelledError:
            msg = "promise channel closed"
            raise ApplicationError(msg) from None
        return self._decode_result(result)

    def done(self) -> bool:
        """Check if the promise is done (non-blocking).

        Rust's ``done`` is ``async`` only to lock the receiver; the single-threaded
        asyncio port needs no lock, so this mirrors ``recv`` in being synchronous.
        """
        return self._rx.done()

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
