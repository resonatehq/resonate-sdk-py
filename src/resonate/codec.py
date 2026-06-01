from __future__ import annotations

import base64
import binascii
from typing import Any, Protocol

import msgspec

from resonate.error import (
    ApplicationError,
    Base64DecodeError,
    SerializationError,
)
from resonate.types import PromiseRecord, Value


class Encryptor(Protocol):
    def encrypt(self, data: bytes) -> bytes:
        """Encrypts the given byte data."""

    def decrypt(self, data: bytes) -> bytes:
        """Decrypts the given byte data."""


class NoopEncryptor:
    """No-op encryptor (passthrough)."""

    def encrypt(self, data: bytes) -> bytes:
        return data

    def decrypt(self, data: bytes) -> bytes:
        return data


class Codec:
    """Handles encoding/decoding of values for the durability boundary.

    Encode: value -> JSON -> encrypt -> base64 -> ``Value { headers, data }``
    Decode: ``Value { headers, data }`` -> base64 -> decrypt -> JSON -> value
    """

    def __init__(self, encryptor: Encryptor) -> None:
        self.encryptor = encryptor

    def encode(self, value: Any | ApplicationError) -> Value:
        """Encode a serializable value into the wire format."""
        if value is None:
            return Value(headers=None, data=None)

        try:
            json_bytes = msgspec.json.encode(
                _encode_error(value) if isinstance(value, ApplicationError) else value
            )
        except (TypeError, ValueError, msgspec.MsgspecError) as exc:
            raise SerializationError(exc) from exc

        encrypted = self.encryptor.encrypt(json_bytes)
        return Value(headers=None, data=base64.b64encode(encrypted).decode("ascii"))

    def decode(self, value: Value) -> Any:
        """Decode a wire-format value across the durability boundary into builtins.

        Crosses the wire (base64 -> decrypt -> JSON) and stops at builtins;
        returns ``None`` for empty or ``null`` ``data``. Reshaping the decoded
        builtins into a concrete type is :meth:`convert`'s job -- mirroring the
        serde split between ``serde_json::from_slice`` and ``from_value::<T>``.
        """
        if not value.data:
            return None

        try:
            data = base64.b64decode(value.data, validate=True)
        except (binascii.Error, ValueError) as exc:
            raise Base64DecodeError(exc) from exc

        try:
            return msgspec.json.decode(self.encryptor.decrypt(data))
        except msgspec.MsgspecError as exc:
            raise SerializationError(exc) from exc

    def convert[T](self, value: Any, type: type[T]) -> T:
        """Coerce an already-decoded value into ``type``.

        Wraps :func:`msgspec.convert` so type-shaping of decoded payloads stays
        inside the codec -- the sole owner of msgspec operations. Distinct from
        :meth:`decode`, which crosses the wire boundary: this only reshapes a
        value already on the Python side (mirrors Rust's
        ``serde_json::from_value::<T>``).
        """
        try:
            return msgspec.convert(value, type)
        except (TypeError, ValueError, msgspec.MsgspecError) as exc:
            raise SerializationError(exc) from exc

    def decode_error(self, value: Value) -> ApplicationError:
        """Decode a rejected promise's wire ``value`` into its originating error.

        Keeps the ``decode`` + error-reconstruction pair behind the codec so the
        durability boundary owns the whole rejected-value path.
        """
        return _deserialize_error(self.decode(value))

    def decode_promise(self, promise: PromiseRecord) -> PromiseRecord:
        """Decode a promise's ``param`` and ``value`` fields in place.

        Only the two ``Value.data`` payloads change; ``msgspec.structs.replace``
        carries every other field (including each ``Value.headers``) through
        untouched, so the record stays robust to new fields.
        """
        return msgspec.structs.replace(
            promise,
            param=msgspec.structs.replace(
                promise.param, data=self.decode(promise.param)
            ),
            value=msgspec.structs.replace(
                promise.value, data=self.decode(promise.value)
            ),
        )


def _encode_error(err: Exception) -> dict[str, str]:
    """Encode an error for durable storage."""
    return {"__type": "error", "message": str(err)}


def _deserialize_error(value: Any) -> ApplicationError:
    """Deserialize an error value from a rejected promise."""
    if isinstance(value, dict):
        msg = value.get("message")
        if isinstance(msg, str):
            return ApplicationError(msg)

    # msgspec.json.encode outputs bytes, so .decode() is necessary here to format the string
    rendered = msgspec.json.encode(value).decode("utf-8")
    return ApplicationError(f"unknown error: {rendered}")


def decode_settled(record: PromiseRecord) -> Any:
    """Map an already-decoded, settled record to its value, raising on rejection.

    Mirrors Go's ``decodeSettled`` / Rust's ``PromiseRecord::as_result``. The
    record's ``value`` has already been decoded by :meth:`Codec.decode_promise`,
    so a resolved payload is returned as-is and any rejected payload is turned
    back into the originating error via :func:`_deserialize_error`. Lives in the
    codec so the durability boundary remains the sole owner of decode; the
    leading underscore marks it codec-internal -- ``context`` is its one
    cross-module caller (idempotent recovery of an already-settled promise).
    """
    match record.state:
        case "resolved":
            return record.value.data
        case "rejected" | "rejected_canceled" | "rejected_timedout":
            raise _deserialize_error(record.value.data)
        case _:
            msg = f"future {record.id} has unexpected state {record.state!r}"
            raise ApplicationError(msg)
