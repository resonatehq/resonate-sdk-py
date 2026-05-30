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

    def encode(self, value: Any) -> Value:
        """Encode a serializable value into the wire format."""
        if value is None:
            return Value(headers=None, data=None)

        try:
            json_bytes = msgspec.json.encode(value)
        except (TypeError, ValueError, msgspec.MsgspecError) as exc:
            raise SerializationError(exc) from exc

        encrypted = self.encryptor.encrypt(json_bytes)
        return Value(headers=None, data=base64.b64encode(encrypted).decode("ascii"))

    def decode[T](self, value: Value, type: type[T]) -> T | None:
        """Decode a wire-format value back into ``type``.

        Returns ``None`` for empty or ``null`` ``data``.
        """
        if not value.data:
            return None

        return self.decode_base64_str(value.data, type)

    def decode_base64_str[T](self, s: str, type: type[T]) -> T | None:
        """Decode a base64-encoded, encrypted JSON string directly into ``type``."""
        try:
            data = base64.b64decode(s, validate=True)
        except (binascii.Error, ValueError) as exc:
            raise Base64DecodeError(exc) from exc

        try:
            return msgspec.json.decode(self.encryptor.decrypt(data), type=type)
        except msgspec.MsgspecError as exc:
            raise SerializationError(exc) from exc

    def decode_promise(self, promise: PromiseRecord) -> PromiseRecord:
        """Decode a promise's ``param`` and ``value`` fields."""
        decoded_param_data = self.decode(promise.param, Any)
        decoded_value_data = self.decode(promise.value, Any)
        return PromiseRecord(
            id=promise.id,
            state=promise.state,
            timeout_at=promise.timeout_at,
            param=Value(headers=promise.param.headers, data=decoded_param_data),
            value=Value(headers=promise.value.headers, data=decoded_value_data),
            tags=promise.tags,
            created_at=promise.created_at,
            settled_at=promise.settled_at,
        )


def encode_error(err: Exception) -> dict[str, str]:
    """Encode an error for durable storage."""
    return {"__type": "error", "message": str(err)}


def deserialize_error(value: Any) -> ApplicationError:
    """Deserialize an error value from a rejected promise."""
    if isinstance(value, dict):
        msg = value.get("message")
        if isinstance(msg, str):
            return ApplicationError(msg)

    # msgspec.json.encode outputs bytes, so .decode() is necessary here to format the string
    rendered = msgspec.json.encode(value).decode("utf-8")
    return ApplicationError(f"unknown error: {rendered}")
