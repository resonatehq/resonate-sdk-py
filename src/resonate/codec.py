from __future__ import annotations

import base64
import binascii
from typing import Any, Protocol

import msgspec

from resonate.error import (
    ApplicationError,
    Base64DecodeError,
    DecodingError,
    ResonateError,
    SerializationError,
    Utf8Error,
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
        """Encode a serializable value into the wire format.

        Mirrors Rust's ``Codec::encode``: a ``null`` value collapses to empty
        ``data`` (the empty string); anything else is JSON-serialized,
        encrypted, and base64-encoded. Raises :class:`SerializationError` if
        ``value`` cannot be serialized, matching ``serde_json::to_value(value)?``.
        """
        try:
            json_val = msgspec.to_builtins(value)
        except (TypeError, ValueError, msgspec.MsgspecError) as exc:
            raise SerializationError(exc) from exc
        if json_val is None:
            return Value(headers=None, data="")
        json_bytes = msgspec.json.encode(json_val)
        encrypted = self.encryptor.encrypt(json_bytes)
        b64 = base64.b64encode(encrypted).decode("ascii")
        return Value(headers=None, data=b64)

    def decode[T](self, value: Value, type: type[T]) -> T | None:
        """Decode a wire-format value back into ``type``.

        Returns ``None`` for empty or ``null`` ``data``; anything that is not a
        string or ``null`` raises :class:`DecodingError`, mirroring Rust's
        ``Codec::decode``.
        """
        match value.data:
            case str() as s if s == "":
                return None
            case str() as s:
                return self.decode_base64_str(s, type)
            case None:
                return None
            case _:
                msg = "expected string or null data"
                raise DecodingError(msg)

    def decode_base64_str[T](self, s: str, type: type[T]) -> T | None:
        """Decode a base64-encoded, encrypted JSON string directly into ``type``.

        Mirrors Rust's ``Codec::decode_base64_str``: an empty string yields
        ``None``; otherwise the string is base64-decoded, decrypted, decoded as
        UTF-8, and parsed as JSON. Each step's failure maps to its mirror error
        (:class:`Base64DecodeError`, :class:`Utf8Error`, :class:`SerializationError`).
        """
        if s == "":
            return None
        try:
            data = base64.b64decode(s, validate=True)
        except (binascii.Error, ValueError) as exc:
            raise Base64DecodeError(exc) from exc
        decrypted = self.encryptor.decrypt(data)
        try:
            json_str = decrypted.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise Utf8Error(exc) from exc
        try:
            return msgspec.json.decode(json_str, type=type)
        except msgspec.MsgspecError as exc:
            raise SerializationError(exc) from exc

    def decode_promise(self, promise: PromiseRecord) -> PromiseRecord:
        """Decode a promise's ``param`` and ``value`` fields.

        Mirrors Rust's ``Codec::decode_promise``: the decoded JSON (or ``null``)
        replaces the base64 ``data`` while the headers are preserved.
        """
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

    def decode_promise_from_json(self, json: Any) -> PromiseRecord:
        """Decode a promise from a raw (parsed) JSON value.

        Parses ``json`` into a :class:`PromiseRecord`, then decodes its
        ``param``/``value`` fields. A parse failure raises :class:`DecodingError`,
        mirroring Rust's ``Codec::decode_promise_from_json``.
        """
        try:
            record = msgspec.convert(json, PromiseRecord)
        except (TypeError, ValueError, msgspec.MsgspecError) as exc:
            msg = f"invalid promise JSON: {exc}"
            raise DecodingError(msg) from exc
        return self.decode_promise(record)

    @staticmethod
    def is_valid_base64(s: str) -> bool:
        """Check if a string is valid base64."""
        try:
            base64.b64decode(s, validate=True)
        except (binascii.Error, ValueError):
            return False
        return True


def encode_error(err: ResonateError) -> dict[str, str]:
    """Encode an error for durable storage."""
    return {"__type": "error", "message": str(err)}


def deserialize_error(value: Any) -> ResonateError:
    """Deserialize an error value from a rejected promise."""
    if isinstance(value, dict):
        msg = value.get("message")
        if isinstance(msg, str):
            return ApplicationError(msg)
    rendered = msgspec.json.encode(value).decode("utf-8")
    return ApplicationError(f"unknown error: {rendered}")
