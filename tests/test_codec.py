from __future__ import annotations

import base64
from typing import Any

import msgspec
import pytest

from resonate.codec import Codec, NoopEncryptor, encode_error
from resonate.error import ApplicationError, ResonateError
from resonate.types import PromiseRecord, Value


def codec() -> Codec:
    return Codec(NoopEncryptor())


# -- encode/decode roundtrip for primitives -----------------------------------


def test_roundtrip_integer() -> None:
    c = codec()
    encoded = c.encode(42)
    assert c.decode(encoded, int) == 42


def test_roundtrip_string() -> None:
    c = codec()
    encoded = c.encode("hello")
    assert c.decode(encoded, str) == "hello"


def test_decode_base64_str_roundtrip() -> None:
    c = codec()
    encoded = c.encode({"x": 1})
    b64_str = encoded.data
    assert isinstance(b64_str, str)
    assert c.decode_base64_str(b64_str, Any) == {"x": 1}


def test_decode_base64_str_empty_returns_none() -> None:
    c = codec()
    assert c.decode_base64_str("", Any) is None


def test_roundtrip_bool() -> None:
    c = codec()
    encoded = c.encode(value=True)
    assert c.decode(encoded, bool) is True


# -- encode/decode roundtrip for objects --------------------------------------


def test_roundtrip_object() -> None:
    c = codec()
    obj = {"func": "f", "args": [1, "two"]}
    encoded = c.encode(obj)
    assert c.decode(encoded, Any) == obj


# -- encode/decode roundtrip for arrays ---------------------------------------


def test_roundtrip_array() -> None:
    c = codec()
    arr = [1, 2, 3]
    encoded = c.encode(arr)
    assert c.decode(encoded, Any) == arr


# -- encode None/null produces empty data -------------------------------------


def test_encode_null_produces_empty_data() -> None:
    c = codec()
    encoded = c.encode(None)
    assert encoded.data == ""
    assert c.decode(encoded, Any) is None


# -- encode produces base64 string --------------------------------------------


def test_encode_produces_valid_base64() -> None:
    c = codec()
    encoded = c.encode("hello")
    data_str = encoded.data
    assert isinstance(data_str, str)
    assert Codec.is_valid_base64(data_str)

    # Decoding base64 should produce valid JSON.
    msgspec.json.decode(base64.b64decode(data_str, validate=True))


# -- decode_promise decodes both param and value ------------------------------


def test_decode_promise_decodes_param_and_value() -> None:
    c = codec()
    param_encoded = c.encode({"func": "f"})
    value_encoded = c.encode({"result": 42})

    record = PromiseRecord(
        id="test",
        state="resolved",
        timeout_at=0,
        param=param_encoded,
        value=value_encoded,
        tags={},
        created_at=0,
        settled_at=1,
    )

    decoded = c.decode_promise(record)
    assert decoded.param.data == {"func": "f"}
    assert decoded.value.data == {"result": 42}


# -- decode invalid base64 returns error --------------------------------------


def test_decode_invalid_base64_returns_error() -> None:
    c = codec()
    bad_value = Value(headers=None, data="not-base64!!!")
    with pytest.raises(ResonateError):
        c.decode(bad_value, Any)


# -- encode error produces correct shape --------------------------------------


def test_encode_error_produces_correct_shape() -> None:
    err = ApplicationError("boom")
    encoded = encode_error(err)
    assert encoded["__type"] == "error"
    assert encoded["message"] == "application error: boom"
