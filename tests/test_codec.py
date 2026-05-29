from __future__ import annotations

import dataclasses
import enum
from typing import Any, NamedTuple, TypedDict

import attrs
import msgspec
import pydantic
import pytest

from resonate.codec import Codec, NoopEncryptor, encode_error
from resonate.error import ApplicationError, ResonateError, SerializationError
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
    assert encoded["message"] == "boom"


# =============================================================================
# User-defined struct styles round-trip through the codec
#
# Users declare the values they pass as durable-function arguments and return
# as results in many different ways. The codec serializes via
# ``msgspec.to_builtins`` and deserializes via ``msgspec.json.decode(type=...)``,
# so a style survives the durability boundary iff msgspec supports it. These
# tests pin which popular styles round-trip -- preserving both value and type --
# and pin that pydantic (which msgspec does NOT support) fails loudly rather
# than silently corrupting data. See also the end-to-end argument-coercion
# variants in ``test_durable.py``.
# =============================================================================


def _roundtrip[T](value: Any, type_: type[T]) -> T | None:
    """Encode then decode ``value`` back into ``type_`` through a real codec."""
    c = codec()
    return c.decode(c.encode(value), type_)


# -- msgspec.Struct (the SDK's own convention) --------------------------------


class MsgspecPoint(msgspec.Struct, frozen=True):
    x: int
    y: int


def test_roundtrip_msgspec_struct() -> None:
    p = MsgspecPoint(x=3, y=4)
    out = _roundtrip(p, MsgspecPoint)
    assert out == p
    assert isinstance(out, MsgspecPoint)


# -- stdlib dataclass ---------------------------------------------------------


@dataclasses.dataclass(frozen=True)
class DataclassPoint:
    x: int
    y: str


def test_roundtrip_dataclass() -> None:
    p = DataclassPoint(x=1, y="a")
    out = _roundtrip(p, DataclassPoint)
    assert out == p
    assert isinstance(out, DataclassPoint)


# -- attrs --------------------------------------------------------------------


@attrs.frozen
class AttrsPoint:
    x: int
    y: str


def test_roundtrip_attrs() -> None:
    p = AttrsPoint(x=1, y="a")
    out = _roundtrip(p, AttrsPoint)
    assert out == p
    assert isinstance(out, AttrsPoint)


# -- typing.NamedTuple (encodes as a JSON array, decodes back to the tuple) ---


class NamedTuplePoint(NamedTuple):
    x: int
    y: str


def test_roundtrip_namedtuple() -> None:
    p = NamedTuplePoint(1, "a")
    # NamedTuple serializes positionally as a tuple (JSON-encoded as an array),
    # not as an object.
    assert msgspec.to_builtins(p) == (1, "a")
    out = _roundtrip(p, NamedTuplePoint)
    assert out == p
    assert isinstance(out, NamedTuplePoint)


# -- typing.TypedDict (decodes back to a plain dict) --------------------------


class TypedDictPoint(TypedDict):
    x: int
    y: str


def test_roundtrip_typeddict() -> None:
    p: TypedDictPoint = {"x": 1, "y": "a"}
    out = _roundtrip(p, TypedDictPoint)
    assert out == {"x": 1, "y": "a"}


# -- enum.Enum / enum.IntEnum -------------------------------------------------


class Color(enum.Enum):
    RED = "red"


class Size(enum.IntEnum):
    LARGE = 2


def test_roundtrip_enum() -> None:
    assert msgspec.to_builtins(Color.RED) == "red"
    out = _roundtrip(Color.RED, Color)
    assert out is Color.RED


def test_roundtrip_int_enum() -> None:
    assert msgspec.to_builtins(Size.LARGE) == 2
    out = _roundtrip(Size.LARGE, Size)
    assert out is Size.LARGE


# -- nested / mixed styles ----------------------------------------------------


@dataclasses.dataclass(frozen=True)
class MixedShape:
    """A dataclass nesting a msgspec.Struct, a collection, and an optional."""

    corner: MsgspecPoint
    label: str | None = None


def test_roundtrip_nested_mixed_styles() -> None:
    shape = MixedShape(corner=MsgspecPoint(x=1, y=2), label="tri")
    out = _roundtrip(shape, MixedShape)
    assert out == shape
    assert isinstance(out, MixedShape)
    assert isinstance(out.corner, MsgspecPoint)


def test_roundtrip_nested_optional_default_none() -> None:
    shape = MixedShape(corner=MsgspecPoint(x=0, y=0))
    out = _roundtrip(shape, MixedShape)
    assert isinstance(out, MixedShape)
    assert out == shape
    assert out.label is None


def test_roundtrip_list_of_dataclasses() -> None:
    points = [DataclassPoint(x=1, y="a"), DataclassPoint(x=2, y="b")]
    out = _roundtrip(points, list[DataclassPoint])
    assert out is not None
    assert out == points
    assert all(isinstance(p, DataclassPoint) for p in out)


# -- pydantic: unsupported by msgspec, must fail loudly -----------------------


class PydanticPoint(pydantic.BaseModel):
    x: int
    y: str


def test_pydantic_encode_is_unsupported() -> None:
    # msgspec.to_builtins cannot encode a pydantic model: the codec must surface
    # a SerializationError rather than silently dropping or mangling the value.
    c = codec()
    with pytest.raises(SerializationError):
        c.encode(PydanticPoint(x=1, y="a"))


def test_pydantic_decode_into_model_is_unsupported() -> None:
    # msgspec cannot construct a pydantic model from builtins either.
    c = codec()
    encoded = c.encode({"x": 1, "y": "a"})
    with pytest.raises(SerializationError):
        c.decode(encoded, PydanticPoint)


def test_pydantic_via_model_dump_roundtrips_as_dict() -> None:
    # The supported escape hatch: dump the model to builtins yourself. The codec
    # then round-trips it as a plain mapping (the SDK never reconstructs the
    # model type for you).
    p = PydanticPoint(x=1, y="a")
    out = _roundtrip(p.model_dump(), Any)
    assert out == {"x": 1, "y": "a"}
