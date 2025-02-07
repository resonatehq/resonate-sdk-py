from __future__ import annotations

from typing import Any

import pytest

from resonate_sdk.encoder import (
    Base64Encoder,
    JsonAndExceptionEncoder,
)


@pytest.mark.parametrize(
    "value",
    ["12321", "hi", "by", None],
)
def test_base64_enconder(value: str) -> None:
    encoder = Base64Encoder()
    encoded = encoder.encode(value)

    assert value == encoder.decode(encoded)


class CustomError(Exception):
    def __init__(self, name: str) -> None:
        super().__init__(name)


@pytest.mark.parametrize(
    "value", [{"value": 1}, CustomError("abc"), TypeError("HERE"), None]
)
def test_json_encoder(value: Any) -> None:
    encoder = JsonAndExceptionEncoder()
    encoded = encoder.encode(value)

    match encoder.decode(encoded):
        case Exception() as decoded:
            assert isinstance(decoded, type(value))
            assert decoded.args == value.args
        case _ as decoded:
            assert value == decoded
