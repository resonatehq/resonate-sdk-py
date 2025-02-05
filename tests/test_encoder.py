from __future__ import annotations

from typing import Any

import pytest

from resonate_sdk.encoder import (
    Base64Encoder,
    JsonAndExceptionEncoder,
)


@pytest.mark.parametrize(
    "value",
    ["12321", "hi", "by"],
)
def test_base64_enconder(value: str) -> None:
    encoder = Base64Encoder()
    encoded = encoder.encode(value)

    assert value == encoder.decode(encoded)


class CustomError(Exception):
    def __init__(self, name: str) -> None:
        super().__init__(name)


@pytest.mark.parametrize("value", [{"value": 1}, CustomError("abc"), TypeError("HERE")])
def test_json_encoder(value: Any) -> None:
    encoder = JsonAndExceptionEncoder()
    encoded = encoder.encode(value)

    decoded = encoder.decode(encoded)
    if not isinstance(decoded, Exception):
        assert value == decoded
    else:
        assert isinstance(decoded, type(value))
        assert decoded.args == value.args
