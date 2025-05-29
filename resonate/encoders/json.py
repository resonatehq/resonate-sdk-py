from __future__ import annotations

import json
from typing import Any


class JsonEncoder:
    def encode(self, obj: Any) -> str | None:
        if obj is None:
            return None

        return json.dumps(obj, default=_encode_exception)

    def decode(self, obj: str | None) -> Any:
        if obj is None:
            return None

        return json.loads(obj, object_hook=_decode_exception)


def _encode_exception(obj: Any) -> dict[str, Any]:
    if isinstance(obj, BaseException):
        return {"__exception__": list(obj.args)}
    msg = f"Object of type {type(obj)} is not JSON serializable"
    raise TypeError(msg)


def _decode_exception(obj: dict[str, Any]) -> Any:
    if "__exception__" in obj and isinstance(obj["__exception__"], list):
        return Exception(*obj["__exception__"])
    return obj
