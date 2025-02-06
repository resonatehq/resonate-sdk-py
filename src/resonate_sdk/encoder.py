from __future__ import annotations

import base64
import importlib
import json
from abc import ABC, abstractmethod
from typing import Any, final


class IEncoder[In, Out](ABC):
    @abstractmethod
    def encode(self, obj: In) -> Out: ...

    @abstractmethod
    def decode(self, data: Out) -> In: ...


@final
class Base64Encoder(IEncoder[str, str]):
    def encode(self, obj: str) -> str:
        return base64.b64encode(obj.encode()).decode()

    def decode(self, data: str) -> str:
        return base64.b64decode(data).decode()


class JsonAndExceptionEncoder(IEncoder[Any, str]):
    def encode(self, obj: Any) -> str:
        match obj:
            case Exception():
                obj = {
                    "__exception__": True,
                    "type": type(obj).__name__,
                    "module": type(obj).__module__,
                    "args": obj.args,
                }
                return json.dumps(obj)
            case _:
                return json.dumps(obj)

    def decode(self, data: str) -> Any:
        obj: Any = json.loads(data)
        match obj:
            case dict():
                if obj.get("__exception__"):
                    module_name: str = obj.get("module", "builtins")
                    class_name: str = obj["type"]
                    args = tuple(obj.get("args", ()))

                    module = importlib.import_module(module_name)
                    exc_class = getattr(module, class_name)
                    assert issubclass(exc_class, Exception)
                    return exc_class(*args)

        return obj
