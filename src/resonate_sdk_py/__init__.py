from __future__ import annotations

from typing import Callable

from mashumaro.mixins.orjson import DataClassORJSONMixin
from typing_extensions import ParamSpec, TypeVar

T = TypeVar("T")
P = ParamSpec("P")


class InputParams(DataClassORJSONMixin): ...


In = TypeVar("In", bound=InputParams)


class OutputParams(DataClassORJSONMixin): ...


Out = TypeVar("Out", bound=OutputParams)


class Context:
    def run(self, fn: Callable[[Context, In], Out], params: In) -> Out:
        _ = params.to_json()
        fn_result = fn(self, params)
        _ = fn_result.to_json()
        return fn_result
