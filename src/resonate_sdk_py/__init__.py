from __future__ import annotations

from typing import Callable

from typing_extensions import Concatenate, ParamSpec, TypeVar

T = TypeVar("T")
P = ParamSpec("P")


class Context:
    def run(
        self,
        fn: Callable[Concatenate[Context, P], T],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> T:
        return fn(self, *args, **kwargs)
