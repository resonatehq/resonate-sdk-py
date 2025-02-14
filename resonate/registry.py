from __future__ import annotations

from collections.abc import Callable, Generator
from concurrent.futures import Future
from typing import Any, Concatenate, overload

from resonate.models.commands import Invoke, Listen
from resonate.models.context import Contextual
from resonate.models.enqueueable import Enqueueable
from resonate.models.handle import Handle

#####################################################################
## Registry
#####################################################################


class Registry:
    def __init__(self) -> None:
        self._registry: dict[str, Callable] = {}
        self._reverse_registry: dict[Callable, str] = {}

    def add(self, name: str, func: Callable) -> None:
        self._registry[name] = func
        self._reverse_registry[func] = name

    def get(self, name: str) -> Callable:
        return self._registry[name]

    def reverse_lookup(self, func: Callable) -> str:
        return self._reverse_registry[func]


#####################################################################
## Function
#####################################################################


class Function[**P, R]:
    @overload
    def __init__(self, name: str, func: Callable[Concatenate[Contextual, P], Generator[Any, Any, R]], cq: Enqueueable[Invoke | Listen]) -> None: ...
    @overload
    def __init__(self, name: str, func: Callable[P, R], cq: Enqueueable[Invoke | Listen]) -> None: ...
    def __init__(self, name: str, func: Callable[Concatenate[Contextual, P], Generator[Any, Any, R]] | Callable[P, R], cq: Enqueueable[Invoke | Listen]) -> None:
        self.name = name
        self.func = func
        self._cq = cq

    @overload
    def __call__(self, ctx: Contextual, *args: P.args, **kwargs: P.kwargs) -> Generator[Any, Any, R]: ...
    @overload
    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> R: ...
    def __call__(self, *args, **kwargs) -> Generator[Any, Any, R] | R:
        return self.func(*args, **kwargs)

    @property
    def __name__(self) -> str:
        return self.name

    def get(self, id: str) -> Handle[R]:
        future = Future[R]()
        handle = Handle(future)
        self._cq.enqueue(Listen(id), future)

        return handle

    def run(self, id: str, *args: P.args, **kwargs: P.kwargs) -> Handle[R]:
        future = Future[R]()
        handle = Handle(future)
        self._cq.enqueue(Invoke(id, self.name, self.func, args, kwargs), future)

        return handle
