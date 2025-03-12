from __future__ import annotations

import threading
from concurrent.futures import Future
from typing import TYPE_CHECKING, Any, Concatenate, Self, overload

from resonate.models.commands import Invoke, Listen
from resonate.models.context import Context
from resonate.models.durable_promise import DurablePromise
from resonate.models.handle import Handle
from resonate.models.options import RunOptions

if TYPE_CHECKING:
    from collections.abc import Callable, Generator
    from resonate.models.enqueueable import Enqueueable

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
    def __init__(self, name: str, version: int, func: Callable[Concatenate[Context, P], Generator[Any, Any, R]], cq: Enqueueable[Invoke | Listen]) -> None: ...
    @overload
    def __init__(self, name: str, version: int, func: Callable[Concatenate[Context, P], R], cq: Enqueueable[Invoke | Listen]) -> None: ...
    @overload
    def __init__(self, name: str, version: int, func: Callable[P, R], cq: Enqueueable[Invoke | Listen]) -> None: ...
    def __init__(self, name: str, version: int, func: Callable[Concatenate[Context, P], Generator[Any, Any, R]] | Callable[Concatenate[Context, P], R] | Callable[P, R], cq: Enqueueable[Invoke | Listen]) -> None:
        self.name = name
        self.func = func
        self.version =version
        self._cq = cq

    @overload
    def __call__(self, ctx: Context, *args: P.args, **kwargs: P.kwargs) -> Generator[Any, Any, R]: ...
    @overload
    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> R: ...
    def __call__(self, *args, **kwargs) -> Generator[Any, Any, R] | R:
        return self.func(*args, **kwargs)

    @property
    def __name__(self) -> str:
        return self.name

    def options(self, *, send_to: str = "default", version: int = 1) -> _Container[P, R]:
        return _Container(RunOptions(send_to=send_to, version=version), name=self.name, func=self.func, cq=self._cq)

    def run(self, id: str, *args: P.args, **kwargs: P.kwargs) -> Handle[R]:
        fp, fv = Future[DurablePromise](), Future[R]()
        self._cq.enqueue(Invoke(id, self.name, self.func, args, kwargs), futures=(fp, fv))
        fp.result()
        return Handle(fv)

    def rpc(self, id: str, *args: P.args, **kwargs: P.kwargs) -> Handle[R]:
        fp, fv = Future[DurablePromise](), Future[R]()
        self._cq.enqueue(Invoke(id, self.name, None, args, kwargs, opts={"target": "default"}), futures=(fp, fv))
        fp.result()
        return Handle(fv)


class _Container[**P, R]:
    def __init__(self, opts: RunOptions, name: str, func: Callable[Concatenate[Context, P], Generator[Any, Any, R]] | Callable[Concatenate[Context, P], R] | Callable[P, R], cq: Enqueueable[Invoke | Listen]) -> None:
        self._opts = opts
        self._cq = cq
        self._name =name
        self._func = func

    def run(self, id: str, *args: P.args, **kwargs: P.kwargs) -> Handle[R]:
        fp, fv = Future[DurablePromise](), Future[R]()
        self._cq.enqueue(Invoke(id, self._name, self._func, args, kwargs), futures=(fp, fv))
        fp.result()
        return Handle(fv)

    def rpc(self, id: str, *args: P.args, **kwargs: P.kwargs) -> Handle[R]:
        fp, fv = Future[DurablePromise](), Future[R]()
        self._cq.enqueue(Invoke(id, self._name, None, args, kwargs, opts={"target": self._opts.send_to}), futures=(fp, fv))
        fp.result()
        return Handle(fv)
