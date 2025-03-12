from __future__ import annotations

from concurrent.futures import Future
from typing import TYPE_CHECKING, Any, Callable, Concatenate, overload

from resonate.dependencies import Dependencies
from resonate.models.commands import Invoke, Listen
from resonate.models.durable_promise import DurablePromise
from resonate.models.handle import Handle
from resonate.models.options import RunOptions
from resonate.registry import Function, Registry
from resonate.scheduler import Scheduler

if TYPE_CHECKING:
    from collections.abc import Generator

    from resonate.models.context import Context
    from resonate.models.enqueueable import Enqueueable


class Resonate:
    def __init__(
        self,
        pid: str | None = None,
        registry: Registry | None = None,
        deps: Dependencies | None = None,
    ) -> None:
        self.deps = deps or Dependencies()
        self._registry = registry or Registry()
        self._scheduler = Scheduler(pid=pid, registry=self._registry, deps=self.deps)

    @overload
    def register[**P, R](self, func: Callable[Concatenate[Context, P], Generator[Any, Any, R]], /, *, name: str | None = None, version: int = 1) -> Function[P, R]: ...
    @overload
    def register[**P, R](self, func: Callable[Concatenate[Context, P], R], /, *, name: str | None = None, version: int = 1) -> Function[P, R]: ...
    @overload
    def register[**P, R](self, func: Callable[P, R], /, *, name: str | None = None, version: int = 1) -> Function[P, R]: ...
    @overload
    def register[**P, R](
        self, *, name: str | None = None, version: int = 1
    ) -> Callable[[Callable[P, R] | Callable[Concatenate[Context, P], Generator[Any, Any, R]] | Callable[Concatenate[Context, P], R]], Function[P, R]]: ...
    def register[**P, R](
        self, *args: Any, **kwargs: Any
    ) -> Callable[[Callable[P, R] | Callable[Concatenate[Context, P], Generator[Any, Any, R]] | Callable[Concatenate[Context, P], R]], Function[P, R]] | Function[P, R]:
        name: str | None = kwargs.get("name")
        kwargs.get("version", 1)

        def wrapper(func: Callable) -> Function[P, R]:
            self._registry.add(name or func.__name__, func)
            return Function(name or func.__name__, func, self._scheduler)

        if args and callable(args[0]):
            return wrapper(args[0])

        return wrapper

    def options(self, *, send_to: str = "default", version: int = 1) -> _Container:
        return _Container(RunOptions(send_to=send_to, version=version), registry=self._registry, cq=self._scheduler)

    @overload
    def run[**P, R](self, id: str, func: Callable[Concatenate[Context, P], Generator[Any, Any, R]], *args: P.args, **kwargs: P.kwargs) -> Handle[R]: ...
    @overload
    def run[**P, R](self, id: str, func: Callable[Concatenate[Context, P], R], *args: P.args, **kwargs: P.kwargs) -> Handle[R]: ...
    @overload
    def run[**P, R](self, id: str, func: Callable[P, R], *args: P.args, **kwargs: P.kwargs) -> Handle[R]: ...
    @overload
    def run(self, id: str, func: str, *args: Any, **kwargs: Any) -> Handle[Any]: ...
    def run[**P, R](
        self, id: str, func: Callable[Concatenate[Context, P], Generator[Any, Any, R]] | Callable[Concatenate[Context, P], R] | Callable[P, R] | str, *args: P.args, **kwargs: P.kwargs
    ) -> Handle[R]:
        match func:
            case str():
                name = func
                func = self._registry.get(func)
            case _:
                name = self._registry.reverse_lookup(func)

        fp, fv = Future[DurablePromise](), Future[R]()
        self._scheduler.enqueue(Invoke(id, name, func, args, kwargs), futures=(fp, fv))
        fp.result()
        return Handle(fv)

    @overload
    def rpc[**P, R](self, id: str, func: Callable[Concatenate[Context, P], Generator[Any, Any, R]], *args: P.args, **kwargs: P.kwargs) -> Handle[R]: ...
    @overload
    def rpc[**P, R](self, id: str, func: Callable[Concatenate[Context, P], R], *args: P.args, **kwargs: P.kwargs) -> Handle[R]: ...
    @overload
    def rpc[**P, R](self, id: str, func: Callable[P, R], *args: P.args, **kwargs: P.kwargs) -> Handle[R]: ...
    @overload
    def rpc(self, id: str, func: str, *args: Any, **kwargs: Any) -> Handle[Any]: ...
    def rpc[**P, R](
        self, id: str, func: Callable[Concatenate[Context, P], Generator[Any, Any, R]] | Callable[Concatenate[Context, P], R] | Callable[P, R] | str, *args: P.args, **kwargs: P.kwargs
    ) -> Handle[R]:
        match func:
            case str():
                name = func
            case _:
                name = self._registry.reverse_lookup(func)

        fp, fv = Future[DurablePromise](), Future[R]()
        self._scheduler.enqueue(Invoke(id, name, None, args, kwargs, opts={"target": "default"}), futures=(fp, fv))
        fp.result()
        return Handle(fv)

    def get(self, id: str) -> Handle[Any]:
        fp, fv = Future[DurablePromise](), Future[Any]()
        self._scheduler.enqueue(Listen(id), futures=(fp, fv))
        fp.result()
        return Handle(fv)


class _Container:
    def __init__(self, opts: RunOptions, registry: Registry, cq: Enqueueable[Invoke | Listen]) -> None:
        self._opts = opts
        self._registry = registry
        self._cq = cq

    @overload
    def run[**P, R](self, id: str, func: Callable[Concatenate[Context, P], Generator[Any, Any, R]], *args: P.args, **kwargs: P.kwargs) -> Handle[R]: ...
    @overload
    def run[**P, R](self, id: str, func: Callable[Concatenate[Context, P], R], *args: P.args, **kwargs: P.kwargs) -> Handle[R]: ...
    @overload
    def run[**P, R](self, id: str, func: Callable[P, R], *args: P.args, **kwargs: P.kwargs) -> Handle[R]: ...
    @overload
    def run(self, id: str, func: str, *args: Any, **kwargs: Any) -> Handle[Any]: ...
    def run[**P, R](
        self, id: str, func: Callable[Concatenate[Context, P], Generator[Any, Any, R]] | Callable[Concatenate[Context, P], R] | Callable[P, R] | str, *args: P.args, **kwargs: P.kwargs
    ) -> Handle[R]:
        match func:
            case str():
                name = func
                func = self._registry.get(func)
            case _:
                name = self._registry.reverse_lookup(func)
        fp, fv = Future[DurablePromise](), Future[R]()
        self._cq.enqueue(Invoke(id, name, func, args, kwargs), futures=(fp, fv))
        fp.result()
        return Handle(fv)

    @overload
    def rpc[**P, R](self, id: str, func: Callable[Concatenate[Context, P], Generator[Any, Any, R]], *args: P.args, **kwargs: P.kwargs) -> Handle[R]: ...
    @overload
    def rpc[**P, R](self, id: str, func: Callable[Concatenate[Context, P], R], *args: P.args, **kwargs: P.kwargs) -> Handle[R]: ...
    @overload
    def rpc[**P, R](self, id: str, func: Callable[P, R], *args: P.args, **kwargs: P.kwargs) -> Handle[R]: ...
    @overload
    def rpc(self, id: str, func: str, *args: Any, **kwargs: Any) -> Handle[Any]: ...
    def rpc[**P, R](
        self, id: str, func: Callable[Concatenate[Context, P], Generator[Any, Any, R]] | Callable[Concatenate[Context, P], R] | Callable[P, R] | str, *args: P.args, **kwargs: P.kwargs
    ) -> Handle[R]:
        match func:
            case str():
                name = func
            case _:
                name = self._registry.reverse_lookup(func)

        fp, fv = Future[DurablePromise](), Future[R]()
        self._cq.enqueue(Invoke(id, name, None, args, kwargs, opts={"target": self._opts.send_to}), futures=(fp, fv))
        fp.result()
        return Handle(fv)
