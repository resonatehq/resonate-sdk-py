from __future__ import annotations

import uuid
from collections.abc import Callable
from concurrent.futures import Future
from typing import TYPE_CHECKING, Any, Concatenate, Unpack, overload

from resonate.dependencies import Dependencies
from resonate.models.commands import Invoke, Listen
from resonate.models.context import (
    LFC,
    LFI,
    RFC,
    RFI,
)
from resonate.models.durable_promise import DurablePromise
from resonate.models.handle import Handle
from resonate.registry import Registry
from resonate.scheduler import Scheduler

if TYPE_CHECKING:
    from collections.abc import Generator

    from resonate.models.options import Options

class Resonate:
    def __init__(
        self,
        gid: str = "default",
        pid: str | None = None,
        registry: Registry | None = None,
        dependencies: Dependencies | None = None,
        scheduler: Scheduler | None = None,
        **opts: Unpack[Options],
    ) -> None:
        self._gid = gid
        self._pid = pid or uuid.uuid4().hex
        self._registry = registry or Registry()
        self._dependencies = dependencies or Dependencies()
        self._opts = opts

        self._scheduler = scheduler or Scheduler(
            ctx=lambda id: Context(id, self._registry, self._dependencies),
            gid=gid,
            pid=pid,
        )

    @property
    def dependencies(self) -> Dependencies:
        return self._dependencies

    def options(self, **opts: Unpack[Options]) -> Resonate:
        return Resonate(
            gid=self._gid,
            pid=self._pid,
            registry=self._registry,
            dependencies=self._dependencies,
            scheduler=self._scheduler,
            **{**self._opts, **opts},
        )

    @overload
    def register[**P, R](self, func: Callable[Concatenate[Context, P], Generator[Any, Any, R]], /, *, name: str | None = None, **opts: Unpack[Options]) -> Function[P, R]: ...
    @overload
    def register[**P, R](self, func: Callable[Concatenate[Context, P], R], /, *, name: str | None = None, **opts: Unpack[Options]) -> Function[P, R]: ...
    @overload
    def register[**P, R](self, func: Callable[P, R], /, *, name: str | None = None, **opts: Unpack[Options]) -> Function[P, R]: ...
    @overload
    def register[**P, R](self, *, name: str | None = None, **opts: Unpack[Options]) -> Callable[[Callable], Function[P, Any]]: ...
    def register[**P, R](self, func: Callable, name: str | None = None, **opts: Unpack[Options]) -> Callable[[Callable], Function[P, R]] | Function[P, R]:
        def wrapper(func: Callable) -> Function[P, R]:
            self._registry.add(name or func.__name__, func)
            return Function(self, name or func.__name__, func, opts)

        if func:
            return wrapper(func)

        return wrapper

    @overload
    def run[**P, R](self, id: str, func: Callable[Concatenate[Context, P], Generator[Any, Any, R]], *args: P.args, **kwargs: P.kwargs) -> Handle[R]: ...
    @overload
    def run[**P, R](self, id: str, func: Callable[Concatenate[Context, P], R], *args: P.args, **kwargs: P.kwargs) -> Handle[R]: ...
    @overload
    def run[**P, R](self, id: str, func: Callable[P, R], *args: P.args, **kwargs: P.kwargs) -> Handle[R]: ...
    @overload
    def run(self, id: str, func: str, *args: Any, **kwargs: Any) -> Handle[Any]: ...
    def run[**P, R](
        self,
        id: str,
        func: Callable[Concatenate[Context, P], Generator[Any, Any, R]] | Callable[Concatenate[Context, P], R] | Callable[P, R] | str, *args: P.args, **kwargs: P.kwargs,
    ) -> Handle[R]:
        match func:
            case str():
                name = func
                func = self._registry.get(func)
            case _:
                name = self._registry.reverse_lookup(func)

        fp, fv = Future[DurablePromise](), Future[R]()
        self._scheduler.enqueue(Invoke(id, name, func, args, kwargs, self._opts), futures=(fp, fv))
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
        self,
        id: str,
        func: Callable[Concatenate[Context, P], Generator[Any, Any, R]] | Callable[Concatenate[Context, P], R] | Callable[P, R] | str, *args: P.args, **kwargs: P.kwargs,
    ) -> Handle[R]:
        match func:
            case str():
                name = func
            case _:
                name = self._registry.reverse_lookup(func)

        fp, fv = Future[DurablePromise](), Future[R]()
        self._scheduler.enqueue(Invoke(id, name, None, args, kwargs, opts=self._opts), futures=(fp, fv))

        fp.result()
        return Handle(fv)

    def get(self, id: str) -> Handle[Any]:
        fp, fv = Future[DurablePromise](), Future[Any]()
        self._scheduler.enqueue(Listen(id), futures=(fp, fv))

        fp.result()
        return Handle(fv)


# Context

class Context:
    def __init__(self, id: str, registry: Registry, dependencies: Dependencies) -> None:
        self.id = id
        self.deps = dependencies
        self._counter = 0
        self._registry = registry

    def lfi(self, func: str | Callable, *args: Any, **kwargs: Any) -> LFI:
        self._counter += 1
        return LFI(f"{self.id}.{self._counter}", self._lfi_func(func), args, kwargs)

    def lfc(self, func: str | Callable, *args: Any, **kwargs: Any) -> LFC:
        self._counter += 1
        return LFC(f"{self.id}.{self._counter}", self._lfi_func(func), args, kwargs)

    def rfi(self, func: str | Callable, *args: Any, **kwargs: Any) -> RFI:
        self._counter += 1
        return RFI(f"{self.id}.{self._counter}", self._rfi_func(func), args, kwargs)

    def rfc(self, func: str | Callable, *args: Any, **kwargs: Any) -> RFC:
        self._counter += 1
        return RFC(f"{self.id}.{self._counter}", self._rfi_func(func), args, kwargs)

    def detached(self, func: str | Callable, *args: Any, **kwargs: Any) -> RFI:
        self._counter += 1
        return RFI(f"{self.id}.{self._counter}", self._rfi_func(func), args, kwargs, mode="detached")

    def _lfi_func(self, f: str | Callable) -> Callable:
        match f:
            case str():
                return self._registry.get(f)
            case Function():
                return f.func
            case Callable():
                return f

    def _rfi_func(self, f: str | Callable) -> str:
        match f:
            case str():
                return f
            case Function():
                return f.name
            case Callable():
                return self._registry.reverse_lookup(f)

# Function

class Function[**P, R]:
    @overload
    def __init__(self, resonate: Resonate, name: str, func: Callable[Concatenate[Context, P], Generator[Any, Any, R]], opts: Options) -> None: ...
    @overload
    def __init__(self, resonate: Resonate, name: str, func: Callable[Concatenate[Context, P], R], opts: Options) -> None: ...
    @overload
    def __init__(self, resonate: Resonate, name: str, func: Callable[P, R], opts: Options) -> None: ...
    def __init__(self, resonate: Resonate, name: str, func: Callable[Concatenate[Context, P], Generator[Any, Any, R]] | Callable[Concatenate[Context, P], R] | Callable[P, R], opts: Options) -> None:
        self._resonate = resonate
        self._name = name
        self._func = func
        self._opts = opts

    @overload
    def __call__(self, ctx: Context, *args: P.args, **kwargs: P.kwargs) -> Generator[Any, Any, R]: ...
    @overload
    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> R: ...
    def __call__(self, *args, **kwargs) -> Generator[Any, Any, R] | R:
        return self._func(*args, **kwargs)

    def options(self, **opts: Unpack[Options]) -> Function[P, R]:
        return Function(self._resonate, self._name, self._func, {**self._opts, **opts})

    def run(self, id: str, *args: P.args, **kwargs: P.kwargs) -> Handle[R]:
        return self._resonate.options(**self._opts).run(id, self._name, *args, **kwargs)

    def rpc(self, id: str, *args: P.args, **kwargs: P.kwargs) -> Handle[R]:
        return self._resonate.options(**self._opts).rpc(id, self._name, *args, **kwargs)
