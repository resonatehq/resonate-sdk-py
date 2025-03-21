from __future__ import annotations

import uuid
from collections.abc import Callable
from concurrent.futures import Future
from typing import TYPE_CHECKING, Any, Concatenate, overload

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
from resonate.models.options import Options
from resonate.registry import Registry
from resonate.scheduler import Scheduler

if TYPE_CHECKING:
    from collections.abc import Generator


class Resonate:
    def __init__(
        self,
        pid: str | None = None,
        opts: Options | None = None,
        unicast: str | None = None,
        anycast: str | None = None,
        registry: Registry | None = None,
        dependencies: Dependencies | None = None,
        scheduler: Scheduler | None = None,
    ) -> None:
        self._pid = pid or uuid.uuid4().hex
        self._opts = opts or Options()

        self.unicast = unicast or f"poller://default/{pid}"
        self.anycast = anycast or f"poller://default/{pid}"

        self._registry = registry or Registry()
        self._dependencies = dependencies or Dependencies()

        self._scheduler = scheduler or Scheduler(
            ctx=lambda id: Context(id, self._opts, self._registry, self._dependencies),
            pid=pid,
            unicast=self.unicast,
            anycast=self.anycast,
        )

    def options(self, *, send_to: str | None = None, timeout: int | None = None, version: int | None = None) -> Resonate:
        return Resonate(
            pid=self._pid,
            opts=self._opts.merge(send_to=send_to, timeout=timeout, version=version),
            unicast=self.unicast,
            anycast=self.anycast,
            registry=self._registry,
            dependencies=self._dependencies,
            scheduler=self._scheduler,
        )

    @overload
    def register[**P, R](
        self, func: Callable[Concatenate[Context, P], R], /, *, name: str | None = None, send_to: str | None = None, timeout: int | None = None, version: int = 1
    ) -> Function[P, R]: ...
    @overload
    def register[**P, R](self, *, name: str | None = None, send_to: str | None = None, timeout: int | None = None, version: int = 1) -> Callable[[Callable], Function[P, Any]]: ...
    def register[**P, R](
        self, *args: Callable | None, name: str | None = None, send_to: str | None = None, timeout: int | None = None, version: int = 1
    ) -> Callable[[Callable], Function[P, R]] | Function[P, R]:
        def wrapper(func: Callable) -> Function[P, R]:
            self._registry.add(func.func if isinstance(func, Function) else func, name or func.__name__, version)
            return Function(self, name or func.__name__, func, self._opts.merge(send_to=send_to, version=version, timeout=timeout))

        if args and callable(args[0]):
            return wrapper(args[0])

        return wrapper

    @overload
    def run[**P, R](self, id: str, func: Callable[Concatenate[Context, P], Generator[Any, Any, R]], *args: P.args, **kwargs: P.kwargs) -> Handle[R]: ...
    @overload
    def run[**P, R](self, id: str, func: Callable[Concatenate[Context, P], R], *args: P.args, **kwargs: P.kwargs) -> Handle[R]: ...
    @overload
    def run(self, id: str, func: str, *args: Any, **kwargs: Any) -> Handle[Any]: ...
    def run[**P, R](
        self,
        id: str,
        func: Callable[Concatenate[Context, P], Generator[Any, Any, R]] | Callable[Concatenate[Context, P], R] | str,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Handle[R]:
        match func:
            case str():
                name = func
                func, version = self._registry.get(func, self._opts.version)
            case Callable():
                name, version = self._registry.get(func.func if isinstance(func, Function) else func, self._opts.version)

        fp, fv = Future[DurablePromise](), Future[R]()
        self._scheduler.enqueue(Invoke(id, name, func, args, kwargs, self._opts.merge(version=version)), futures=(fp, fv))

        fp.result()
        return Handle(fv)

    @overload
    def rpc[**P, R](self, id: str, func: Callable[Concatenate[Context, P], Generator[Any, Any, R]], *args: P.args, **kwargs: P.kwargs) -> Handle[R]: ...
    @overload
    def rpc[**P, R](self, id: str, func: Callable[Concatenate[Context, P], R], *args: P.args, **kwargs: P.kwargs) -> Handle[R]: ...
    @overload
    def rpc(self, id: str, func: str, *args: Any, **kwargs: Any) -> Handle[Any]: ...
    def rpc[**P, R](
        self,
        id: str,
        func: Callable[Concatenate[Context, P], Generator[Any, Any, R]] | Callable[Concatenate[Context, P], R] | str,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Handle[R]:
        match func:
            case str():
                name, version = func, self._registry.latest(func)
            case Callable():
                name, version = self._registry.get(func.func if isinstance(func, Function) else func, self._opts.version)

        fp, fv = Future[DurablePromise](), Future[R]()
        self._scheduler.enqueue(Invoke(id, name, None, args, kwargs, self._opts.merge(version=version)), futures=(fp, fv))

        fp.result()
        return Handle(fv)

    def get(self, id: str) -> Handle[Any]:
        fp, fv = Future[DurablePromise](), Future[Any]()
        self._scheduler.enqueue(Listen(id), futures=(fp, fv))

        fp.result()
        return Handle(fv)


# Context


class Context:
    def __init__(self, id: str, opts: Options, registry: Registry, dependencies: Dependencies) -> None:
        self._id = id
        self._opts = opts
        self._registry = registry
        self._dependencies = dependencies
        self._counter = 0
        self._attempt = 0

    @property
    def id(self) -> str:
        return self._id

    @property
    def deps(self) -> Dependencies:
        return self._dependencies

    @property
    def info(self) -> None:
        raise NotImplementedError

    @overload
    def lfi[**P, R](self, func: Callable[Concatenate[Context, P], Generator[Any, Any, R]], *args: P.args, **kwargs: P.kwargs) -> LFI: ...
    @overload
    def lfi[**P, R](self, func: Callable[Concatenate[Context, P], R], *args: P.args, **kwargs: P.kwargs) -> LFI: ...
    @overload
    def lfi(self, func: str, *args: Any, **kwargs: Any) -> LFI: ...
    def lfi(self, func: Callable | str, *args: Any, **kwargs: Any) -> LFI:
        self._counter += 1
        func, version, versions = self._lfi_func(func)
        return LFI(f"{self.id}.{self._counter}", func, args, kwargs, self._opts.merge(version=version), versions)

    @overload
    def lfc[**P, R](self, func: Callable[Concatenate[Context, P], Generator[Any, Any, R]], *args: P.args, **kwargs: P.kwargs) -> LFC: ...
    @overload
    def lfc[**P, R](self, func: Callable[Concatenate[Context, P], R], *args: P.args, **kwargs: P.kwargs) -> LFC: ...
    @overload
    def lfc(self, func: str, *args: Any, **kwargs: Any) -> LFC: ...
    def lfc(self, func: Callable | str, *args: Any, **kwargs: Any) -> LFC:
        self._counter += 1
        func, version, versions = self._lfi_func(func)
        return LFC(f"{self.id}.{self._counter}", func, args, kwargs, self._opts.merge(version=version), versions)

    @overload
    def rfi[**P, R](self, func: Callable[Concatenate[Context, P], Generator[Any, Any, R]], *args: P.args, **kwargs: P.kwargs) -> RFI: ...
    @overload
    def rfi[**P, R](self, func: Callable[Concatenate[Context, P], R], *args: P.args, **kwargs: P.kwargs) -> RFI: ...
    @overload
    def rfi(self, func: str, *args: Any, **kwargs: Any) -> RFI: ...
    def rfi(self, func: Callable | str, *args: Any, **kwargs: Any) -> RFI:
        self._counter += 1
        func, version, versions = self._rfi_func(func)
        return RFI(f"{self.id}.{self._counter}", func, args, kwargs, self._opts.merge(version=version), versions)

    @overload
    def rfc[**P, R](self, func: Callable[Concatenate[Context, P], Generator[Any, Any, R]], *args: P.args, **kwargs: P.kwargs) -> RFC: ...
    @overload
    def rfc[**P, R](self, func: Callable[Concatenate[Context, P], R], *args: P.args, **kwargs: P.kwargs) -> RFC: ...
    @overload
    def rfc(self, func: str, *args: Any, **kwargs: Any) -> RFC: ...
    def rfc(self, func: Callable | str, *args: Any, **kwargs: Any) -> RFC:
        self._counter += 1
        func, version, versions = self._rfi_func(func)
        return RFC(f"{self.id}.{self._counter}", func, args, kwargs, self._opts.merge(version=version), versions)

    @overload
    def detached[**P, R](self, func: Callable[Concatenate[Context, P], Generator[Any, Any, R]], *args: P.args, **kwargs: P.kwargs) -> RFI: ...
    @overload
    def detached[**P, R](self, func: Callable[Concatenate[Context, P], R], *args: P.args, **kwargs: P.kwargs) -> RFI: ...
    @overload
    def detached(self, func: str, *args: Any, **kwargs: Any) -> RFI: ...
    def detached(self, func: Callable | str, *args: Any, **kwargs: Any) -> RFI:
        self._counter += 1
        func, version, versions = self._rfi_func(func)
        return RFI(f"{self.id}.{self._counter}", func, args, kwargs, self._opts.merge(version=version), versions, mode="detached")

    def _lfi_func(self, f: str | Callable) -> tuple[Callable, int, dict[int, Callable] | None]:
        match f:
            case str():
                return *self._registry.get(f), self._registry.all(f)
            case Callable():
                return f, self._registry.latest(f), None

    def _rfi_func(self, f: str | Callable) -> tuple[str, int, set[int] | None]:
        match f:
            case str():
                return f, self._registry.latest(f), None
            case Callable():
                return *self._registry.get(f), self._registry.all(f)


# Function


class Function[**P, R]:
    @overload
    def __init__(self, resonate: Resonate, name: str, func: Callable[Concatenate[Context, P], Generator[Any, Any, R]], opts: Options) -> None: ...
    @overload
    def __init__(self, resonate: Resonate, name: str, func: Callable[Concatenate[Context, P], R], opts: Options) -> None: ...
    def __init__(self, resonate: Resonate, name: str, func: Callable[Concatenate[Context, P], Generator[Any, Any, R]] | Callable[Concatenate[Context, P], R], opts: Options) -> None:
        self._resonate = resonate
        self._name = name
        self._func = func
        self._opts = opts

    @property
    def name(self) -> str:
        return self._name

    @property
    def func(self) -> Callable:
        return self._func

    @property
    def __name__(self) -> str:
        return self._name

    def __call__(self, ctx: Context, *args: P.args, **kwargs: P.kwargs) -> Generator[Any, Any, R] | R:
        return self._func(ctx, *args, **kwargs)

    def options(self, *, send_to: str | None = None, timeout: int | None = None, version: int | None = None) -> Function[P, Generator[Any, Any, R] | R]:
        return Function(self._resonate, self._name, self._func, self._opts.merge(send_to=send_to, timeout=timeout, version=version))

    def run(self, id: str, *args: P.args, **kwargs: P.kwargs) -> Handle[R]:
        return self._resonate.options(**self._opts.to_dict()).run(id, self._name, *args, **kwargs)

    def rpc(self, id: str, *args: P.args, **kwargs: P.kwargs) -> Handle[R]:
        return self._resonate.options(**self._opts.to_dict()).rpc(id, self._name, *args, **kwargs)
