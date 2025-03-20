from __future__ import annotations

import uuid
from collections.abc import Callable
from concurrent.futures import Future
from typing import TYPE_CHECKING, Any, Concatenate, overload

from resonate import utils
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
        gid: str = "default",
        pid: str | None = None,
        opts: Options | None = None,
        registry: Registry | None = None,
        dependencies: Dependencies | None = None,
        scheduler: Scheduler | None = None,
    ) -> None:
        self._gid = gid
        self._pid = pid or uuid.uuid4().hex
        self._opts = opts or Options()

        self._registry = registry or Registry()
        self._dependencies = dependencies or Dependencies()

        self._scheduler = scheduler or Scheduler(
            ctx=lambda id: Context(id, self._registry, self._dependencies, self._opts),
            gid=gid,
            pid=pid,
        )

    def options(self, *, send_to: str | None = None, timeout: int | None = None, version: int | None = None) -> Resonate:
        return Resonate(
            gid=self._gid,
            pid=self._pid,
            opts=self._opts.merge(send_to=send_to, timeout=timeout, version=version),
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
            utils.validate(version > 0, "version must be greater than 0")
            if func.__name__ == "<lambda>":
                utils.validate(name is not None, "registering a lambda requires setting a name.")

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
                func, version = self._registry.get(func)
            case Callable():
                name, version = self._registry.get(func.func if isinstance(func, Function) else func)

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
                name, version = self._registry.get(func.func if isinstance(func, Function) else func)

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
    def __init__(self, id: str, registry: Registry, dependencies: Dependencies, opts: Options) -> None:
        self.id = id
        self.deps = dependencies
        self._counter = 0
        self._registry = registry
        self._opts = opts

    def lfi(self, func: str | Callable, *args: Any, **kwargs: Any) -> LFI:
        self._counter += 1
        func, version, versions = self._lfi_func(func)
        return LFI(f"{self.id}.{self._counter}", func, args, kwargs, self._opts.merge(version=version), versions)

    def lfc(self, func: str | Callable, *args: Any, **kwargs: Any) -> LFC:
        self._counter += 1
        func, version, versions = self._lfi_func(func)
        return LFC(f"{self.id}.{self._counter}", func, args, kwargs, self._opts.merge(version=version), versions)

    def rfi(self, func: str | Callable, *args: Any, **kwargs: Any) -> RFI:
        self._counter += 1
        func, version, versions = self._rfi_func(func)
        return RFI(f"{self.id}.{self._counter}", func, args, kwargs, self._opts.merge(version=version), versions)

    def rfc(self, func: str | Callable, *args: Any, **kwargs: Any) -> RFC:
        self._counter += 1
        func, version, versions = self._rfi_func(func)
        return RFC(f"{self.id}.{self._counter}", func, args, kwargs, self._opts.merge(version=version), versions)

    def detached(self, func: str | Callable, *args: Any, **kwargs: Any) -> RFI:
        self._counter += 1
        func, version, versions = self._rfi_func(func)
        return RFI(f"{self.id}.{self._counter}", func, args, kwargs, self._opts.merge(version=version), versions, mode="detached")

    def _lfi_func(self, f: str | Callable) -> tuple[Callable, int, dict[int, Callable] | None]:
        match f:
            case str():
                return *self._registry.get(f), self._registry.all(f)
            case Callable():
                return f, self._registry.latest(f), None

    def _rfi_func(self, f: str | Callable) -> tuple[str, int, dict[int, str] | None]:
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
