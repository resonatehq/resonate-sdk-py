from __future__ import annotations

import uuid
from collections.abc import Callable
from concurrent.futures import Future
from dataclasses import dataclass, field
from inspect import isgeneratorfunction
from typing import TYPE_CHECKING, Any, Concatenate, Protocol, overload

from resonate.dependencies import Dependencies
from resonate.errors import ResonateValidationError
from resonate.models.commands import Invoke, Listen
from resonate.models.context import (
    LFC,
    LFI,
    RFC,
    RFI,
    Info,
)
from resonate.models.durable_promise import DurablePromise
from resonate.models.handle import Handle
from resonate.models.options import Options
from resonate.models.retry_policies import Exponential, Never
from resonate.registry import Registry
from resonate.scheduler import Scheduler

if TYPE_CHECKING:
    from collections.abc import Generator

    from resonate.models.retry_policies import RetryPolicy


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
            ctx=lambda id, info: Context(id, info, self._opts, self._registry, self._dependencies),
            pid=pid,
            unicast=self.unicast,
            anycast=self.anycast,
        )

    def options(
        self,
        *,
        retry_policy: RetryPolicy | None = None,
        send_to: str | None = None,
        tags: dict[str, str] | None = None,
        timeout: int | None = None,
        version: int | None = None,
    ) -> Resonate:
        return Resonate(
            pid=self._pid,
            opts=self._opts.merge(send_to=send_to, timeout=timeout, version=version, tags=tags, retry_policy=retry_policy),
            unicast=self.unicast,
            anycast=self.anycast,
            registry=self._registry,
            dependencies=self._dependencies,
            scheduler=self._scheduler,
        )

    @overload
    def register[**P, R](
        self,
        func: Callable[Concatenate[Context, P], R],
        /,
        *,
        name: str | None = None,
        send_to: str | None = None,
        timeout: int | None = None,
        version: int = 1,
        tags: dict[str, str] | None = None,
        retry_policy: RetryPolicy | None = None,
    ) -> Function[P, R]: ...
    @overload
    def register[**P, R](
        self,
        *,
        name: str | None = None,
        send_to: str | None = None,
        timeout: int | None = None,
        version: int = 1,
        tags: dict[str, str] | None = None,
        retry_policy: RetryPolicy | None = None,
    ) -> Callable[[Callable], Function[P, Any]]: ...
    def register[**P, R](
        self,
        *args: Callable | None,
        name: str | None = None,
        send_to: str | None = None,
        timeout: int | None = None,
        version: int = 1,
        tags: dict[str, str] | None = None,
        retry_policy: RetryPolicy | None = None,
    ) -> Callable[[Callable], Function[P, R]] | Function[P, R]:
        def wrapper(func: Callable) -> Function[P, R]:
            self._registry.add(func.func if isinstance(func, Function) else func, name or func.__name__, version)
            return Function(self, name or func.__name__, func, self._opts.merge(send_to=send_to, version=version, timeout=timeout, tags=tags, retry_policy=retry_policy))

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
        self._scheduler.step(Invoke(id, name, func, args, kwargs, self._opts.merge(version=version)), futures=(fp, fv))

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
        self._scheduler.step(Invoke(id, name, None, args, kwargs, self._opts.merge(version=version)), futures=(fp, fv))

        fp.result()
        return Handle(fv)

    def get(self, id: str) -> Handle[Any]:
        fp, fv = Future[DurablePromise](), Future[Any]()
        self._scheduler.step(Listen(id), futures=(fp, fv))

        fp.result()
        return Handle(fv)


# Convention


class Convention(Protocol):
    def __init__(self, *args: Any, **kwargs: Any) -> None: ...
    @property
    def opts(self) -> Options: ...
    def format(self) -> tuple[Any, dict[str, str], int, dict[str, str] | None]: ...
    def options(self, send_to: str | None, tags: dict[str, str] | None, timeout: int | None, version: int | None) -> None: ...


@dataclass
class DefaultConvention:
    func: str
    args: tuple[Any, ...]
    kwargs: dict[str, Any]
    versions: set[int] | None
    opts: Options = field(default_factory=Options)

    def __post_init__(self) -> None:
        # Initially, timeout is set to the parent context timeout. This is the upper bound for the timeout.
        self._max_timeout = self.opts.timeout

    def format(self) -> tuple[Any, dict[str, str], int, dict[str, str] | None]:
        if self.versions is not None:
            assert self.opts.version in self.versions
        assert self.opts.version > 0
        return ({"func": self.func, "args": self.args, "kwargs": self.kwargs, "version": self.opts.version}, {**self.opts.tags, "resonate:invoke": self.opts.send_to}, self.opts.timeout, None)

    def options(self, send_to: str | None, tags: dict[str, str] | None, timeout: int | None, version: int | None) -> None:
        if version is not None and self.versions is not None and version not in self.versions:
            msg = f"version={version} not found."
            raise ResonateValidationError(msg)
        if timeout is not None:
            timeout = min(self._max_timeout, timeout)
        self.opts = self.opts.merge(send_to=send_to, timeout=timeout, version=version, tags=tags)


@dataclass
class XConvention:
    data: Any
    tags: dict[str, str]
    timeout: int
    headers: dict[str, str] | None
    opts: Options = field(default_factory=Options)

    def format(self) -> tuple[Any, dict[str, str], int, dict[str, str] | None]:
        return (self.data, self.tags, self.timeout, self.headers)

    def options(self, send_to: str | None, tags: dict[str, str] | None, timeout: int | None, version: int | None) -> None:
        return


# Context
class Context:
    def __init__(self, id: str, info: Info, opts: Options, registry: Registry, dependencies: Dependencies) -> None:
        self._id = id
        self._info = info
        self._opts = opts
        self._registry = registry
        self._dependencies = dependencies
        self._counter = 0

    @property
    def id(self) -> str:
        return self._id

    @property
    def deps(self) -> Dependencies:
        return self._dependencies

    @property
    def info(self) -> Info:
        return self._info

    @overload
    def lfi[**P, R](self, func: Callable[Concatenate[Context, P], Generator[Any, Any, R]], *args: P.args, **kwargs: P.kwargs) -> LFI: ...
    @overload
    def lfi[**P, R](self, func: Callable[Concatenate[Context, P], R], *args: P.args, **kwargs: P.kwargs) -> LFI: ...
    @overload
    def lfi(self, func: str, *args: Any, **kwargs: Any) -> LFI: ...
    def lfi(self, func: Callable | str, *args: Any, **kwargs: Any) -> LFI:
        self._counter += 1
        func, version, versions = self._lfi_func(func)
        retry_policy = Never() if isgeneratorfunction(func) else Exponential()
        return LFI(f"{self.id}.{self._counter}", func, args, kwargs, Options(version=version, retry_policy=retry_policy, timeout=self._opts.timeout), versions)

    @overload
    def lfc[**P, R](self, func: Callable[Concatenate[Context, P], Generator[Any, Any, R]], *args: P.args, **kwargs: P.kwargs) -> LFC: ...
    @overload
    def lfc[**P, R](self, func: Callable[Concatenate[Context, P], R], *args: P.args, **kwargs: P.kwargs) -> LFC: ...
    @overload
    def lfc(self, func: str, *args: Any, **kwargs: Any) -> LFC: ...
    def lfc(self, func: Callable | str, *args: Any, **kwargs: Any) -> LFC:
        self._counter += 1
        retry_policy = Never() if isgeneratorfunction(func) else Exponential()
        func, version, versions = self._lfi_func(func)
        return LFC(f"{self.id}.{self._counter}", func, args, kwargs, Options(version=version, retry_policy=retry_policy, timeout=self._opts.timeout), versions)

    @overload
    def rfi[**P, R](self, func: Callable[Concatenate[Context, P], Generator[Any, Any, R]], *args: P.args, **kwargs: P.kwargs) -> RFI: ...
    @overload
    def rfi[**P, R](self, func: Callable[Concatenate[Context, P], R], *args: P.args, **kwargs: P.kwargs) -> RFI: ...
    @overload
    def rfi(self, func: str, *args: Any, **kwargs: Any) -> RFI: ...
    @overload
    def rfi(self, cmd: XConvention, /) -> RFI: ...
    def rfi(self, func: Callable | str | XConvention, *args: Any, **kwargs: Any) -> RFI:
        self._counter += 1
        id = f"{self.id}.{self._counter}"
        match func:
            case XConvention():
                return RFI(id, func)
            case _:
                func, version, versions = self._rfi_func(func)
                return RFI(id, DefaultConvention(func, args, kwargs, versions, Options(version=version, timeout=self._opts.timeout)))

    @overload
    def rfc[**P, R](self, func: Callable[Concatenate[Context, P], Generator[Any, Any, R]], *args: P.args, **kwargs: P.kwargs) -> RFC: ...
    @overload
    def rfc[**P, R](self, func: Callable[Concatenate[Context, P], R], *args: P.args, **kwargs: P.kwargs) -> RFC: ...
    @overload
    def rfc(self, func: str, *args: Any, **kwargs: Any) -> RFC: ...
    @overload
    def rfc(self, cmd: XConvention, /) -> RFC: ...
    def rfc(self, func: Callable | str | XConvention, *args: Any, **kwargs: Any) -> RFC:
        self._counter += 1
        id = f"{self.id}.{self._counter}"
        match func:
            case XConvention():
                return RFC(id, func)
            case _:
                func, version, versions = self._rfi_func(func)
                return RFC(id, DefaultConvention(func, args, kwargs, versions, Options(version=version, timeout=self._opts.timeout)))

    @overload
    def detached[**P, R](self, func: Callable[Concatenate[Context, P], Generator[Any, Any, R]], *args: P.args, **kwargs: P.kwargs) -> RFI: ...
    @overload
    def detached[**P, R](self, func: Callable[Concatenate[Context, P], R], *args: P.args, **kwargs: P.kwargs) -> RFI: ...
    @overload
    def detached(self, func: str, *args: Any, **kwargs: Any) -> RFI: ...
    @overload
    def detached(self, cmd: XConvention, /) -> RFI: ...
    def detached(self, func: Callable | str | XConvention, *args: Any, **kwargs: Any) -> RFI:
        self._counter += 1
        id = f"{self.id}.{self._counter}"
        match func:
            case XConvention():
                return RFI(id, func, mode="detached")
            case _:
                func, version, versions = self._rfi_func(func)
                return RFI(id, DefaultConvention(func, args, kwargs, versions, Options(version=version)), mode="detached")

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

    def options(
        self,
        *,
        retry_policy: RetryPolicy | None = None,
        send_to: str | None = None,
        tags: dict[str, str] | None = None,
        timeout: int | None = None,
        version: int | None = None,
    ) -> Function[P, Generator[Any, Any, R] | R]:
        return Function(
            self._resonate,
            self._name,
            self._func,
            self._opts.merge(retry_policy=retry_policy, send_to=send_to, tags=tags, timeout=timeout, version=version),
        )

    def run(self, id: str, *args: P.args, **kwargs: P.kwargs) -> Handle[R]:
        return self._resonate.options(**self._opts.to_dict()).run(id, self._name, *args, **kwargs)

    def rpc(self, id: str, *args: P.args, **kwargs: P.kwargs) -> Handle[R]:
        return self._resonate.options(**self._opts.to_dict()).rpc(id, self._name, *args, **kwargs)
