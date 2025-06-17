from __future__ import annotations

import copy
import functools
import inspect
import logging
import random
import time
import uuid
from concurrent.futures import Future
from typing import TYPE_CHECKING, Any, Concatenate, Literal, ParamSpec, TypeVar, TypeVarTuple, overload

from resonate import utils
from resonate.bridge import Bridge
from resonate.conventions import Base, Local, Remote, Sleep
from resonate.coroutine import LFC, LFI, RFC, RFI, Promise
from resonate.dependencies import Dependencies
from resonate.loggers import ContextLogger
from resonate.message_sources import LocalMessageSource, Poller
from resonate.models.handle import Handle
from resonate.options import Options
from resonate.registry import Registry
from resonate.stores import LocalStore, RemoteStore

if TYPE_CHECKING:
    from collections.abc import Callable, Generator, Sequence

    from resonate.models.context import Info
    from resonate.models.encoder import Encoder
    from resonate.models.logger import Logger
    from resonate.models.message_source import MessageSource
    from resonate.models.retry_policy import RetryPolicy
    from resonate.models.store import PromiseStore, Store


class Resonate:
    def __init__(
        self,
        *,
        pid: str | None = None,
        ttl: int = 10,
        group: str = "default",
        registry: Registry | None = None,
        dependencies: Dependencies | None = None,
        log_level: int | Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = logging.NOTSET,
        store: Store | None = None,
        message_source: MessageSource | None = None,
    ) -> None:
        # enforce mutual inclusion/exclusion of store and message source
        assert (store is None) == (message_source is None), "store and message source must both be set or both be unset"
        assert not isinstance(store, LocalStore) or isinstance(message_source, LocalMessageSource), "message source must be local message source"
        assert not isinstance(store, RemoteStore) or not isinstance(message_source, LocalMessageSource), "message source must not be local message source"

        self._started = False
        self._pid = pid or uuid.uuid4().hex
        self._ttl = ttl
        self._group = group
        self._opts = Options()
        self._registry = registry or Registry()
        self._dependencies = dependencies or Dependencies()
        self._log_level = log_level

        if store and message_source:
            self._store = store
            self._message_source = message_source
        else:
            self._store = LocalStore()
            self._message_source = self._store.message_source(self._group, self._pid)

        self._bridge = Bridge(
            lambda id, cid, info: Context(id, cid, info, self._registry, self._dependencies, ContextLogger(cid, id, self._log_level)),
            self._pid,
            self._ttl,
            self._opts,
            self._message_source.anycast,
            self._message_source.unicast,
            self._registry,
            self._store,
            self._message_source,
        )

    @classmethod
    def local(
        cls,
        pid: str | None = None,
        ttl: int = 10,
        group: str = "default",
        registry: Registry | None = None,
        dependencies: Dependencies | None = None,
        log_level: int = logging.INFO,
    ) -> Resonate:
        # pid
        if pid is not None and not isinstance(pid, str):
            msg = f"pid must be `str | None`, got {utils.type_name(pid)}"
            raise TypeError(msg)

        # ttl
        if not isinstance(ttl, int):
            msg = f"ttl must be `int`, got {utils.type_name(ttl)}"
            raise TypeError(msg)
        if ttl <= 0:
            msg = f"ttl must be > 0, got {ttl}"
            raise ValueError(msg)

        # group
        if not isinstance(group, str):
            msg = f"group must be `str`, got {utils.type_name(group)}"
            raise TypeError(msg)

        # registry
        if registry is not None and not isinstance(registry, Registry):
            msg = f"registry must be `Registry | None`, got {utils.type_name(registry)}"
            raise TypeError(msg)

        # dependencies
        if dependencies is not None and not isinstance(dependencies, Dependencies):
            msg = f"dependencies must be `Dependencies | None`, got {utils.type_name(dependencies)}"
            raise TypeError(msg)

        # log_level
        if not isinstance(log_level, int):
            msg = f"log_level must be `int`, got {utils.type_name(log_level)}"
            raise TypeError(msg)

        pid = pid or uuid.uuid4().hex
        store = LocalStore()

        return cls(
            pid=pid,
            ttl=ttl,
            group=group,
            registry=registry,
            dependencies=dependencies,
            log_level=log_level,
            store=store,
            message_source=store.message_source(group=group, id=pid),
        )

    @classmethod
    def remote(
        cls,
        host: str | None = None,
        store_port: str | None = None,
        message_source_port: str | None = None,
        pid: str | None = None,
        ttl: int = 10,
        group: str = "default",
        registry: Registry | None = None,
        dependencies: Dependencies | None = None,
        log_level: int = logging.INFO,
    ) -> Resonate:
        # host
        if host is not None and not isinstance(host, str):
            msg = f"host must be `str | None`, got {utils.type_name(host)}"
            raise TypeError(msg)

        # store_port
        if store_port is not None and not isinstance(store_port, str):
            msg = f"store_port must be `str | None`, got {utils.type_name(store_port)}"
            raise TypeError(msg)

        # message_source_port
        if message_source_port is not None and not isinstance(message_source_port, str):
            msg = f"message_source_port must be `str | None`, got {utils.type_name(message_source_port)}"
            raise TypeError(msg)

        # pid
        if pid is not None and not isinstance(pid, str):
            msg = f"pid must be `str | None`, got {utils.type_name(pid)}"
            raise TypeError(msg)

        # ttl
        if not isinstance(ttl, int):
            msg = f"ttl must be `int`, got {utils.type_name(ttl)}"
            raise TypeError(msg)

        # group
        if not isinstance(group, str):
            msg = f"group must be `str`, got {utils.type_name(group)}"
            raise TypeError(msg)

        # registry
        if registry is not None and not isinstance(registry, Registry):
            msg = f"registry must be `Registry | None`, got {utils.type_name(registry)}"
            raise TypeError(msg)

        # dependencies
        if dependencies is not None and not isinstance(dependencies, Dependencies):
            msg = f"dependencies must be `Dependencies | None`, got {utils.type_name(dependencies)}"
            raise TypeError(msg)

        # log_level
        if not isinstance(log_level, int):
            msg = f"log_level must be `int`, got {utils.type_name(log_level)}"
            raise TypeError(msg)

        pid = pid or uuid.uuid4().hex

        return cls(
            pid=pid,
            ttl=ttl,
            group=group,
            registry=registry,
            dependencies=dependencies,
            log_level=log_level,
            store=RemoteStore(host=host, port=store_port),
            message_source=Poller(group=group, id=pid, host=host, port=message_source_port),
        )

    @property
    def promises(self) -> PromiseStore:
        return self._store.promises

    def start(self) -> None:
        if not self._started:
            self._bridge.start()

    def stop(self) -> None:
        self._started = False
        self._bridge.stop()

    def options(
        self,
        *,
        encoder: Encoder[Any, str | None] | None = None,
        idempotency_key: str | Callable[[str], str] | None = None,
        retry_policy: RetryPolicy | Callable[[Callable], RetryPolicy] | None = None,
        tags: dict[str, str] | None = None,
        target: str | None = None,
        timeout: float | None = None,
        version: int | None = None,
    ) -> Resonate:
        copied: Resonate = copy.copy(self)
        copied._opts = self._opts.merge(
            encoder=encoder,
            idempotency_key=idempotency_key,
            retry_policy=retry_policy,
            tags=tags,
            target=target,
            timeout=timeout,
            version=version,
        )

        return copied

    @overload
    def register[**P, R](
        self,
        func: Callable[Concatenate[Context, P], R],
        /,
        *,
        name: str | None = None,
        version: int = 1,
    ) -> Function[P, R]: ...
    @overload
    def register[**P, R](
        self,
        *,
        name: str | None = None,
        version: int = 1,
    ) -> Callable[[Callable[Concatenate[Context, P], R]], Function[P, R]]: ...
    def register[**P, R](
        self,
        *args: Callable[Concatenate[Context, P], R] | None,
        name: str | None = None,
        version: int = 1,
    ) -> Function[P, R] | Callable[[Callable[Concatenate[Context, P], R]], Function[P, R]]:
        if name is not None and not isinstance(name, str):
            msg = f"name must be `str | None`, got {utils.type_name(name)}"
            raise TypeError(msg)

        if not isinstance(version, int):
            msg = f"version must be `int`, got {utils.type_name(version)}"
            raise TypeError(msg)

        def wrapper(func: Callable[..., Any]) -> Function[P, R]:
            if not callable(func):
                msg = "func must be Callable"
                raise TypeError(msg)

            if isinstance(func, Function):
                func = func.func

            self._registry.add(func, name, version)
            return Function(self, name or func.__name__, func, self._opts.merge(version=version))

        if args and args[0] is not None:
            return wrapper(args[0])

        return wrapper

    @overload
    def run[**P, R](
        self,
        id: str,
        func: Callable[Concatenate[Context, P], Generator[Any, Any, R] | R],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Handle[R]: ...
    @overload
    def run(
        self,
        id: str,
        func: str,
        *args: Any,
        **kwargs: Any,
    ) -> Handle[Any]: ...
    def run[**P, R](
        self,
        id: str,
        func: Callable[Concatenate[Context, P], Generator[Any, Any, R] | R] | str,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Handle[R]:
        # id
        if not isinstance(id, str):
            msg = f"id must be `str`, got {utils.type_name(id)}"
            raise TypeError(msg)
        # func
        if not (callable(func) or isinstance(func, str)):
            msg = f"func must be `Callable | str`, got {utils.type_name(func)}"
            raise TypeError(msg)
        # tuple
        if not isinstance(args, tuple):
            msg = f"args must be `tuple`, got {utils.type_name(args)}"
            raise TypeError(args)
        # dict
        if not isinstance(kwargs, dict):
            msg = f"kwargs must be `dict`, got {utils.type_name(kwargs)}"
            raise TypeError(kwargs)

        self.start()
        future = Future[R]()

        name, func, version = self._registry.get(func, self._opts.version)
        opts = self._opts.merge(version=version)

        self._bridge.run(Remote(id, id, id, name, args, kwargs, opts), func, args, kwargs, opts, future)
        return Handle(future)

    @overload
    def rpc[**P, R](
        self,
        id: str,
        func: Callable[Concatenate[Context, P], Generator[Any, Any, R] | R],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Handle[R]: ...
    @overload
    def rpc(
        self,
        id: str,
        func: str,
        *args: Any,
        **kwargs: Any,
    ) -> Handle[Any]: ...
    def rpc[**P, R](
        self,
        id: str,
        func: Callable[Concatenate[Context, P], Generator[Any, Any, R] | R] | str,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Handle[R]:
        # id
        if not isinstance(id, str):
            msg = f"id must be `str`, got {utils.type_name(id)}"
            raise TypeError(msg)
        # func
        if not (callable(func) or isinstance(func, str)):
            msg = f"func must be `Callable | str`, got {utils.type_name(func)}"
            raise TypeError(msg)
        # tuple
        if not isinstance(args, tuple):
            msg = f"args must be `tuple`, got {utils.type_name(args)}"
            raise TypeError(args)
        # dict
        if not isinstance(kwargs, dict):
            msg = f"kwargs must be `dict`, got {utils.type_name(kwargs)}"
            raise TypeError(kwargs)

        self.start()
        future = Future[R]()

        if isinstance(func, str):
            name = func
            version = self._registry.latest(func)
        else:
            name, _, version = self._registry.get(func, self._opts.version)

        opts = self._opts.merge(version=version)
        self._bridge.rpc(Remote(id, id, id, name, args, kwargs, opts), opts, future)
        return Handle(future)

    def get(self, id: str) -> Handle[Any]:
        # id
        if not isinstance(id, str):
            msg = f"id must be `str`, got {utils.type_name(id)}"
            raise TypeError(msg)

        self.start()
        future = Future()

        self._bridge.get(id, self._opts, future)
        return Handle(future)

    def set_dependency(self, name: str, obj: Any) -> None:
        # name
        if not isinstance(name, str):
            msg = f"name must be `str`, got {utils.type_name(name)}"
            raise TypeError(msg)

        self._dependencies.add(name, obj)


class Context:
    def __init__(self, id: str, cid: str, info: Info, registry: Registry, dependencies: Dependencies, logger: Logger) -> None:
        self._id = id
        self._cid = cid
        self._info = info
        self._registry = registry
        self._dependencies = dependencies
        self._logger = logger
        self._random = Random(self)
        self._time = Time(self)
        self._counter = 0

    def __repr__(self) -> str:
        return f"Context(id={self._id}, cid={self._cid}, info={self._info})"

    @property
    def id(self) -> str:
        return self._id

    @property
    def info(self) -> Info:
        return self._info

    @property
    def logger(self) -> Logger:
        return self._logger

    @property
    def random(self) -> Random:
        return self._random

    @property
    def time(self) -> Time:
        return self._time

    def get_dependency[T](self, key: str, default: T = None) -> Any | T:
        if not isinstance(key, str):
            msg = f"key must be `str`, got {utils.type_name(key)}"
            raise TypeError(msg)

        return self._dependencies.get(key, default)

    def lfi[**P, R](
        self,
        func: Callable[Concatenate[Context, P], Generator[Any, Any, R] | R],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> LFI[R]:
        if isinstance(func, Function):
            func = func.func

        if not inspect.isfunction(func):
            msg = "provided callable must be a function"
            raise ValueError(msg)

        opts = Options(version=self._registry.latest(func))
        return LFI(Local(self._next(), self._cid, self._id, opts), func, args, kwargs, opts)

    def lfc[**P, R](
        self,
        func: Callable[Concatenate[Context, P], Generator[Any, Any, R] | R],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> LFC[R]:
        if isinstance(func, Function):
            func = func.func

        if not inspect.isfunction(func):
            msg = "provided callable must be a function"
            raise ValueError(msg)

        opts = Options(version=self._registry.latest(func))
        return LFC(Local(self._next(), self._cid, self._id, opts), func, args, kwargs, opts)

    @overload
    def rfi[**P, R](
        self,
        func: Callable[Concatenate[Context, P], Generator[Any, Any, R] | R],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> RFI[R]: ...
    @overload
    def rfi(
        self,
        func: str,
        *args: Any,
        **kwargs: Any,
    ) -> RFI: ...
    def rfi(
        self,
        func: Callable | str,
        *args: Any,
        **kwargs: Any,
    ) -> RFI:
        name, _, version = (func, None, self._registry.latest(func)) if isinstance(func, str) else self._registry.get(func)
        return RFI(Remote(self._next(), self._cid, self._id, name, args, kwargs, Options(version=version)))

    @overload
    def rfc[**P, R](
        self,
        func: Callable[Concatenate[Context, P], Generator[Any, Any, R] | R],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> RFC[R]: ...
    @overload
    def rfc(
        self,
        func: str,
        *args: Any,
        **kwargs: Any,
    ) -> RFC: ...
    def rfc(
        self,
        func: Callable | str,
        *args: Any,
        **kwargs: Any,
    ) -> RFC:
        name, _, version = (func, None, self._registry.latest(func)) if isinstance(func, str) else self._registry.get(func)
        return RFC(Remote(self._next(), self._cid, self._id, name, args, kwargs, Options(version=version)))

    @overload
    def detached[**P, R](
        self,
        func: Callable[Concatenate[Context, P], Generator[Any, Any, R] | R],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> RFI[R]: ...
    @overload
    def detached(
        self,
        func: str,
        *args: Any,
        **kwargs: Any,
    ) -> RFI: ...
    def detached(
        self,
        func: Callable | str,
        *args: Any,
        **kwargs: Any,
    ) -> RFI:
        name, _, version = (func, None, self._registry.latest(func)) if isinstance(func, str) else self._registry.get(func)
        return RFI(Remote(self._next(), self._cid, self._id, name, args, kwargs, Options(version=version)), mode="detached")

    @overload
    def typesafe[T](self, cmd: LFI[T] | RFI[T]) -> Generator[LFI[T] | RFI[T], Promise[T], Promise[T]]: ...
    @overload
    def typesafe[T](self, cmd: LFC[T] | RFC[T] | Promise[T]) -> Generator[LFC[T] | RFC[T] | Promise[T], T, T]: ...
    def typesafe(self, cmd: LFI | RFI | LFC | RFC | Promise) -> Generator[LFI | RFI | LFC | RFC | Promise, Any, Any]:
        return (yield cmd)

    def sleep(self, secs: float) -> RFC[None]:
        if not isinstance(secs, int | float):
            msg = f"secs must be `float`, got {utils.type_name(secs)}"
            raise TypeError(msg)

        return RFC(Sleep(self._next(), secs))

    def promise(
        self,
        *,
        id: str | None = None,
        timeout: float | None = None,
        idempotency_key: str | None = None,
        data: Any = None,
        tags: dict[str, str] | None = None,
    ) -> RFI:
        if id is not None and not isinstance(id, str):
            msg = f"id must be `str | None`, got {utils.type_name(id)}"
            raise TypeError(msg)

        if timeout is not None and isinstance(timeout, int | float):
            msg = f"timeout must be `float`, got {utils.type_name(timeout)}"
            raise TypeError(msg)

        if idempotency_key is not None and not isinstance(idempotency_key, str):
            msg = f"idempotency_key must be `str | None`, got {utils.type_name(idempotency_key)}"
            raise TypeError(msg)

        if tags is not None and not isinstance(tags, dict):
            msg = f"tags must be `dict | None`, got {utils.type_name(tags)}"
            raise TypeError(tags)

        default_id = self._next()
        id = id or default_id

        return RFI(
            Base(
                id,
                timeout or 31536000,
                idempotency_key or id,
                data,
                tags,
            ),
        )

    def _next(self) -> str:
        self._counter += 1
        return f"{self._id}.{self._counter}"


class Time:
    def __init__(self, ctx: Context) -> None:
        self._ctx = ctx

    def strftime(self, format: str) -> LFC[str]:
        return self._ctx.lfc(lambda _: self._ctx.get_dependency("resonate:time", time).strftime(format))

    def time(self) -> LFC[float]:
        return self._ctx.lfc(lambda _: self._ctx.get_dependency("resonate:time", time).time())


class Random:
    def __init__(self, ctx: Context) -> None:
        self._ctx = ctx

    def betavariate(self, alpha: float, beta: float) -> LFC[float]:
        return self._ctx.lfc(lambda _: self._ctx.get_dependency("resonate:random", random).betavariate(alpha, beta))

    def choice[T](self, seq: Sequence[T]) -> LFC[T]:
        return self._ctx.lfc(lambda _: self._ctx.get_dependency("resonate:random", random).choice(seq))

    def expovariate(self, lambd: float = 1) -> LFC[float]:
        return self._ctx.lfc(lambda _: self._ctx.get_dependency("resonate:random", random).expovariate(lambd))

    def getrandbits(self, k: int) -> LFC[int]:
        return self._ctx.lfc(lambda _: self._ctx.get_dependency("resonate:random", random).getrandbits(k))

    def randint(self, a: int, b: int) -> LFC[int]:
        return self._ctx.lfc(lambda _: self._ctx.get_dependency("resonate:random", random).randint(a, b))

    def random(self) -> LFC[float]:
        return self._ctx.lfc(lambda _: self._ctx.get_dependency("resonate:random", random).random())

    def randrange(self, start: int, stop: int | None = None, step: int = 1) -> LFC[int]:
        return self._ctx.lfc(lambda _: self._ctx.get_dependency("resonate:random", random).randrange(start, stop, step))

    def triangular(self, low: float = 0, high: float = 1, mode: float | None = None) -> LFC[float]:
        return self._ctx.lfc(lambda _: self._ctx.get_dependency("resonate:random", random).triangular(low, high, mode))


class Function[**P, R]:
    __name__: str
    __type_params__: tuple[TypeVar | ParamSpec | TypeVarTuple, ...] = ()

    def __init__(self, resonate: Resonate, name: str, func: Callable[Concatenate[Context, P], R], opts: Options) -> None:
        # updates the following attributes:
        # __module__
        # __name__
        # __qualname__
        # __doc__
        # __annotations__
        # __type_params__
        # __dict__
        functools.update_wrapper(self, func)

        self._resonate = resonate
        self._name = name
        self._func = func
        self._opts = opts

    @property
    def name(self) -> str:
        return self._name

    @property
    def func(self) -> Callable[Concatenate[Context, P], R]:
        return self._func

    def __call__(self, ctx: Context, *args: P.args, **kwargs: P.kwargs) -> R:
        return self._func(ctx, *args, **kwargs)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, Function):
            return self._func == other._func
        if callable(other):
            return self._func == other
        return NotImplemented

    def __hash__(self) -> int:
        # Helpful for ensuring proper registry lookups, a function and an instance of Function
        # that wraps the same function has the same identity.
        return self._func.__hash__()

    def options(
        self,
        *,
        encoder: Encoder[Any, str | None] | None = None,
        idempotency_key: str | Callable[[str], str] | None = None,
        retry_policy: RetryPolicy | Callable[[Callable], RetryPolicy] | None = None,
        tags: dict[str, str] | None = None,
        target: str | None = None,
        timeout: float | None = None,
        version: int | None = None,
    ) -> Function[P, R]:
        self._opts = self._opts.merge(
            encoder=encoder,
            idempotency_key=idempotency_key,
            retry_policy=retry_policy,
            tags=tags,
            target=target,
            timeout=timeout,
            version=version,
        )
        return self

    def run[T](self: Function[P, Generator[Any, Any, T] | T], id: str, *args: P.args, **kwargs: P.kwargs) -> Handle[T]:
        resonate = self._resonate.options(
            encoder=self._opts.encoder,
            idempotency_key=self._opts.idempotency_key,
            retry_policy=self._opts.retry_policy,
            tags=self._opts.tags,
            target=self._opts.target,
            timeout=self._opts.timeout,
            version=self._opts.version,
        )
        return resonate.run(id, self._func, *args, **kwargs)

    def rpc[T](self: Function[P, Generator[Any, Any, T] | T], id: str, *args: P.args, **kwargs: P.kwargs) -> Handle[T]:
        resonate = self._resonate.options(
            encoder=self._opts.encoder,
            idempotency_key=self._opts.idempotency_key,
            retry_policy=self._opts.retry_policy,
            tags=self._opts.tags,
            target=self._opts.target,
            timeout=self._opts.timeout,
            version=self._opts.version,
        )
        return resonate.rpc(id, self._func, *args, **kwargs)
