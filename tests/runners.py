from __future__ import annotations

import uuid
from collections.abc import Callable
from concurrent.futures import Future
from inspect import isgeneratorfunction
from typing import Protocol

from resonate.models.commands import Invoke
from resonate.models.context import LFC, LFI, RFC, RFI
from resonate.registry import Registry
from resonate.scheduler import Scheduler
from resonate.stores.local import LocalStore

# Context


class LocalContext:
    def __init__(self, *args, **kwargs):
        pass

    def lfi(self, func: str | Callable, *args, **kwargs) -> LFI:
        assert not isinstance(func, str)
        return LFI(str(uuid.uuid4()), func, args, kwargs)

    def lfc(self, func: str | Callable, *args, **kwargs) -> LFC:
        assert not isinstance(func, str)
        return LFC(str(uuid.uuid4()), func, args, kwargs)

    def rfi(self, func: str | Callable, *args, **kwargs) -> LFI:
        assert not isinstance(func, str)
        return LFI(str(uuid.uuid4()), func, args, kwargs)

    def rfc(self, func: str | Callable, *args, **kwargs) -> LFC:
        assert not isinstance(func, str)
        return LFC(str(uuid.uuid4()), func, args, kwargs)

    def detached(self, func: str | Callable, *args, **kwargs) -> LFI:
        assert not isinstance(func, str)
        return LFI(str(uuid.uuid4()), func, args, kwargs)


class RemoteContext:
    def __init__(self, *args, **kwargs):
        pass

    def lfi(self, func: str | Callable, *args, **kwargs) -> RFI:
        assert not isinstance(func, str)
        return RFI(str(uuid.uuid4()), func.__name__, args, kwargs)

    def lfc(self, func: str | Callable, *args, **kwargs) -> RFC:
        assert not isinstance(func, str)
        return RFC(str(uuid.uuid4()), func.__name__, args, kwargs)

    def rfi(self, func: str | Callable, *args, **kwargs) -> RFI:
        assert not isinstance(func, str)
        return RFI(str(uuid.uuid4()), func.__name__, args, kwargs)

    def rfc(self, func: str | Callable, *args, **kwargs) -> RFC:
        assert not isinstance(func, str)
        return RFC(str(uuid.uuid4()), func.__name__, args, kwargs)

    def detached(self, func: str | Callable, *args, **kwargs) -> RFI:
        assert not isinstance(func, str)
        return RFI(str(uuid.uuid4()), func.__name__, args, kwargs)


# Runners


class Runner(Protocol):
    def run[**P, R](self, id: str, func: Callable[P, R], *args: P.args, **kwargs: P.kwargs) -> R: ...


class SimpleRunner:
    def run[**P, R](self, id: str, func: Callable[P, R], *args: P.args, **kwargs: P.kwargs) -> R:
        return self._run(func, args, kwargs)

    def _run[T](self, func: Callable[..., T], args: tuple, kwargs: dict) -> T:
        if not isgeneratorfunction(func):
            return func(*args, **kwargs)

        g = func(LocalContext(), *args, **kwargs)
        v = None

        try:
            while True:
                match g.send(v):
                    case LFI(_, func, args, kwargs):
                        v = (func, args, kwargs)
                    case LFC(_, func, args, kwargs) | (func, args, kwargs):
                        v = self._run(func, args, kwargs)
        except StopIteration as e:
            return e.value


class ResonateRunner:
    def __init__(self, registry: Registry):
        self.registry = registry

        # create store
        store = LocalStore()

        # create scheduler and connect store
        self.scheduler = Scheduler(registry=registry, store=store)

    def run[**P, R](self, id: str, func: Callable[P, R], *args: P.args, **kwargs: P.kwargs) -> R:
        fp = Future()
        fv = Future[R]()
        self.scheduler.enqueue(Invoke(id, self.registry.reverse_lookup(func), func, args, kwargs), (fp, fv))

        while not fv.done():
            self.scheduler.step()

        return fv.result()


class ResonateLFXRunner:
    def __init__(self, registry: Registry):
        self.registry = registry

        # create scheduler
        self.scheduler = Scheduler(ctx=LocalContext, registry=registry)

    def run[**P, R](self, id: str, func: Callable[P, R], *args: P.args, **kwargs: P.kwargs) -> R:
        fp = Future()
        fv = Future[R]()
        self.scheduler.enqueue(Invoke(id, self.registry.reverse_lookup(func), func, args, kwargs), (fp, fv))

        while not fv.done():
            self.scheduler.step()

        return fv.result()


class ResonateRFXRunner:
    def __init__(self, registry: Registry):
        self.registry = registry

        # create store
        store = LocalStore()

        # create scheduler and connect store
        self.scheduler = Scheduler(ctx=RemoteContext, registry=registry, store=store)

    def run[**P, R](self, id: str, func: Callable[P, R], *args: P.args, **kwargs: P.kwargs) -> R:
        fp = Future()
        fv = Future[R]()
        self.scheduler.enqueue(Invoke(id, self.registry.reverse_lookup(func), func, args, kwargs), (fp, fv))

        while not fv.done():
            self.scheduler.step()

        return fv.result()
