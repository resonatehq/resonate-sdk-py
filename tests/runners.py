from __future__ import annotations

import sys
import uuid
from concurrent.futures import Future
from inspect import isgeneratorfunction
from typing import TYPE_CHECKING, Any, Protocol

from resonate.conventions import Base, Local
from resonate.coroutine import LFC, LFI, RFC, RFI
from resonate.models.commands import (
    CancelPromiseReq,
    CancelPromiseRes,
    Command,
    CreateCallbackReq,
    CreatePromiseReq,
    CreatePromiseRes,
    CreatePromiseWithTaskReq,
    CreatePromiseWithTaskRes,
    Invoke,
    Network,
    Receive,
    RejectPromiseReq,
    RejectPromiseRes,
    ResolvePromiseReq,
    ResolvePromiseRes,
    Resume,
)
from resonate.models.task import Task
from resonate.options import Options
from resonate.registry import Registry
from resonate.resonate import Remote
from resonate.scheduler import Scheduler
from resonate.stores import LocalStore

if TYPE_CHECKING:
    from collections.abc import Callable

    from resonate.models.context import Info


# Context
class Context:
    def __init__(self, registry: Registry) -> None:
        self._registry = registry

    @property
    def id(self) -> str:
        raise NotImplementedError

    @property
    def info(self) -> Info:
        raise NotImplementedError

    def lfi(self, func: str | Callable, *args: Any, **kwargs: Any) -> LFI:
        assert not isinstance(func, str)
        return LFI(Local(uuid.uuid4().hex), func, args, kwargs)

    def lfc(self, func: str | Callable, *args: Any, **kwargs: Any) -> LFC:
        assert not isinstance(func, str)
        return LFC(Local(uuid.uuid4().hex), func, args, kwargs)

    def rfi(self, func: str | Callable, *args: Any, **kwargs: Any) -> RFI:
        assert not isinstance(func, str)
        return RFI(Remote(uuid.uuid4().hex, func.__name__, args, kwargs))

    def rfc(self, func: str | Callable, *args: Any, **kwargs: Any) -> RFC:
        assert not isinstance(func, str)
        return RFC(Remote(uuid.uuid4().hex, func.__name__, args, kwargs))

    def detached(self, func: str | Callable, *args: Any, **kwargs: Any) -> RFI:
        assert not isinstance(func, str)
        return RFI(Remote(uuid.uuid4().hex, func.__name__, args, kwargs), mode="detached")


class LocalContext:
    def __init__(self, registry: Registry) -> None:
        self._registry = registry

    @property
    def id(self) -> str:
        raise NotImplementedError

    @property
    def info(self) -> Info:
        raise NotImplementedError

    def lfi(self, func: str | Callable, *args: Any, **kwargs: Any) -> LFI:
        assert not isinstance(func, str)
        return LFI(Local(uuid.uuid4().hex), func, args, kwargs)

    def lfc(self, func: str | Callable, *args: Any, **kwargs: Any) -> LFC:
        assert not isinstance(func, str)
        return LFC(Local(uuid.uuid4().hex), func, args, kwargs)

    def rfi(self, func: str | Callable, *args: Any, **kwargs: Any) -> LFI:
        assert not isinstance(func, str)
        return LFI(Local(uuid.uuid4().hex), func, args, kwargs)

    def rfc(self, func: str | Callable, *args: Any, **kwargs: Any) -> LFC:
        assert not isinstance(func, str)
        return LFC(Local(uuid.uuid4().hex), func, args, kwargs)

    def detached(self, func: str | Callable, *args: Any, **kwargs: Any) -> LFI:
        assert not isinstance(func, str)
        return LFI(Local(uuid.uuid4().hex), func, args, kwargs)


class RemoteContext:
    def __init__(self, registry: Registry) -> None:
        self._registry = registry

    @property
    def id(self) -> str:
        raise NotImplementedError

    @property
    def info(self) -> Info:
        raise NotImplementedError

    def lfi(self, func: str | Callable, *args: Any, **kwargs: Any) -> RFI:
        assert not isinstance(func, str)
        return RFI(Remote(uuid.uuid4().hex, func.__name__, args, kwargs))

    def lfc(self, func: str | Callable, *args: Any, **kwargs: Any) -> RFC:
        assert not isinstance(func, str)
        return RFC(Remote(uuid.uuid4().hex, func.__name__, args, kwargs))

    def rfi(self, func: str | Callable, *args: Any, **kwargs: Any) -> RFI:
        assert not isinstance(func, str)
        return RFI(Remote(uuid.uuid4().hex, func.__name__, args, kwargs))

    def rfc(self, func: str | Callable, *args: Any, **kwargs: Any) -> RFC:
        assert not isinstance(func, str)
        return RFC(Remote(uuid.uuid4().hex, func.__name__, args, kwargs))

    def detached(self, func: str | Callable, *args: Any, **kwargs: Any) -> RFI:
        assert not isinstance(func, str)
        return RFI(Remote(uuid.uuid4().hex, func.__name__, args, kwargs), mode="detached")


# Runners


class Runner(Protocol):
    def run[**P, R](self, id: str, func: Callable[P, R], *args: P.args, **kwargs: P.kwargs) -> R: ...


class SimpleRunner:
    def run[**P, R](self, id: str, func: Callable[P, R], *args: P.args, **kwargs: P.kwargs) -> R:
        return self._run(func, args, kwargs)

    def _run(self, func: Callable, args: tuple, kwargs: dict) -> Any:
        if not isgeneratorfunction(func):
            return func(*args, **kwargs)

        g = func(LocalContext(Registry()), *args, **kwargs)
        v = None

        try:
            while True:
                match g.send(v):
                    case LFI(_, func, args, kwargs):
                        v = (func, args, kwargs)
                    case LFC(_, func, args, kwargs):
                        v = self._run(func, args, kwargs)
                    case (func, args, kwargs):
                        v = self._run(func, args, kwargs)
        except StopIteration as e:
            return e.value


class ResonateRunner:
    def __init__(self, registry: Registry) -> None:
        # registry
        self.registry = registry

        # store
        self.store = LocalStore()

        # create scheduler and connect store
        self.scheduler = Scheduler(ctx=lambda *_: Context(self.registry))

    def run[**P, R](self, id: str, func: Callable[P, R], *args: P.args, **kwargs: P.kwargs) -> R:
        cmds: list[Command] = []
        time = 0
        future = Future[R]()

        conv = Remote(id, func.__name__, args, kwargs)

        promise, _ = self.store.promises.create_with_task(
            id=conv.id,
            ikey=conv.idempotency_key,
            timeout=conv.timeout,
            headers=conv.headers,
            data=conv.data,
            tags=conv.tags,
            pid=self.scheduler.pid,
            ttl=sys.maxsize,
        )

        cmds.append(Invoke(id, conv, func, args, kwargs, promise=promise))

        while cmds:
            time += 1
            next = self.scheduler.step(cmds.pop(0), future if time == 1 else None)

            for req in next.reqs:
                match req:
                    case Network(_id, cid, CreatePromiseReq(id, timeout, ikey, strict, headers, data, tags)):
                        promise = self.store.promises.create(
                            id=id,
                            timeout=timeout,
                            ikey=ikey,
                            strict=strict,
                            headers=headers,
                            data=data,
                            tags=tags,
                        )
                        cmds.append(Receive(_id, cid, CreatePromiseRes(promise)))

                    case Network(_id, cid, CreatePromiseWithTaskReq(id, timeout, pid, ttl, ikey, strict, headers, data, tags)):
                        promise, task = self.store.promises.create_with_task(
                            id=id,
                            timeout=timeout,
                            pid=pid,
                            ttl=ttl,
                            ikey=ikey,
                            strict=strict,
                            headers=headers,
                            data=data,
                            tags=tags,
                        )
                        cmds.append(Receive(_id, cid, CreatePromiseWithTaskRes(promise, task)))

                    case Network(_id, cid, ResolvePromiseReq(id, ikey, strict, headers, data)):
                        promise = self.store.promises.resolve(
                            id=id,
                            ikey=ikey,
                            strict=strict,
                            headers=headers,
                            data=data,
                        )
                        cmds.append(Receive(_id, cid, ResolvePromiseRes(promise)))

                    case Network(_id, cid, RejectPromiseReq(id, ikey, strict, headers, data)):
                        promise = self.store.promises.reject(
                            id=id,
                            ikey=ikey,
                            strict=strict,
                            headers=headers,
                            data=data,
                        )
                        cmds.append(Receive(_id, cid, RejectPromiseRes(promise)))

                    case Network(_id, cid, CancelPromiseReq(id, ikey, strict, headers, data)):
                        promise = self.store.promises.cancel(
                            id=id,
                            ikey=ikey,
                            strict=strict,
                            headers=headers,
                            data=data,
                        )
                        cmds.append(Receive(_id, cid, CancelPromiseRes(promise)))

                    case Network(_id, cid, CreateCallbackReq(id, promise_id, root_promise_id, timeout, recv)):
                        promise, callback = self.store.promises.callback(
                            id=id,
                            promise_id=promise_id,
                            root_promise_id=root_promise_id,
                            timeout=timeout,
                            recv=recv,
                        )
                        if promise.completed:
                            assert not callback
                            cmds.append(Resume(_id, cid, promise))

                    case _:
                        raise NotImplementedError

            for _, msg in self.store.step():
                match msg:
                    case {"type": "invoke", "task": {"id": id, "counter": counter}}:
                        task = Task(id=id, counter=counter, store=self.store)
                        root, leaf = task.claim(pid=self.scheduler.pid, ttl=sys.maxsize)
                        assert root.pending
                        assert not leaf

                        _, func, version = self.registry.get(root.param.data["func"])

                        cmds.append(
                            Invoke(
                                root.id,
                                Base(
                                    root.id,
                                    root.timeout,
                                    root.ikey_for_create,
                                    root.param.headers,
                                    root.param.data,
                                    root.tags,
                                ),
                                func,
                                root.param.data["args"],
                                root.param.data["kwargs"],
                                Options(version=version),
                                root,
                            )
                        )

                    case {"type": "resume", "task": {"id": id, "counter": counter}}:
                        task = Task(id=id, counter=counter, store=self.store)
                        root, leaf = task.claim(pid=self.scheduler.pid, ttl=sys.maxsize)
                        assert root.pending
                        assert leaf
                        assert leaf.completed

                        cmds.append(
                            Resume(
                                id=leaf.id,
                                cid=root.id,
                                promise=leaf,
                            )
                        )

                    case _:
                        raise NotImplementedError

        return future.result()


class ResonateLFXRunner(ResonateRunner):
    def __init__(self, registry: Registry) -> None:
        self.registry = registry

        # create store
        self.store = LocalStore()

        # create scheduler
        self.scheduler = Scheduler(ctx=lambda *_: LocalContext(self.registry))


class ResonateRFXRunner(ResonateRunner):
    def __init__(self, registry: Registry) -> None:
        self.registry = registry

        # create store
        self.store = LocalStore()

        # create scheduler and connect store
        self.scheduler = Scheduler(ctx=lambda *_: RemoteContext(self.registry))
