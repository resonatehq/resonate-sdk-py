from __future__ import annotations

import sys
import uuid
from concurrent.futures import Future
from inspect import isgeneratorfunction
from typing import TYPE_CHECKING, Any, Protocol

from resonate.conventions import Local, Sleep
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


class Info:
    def __init__(self) -> None:
        self._attempt = 1

    @property
    def attempt(self) -> int:
        return self._attempt


# Context
class Context:
    def __init__(self, registry: Registry) -> None:
        self._registry = registry

    def lfi(self, func: str | Callable, *args: Any, **kwargs: Any) -> LFI:
        assert not isinstance(func, str)
        return LFI(Local(func, args, kwargs, Options(str(uuid.uuid4())), self._registry))

    def lfc(self, func: str | Callable, *args: Any, **kwargs: Any) -> LFC:
        assert not isinstance(func, str)
        return LFC(Local(func, args, kwargs, Options(str(uuid.uuid4())), self._registry))

    def rfi(self, func: str | Callable, *args: Any, **kwargs: Any) -> RFI:
        assert not isinstance(func, str)
        return RFI(Remote(func, args, kwargs, Options(str(uuid.uuid4())), self._registry))

    def rfc(self, func: str | Callable, *args: Any, **kwargs: Any) -> RFC:
        assert not isinstance(func, str)
        return RFC(Remote(func, args, kwargs, Options(str(uuid.uuid4())), self._registry))

    def detached(self, func: str | Callable, *args: Any, **kwargs: Any) -> RFI:
        assert not isinstance(func, str)
        return RFI(Remote(func, args, kwargs, Options(str(uuid.uuid4())), self._registry), mode="detached")

    def sleep(self, secs: int) -> RFC:
        return RFC(Sleep(str(uuid.uuid4()), secs))

    @property
    def info(self) -> Info:
        return Info()


class LocalContext:
    def __init__(self, registry: Registry) -> None:
        self._registry = registry

    def lfi(self, func: str | Callable, *args: Any, **kwargs: Any) -> LFI:
        assert not isinstance(func, str)
        return LFI(Local(func, args, kwargs, Options(str(uuid.uuid4())), self._registry))

    def lfc(self, func: str | Callable, *args: Any, **kwargs: Any) -> LFC:
        assert not isinstance(func, str)
        return LFC(Local(func, args, kwargs, Options(str(uuid.uuid4())), self._registry))

    def rfi(self, func: str | Callable, *args: Any, **kwargs: Any) -> LFI:
        assert not isinstance(func, str)
        return LFI(Local(func, args, kwargs, Options(str(uuid.uuid4())), self._registry))

    def rfc(self, func: str | Callable, *args: Any, **kwargs: Any) -> LFC:
        assert not isinstance(func, str)
        return LFC(Local(func, args, kwargs, Options(str(uuid.uuid4())), self._registry))

    def detached(self, func: str | Callable, *args: Any, **kwargs: Any) -> LFI:
        assert not isinstance(func, str)
        return LFI(Local(func, args, kwargs, Options(str(uuid.uuid4())), self._registry))

    @property
    def info(self) -> Info:
        return Info()


class RemoteContext:
    def __init__(self, registry: Registry) -> None:
        self._registry = registry

    def lfi(self, func: str | Callable, *args: Any, **kwargs: Any) -> RFI:
        assert not isinstance(func, str)
        return RFI(Remote(func, args, kwargs, Options(str(uuid.uuid4())), self._registry))

    def lfc(self, func: str | Callable, *args: Any, **kwargs: Any) -> RFC:
        assert not isinstance(func, str)
        return RFC(Remote(func, args, kwargs, Options(str(uuid.uuid4())), self._registry))

    def rfi(self, func: str | Callable, *args: Any, **kwargs: Any) -> RFI:
        assert not isinstance(func, str)
        return RFI(Remote(func, args, kwargs, Options(str(uuid.uuid4())), self._registry))

    def rfc(self, func: str | Callable, *args: Any, **kwargs: Any) -> RFC:
        assert not isinstance(func, str)
        return RFC(Remote(func, args, kwargs, Options(str(uuid.uuid4())), self._registry))

    def detached(self, func: str | Callable, *args: Any, **kwargs: Any) -> RFI:
        assert not isinstance(func, str)
        return RFI(Remote(func, args, kwargs, Options(str(uuid.uuid4())), self._registry), mode="detached")

    @property
    def info(self) -> Info:
        return Info()


# Runners


class Runner(Protocol):
    def run[**P, R](self, id: str, func: Callable[P, R], *args: P.args, **kwargs: P.kwargs) -> R: ...


class SimpleRunner:
    def run[**P, R](self, id: str, func: Callable[P, R], *args: P.args, **kwargs: P.kwargs) -> R:
        return self._run(func, args, kwargs)

    def _run[T](self, func: Callable[..., T], args: tuple, kwargs: dict) -> T:
        if not isgeneratorfunction(func):
            return func(*args, **kwargs)

        g = func(LocalContext(Registry()), *args, **kwargs)
        v = None

        try:
            while True:
                match g.send(v):
                    case LFI(conv):
                        v = (conv.func, conv.args, conv.kwargs)
                    case LFC(conv):
                        v = self._run(conv.func, conv.args, conv.kwargs)
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

        self.store.promises.create_with_task(
            id=id,
            timeout=sys.maxsize,
            pid=self.scheduler.pid,
            ttl=sys.maxsize,
        )

        cmds.append(Invoke(id, func, args, kwargs))

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

                        cmds.append(
                            Invoke(
                                root.id,
                                self.registry.get(root.param.data["func"])[0],
                                root.param.data["args"],
                                root.param.data["kwargs"],
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
