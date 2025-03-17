from __future__ import annotations

import sys
import uuid
from concurrent.futures import Future
from inspect import isgeneratorfunction
from typing import TYPE_CHECKING, Any, Protocol

from resonate.models.commands import (
    CancelPromiseReq,
    CancelPromiseRes,
    CreateCallbackReq,
    CreateCallbackRes,
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
from resonate.models.context import LFC, LFI, RFC, RFI
from resonate.models.task import Task
from resonate.scheduler import Scheduler
from resonate.stores.local import LocalStore

if TYPE_CHECKING:
    from collections.abc import Callable

    from resonate.registry import Registry

# Context

class Context:
    def lfi(self, func: str | Callable, *args: Any, **kwargs: Any) -> LFI:
        assert not isinstance(func, str)
        return LFI(str(uuid.uuid4()), func, args, kwargs)

    def lfc(self, func: str | Callable, *args: Any, **kwargs: Any) -> LFC:
        assert not isinstance(func, str)
        return LFC(str(uuid.uuid4()), func, args, kwargs)

    def rfi(self, func: str | Callable, *args: Any, **kwargs: Any) -> RFI:
        assert not isinstance(func, str)
        return RFI(str(uuid.uuid4()), func.__name__, args, kwargs)

    def rfc(self, func: str | Callable, *args: Any, **kwargs: Any) -> RFC:
        assert not isinstance(func, str)
        return RFC(str(uuid.uuid4()), func.__name__, args, kwargs)

    def detached(self, func: str | Callable, *args: Any, **kwargs: Any) -> RFI:
        assert not isinstance(func, str)
        return RFI(str(uuid.uuid4()), func.__name__, args, kwargs)


class LocalContext:
    def lfi(self, func: str | Callable, *args: Any, **kwargs: Any) -> LFI:
        assert not isinstance(func, str)
        return LFI(str(uuid.uuid4()), func, args, kwargs)

    def lfc(self, func: str | Callable, *args: Any, **kwargs: Any) -> LFC:
        assert not isinstance(func, str)
        return LFC(str(uuid.uuid4()), func, args, kwargs)

    def rfi(self, func: str | Callable, *args: Any, **kwargs: Any) -> LFI:
        assert not isinstance(func, str)
        return LFI(str(uuid.uuid4()), func, args, kwargs)

    def rfc(self, func: str | Callable, *args: Any, **kwargs: Any) -> LFC:
        assert not isinstance(func, str)
        return LFC(str(uuid.uuid4()), func, args, kwargs)

    def detached(self, func: str | Callable, *args: Any, **kwargs: Any) -> LFI:
        assert not isinstance(func, str)
        return LFI(str(uuid.uuid4()), func, args, kwargs)


class RemoteContext:
    def lfi(self, func: str | Callable, *args: Any, **kwargs: Any) -> RFI:
        assert not isinstance(func, str)
        return RFI(str(uuid.uuid4()), func.__name__, args, kwargs)

    def lfc(self, func: str | Callable, *args: Any, **kwargs: Any) -> RFC:
        assert not isinstance(func, str)
        return RFC(str(uuid.uuid4()), func.__name__, args, kwargs)

    def rfi(self, func: str | Callable, *args: Any, **kwargs: Any) -> RFI:
        assert not isinstance(func, str)
        return RFI(str(uuid.uuid4()), func.__name__, args, kwargs)

    def rfc(self, func: str | Callable, *args: Any, **kwargs: Any) -> RFC:
        assert not isinstance(func, str)
        return RFC(str(uuid.uuid4()), func.__name__, args, kwargs)

    def detached(self, func: str | Callable, *args: Any, **kwargs: Any) -> RFI:
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
    def __init__(self, registry: Registry) -> None:
        # registry
        self.registry = registry

        # store
        self.store = LocalStore()

        # create scheduler and connect store
        self.scheduler = Scheduler(ctx=lambda _: Context())

    def run[**P, R](self, id: str, func: Callable[P, R], *args: P.args, **kwargs: P.kwargs) -> R:
        time = 0

        fp = Future()
        fv = Future[R]()
        self.scheduler.enqueue(Invoke(id, self.registry.reverse_lookup(func), func, args, kwargs), (fp, fv))

        while not fv.done():
            time += 1

            for req in self.scheduler.step(time):
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
                        self.scheduler.enqueue(Receive(_id, cid, CreatePromiseRes(promise)))

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
                        self.scheduler.enqueue(Receive(_id, cid, CreatePromiseWithTaskRes(promise, task)))

                    case Network(_id, cid, ResolvePromiseReq(id, ikey, strict, headers, data)):
                        promise = self.store.promises.resolve(
                            id=id,
                            ikey=ikey,
                            strict=strict,
                            headers=headers,
                            data=data,
                        )
                        self.scheduler.enqueue(Receive(_id, cid, ResolvePromiseRes(promise)))

                    case Network(_id, cid, RejectPromiseReq(id, ikey, strict, headers, data)):
                        promise = self.store.promises.reject(
                            id=id,
                            ikey=ikey,
                            strict=strict,
                            headers=headers,
                            data=data,
                        )
                        self.scheduler.enqueue(Receive(_id, cid, RejectPromiseRes(promise)))

                    case Network(_id, cid, CancelPromiseReq(id, ikey, strict, headers, data)):
                        promise = self.store.promises.cancel(
                            id=id,
                            ikey=ikey,
                            strict=strict,
                            headers=headers,
                            data=data,
                        )
                        self.scheduler.enqueue(Receive(_id, cid, CancelPromiseRes(promise)))

                    case Network(_id, cid, CreateCallbackReq(id, promise_id, root_promise_id, timeout, recv)):
                        promise, callback = self.store.promises.callback(
                            id=id,
                            promise_id=promise_id,
                            root_promise_id=root_promise_id,
                            timeout=timeout,
                            recv=recv,
                        )
                        self.scheduler.enqueue(Receive(_id, cid, CreateCallbackRes(promise, callback)))

                    case _:
                        raise NotImplementedError

            for _, msg in self.store.step():
                match msg:
                    case {"type": "invoke", "task": {"id": id, "counter": counter}}:
                        task = Task(id=id, counter=counter, store=self.store)
                        root, leaf = task.claim(pid=self.scheduler.pid, ttl=sys.maxsize)
                        assert root.pending
                        assert not leaf

                        self.scheduler.enqueue(Invoke(
                            root.id,
                            root.param.data["func"],
                            self.registry.get(root.param.data["func"]),
                            root.param.data["args"],
                            root.param.data["kwargs"],
                            {},
                            (root, task),
                        ))

                    case {"type": "resume", "task": {"id": id, "counter": counter}}:
                        task = Task(id=id, counter=counter, store=self.store)
                        root, leaf = task.claim(pid=self.scheduler.pid, ttl=sys.maxsize)
                        assert root.pending
                        assert leaf
                        assert leaf.completed

                        self.scheduler.enqueue(Resume(
                            id=leaf.id,
                            cid=root.id,
                            promise=leaf,
                            task=task,
                            invoke=Invoke(
                                root.id,
                                root.param.data["func"],
                                self.registry.get(root.param.data["func"]),
                                root.param.data["args"],
                                root.param.data["kwargs"],
                                {},
                                (root, task),
                            ),
                        ))

                    case _:
                        raise NotImplementedError

        return fv.result()


class ResonateLFXRunner(ResonateRunner):
    def __init__(self, registry: Registry) -> None:
        self.registry = registry

        # create store
        self.store = LocalStore()

        # create scheduler
        self.scheduler = Scheduler(ctx=lambda _: LocalContext())


class ResonateRFXRunner(ResonateRunner):
    def __init__(self, registry: Registry) -> None:
        self.registry = registry

        # create store
        self.store = LocalStore()

        # create scheduler and connect store
        self.scheduler = Scheduler(ctx=lambda _: RemoteContext())
