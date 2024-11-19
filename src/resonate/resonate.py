from __future__ import annotations

from typing import TYPE_CHECKING, Any, Callable, TypeVar, overload
from uuid import uuid4

from typing_extensions import Concatenate, ParamSpec

from resonate.collections import FunctionRegistry
from resonate.dependencies import Dependencies
from resonate.options import Options
from resonate.scheduler.scheduler import Scheduler
from resonate.scheduler.traits import IScheduler
from resonate.stores.local import LocalStore, MemoryStorage
from resonate.task_sources.poller import Poller
from resonate.task_sources.traits import ITaskSource

if TYPE_CHECKING:
    from collections.abc import Coroutine, Generator

    from resonate.context import Context
    from resonate.record import Handle
    from resonate.scheduler.traits import IScheduler
    from resonate.stores.remote import RemoteStore
    from resonate.task_sources.traits import ITaskSource
    from resonate.typing import DurableCoro, DurableFn, Yieldable

P = ParamSpec("P")
T = TypeVar("T")


class Resonate:
    def __init__(
        self,
        pid: str | None = None,
        store: LocalStore | RemoteStore | None = None,
        scheduler: IScheduler | None = None,
        task_source: ITaskSource | None = None,
    ) -> None:
        self._deps = Dependencies()
        self._fn_registry = FunctionRegistry()
        self.pid = pid if pid is not None else uuid4().hex
        self._scheduler: IScheduler = (
            scheduler
            if scheduler is not None
            else Scheduler(
                fn_registry=self._fn_registry,
                deps=self._deps,
                pid=self.pid,
                store=store if store is not None else LocalStore(MemoryStorage()),
            )
        )

        self._task_source: ITaskSource = (
            task_source
            if task_source is not None
            else Poller(
                scheduler=self._scheduler, url="http://localhost:8002", group="default"
            )
        )

        self._scheduler.set_default_recv(self._task_source.default_recv(pid=self.pid))

    @overload
    def register(
        self,
        func: Callable[
            Concatenate[Context, P],
            Generator[Yieldable, Any, Any],
        ],
        name: str | None = None,
    ) -> None: ...
    @overload
    def register(
        self,
        func: Callable[Concatenate[Context, P], Coroutine[Any, Any, Any]],
        name: str | None = None,
    ) -> None: ...
    @overload
    def register(
        self,
        func: Callable[Concatenate[Context, P], Any],
        name: str | None = None,
    ) -> None: ...
    def register(
        self,
        func: DurableCoro[P, Any] | DurableFn[P, Any],
        name: str | None = None,
    ) -> None:
        if name is None:
            name = func.__name__
        self._fn_registry.add(name, (func, Options(durable=True)))

    def lfi(
        self,
        id: str,
        func: Callable[
            Concatenate[Context, P],
            Generator[Yieldable, Any, T],
        ]
        | Callable[Concatenate[Context, P], T]
        | Callable[Concatenate[Context, P], Coroutine[Any, Any, T]],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Handle[T]:
        return self._scheduler.lfi(id, func, *args, **kwargs)

    def rfi(
        self,
        id: str,
        func: str,
        /,
        *args: Any,  # noqa: ANN401
        **kwargs: Any,  # noqa: ANN401
    ) -> Handle[Any]:
        return self._scheduler.rfi(id, func, *args, **kwargs)
