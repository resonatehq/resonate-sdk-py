from __future__ import annotations

import uuid
from typing import TYPE_CHECKING, Any, Callable, TypeVar, overload

from typing_extensions import Concatenate, ParamSpec

from resonate import targets
from resonate.scheduler import Scheduler
from resonate.shells.poller import Poller
from resonate.stores.local import IStorage, LocalStore, MemoryStorage
from resonate.stores.remote import RemoteStore
from resonate.tracing.stdout import StdOutAdapter

if TYPE_CHECKING:
    from collections.abc import Coroutine, Generator

    from resonate.context import Context
    from resonate.record import Handle
    from resonate.retry_policy import RetryPolicy
    from resonate.tracing import IAdapter
    from resonate.typing import DurableCoro, DurableFn, Yieldable

P = ParamSpec("P")
T = TypeVar("T")


class Resonate:
    def __init__(
        self,
        url: str | None = None,
        polling_url: str | None = None,
        storage: IStorage | None = None,
        adapter: IAdapter | None = None,
        group: str = "default",
    ) -> None:
        pid = uuid.uuid4().hex
        store: RemoteStore | LocalStore
        if url is not None:
            assert (
                storage is None
            ), "Cannot provide a url and a implementation of `IStorage`"
            store = RemoteStore(url=url)

        else:
            storage = storage if storage is not None else MemoryStorage()
            store = LocalStore(storage)

        adapter = adapter if adapter is not None else StdOutAdapter()
        self._scheduler = Scheduler(pid=pid, store=store, adapter=adapter, group=group)
        if isinstance(store, RemoteStore):
            assert polling_url is not None
            self._poller = Poller(self._scheduler, url=f"{polling_url}/{group}/{pid}")

    @overload
    def register(
        self,
        func: Callable[Concatenate[Context, P], Generator[Yieldable, Any, Any]],
        name: str | None = None,
        retry_policy: RetryPolicy | None = None,
    ) -> None: ...
    @overload
    def register(
        self,
        func: Callable[Concatenate[Context, P], Coroutine[Any, Any, Any]],
        name: str | None = None,
        retry_policy: RetryPolicy | None = None,
    ) -> None: ...
    @overload
    def register(
        self,
        func: Callable[Concatenate[Context, P], Any],
        name: str | None = None,
        retry_policy: RetryPolicy | None = None,
    ) -> None: ...
    def register(
        self,
        func: DurableCoro[P, T] | DurableFn[P, T],
        name: str | None = None,
        retry_policy: RetryPolicy | None = None,
    ) -> None:
        return self._scheduler.register(func=func, name=name, retry_policy=retry_policy)

    def lfi(
        self,
        id: str,
        func: DurableCoro[P, T] | DurableFn[P, T],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Handle[T]:
        return self._scheduler.lfi(id, func, *args, **kwargs)

    def lfc(
        self,
        id: str,
        func: DurableCoro[P, T] | DurableFn[P, T],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> T:
        return self.lfi(id, func, *args, **kwargs).result()

    def rfi(
        self, id: str, func: str, args: tuple[Any, ...], *, target: str | None = None
    ) -> Handle[Any]:
        if target is None:
            target = targets.poll(target="default")
        return self._scheduler.rfi(id, func, args, target=target)

    def rfc(
        self, id: str, func: str, args: tuple[Any, ...], *, target: str | None = None
    ) -> Any:  # noqa: ANN401
        return self.rfi(id, func, args, target=target).result()
