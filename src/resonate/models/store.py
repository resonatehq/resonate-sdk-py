from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal, Protocol, final

if TYPE_CHECKING:
    from resonate.models.durable_promise import DurablePromise
    from resonate.models.encoder import Encoder
    from resonate.models.message import InvokeMesg, ResumeMesg
    from resonate.models.task import Task


class Store(Protocol):
    @property
    def encoder(self) -> Encoder: ...

    @property
    def promises(self) -> PromiseStore: ...

    @property
    def tasks(self) -> TaskStore: ...


class PromiseStore(Protocol):
    def create(
        self,
        *,
        id: str,
        timeout: int,
        ikey: str | None = None,
        strict: bool = False,
        headers: dict[str, str] | None = None,
        data: str | None = None,
        tags: dict[str, str] | None = None,
    ) -> DurablePromise: ...

    def create_with_task(
        self,
        *,
        id: str,
        timeout: int,
        pid: str,
        ttl: int,
        ikey: str | None = None,
        strict: bool = False,
        headers: dict[str, str] | None = None,
        data: str | None = None,
        tags: dict[str, str] | None = None,
    ) -> tuple[DurablePromise, Task | None]: ...

    def resolve(
        self,
        *,
        id: str,
        ikey: str | None = None,
        strict: bool = False,
        headers: dict[str, str] | None = None,
        data: str | None = None,
    ) -> DurablePromise: ...

    def reject(
        self,
        *,
        id: str,
        ikey: str | None = None,
        strict: bool = False,
        headers: dict[str, str] | None = None,
        data: str | None = None,
    ) -> DurablePromise: ...

    def cancel(
        self,
        *,
        id: str,
        ikey: str | None = None,
        strict: bool = False,
        headers: dict[str, str] | None = None,
        data: str | None = None,
    ) -> DurablePromise: ...


class TaskStore(Protocol):
    def claim(
        self,
        *,
        id: str,
        counter: int,
        pid: str,
        ttl: int,
    ) -> InvokeMesg | ResumeMesg: ...

    def complete(
        self,
        *,
        id: str,
        counter: int,
    ) -> None: ...

    def heartbeat(
        self,
        *,
        pid: str,
    ) -> int: ...
