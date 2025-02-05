from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from resonate_sdk.store.models import (
        DurablePromiseRecord,
        InvokeMesg,
        ResumeMesg,
        TaskRecord,
    )


class IPromiseStore(ABC):
    @abstractmethod
    def create(
        self,
        *,
        id: str,
        ikey: str | None,
        strict: bool,
        headers: dict[str, str] | None,
        data: str | None,
        timeout: int,
        tags: dict[str, str] | None,
    ) -> DurablePromiseRecord: ...

    @abstractmethod
    def create_with_task(
        self,
        *,
        id: str,
        ikey: str | None,
        strict: bool,
        headers: dict[str, str] | None,
        data: str | None,
        timeout: int,
        tags: dict[str, str] | None,
        pid: str,
        ttl: int,
    ) -> tuple[DurablePromiseRecord, TaskRecord | None]: ...
    @abstractmethod
    def resolve(
        self,
        *,
        id: str,
        ikey: str | None,
        strict: bool,
        headers: dict[str, str] | None,
        data: str | None,
    ) -> DurablePromiseRecord: ...

    @abstractmethod
    def reject(
        self,
        *,
        id: str,
        ikey: str | None,
        strict: bool,
        headers: dict[str, str] | None,
        data: str | None,
    ) -> DurablePromiseRecord: ...

    @abstractmethod
    def cancel(
        self,
        *,
        id: str,
        ikey: str | None,
        strict: bool,
        headers: dict[str, str] | None,
        data: str | None,
    ) -> DurablePromiseRecord: ...


class ITaskStore(ABC):
    @abstractmethod
    def claim(
        self, *, id: str, counter: int, pid: str, ttl: int
    ) -> InvokeMesg | ResumeMesg: ...

    @abstractmethod
    def complete(self, *, id: str, counter: int) -> None: ...

    @abstractmethod
    def heartbeat(self, *, pid: str) -> int: ...


class IStore(ABC):
    @property
    @abstractmethod
    def promises(self) -> IPromiseStore: ...

    @property
    @abstractmethod
    def tasks(self) -> ITaskStore: ...
