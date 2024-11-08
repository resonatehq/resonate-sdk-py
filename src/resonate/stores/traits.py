from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from resonate.record import (
        DurablePromiseRecord,
    )
    from resonate.typing import Data, Headers, IdempotencyKey, State, Tags


class IPromiseStore(ABC):
    @abstractmethod
    def create(  # noqa: PLR0913
        self,
        *,
        id: str,
        ikey: IdempotencyKey,
        strict: bool,
        headers: Headers,
        data: Data,
        timeout: int,
        tags: Tags,
    ) -> DurablePromiseRecord: ...

    @abstractmethod
    def cancel(
        self,
        *,
        id: str,
        ikey: IdempotencyKey,
        strict: bool,
        headers: Headers,
        data: Data,
    ) -> DurablePromiseRecord: ...

    @abstractmethod
    def resolve(
        self,
        *,
        id: str,
        ikey: IdempotencyKey,
        strict: bool,
        headers: Headers,
        data: Data,
    ) -> DurablePromiseRecord: ...

    @abstractmethod
    def reject(
        self,
        *,
        id: str,
        ikey: IdempotencyKey,
        strict: bool,
        headers: Headers,
        data: Data,
    ) -> DurablePromiseRecord: ...

    @abstractmethod
    def get(self, *, id: str) -> DurablePromiseRecord: ...

    @abstractmethod
    def search(
        self, *, id: str, state: State, tags: Tags, limit: int | None = None
    ) -> list[DurablePromiseRecord]: ...