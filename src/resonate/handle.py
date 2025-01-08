from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Generic, TypeVar, final

if TYPE_CHECKING:
    from concurrent.futures import Future


T = TypeVar("T")


@final
@dataclass(frozen=True)
class Handle(Generic[T]):
    id: str
    f: Future[T] = field(repr=False)

    def result(self, timeout: float | None = None) -> T:
        return self.f.result(timeout=timeout)
