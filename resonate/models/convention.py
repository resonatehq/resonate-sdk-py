from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from collections.abc import Callable


class Convention(Protocol):
    @property
    def id(self) -> str: ...
    @property
    def idempotency_key(self) -> str | None: ...
    @property
    def headers(self) -> dict[str, str] | None: ...
    @property
    def data(self) -> Any: ...
    @property
    def timeout(self) -> int: ...
    @property
    def tags(self) -> dict[str, str] | None: ...

    def options(
        self,
        id: str | None = None,
        idempotency_key: str | Callable[[str], str] | None = None,
        target: str | None = None,
        tags: dict[str, str] | None = None,
        timeout: int | None = None,
        version: int | None = None,
    ) -> Convention: ...
