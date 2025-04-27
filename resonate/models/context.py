from __future__ import annotations

from typing import Protocol


class Context(Protocol):
    pass


class Info(Protocol):
    @property
    def idempotency_key(self) -> str: ...
    @property
    def timeout(self) -> int: ...
    @property
    def tags(self) -> dict[str, str]: ...
    @property
    def attempt(self) -> int: ...
