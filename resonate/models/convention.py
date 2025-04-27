from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from collections.abc import Callable

    from resonate.models.retry_policy import RetryPolicy
    from resonate.options import Options


class LConv(Protocol):
    @property
    def id(self) -> str: ...
    @property
    def func(self) -> Callable: ...
    @property
    def args(self) -> tuple[Any, ...]: ...
    @property
    def kwargs(self) -> dict[str, Any]: ...
    @property
    def opts(self) -> Options: ...

    def options(
        self,
        durable: bool | None,
        id: str | None,
        idempotency_key: str | Callable[[str], str] | None,
        retry_policy: RetryPolicy | Callable[[Callable], RetryPolicy] | None,
        tags: dict[str, str] | None = None,
        timeout: int | None = None,
        version: int | None = None,
    ) -> LConv: ...


class RConv(Protocol):
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
    def tags(self) -> dict[str, str]: ...

    def options(
        self,
        id: str | None,
        idempotency_key: str | Callable[[str], str] | None,
        send_to: str | None,
        tags: dict[str, str] | None,
        timeout: int | None,
        version: int | None,
    ) -> RConv: ...
