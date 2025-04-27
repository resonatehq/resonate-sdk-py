from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable

    from resonate.options import Options


@dataclass
class Base:
    headers: dict[str, str] | None
    data: Any
    opts: Options

    @property
    def id(self) -> str:
        return self.opts.id

    @property
    def idempotency_key(self) -> str:
        return self.opts.idempotency_key(self.id) if callable(self.opts.idempotency_key) else self.opts.idempotency_key

    @property
    def timeout(self) -> int:
        return self.opts.timeout

    @property
    def tags(self) -> dict[str, str]:
        return self.opts.tags

    def options(
        self,
        id: str | None,
        idempotency_key: str | Callable[[str], str] | None,
        send_to: str | None,
        tags: dict[str, str] | None,
        timeout: int | None,
        version: int | None,
    ) -> Base:
        self.opts = self.opts.merge(id=id, idempotency_key=idempotency_key, tags=tags, timeout=timeout)
        return self
