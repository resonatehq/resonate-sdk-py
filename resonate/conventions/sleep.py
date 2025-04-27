from __future__ import annotations

import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable


@dataclass
class Sleep:
    id: str
    secs: int

    @property
    def idempotency_key(self) -> str:
        return self.id

    @property
    def headers(self) -> dict[str, str] | None:
        return None

    @property
    def timeout(self) -> int:
        return int((time.time() + self.secs) * 1000)

    @property
    def data(self) -> Any:
        return None

    @property
    def tags(self) -> dict[str, str]:
        return {"resonate:timeout": "true"}

    def options(
        self,
        id: str | None,
        idempotency_key: str | Callable[[str], str] | None,
        send_to: str | None,
        tags: dict[str, str] | None,
        timeout: int | None,
        version: int | None,
    ) -> Sleep:
        self.id = id or self.id
        return self
