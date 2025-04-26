from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any


@dataclass
class Sleep:
    id: str
    secs: int

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

    def options(self, id: str | None, send_to: str | None, tags: dict[str, str] | None, timeout: int | None, version: int | None) -> Sleep:
        self.id = id or self.id
        return self
