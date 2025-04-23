from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any

from resonate.options import Options


@dataclass
class Sleep:
    secs: int
    opts: Options = field(default_factory=Options)

    @property
    def data(self) -> Any:
        return None

    @property
    def tags(self) -> dict[str, str]:
        return {"resonate:timeout": "true"}

    @property
    def timeout(self) -> int:
        return int((time.time() + self.secs) * 1000)

    @property
    def headers(self) -> dict[str, str] | None:
        return None

    def options(self, send_to: str | None, tags: dict[str, str] | None, timeout: int | None, version: int | None) -> None:
        return
