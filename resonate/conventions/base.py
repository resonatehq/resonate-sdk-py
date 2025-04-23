from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from resonate.errors.errors import ResonateValidationError
from resonate.options import Options


@dataclass
class Base:
    data: Any
    headers: dict[str, str] | None
    opts: Options = field(default_factory=Options)

    def format(self) -> tuple[Any, dict[str, str], int, dict[str, str] | None]:
        return self.data, self.opts.tags, self.opts.timeout, self.headers

    def options(self, send_to: str | None, tags: dict[str, str] | None, timeout: int | None, version: int | None) -> None:
        self.opts.merge(timeout=timeout, tags=tags)
