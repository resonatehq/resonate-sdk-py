from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
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
    def timeout(self) -> int:
        return self.opts.timeout

    @property
    def tags(self) -> dict[str, str]:
        return self.opts.tags

    def options(self, id: str | None, send_to: str | None, tags: dict[str, str] | None, timeout: int | None, version: int | None) -> Base:
        self.opts = self.opts.merge(id=id, tags=tags, timeout=timeout)
        return self
