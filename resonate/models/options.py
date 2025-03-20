from __future__ import annotations

import sys
from dataclasses import dataclass

from typing_extensions import TypedDict


@dataclass(frozen=True)
class Options:
    send_to: str = "default"
    timeout: int = sys.maxsize
    version: int = 0

    def merge(self, *, send_to: str | None = None, timeout: int | None = None, version: int | None = None) -> Options:
        return Options(
            send_to=send_to or self.send_to,
            timeout=timeout or self.timeout,
            version=version or self.version,
        )

    def to_dict(self) -> DictOptions:
        return DictOptions(
            send_to=self.send_to,
            timeout=self.timeout,
            version=self.version,
        )


class DictOptions(TypedDict):
    send_to: str
    timeout: int
    version: int
