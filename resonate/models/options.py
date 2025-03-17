from __future__ import annotations

from dataclasses import dataclass
import sys

from typing_extensions import TypedDict


@dataclass(frozen=True)
class Options:
    send_to: str = "default"
    version: int = -1
    timeout: int = sys.maxsize

    def merge(self, *, send_to: str | None = None, version: int | None = None, timeout: int | None = None) -> Options:
        return Options(
            send_to=send_to or self.send_to,
            version=version or self.version,
            timeout=timeout or self.timeout,
        )

    def to_dict(self) -> DictOptions:
        return DictOptions(
            send_to=self.send_to,
            version=self.version,
            timeout=self.timeout,
        )


class DictOptions(TypedDict):
    send_to: str
    version: int
    timeout: int
