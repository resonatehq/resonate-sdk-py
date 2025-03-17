from __future__ import annotations

from dataclasses import dataclass

from typing_extensions import TypedDict


@dataclass(frozen=True)
class Options:
    send_to: str = "default"
    version: int = -1

    def merge(self, *, send_to: str | None = None, version: int | None = None) -> Options:
        return Options(
            send_to=send_to or self.send_to,
            version=version or self.version,
        )

    def to_dict(self) -> DictOptions:
        return DictOptions(
            send_to=self.send_to,
            version=self.version,
        )

class DictOptions(TypedDict):
    send_to: str
    version: int
