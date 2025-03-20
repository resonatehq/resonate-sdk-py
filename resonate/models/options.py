from __future__ import annotations

import sys
from dataclasses import dataclass

from typing_extensions import TypedDict

from resonate import utils


@dataclass(frozen=True)
class Options:
    send_to: str = "default"
    timeout: int = sys.maxsize
    version: int = 0

    def __post_init__(self) -> None:
        utils.validate(self.version >= 0, "version must be greater or equal than 0")
        utils.validate(self.timeout >= 0, "timeout must be greater or equal than 0")

    def merge(self, *, send_to: str | None = None, timeout: int | None = None, version: int | None = None) -> Options:
        if version:
            utils.validate(version >= 0, "version must be greater or equal than 0")
        if timeout:
            utils.validate(timeout >= 0, "timeout mut be greater or equal than 0")
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
