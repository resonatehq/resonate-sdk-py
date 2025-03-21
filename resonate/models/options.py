from __future__ import annotations

import sys
from dataclasses import dataclass

from typing_extensions import TypedDict

from resonate.errors import ResonateValidationError


@dataclass(frozen=True)
class Options:
    send_to: str = "poller://default"
    timeout: int = sys.maxsize
    version: int = 0

    def __post_init__(self) -> None:
        if not (self.version >= 0):
            msg = "version must be greater or equal than 0"
            raise ResonateValidationError(msg)
        if not (self.timeout >= 0):
            msg = "timeout must be greater or equal than 0"
            raise ResonateValidationError(msg)

    def merge(self, *, send_to: str | None = None, timeout: int | None = None, version: int | None = None) -> Options:
        if version and not (version >= 0):
            msg = "version must be greater or equal than 0"
            raise ResonateValidationError(msg)
        if timeout and not (timeout >= 0):
            msg = "timeout mut be greater or equal than 0"
            raise ResonateValidationError(msg)
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
