from __future__ import annotations

import sys
from dataclasses import dataclass
from typing import TYPE_CHECKING

from typing_extensions import TypedDict

from resonate.errors import ResonateValidationError

if TYPE_CHECKING:
    from resonate.models.retry_policies import RetryPolicy


@dataclass(frozen=True)
class Options:
    send_to: str = "poller://default"
    timeout: int = sys.maxsize
    version: int = 0
    tags: dict[str, str] | None = None
    retry_policy: RetryPolicy | None = None

    def __post_init__(self) -> None:
        if not (self.version >= 0):
            msg = "version must be greater or equal than 0"
            raise ResonateValidationError(msg)
        if not (self.timeout >= 0):
            msg = "timeout must be greater or equal than 0"
            raise ResonateValidationError(msg)

    def merge(self, *, send_to: str | None = None, timeout: int | None = None, version: int | None = None, tags: dict[str, str] | None = None, retry_policy: RetryPolicy | None = None) -> Options:
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
            tags=tags or self.tags,
            retry_policy=retry_policy or self.retry_policy
        )

    def to_dict(self) -> DictOptions:
        return DictOptions(
            send_to=self.send_to,
            timeout=self.timeout,
            version=self.version,
            tags=self.tags,
            retry_policy=self.retry_policy
        )


class DictOptions(TypedDict):
    send_to: str
    timeout: int
    version: int
    tags: dict[str, str] | None
    retry_policy: RetryPolicy | None
