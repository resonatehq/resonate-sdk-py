from __future__ import annotations

import sys
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from typing_extensions import TypedDict

from resonate.errors import ResonateValidationError
from resonate.models.retry_policies import Never

if TYPE_CHECKING:
    from resonate.models.retry_policies import RetryPolicy


@dataclass(frozen=True)
class Options:
    retry_policy: RetryPolicy = field(default_factory=Never)
    send_to: str = "poll://default"
    tags: dict[str, str] = field(default_factory=dict)
    timeout: int = sys.maxsize
    version: int = 0

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
            send_to=send_to if send_to is not None else self.send_to,
            timeout=timeout if timeout is not None else self.timeout,
            version=version  if version is not None else self.version,
            tags=tags if tags is not None else  self.tags,
            retry_policy=retry_policy if retry_policy is not None else self.retry_policy
        )

    def to_dict(self) -> DictOptions:
        return DictOptions(
            retry_policy=self.retry_policy,
            send_to=self.send_to,
            tags=self.tags,
            timeout=self.timeout,
            version=self.version,
        )


class DictOptions(TypedDict):
    retry_policy: RetryPolicy
    send_to: str
    tags: dict[str, str]
    timeout: int
    version: int
