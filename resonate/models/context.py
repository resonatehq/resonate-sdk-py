from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Literal, Protocol, Self

from resonate.errors import ResonateValidationError
from resonate.models.options import Options

if TYPE_CHECKING:
    from collections.abc import Callable

    from resonate.models.retry_policies import RetryPolicy


class Info(Protocol):
    @property
    def attempt(self) -> int: ...

class Context(Protocol):
    def __init__(self, *args: Any, **kwargs: Any) -> None: ...
    def lfi(self, *args: Any, **kwargs: Any) -> LFI | LFC | RFI | RFC: ...
    def lfc(self, *args: Any, **kwargs: Any) -> LFI | LFC | RFI | RFC: ...
    def rfi(self, *args: Any, **kwargs: Any) -> LFI | LFC | RFI | RFC: ...
    def rfc(self, *args: Any, **kwargs: Any) -> LFI | LFC | RFI | RFC: ...
    def detached(self, *args: Any, **kwargs: Any) -> LFI | LFC | RFI | RFC: ...
    @property
    def info(self) ->Info:...


type Yieldable = LFI | LFC | RFI | RFC | AWT


@dataclass
class LFX:
    id: str
    func: Callable[..., Any]
    args: tuple[Any, ...]
    kwargs: dict[str, Any]
    opts: Options = field(default_factory=Options)
    versions: dict[int, Callable] | None = None

    def __post_init__(self) -> None:
        self._initial_timeout = self.opts.timeout

    def options(self, *, id: str | None = None, send_to: str | None = None, timeout: int | None = None, version: int | None = None, tags: dict[str, str] | None = None, retry_policy: RetryPolicy | None = None) -> Self:
        if version is not None and self.versions is not None:
            if version not in self.versions:
                msg = f"version={version} not found."
                raise ResonateValidationError(msg)
            self.func = self.versions[version]

        if timeout is not None:
            timeout = min(self._initial_timeout, timeout)

        self.id = id or self.id
        self.opts = self.opts.merge(send_to=send_to, timeout=timeout, version=version, tags=tags, retry_policy=retry_policy)
        return self


@dataclass
class LFI(LFX):
    pass


@dataclass
class LFC(LFX):
    pass


@dataclass
class RFX:
    id: str
    func: str
    args: tuple[Any, ...]
    kwargs: dict[str, Any]
    opts: Options = field(default_factory=Options)
    versions: set[int] | None = None

    def __post_init__(self) -> None:
        self._initial_timeout = self.opts.timeout

    def options(self, *, id: str | None = None, send_to: str | None = None, timeout: int | None = None, version: int | None = None, tags: dict[str, str] | None = None, retry_policy: RetryPolicy | None = None) -> Self:
        if version is not None and self.versions is not None and version not in self.versions:
            msg = f"version={version} not found."
            raise ResonateValidationError(msg)

        if timeout is not None:
            timeout = min(self._initial_timeout, timeout)

        self.id = id or self.id
        self.opts = self.opts.merge(send_to=send_to, timeout=timeout, version=version, tags=tags, retry_policy=retry_policy)
        return self


@dataclass
class RFI(RFX):
    mode: Literal["attached", "detached"] = "attached"


@dataclass
class RFC(RFX):
    pass


@dataclass
class AWT:
    id: str
    cid: str
