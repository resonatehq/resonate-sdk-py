from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Literal, Protocol, Self

from resonate import utils
from resonate.models.options import Options

if TYPE_CHECKING:
    from collections.abc import Callable


class Context(Protocol):
    def __init__(self, *args: Any, **kwargs: Any) -> None: ...
    def lfi(self, func: str | Callable, *args: Any, **kwargs: Any) -> LFI | LFC | RFI | RFC: ...
    def lfc(self, func: str | Callable, *args: Any, **kwargs: Any) -> LFI | LFC | RFI | RFC: ...
    def rfi(self, func: str | Callable, *args: Any, **kwargs: Any) -> LFI | LFC | RFI | RFC: ...
    def rfc(self, func: str | Callable, *args: Any, **kwargs: Any) -> LFI | LFC | RFI | RFC: ...
    def detached(self, func: str | Callable, *args: Any, **kwargs: Any) -> LFI | LFC | RFI | RFC: ...


type Yieldable = LFI | LFC | RFI | RFC | AWT


@dataclass
class LFX:
    id: str
    func: Callable[..., Any]
    args: tuple[Any, ...]
    kwargs: dict[str, Any]
    opts: Options = field(default_factory=Options)
    versions: dict[int, Callable] | None = field(default=None)

    def options(self, *, id: str | None = None, send_to: str | None = None, timeout: int | None = None, version: int | None = None) -> Self:
        if version is not None and self.versions is not None:
            utils.validate(version in self.versions, f"version={version} not found.")
            self.func = self.versions[version]

        self.id = id or self.id
        self.opts = self.opts.merge(send_to=send_to, timeout=timeout, version=version)
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
    versions: dict[int, str] | None = field(default=None)

    def options(self, *, id: str | None = None, send_to: str | None = None, timeout: int | None = None, version: int | None = None) -> Self:
        if version is not None and self.versions is not None:
            utils.validate(version in self.versions, f"version={version} not found.")
            self.func = self.versions[version]

        self.id = id or self.id
        self.opts = self.opts.merge(send_to=send_to, timeout=timeout, version=version)
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
