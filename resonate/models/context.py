from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Literal, Protocol, Self


class Contextual(Protocol):
    def __init__(self, *args: Any, **kwargs: Any) -> None: ...
    def lfi(self, func: str | Callable, *args: Any, **kwargs: Any) -> Yieldable: ...
    def lfc(self, func: str | Callable, *args: Any, **kwargs: Any) -> Yieldable: ...
    def rfi(self, func: str | Callable, *args: Any, **kwargs: Any) -> Yieldable: ...
    def rfc(self, func: str | Callable, *args: Any, **kwargs: Any) -> Yieldable: ...
    def detached(self, func: str | Callable, *args: Any, **kwargs: Any) -> Yieldable: ...


type Yieldable = LFI | LFC | RFI | RFC | AWT


@dataclass
class LFX:
    id: str
    func: Callable[..., Any]
    args: tuple[Any, ...]
    kwargs: dict[str, Any]
    opts: dict[str, Any] = field(default_factory=dict)

    def options(self, **opts: Any) -> Self:
        self.id = opts.get("id", self.id)  # quite possibly too clever
        self.opts = opts

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
    opts: dict[str, Any] = field(default_factory=dict)

    def options(self, **opts: Any) -> Self:
        self.id = opts.get("id", self.id)  # quite possibly too clever
        self.opts = opts

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
