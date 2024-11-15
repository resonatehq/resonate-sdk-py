from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, TypeVar, final

from typing_extensions import Self

from resonate.commands import Command, CreateDurablePromiseReq
from resonate.options import Options

if TYPE_CHECKING:
    from resonate.dataclasses import Invocation


T = TypeVar("T")


@final
@dataclass
class RFI:
    exec_unit: (
        Invocation[Any]
        | tuple[str, tuple[Any, ...], dict[str, Any]]
        | CreateDurablePromiseReq
    )
    opts: Options = field(default=Options())

    def options(self, id: str | None = None) -> Self:
        assert not isinstance(
            self.exec_unit, CreateDurablePromiseReq
        ), "Options must be set on the cmd."
        self.opts = Options(id=id)
        return self


@final
@dataclass
class RFC:
    exec_unit: (
        Invocation[Any]
        | tuple[str, tuple[Any, ...], dict[str, Any]]
        | CreateDurablePromiseReq
    )
    opts: Options = field(default=Options())

    def options(self, id: str | None = None) -> Self:
        assert not isinstance(
            self.exec_unit, CreateDurablePromiseReq
        ), "Options must be set on the cmd."
        self.opts = Options(id=id)
        return self

    def to_invocation(self) -> RFI:
        return RFI(self.exec_unit, self.opts)


@final
@dataclass
class LFC:
    exec_unit: Invocation[Any] | Command
    opts: Options = field(default=Options())

    def options(
        self,
        *,
        id: str | None = None,
        durable: bool = True,
    ) -> Self:
        self.opts = Options(durable=durable, id=id)
        return self

    def to_invocation(self) -> LFI:
        return LFI(self.exec_unit, opts=self.opts)


@final
@dataclass
class DI:
    """
    Dataclass that contains all required information to do a
    deferred invocation.
    """

    id: str
    coro: Invocation[Any]
    opts: Options = field(default=Options())

    def options(self) -> Self:
        self.opts = Options(durable=True, id=self.id)
        return self


@final
@dataclass
class LFI:
    exec_unit: Invocation[Any] | Command
    opts: Options = field(default=Options())

    def options(
        self,
        *,
        id: str | None = None,
        durable: bool = True,
    ) -> Self:
        self.opts = Options(durable=durable, id=id)
        return self
