from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, TypeVar, final

from typing_extensions import Self

from resonate.commands import Command, DurablePromise
from resonate.options import Options

if TYPE_CHECKING:
    from resonate.dataclasses import Invocation


T = TypeVar("T")


@final
@dataclass
class RFI:
    unit: Invocation[Any] | DurablePromise
    opts: Options = field(default=Options())

    def options(self, id: str | None = None, send_to: str | None = None) -> Self:
        assert not isinstance(
            self.unit, DurablePromise
        ), "Options must be set on the cmd."
        self.opts = Options(id=id, send_to=send_to)
        return self


@final
@dataclass
class RFC:
    unit: Invocation[Any] | DurablePromise
    opts: Options = field(default=Options())

    def options(self, id: str | None = None, send_to: str | None = None) -> Self:
        assert not isinstance(
            self.unit, DurablePromise
        ), "Options must be set on the cmd."
        self.opts = Options(id=id, send_to=send_to)
        return self

    def to_invocation(self) -> RFI:
        return RFI(self.unit, self.opts)


@final
@dataclass
class LFC:
    unit: Invocation[Any] | Command
    opts: Options = field(default=Options())

    def options(
        self,
        *,
        id: str | None = None,
        durable: bool = True,
    ) -> Self:
        self.opts = Options(durable=durable, id=id)
        return self

    def to_lfi(self) -> LFI:
        return LFI(self.unit, opts=self.opts)


@final
@dataclass
class DI:
    """
    Dataclass that contains all required information to do a
    deferred invocation.
    """

    id: str
    unit: Invocation[Any]
    opts: Options = field(default=Options())

    def options(self) -> Self:
        self.opts = Options(durable=True, id=self.id)
        return self


@final
@dataclass
class LFI:
    unit: Invocation[Any] | Command
    opts: Options = field(default=Options())

    def options(
        self,
        *,
        id: str | None = None,
        durable: bool = True,
    ) -> Self:
        self.opts = Options(durable=durable, id=id)
        return self
