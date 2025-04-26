from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal, Self

from resonate.models.result import Ko, Ok, Result

if TYPE_CHECKING:
    from collections.abc import Generator

    from resonate.models.convention import LConv, RConv
    from resonate.models.retry_policy import RetryPolicy


@dataclass
class LFX:
    conv: LConv

    @property
    def id(self) -> str:
        return self.conv.id

    def options(
        self,
        *,
        id: str | None = None,
        durable: bool | None = None,
        retry_policy: RetryPolicy | None = None,
        tags: dict[str, str] | None = None,
        timeout: int | None = None,
        version: int | None = None,
    ) -> Self:
        self.conv = self.conv.options(id, durable, retry_policy, tags, timeout, version)
        return self


@dataclass
class LFI(LFX):
    pass


@dataclass
class LFC(LFX):
    pass


@dataclass
class RFX:
    conv: RConv

    @property
    def id(self) -> str:
        return self.conv.id

    def options(
        self,
        *,
        id: str | None = None,
        send_to: str | None = None,
        tags: dict[str, str] | None = None,
        timeout: int | None = None,
        version: int | None = None,
    ) -> Self:
        self.conv = self.conv.options(id, send_to, tags, timeout, version)
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


@dataclass
class TRM:
    id: str
    result: Result


@dataclass(frozen=True)
class Promise:
    id: str
    cid: str


type Yieldable = LFI | LFC | RFI | RFC | Promise


class Coroutine:
    def __init__(self, id: str, cid: str, gen: Generator[Yieldable, Any, Any]) -> None:
        self.id = id
        self.cid = cid
        self.gen = gen

        self.done = False
        self.skip = False
        self.next: type[None | AWT] | tuple[type[Result], ...] = type(None)
        self.unyielded: list[AWT | TRM] = []

    def __repr__(self) -> str:
        return f"Coroutine(done={self.done})"

    def send(self, value: None | AWT | Result) -> LFI | RFI | AWT | TRM:
        assert self.done or isinstance(value, self.next), "AWT must follow LFI/RFI. Value must follow AWT."
        assert not self.skip or isinstance(value, AWT), "If skipped, value must be an AWT."

        if self.done:
            # When done, yield all unyielded values to enforce structured concurrency, the final
            # value must be a TRM.

            match self.unyielded:
                case []:
                    raise StopIteration
                case [trm]:
                    assert isinstance(trm, TRM), "Last unyielded value must be a TRM."
                    self.unyielded = []
                    return trm
                case [head, *tail]:
                    self.unyielded = tail
                    return head
        try:
            match value, self.skip:
                case None, _:
                    yielded = next(self.gen)
                case Ok(v), _:
                    yielded = self.gen.send(v)
                case Ko(e), _:
                    yielded = self.gen.throw(e)
                case awt, True:
                    # When skipped, pretend as if the generator yielded a promise
                    self.skip = False
                    yielded = Promise(awt.id, self.cid)
                case awt, False:
                    yielded = self.gen.send(Promise(awt.id, self.cid))

            match yielded:
                case LFI(conv) | RFI(conv, mode="attached"):
                    # LFIs and attached RFIs require an AWT
                    self.next = AWT
                    self.unyielded.append(AWT(conv.id))
                    command = yielded
                case LFC(conv):
                    # LFCs can be converted to an LFI+AWT
                    self.next = AWT
                    self.skip = True
                    command = LFI(conv)
                case RFC(conv):
                    # RFCs can be converted to an RFI+AWT
                    self.next = AWT
                    self.skip = True
                    command = RFI(conv)
                case Promise(id):
                    # When a promise is yielded we can remove it from unyielded
                    self.next = (Ok, Ko)
                    self.unyielded = [y for y in self.unyielded if y.id != id]
                    command = AWT(id)
                case _:
                    assert isinstance(yielded, RFI), "Yielded must be an RFI."
                    assert yielded.mode == "detached", "RFI must be detached."
                    self.next = AWT
                    command = yielded

        except StopIteration as e:
            self.done = True
            self.unyielded.append(TRM(self.id, Ok(e.value)))
            return self.unyielded.pop(0)
        except Exception as e:
            self.done = True
            self.unyielded.append(TRM(self.id, Ko(e)))
            return self.unyielded.pop(0)
        else:
            assert not isinstance(yielded, Promise) or yielded.cid == self.cid, "If promise, cids must match."
            return command
