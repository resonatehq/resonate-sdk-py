from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Generic, TypeVar, final

if TYPE_CHECKING:
    from resonate.promise import Promise

T = TypeVar("T")


@final
@dataclass
class CmdBuffer(Generic[T]):
    max_length: int
    elements: list[list[T]] = field(init=False, default_factory=list)

    def append(self, e: T, /) -> None:
        if len(self.elements) == 0:
            self.elements.append([e])
        elif len(self.elements[-1]) < self.max_length:
            self.elements[-1].append(e)
        else:
            self.elements.append([e])

    def clear(self) -> None:
        self.elements.clear()


@final
class CommandBuffer(Generic[T]):
    def __init__(self, maxlen: int | None) -> None:
        self._maxlen = maxlen
        self.cmds: list[T] = []
        self.promises: list[Promise[Any]] = []

    def add(self, cmd: T, promise: Promise[Any]) -> None:
        assert len(self.cmds) == len(self.promises)
        assert not self.is_full()
        self.cmds.append(cmd)
        self.promises.append(promise)

    def is_full(self) -> bool:
        if self._maxlen is None:
            return False
        return self._maxlen == len(self.cmds)

    def is_empty(self) -> bool:
        return len(self.cmds) == 0

    def pop_all(self) -> tuple[list[T], list[Promise[Any]]]:
        cmds, self.cmds = self.cmds, []
        promises, self.promises = self.promises, []
        assert len(cmds) == len(promises)
        return cmds, promises
