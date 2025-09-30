from __future__ import annotations

import queue
import asyncio
from typing import TYPE_CHECKING
from typing_extensions import Callable
from resonate.models.commands import Command, Listen

if TYPE_CHECKING:
    from collections.abc import Generator
    from concurrent.futures import Future


class Handle[T]:
    def __init__(self, id: str, f: Future[T], s: Callable[[Future], None]) -> None:
        self._id = id
        self._f = f
        self._s = s
        self._subscribed = self._f.done()

    @property
    def id(self) -> str:
        return self._id

    @property
    def future(self) -> Future[T]:
        return self._f

    def _subscribe(self) -> None:
        print("subscribing")
        self._s(self._f)
        self._subscribed = True

    def done(self) -> bool:
        if not self._subscribed:
            self._subscribe()
        return self._f.done()

    def result(self, timeout: float | None = None) -> T:
        if not self._subscribed:
            self._subscribe()
        return self._f.result(timeout)

    def __await__(self) -> Generator[None, None, T]:
        if not self._subscribed:
            self._subscribe()
        return asyncio.wrap_future(self._f).__await__()
