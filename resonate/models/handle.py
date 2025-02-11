from __future__ import annotations

from concurrent.futures import Future


class Handle[T]:
    def __init__(self, f: Future[T]) -> None:
        self._f = f

    def done(self) -> bool:
        return self._f.done()

    def result(self) -> T:
        return self._f.result()
