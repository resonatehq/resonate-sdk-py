from __future__ import annotations

import heapq


class DelayQ[T]:
    def __init__(self) -> None:
        self._delayed: list[tuple[int, T]] = []

    def add(self, item: T, delay: int) -> None:
        heapq.heappush(self._delayed, (delay, item))

    def get(self, time: int) -> list[T]:
        items: list[T] = []
        while self._delayed and self._delayed[0][0] <= time:
            items.append(heapq.heappop(self._delayed)[-1])
        return items
