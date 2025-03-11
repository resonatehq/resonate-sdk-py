from __future__ import annotations

from typing import Any


class Dependencies:
    def __init__(self) -> None:
        self._deps: dict[str, Any] = {}

    def add(self, key: str, obj: Any) -> None:
        if key in self._deps:
            msg = f"key={key} is already taken."
            raise KeyError(msg)
        self._deps[key] = obj

    def get(self, key: str) -> Any:
        return self._deps[key]
