from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable


# Registry

class Registry:
    def __init__(self) -> None:
        self._registry: dict[str, Callable] = {}
        self._reverse_registry: dict[Callable, str] = {}

    def add(self, name: str, func: Callable) -> None:
        self._registry[name] = func
        self._reverse_registry[func] = name

    def get(self, name: str) -> Callable:
        return self._registry[name]

    def reverse_lookup(self, func: Callable) -> str:
        return self._reverse_registry[func]
