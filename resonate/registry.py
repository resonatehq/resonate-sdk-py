from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable


# Registry


class Registry:
    def __init__(self) -> None:
        self._registry: dict[str, dict[int, Callable]] = {}
        self._reverse_registry: dict[Callable, dict[int, str]] = {}

    def add(self, func: Callable, name: str, version: int = 1) -> None:
        # Check for duplicate name and version in _registry
        if version in self._registry.get(name, {}):
            msg = f"Function {func.__name__} already registered under '{name}' with version {version}."
            raise ValueError(msg)

        # Check if the function is already registered with the same version under a different name
        if version in self._reverse_registry.get(func, {}):
            msg = f"Function {func.__name__} already registered under '{self._reverse_registry[func][version]}' with version {version}."
            raise ValueError(msg)

        self._registry.setdefault(name, {})[version] = func
        self._reverse_registry.setdefault(func, {})[version] = name

    def get(self, name: str, version: int = -1) -> tuple[Callable, int]:
        match version:
            case -1:
                version = max(self._registry[name].keys())
                return self._registry[name][version], version
            case _:
                return self._registry[name][version], version

    def list(self, name: str) -> dict[int, Callable]:
        return self._registry.get(name, {})

    def latest(self, name: str) -> int:
        return max(self._registry[name]) if name in self._registry else 0

    def reverse_lookup(self, func: Callable, version: int = -1) -> tuple[str, int]:
        match version:
            case -1:
                version = max(self._reverse_registry[func].keys())
                return self._reverse_registry[func][version], version
            case _:
                return self._reverse_registry[func][version], version

    def reverse_list(self, func: Callable) -> dict[int, str]:
        return self._reverse_registry.get(func, {})

    def reverse_latest(self, func: Callable) -> int:
        return max(self._reverse_registry[func]) if func in self._reverse_registry else 0
