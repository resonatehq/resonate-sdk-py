from __future__ import annotations

from collections.abc import Callable
from typing import overload

# Registry


class Registry:
    def __init__(self) -> None:
        self._forward_registry: dict[str, dict[int, Callable]] = {}
        self._reverse_registry: dict[Callable, dict[int, str]] = {}

    def add(self, func: Callable, name: str, version: int = 1) -> None:
        # validate version > 0

        # Check for duplicate name and version in _registry
        if version in self._forward_registry.get(name, {}):
            msg = f"Function {func.__name__} already registered under '{name}' with version {version}."
            raise ValueError(msg)

        # Check if the function is already registered with the same version under a different name
        if version in self._reverse_registry.get(func, {}):
            msg = f"Function {func.__name__} already registered under '{self._reverse_registry[func][version]}' with version {version}."
            raise ValueError(msg)

        self._forward_registry.setdefault(name, {})[version] = func
        self._reverse_registry.setdefault(func, {})[version] = name

    @overload
    def get(self, func: str, version: int = 0) -> tuple[Callable, int]:...
    @overload
    def get(self, func: Callable, version: int = 0) -> tuple[str, int]:...
    def get(self, func: str | Callable, version: int = 0) -> tuple[Callable | str, int]:
        # validate func/version in registry

        match func:
            case str():
                match version:
                    case 0:
                        version = max(self._forward_registry[func].keys())
                        return self._forward_registry[func][version], version
                    case _:
                        return self._forward_registry[func][version], version

            case Callable():
                match version:
                    case 0:
                        version = max(self._reverse_registry[func].keys())
                        return self._reverse_registry[func][version], version
                    case _:
                        return self._reverse_registry[func][version], version

    @overload
    def all(self, func: str) -> dict[int, Callable]:...
    @overload
    def all(self, func: Callable) -> dict[int, str]:...
    def all(self, func: str | Callable) -> dict[int, Callable] | dict[int, str]:
        match func:
            case str():
                return self._forward_registry.get(func, {})
            case Callable():
                return self._reverse_registry.get(func, {})

    @overload
    def latest(self, func: str) -> int:...
    @overload
    def latest(self, func: Callable) -> int:...
    def latest(self, func: str | Callable) -> int:
        match func:
            case str():
                return max(self._forward_registry[func]) if func in self._forward_registry else 0
            case Callable():
                return max(self._reverse_registry[func]) if func in self._reverse_registry else 0
