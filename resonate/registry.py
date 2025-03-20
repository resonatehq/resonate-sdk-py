from __future__ import annotations

from collections.abc import Callable
from typing import overload

from resonate import utils

# Registry


class Registry:
    def __init__(self) -> None:
        self._forward_registry: dict[str, dict[int, Callable]] = {}
        self._reverse_registry: dict[Callable, dict[int, str]] = {}

    def add(self, func: Callable, name: str, version: int = 1) -> None:
        utils.validate(version > 0, "version must be greater than 0")
        utils.validate(version not in self._forward_registry.get(name, {}), f"function {func.__name__} already registered under name={name} with version {version}.")
        utils.validate(
            version not in self._reverse_registry.get(func, {}), lambda: f"function {func.__name__} already registered under name={self._reverse_registry[func][version]} with version {version}."
        )

        self._forward_registry.setdefault(name, {})[version] = func
        self._reverse_registry.setdefault(func, {})[version] = name

    @overload
    def get(self, func: str, version: int = 0) -> tuple[Callable, int]: ...
    @overload
    def get(self, func: Callable, version: int = 0) -> tuple[str, int]: ...
    def get(self, func: str | Callable, version: int = 0) -> tuple[Callable | str, int]:
        utils.validate(version >= 0, "version must be greater or equal than 0")
        utils.validate(func in (self._forward_registry if isinstance(func, str) else self._reverse_registry), lambda: f"function={func if isinstance(func, str) else func.__name__} is not registered.")

        versions = self._forward_registry[func] if isinstance(func, str) else self._reverse_registry[func]
        resolved_version = max(versions.keys()) if version == 0 else version

        utils.validate(resolved_version in versions, f"version={version} is not registered for function={func if isinstance(func, str) else func.__name__}")

        return versions[resolved_version], resolved_version

    @overload
    def all(self, func: str) -> dict[int, Callable]: ...
    @overload
    def all(self, func: Callable) -> dict[int, str]: ...
    def all(self, func: str | Callable) -> dict[int, Callable] | dict[int, str]:
        match func:
            case str():
                return self._forward_registry.get(func, {})
            case Callable():
                return self._reverse_registry.get(func, {})

    @overload
    def latest(self, func: str) -> int: ...
    @overload
    def latest(self, func: Callable) -> int: ...
    def latest(self, func: str | Callable) -> int:
        match func:
            case str():
                return max(self._forward_registry[func]) if func in self._forward_registry else 0
            case Callable():
                return max(self._reverse_registry[func]) if func in self._reverse_registry else 0
