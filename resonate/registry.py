from __future__ import annotations

import inspect
from collections.abc import Callable
from typing import overload

from resonate.errors import ResonateRegistryError


class Registry:
    def __init__(self) -> None:
        self._forward_registry: dict[str, dict[int, tuple[str, Callable, int]]] = {}
        self._reverse_registry: dict[Callable, tuple[str, Callable, int]] = {}

    def add(self, func: Callable, name: str | None = None, version: int = 1) -> None:
        if not inspect.isfunction(func):
            msg = "provided callable must be a function"
            raise ValueError(msg)

        if not name and func.__name__ == "<lambda>":
            raise ResonateRegistryError("Function name is required when registering a lambda function", 1001, "REGISTRY_NAME_REQUIRED")

        if not version > 0:
            raise ResonateRegistryError(f"Function version must be greater than zero ({version} provided)", 1000, "REGISTRY_VERSION_INVALID")

        name = name or func.__name__
        if version in self._forward_registry.get(name, {}) or func in self._reverse_registry:
            raise ResonateRegistryError(f"Function '{name}' (version {version}) is already registered", 1002, "REGISTRY_FUNCTION_ALREADY_REGISTERED")

        item = (name, func, version)
        self._forward_registry.setdefault(name, {})[version] = item
        self._reverse_registry[func] = item

    @overload
    def get(self, func: str, version: int = 0) -> tuple[str, Callable, int]: ...
    @overload
    def get(self, func: Callable, version: int = 0) -> tuple[str, Callable, int]: ...
    def get(self, func: str | Callable, version: int = 0) -> tuple[str, Callable, int]:
        func_name = func if isinstance(func, str) else getattr(func, '__name__', 'unknown')
        if func not in (self._forward_registry if isinstance(func, str) else self._reverse_registry):
            raise ResonateRegistryError(f"Function '{func_name}' is not registered. Will drop.", 1003, "REGISTRY_FUNCTION_NOT_REGISTERED")

        if version != 0 and version not in (self._forward_registry[func] if isinstance(func, str) else (self._reverse_registry[func][2],)):
            raise ResonateRegistryError(f"Function '{func_name}' (version {version}) is not registered. Will drop.", 1003, "REGISTRY_FUNCTION_NOT_REGISTERED")

        match func:
            case str():
                vers = max(self._forward_registry[func]) if version == 0 else version
                return self._forward_registry[func][vers]

            case Callable():
                return self._reverse_registry[func]

    @overload
    def latest(self, func: str, default: int = 1) -> int: ...
    @overload
    def latest(self, func: Callable, default: int = 1) -> int: ...
    def latest(self, func: str | Callable, default: int = 1) -> int:
        match func:
            case str():
                return max(self._forward_registry.get(func, [default]))
            case Callable():
                _, _, version = self._reverse_registry.get(func, (None, None, default))
                return version
