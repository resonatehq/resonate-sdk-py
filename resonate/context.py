from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from resonate.models.context import LFC, LFI, RFC, RFI
from resonate.registry import Function, Registry

if TYPE_CHECKING:
    from resonate.dependencies import Dependencies


class Context:
    def __init__(self, id: str, registry: Registry, deps: Dependencies) -> None:
        self.id = id
        self.deps = deps
        self._counter = 0
        self._registry = registry

    def lfi(self, func: str | Callable, *args: Any, **kwargs: Any) -> LFI:
        self._counter += 1
        return LFI(f"{self.id}.{self._counter}", self._lfi_func(func), args, kwargs)

    def lfc(self, func: str | Callable, *args: Any, **kwargs: Any) -> LFC:
        self._counter += 1
        return LFC(f"{self.id}.{self._counter}", self._lfi_func(func), args, kwargs)

    def rfi(self, func: str | Callable, *args: Any, **kwargs: Any) -> RFI:
        self._counter += 1
        return RFI(f"{self.id}.{self._counter}", self._rfi_func(func), args, kwargs)

    def rfc(self, func: str | Callable, *args: Any, **kwargs: Any) -> RFC:
        self._counter += 1
        return RFC(f"{self.id}.{self._counter}", self._rfi_func(func), args, kwargs)

    def detached(self, func: str | Callable, *args: Any, **kwargs: Any) -> RFI:
        self._counter += 1
        return RFI(f"{self.id}.{self._counter}", self._rfi_func(func), args, kwargs, mode="detached")

    def _lfi_func(self, f: str | Callable) -> Callable:
        match f:
            case str():
                return self._registry.get(f)
            case Function():
                return f.func
            case Callable():
                return f

    def _rfi_func(self, f: str | Callable) -> str:
        match f:
            case str():
                return f
            case Function():
                return f.name
            case Callable():
                return self._registry.reverse_lookup(f)
