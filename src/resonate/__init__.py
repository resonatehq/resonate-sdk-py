from __future__ import annotations

import time
from typing import Any

#: Protocol version string sent in all requests. Mirrors Rust's ``PROTOCOL_VERSION``.
PROTOCOL_VERSION = "2026-04-01"


def now_ms() -> int:
    """Return the current time in milliseconds since the UNIX epoch."""
    return time.time_ns() // 1_000_000


class DependencyMap:
    """Type-keyed container for application dependencies (DB pools, clients, config)."""

    def __init__(self) -> None:
        self._map: dict[type, Any] = {}

    def insert[T](self, value: T) -> None:
        """Store a dependency, keyed by its concrete type."""
        self._map[type(value)] = value

    def get[T](self, type: type[T]) -> T:
        """Retrieve a dependency by type. Raises ``KeyError`` if not found."""
        try:
            return self._map[type]
        except KeyError:
            msg = (
                f"Dependency `{type.__qualname__}` not found. "
                "Did you forget to call `.with_dependency()`?"
            )
            raise KeyError(msg) from None

    def __repr__(self) -> str:
        return f"DependencyMap(len={len(self._map)})"
