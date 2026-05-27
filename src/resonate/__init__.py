from __future__ import annotations

from typing import Any


def hello() -> str:
    return "Hello from resonate!"


class DependencyMap:
    """Type-keyed container for application dependencies (DB pools, clients, config).

    Stored on ``Resonate`` and shared with every ``Context`` and ``Info``. All
    dependencies should be added **before** the system starts processing tasks.
    Mirrors Rust's ``DependencyMap`` (``lib.rs``): a map keyed by concrete type.
    """

    def __init__(self) -> None:
        self._map: dict[type, Any] = {}

    def insert[T](self, value: T) -> None:
        """Store a dependency, keyed by its concrete type.

        Mirrors Rust's ``DependencyMap::insert``: where Rust keys by
        ``TypeId::of::<T>()``, Python keys by ``type(value)``.
        """
        self._map[type(value)] = value

    def get[T](self, type: type[T]) -> T:
        """Retrieve a dependency by type. Raises ``KeyError`` if not found.

        Mirrors Rust's ``DependencyMap::get``, which panics on a missing
        dependency. Where Rust resolves the lookup key from the ``T`` generic,
        Python takes the type explicitly as ``type``.
        """
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
