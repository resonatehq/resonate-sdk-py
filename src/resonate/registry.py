from __future__ import annotations

from typing import TYPE_CHECKING, Concatenate

from resonate.error import AlreadyRegisteredError

if TYPE_CHECKING:
    from collections.abc import Callable
    from typing import Any

    from resonate.context import Context


class Registry:
    """Maps a ``(name, version)`` pair to the registered callable.

    The version is explicit -- never "latest" -- so a lookup is deterministic
    regardless of what is registered afterwards: a task records its version in
    ``TaskData`` at create time and resolves the *same* implementation on every replay.

    Names stay explicit -- passed at register time, not derived from the Python
    function -- so they remain stable across renames. The callable is stored
    as-is; the ctx-first convention is applied at invocation time (see
    :func:`resonate.durable.invoke`), not checked here.
    """

    def __init__(self) -> None:
        self._by_key: dict[
            tuple[str, int], Callable[Concatenate[Context, ...], Any]
        ] = {}

    def register(
        self, name: str, fn: Callable[Concatenate[Context, ...], Any], version: int = 1
    ) -> None:
        """Store ``fn`` under ``(name, version)``."""
        if not name:
            msg = "name is required"
            raise ValueError(msg)
        if version < 1:
            msg = "version must be >= 1"
            raise ValueError(msg)
        key = (name, version)
        if key in self._by_key:
            raise AlreadyRegisteredError(name, version)
        self._by_key[key] = fn

    def get(
        self, name: str, version: int = 1
    ) -> Callable[Concatenate[Context, ...], Any] | None:
        return self._by_key.get((name, version))
