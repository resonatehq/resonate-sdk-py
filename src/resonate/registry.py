from __future__ import annotations

from typing import TYPE_CHECKING

from resonate.durable import DurableFunction
from resonate.error import AlreadyRegisteredError, ApplicationError

if TYPE_CHECKING:
    from collections.abc import Callable
    from typing import Any


class Registry:
    """Maps a task's function name to a validated, prebuilt :class:`DurableFunction`.

    Mirrors Go's ``Registry``: names are explicit -- passed at register time, not
    derived from the Python function -- so they stay stable across renames. The
    function's shape (kind, env injection) is detected by reflection when it is
    registered.
    """

    def __init__(self) -> None:
        self._by_name: dict[str, DurableFunction] = {}

    def register(self, name: str, fn: Callable[..., Any]) -> None:
        """Validate ``fn`` and store it under ``name``.

        Raises :class:`ApplicationError` if ``name`` is empty,
        :class:`AlreadyRegisteredError` if it is already taken, and propagates the
        validation error from :func:`~resonate.durable.DurableFunction` if
        ``fn`` has an unsupported shape.
        """
        if not name:
            msg = "name is required"
            raise ApplicationError(msg)
        if name in self._by_name:
            raise AlreadyRegisteredError(name)
        self._by_name[name] = DurableFunction(fn)

    def get(self, name: str) -> DurableFunction | None:
        return self._by_name.get(name)

    def contains(self, name: str) -> bool:
        return name in self._by_name

    def names(self) -> list[str]:
        return list(self._by_name.keys())

    def __len__(self) -> int:
        return len(self._by_name)
