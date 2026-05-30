from __future__ import annotations

from typing import TYPE_CHECKING

from resonate.durable import DurableFunction
from resonate.error import AlreadyRegisteredError, ApplicationError

if TYPE_CHECKING:
    from collections.abc import Callable
    from typing import Any


class Registry:
    """Maps a ``(name, version)`` pair to a validated :class:`DurableFunction`.

    Diverges from the Rust/Go reference SDKs, which key purely on ``name``: here
    the same name may be registered at several versions so multiple
    implementations can coexist (e.g. during a rolling deploy). The version is
    explicit -- never "latest" -- so a lookup is deterministic regardless of what
    is registered afterwards: a task records its version in ``TaskData`` at
    create time and resolves the *same* implementation on every replay. (Version
    ``0`` once meant "latest registered", which broke replay the moment a newer
    version was registered; it is now invalid.)

    Names stay explicit -- passed at register time, not derived from the Python
    function -- so they remain stable across renames. The function's shape is
    detected by reflection when it is registered.
    """

    def __init__(self) -> None:
        self._by_key: dict[tuple[str, int], DurableFunction] = {}

    def register(self, name: str, fn: Callable[..., Any], version: int = 1) -> None:
        """Validate ``fn`` and store it under ``(name, version)``.

        Raises :class:`ApplicationError` if ``name`` is empty or ``version`` is
        below ``1``, :class:`AlreadyRegisteredError` if the ``(name, version)``
        pair is already taken, and propagates the validation error from
        :class:`~resonate.durable.DurableFunction` if ``fn`` has an unsupported
        shape.
        """
        if not name:
            msg = "name is required"
            raise ApplicationError(msg)
        if version < 1:
            msg = "version must be >= 1"
            raise ApplicationError(msg)
        key = (name, version)
        if key in self._by_key:
            raise AlreadyRegisteredError(name, version)
        self._by_key[key] = DurableFunction(fn)

    def get(self, name: str, version: int = 1) -> DurableFunction | None:
        return self._by_key.get((name, version))
