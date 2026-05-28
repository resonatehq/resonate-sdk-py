from __future__ import annotations

from typing import TYPE_CHECKING

import msgspec

if TYPE_CHECKING:
    from resonate import DependencyMap


class Info(msgspec.Struct, frozen=True, kw_only=True):
    """Read-only execution metadata for leaf functions.

    Cannot spawn durable sub-tasks.
    """

    id: str
    parent_id: str
    origin_id: str
    branch_id: str
    timeout_at: int
    func_name: str
    tags: dict[str, str]
    deps: DependencyMap

    def get_dependency[T](self, type: type[T]) -> T:
        """Retrieve a dependency by type. Raises ``KeyError`` if not found."""
        return self.deps.get(type)
