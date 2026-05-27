from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from resonate import DependencyMap


@dataclass(frozen=True, slots=True)
class Info:
    """Read-only execution metadata for leaf functions.

    Cannot spawn durable sub-tasks -- no run/rpc methods. Mirrors Rust's
    ``Info``: the fields are read-only (Rust exposes them via accessors; the
    frozen dataclass exposes them as attributes), and ``deps`` is shared from
    the owning ``Resonate`` instance.
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
        """Retrieve a dependency by type. Raises ``KeyError`` if not found.

        Mirrors Rust's ``Info::get_dependency``. Where Rust resolves the
        dependency from the ``T`` generic, Python takes the type explicitly as
        ``type`` (the same convention as :meth:`~resonate.codec.Codec.decode`).
        """
        return self.deps.get(type)
