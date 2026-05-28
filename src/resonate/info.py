from __future__ import annotations

import msgspec


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
