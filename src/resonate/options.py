from __future__ import annotations

from typing import final


@final
class Options:
    def __init__(
        self,
        *,
        durable: bool = True,
        id: str | None = None,
    ) -> None:
        self.durable = durable
        self.id = id
