from __future__ import annotations

from dataclasses import field
from datetime import timedelta

import msgspec


class Options(msgspec.Struct, frozen=True, kw_only=True):
    tags: dict[str, str] = field(default_factory=dict)
    target: str = field(default="default")
    timeout: timedelta = field(default=timedelta(days=1))
    version: int = field(default=0)


def is_url(s: str) -> bool:
    return "://" in s
