from __future__ import annotations

from dataclasses import dataclass, field
from datetime import timedelta


@dataclass(frozen=True, slots=True)
class Options:
    tags: dict[str, str] = field(default_factory=dict)
    target: str = field(default="default")
    timeout: timedelta = field(default=timedelta(days=1))
    version: int = field(default=0)


def is_url(s: str) -> bool:
    return "://" in s
