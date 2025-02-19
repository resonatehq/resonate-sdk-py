from __future__ import annotations

from dataclasses import dataclass
from typing import Any

type Result[T] = Ok[T] | Ko


@dataclass
class Ok[T]:
    value: T


@dataclass
class Ko:
    value: Any
