from __future__ import annotations

from dataclasses import dataclass
from typing import Any

type Result = Ok | Ko


@dataclass
class Ok:
    value: Any


@dataclass
class Ko:
    value: Any
