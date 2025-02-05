from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


@dataclass(frozen=True)
class ResonateError(Exception):
    msg: str
    code: Literal[
        "UNKNOWN",
        "STORE_PAYLOAD",
        "STORE_UNAUTHORIZED",
        "STORE_FORBIDDEN",
        "STORE_NOT_FOUND",
        "STORE_ALREADY_EXISTS",
        "STORE_ENCODER",
    ]
