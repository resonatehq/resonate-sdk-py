from __future__ import annotations

from dataclasses import dataclass
from typing import Union

from typing_extensions import TypeAlias


@dataclass(frozen=True)
class Invoke:
    promise_id: str


@dataclass(frozen=True)
class Resume:
    root_promise_id: str
    leaf_promise_id: str


Task: TypeAlias = Union[Invoke, Resume]
