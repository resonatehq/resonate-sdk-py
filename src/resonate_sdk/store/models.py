from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, final

if TYPE_CHECKING:
    from resonate_sdk.typing import State


@final
@dataclass(frozen=True)
class Value:
    data: str | None
    headers: dict[str, str]


@final
@dataclass(frozen=True)
class DurablePromiseRecord:
    state: State
    id: str
    timeout: int
    param: Value
    value: Value
    created_on: int
    completed_on: int | None
    ikey_for_create: str | None
    ikey_for_complete: str | None
    tags: dict[str, str] | None


@final
@dataclass(frozen=True)
class InvokeMesg:
    root: DurablePromiseRecord


@final
@dataclass(frozen=True)
class ResumeMesg:
    root: DurablePromiseRecord
    leaf: DurablePromiseRecord


@final
@dataclass(frozen=True)
class TaskRecord:
    id: str
    counter: int
