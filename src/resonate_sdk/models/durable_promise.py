from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal, final

if TYPE_CHECKING:
    from resonate_sdk.encoder import IEncoder
    from resonate_sdk.stores.traits import IStore


class DurablePromise:
    def __init__(
        self,
        record: DurablePromiseRecord,
        encoder: IEncoder[Any, str | None],
        store: IStore,
    ) -> None:
        self.id = record.id
        self.state = record.state
        self.timeout = record.timeout
        self.param = record.param
        self.value = record.value
        self.ikey_for_create = record.ikey_for_create
        self.ikey_for_complete = record.ikey_for_complete
        self.tags = record.tags or {}
        self.created_on = record.created_on
        self.completed_on = record.completed_on
        self.encoder = encoder
        self.store = store

    def params(self) -> Any:
        return self.encoder.decode(self.param.data)

    def result(self) -> Any:
        return self.encoder.decode(self.value.data)

    @property
    def pending(self) -> bool:
        return self.state == "PENDING"

    @property
    def completed(self) -> bool:
        return not self.pending

    @property
    def resolved(self) -> bool:
        return self.state == "RESOLVED"

    @property
    def rejected(self) -> bool:
        return self.state == "REJECTED"

    @property
    def canceled(self) -> bool:
        return self.state == "REJECTED_CANCELED"

    @property
    def timedout(self) -> bool:
        return self.state == "REJECTED_TIMEDOUT"

    def resolve(self, value: Any) -> None: ...
    def reject(self, value: Any) -> None: ...
    def cancel(self, value: Any) -> None: ...


type State = Literal[
    "PENDING", "RESOLVED", "REJECTED", "REJECTED_CANCELED", "REJECTED_TIMEDOUT"
]


@final
@dataclass(frozen=True)
class DurablePromiseRecord:
    id: str
    state: State
    timeout: int
    param: Value
    value: Value
    ikey_for_create: str | None
    ikey_for_complete: str | None
    tags: dict[str, str] | None
    created_on: int
    completed_on: int | None


@final
@dataclass(frozen=True)
class Value:
    headers: dict[str, str]
    data: str | None
