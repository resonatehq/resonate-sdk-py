from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal

if TYPE_CHECKING:
    from resonate.models.store import Store


@dataclass
class DurablePromise:
    id: str
    state: Literal["PENDING", "RESOLVED", "REJECTED", "REJECTED_CANCELED", "REJECTED_TIMEDOUT"]
    timeout: int
    ikey_for_create: str | None
    ikey_for_complete: str | None
    param: DurablePromiseValue
    value: DurablePromiseValue
    tags: dict[str, str]
    created_on: int
    completed_on: int | None

    store: Store

    def params(self) -> Any:
        return self.store.encoder.decode(self.param.data)

    def result(self) -> Any:
        return self.store.encoder.decode(self.value.data)

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

    @classmethod
    def from_dict(cls, store: Store, data: dict[str, Any]) -> DurablePromise:
        return cls(
            store=store,
            id=data["id"],
            state=data["state"],
            timeout=data["timeout"],
            ikey_for_create=data.get("idempotencyKeyForCreate"),
            ikey_for_complete=data.get("idempotencyKeyForComplete"),
            param=DurablePromiseValue.from_dict(data["param"]),
            value=DurablePromiseValue.from_dict(data["value"]),
            tags=data.get("tags", {}),
            created_on=data["createdOn"],
            completed_on=data.get("completedOn"),
        )

@dataclass
class DurablePromiseValue:
    headers: dict[str, str] | None
    data: str | None

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> DurablePromiseValue:
        return cls(headers=data.get("headers"), data=data.get("data"))
