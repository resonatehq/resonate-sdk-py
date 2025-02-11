from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal

if TYPE_CHECKING:
    from resonate.models.store import Store


@dataclass
class DurablePromise:
    id: str
    state: Literal[
        "PENDING", "RESOLVED", "REJECTED", "REJECTED_CANCELED", "REJECTED_TIMEDOUT"
    ]
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

    def resolve(self, headers: dict[str, str] | None, data: str | None) -> None:
        resolved = self.store.promises.resolve(
            id=self.id,
            ikey=self.ikey_for_complete,
            strict=False,
            headers=headers,
            data=data,
        )
        self.state = resolved.state
        self.ikey_for_complete = resolved.ikey_for_complete
        self.value = resolved.value
        self.completed_on = resolved.completed_on

    def reject(self, headers: dict[str, str] | None, data: str | None) -> None:
        rejected = self.store.promises.reject(
            id=self.id,
            ikey=self.ikey_for_complete,
            strict=False,
            headers=headers,
            data=data,
        )
        self.state = rejected.state
        self.ikey_for_complete = rejected.ikey_for_complete
        self.value = rejected.value
        self.completed_on = rejected.completed_on

    def cancel(self, headers: dict[str, str] | None, data: str | None) -> None:
        canceled = self.store.promises.cancel(
            id=self.id,
            ikey=self.ikey_for_complete,
            strict=False,
            headers=headers,
            data=data,
        )
        self.state = canceled.state
        self.ikey_for_complete = canceled.ikey_for_complete
        self.value = canceled.value
        self.completed_on = canceled.completed_on

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
