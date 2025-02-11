from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal

if TYPE_CHECKING:
    from resonate.models.callback import Callback
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
        return self.param.data

    def result(self) -> Any:
        return self.value.data

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
        self._complete(resolved)

    def reject(self, headers: dict[str, str] | None, data: str | None) -> None:
        rejected = self.store.promises.reject(
            id=self.id,
            ikey=self.ikey_for_complete,
            strict=False,
            headers=headers,
            data=data,
        )
        self._complete(rejected)

    def cancel(self, headers: dict[str, str] | None, data: str | None) -> None:
        canceled = self.store.promises.cancel(
            id=self.id,
            ikey=self.ikey_for_complete,
            strict=False,
            headers=headers,
            data=data,
        )
        self._complete(canceled)

    def callback(self, id: str, root_promise_id: str, recv: str) -> Callback | None:
        promise, callback = self.store.promises.callback(
            id=id,
            promise_id=self.id,
            root_promise_id=root_promise_id,
            timeout=self.timeout,
            recv=recv,
        )
        if callback is None:
            self._complete(promise)
        return callback

    def _complete(self, promise: DurablePromise) -> None:
        assert promise.completed
        self.state = promise.state
        self.ikey_for_complete = promise.ikey_for_complete
        self.value = promise.value
        self.completed_on = promise.completed_on

    @classmethod
    def from_dict(
        cls, store: Store, data: dict[str, Any], param: Any, value: Any
    ) -> DurablePromise:
        return cls(
            store=store,
            id=data["id"],
            state=data["state"],
            timeout=data["timeout"],
            ikey_for_create=data.get("idempotencyKeyForCreate"),
            ikey_for_complete=data.get("idempotencyKeyForComplete"),
            param=DurablePromiseValue(data["param"].get("headers"), param),
            value=DurablePromiseValue(data["value"].get("headers"), value),
            tags=data.get("tags", {}),
            created_on=data["createdOn"],
            completed_on=data.get("completedOn"),
        )


@dataclass
class DurablePromiseValue:
    headers: dict[str, str] | None
    data: Any
