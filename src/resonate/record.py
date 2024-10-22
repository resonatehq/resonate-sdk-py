from __future__ import annotations

import json
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, TypedDict, final

from typing_extensions import Self

if TYPE_CHECKING:
    from resonate.encoders import IEncoder
    from resonate.typing import Data, Headers, IdempotencyKey, State, Tags


class Decode(ABC):
    @classmethod
    @abstractmethod
    def decode(
        cls, data: dict[str, Any], encoder: IEncoder[str, str] | None = None
    ) -> Self: ...


class _InvokeInfo(TypedDict):
    func_name: str
    args: list[Any]
    kwargs: dict[str, Any]


@final
@dataclass(frozen=True)
class Param:
    data: Data
    headers: Headers

    def func_data(self) -> _InvokeInfo:
        assert self.data is not None, "Data is needed to get this information."
        data_dict = json.loads(self.data)
        return {
            "func_name": data_dict["func"],
            "args": data_dict["args"],
            "kwargs": data_dict["kwargs"],
        }


@final
@dataclass(frozen=True)
class Value:
    data: Data
    headers: Headers


@final
@dataclass(frozen=True)
class CallbackRecord(Decode):
    callback_id: str
    promise_id: str
    timeout: int
    created_on: int

    @classmethod
    def decode(
        cls, data: dict[str, Any], encoder: IEncoder[str, str] | None = None
    ) -> Self:
        assert encoder is None
        return cls(
            callback_id=data["id"],
            promise_id=data["promiseId"],
            timeout=data["timeout"],
            created_on=data["createdOn"],
        )


@final
@dataclass(frozen=True)
class DurablePromiseRecord(Decode):
    state: State
    promise_id: str
    timeout: int
    param: Param
    value: Value
    created_on: int
    completed_on: int | None
    idempotency_key_for_create: IdempotencyKey
    idempotency_key_for_complete: IdempotencyKey
    tags: Tags

    def is_completed(self) -> bool:
        return not self.is_pending()

    def is_timeout(self) -> bool:
        return self.state == "REJECTED_TIMEDOUT"

    def is_canceled(self) -> bool:
        return self.state == "REJECTED_CANCELED"

    def is_rejected(self) -> bool:
        return self.state == "REJECTED"

    def is_resolved(self) -> bool:
        return self.state == "RESOLVED"

    def is_pending(self) -> bool:
        return self.state == "PENDING"

    @classmethod
    def decode(
        cls, data: dict[str, Any], encoder: IEncoder[str, str] | None = None
    ) -> Self:
        assert encoder is not None, "Enconder needed to decoded durable promise"
        if data["param"]:
            param = Param(
                data=encoder.decode(data["param"]["data"]),
                headers=data["param"].get("headers"),
            )
        else:
            param = Param(data=None, headers=None)

        if data["value"]:
            value = Value(
                data=encoder.decode(data["value"]["data"]),
                headers=data["value"].get("headers"),
            )
        else:
            value = Value(data=None, headers=None)
        return cls(
            promise_id=data["id"],
            state=data["state"],
            param=param,
            value=value,
            timeout=data["timeout"],
            tags=data.get("tags"),
            created_on=data["createdOn"],
            completed_on=data.get("completedOn"),
            idempotency_key_for_complete=data.get("idempotencyKeyForComplete"),
            idempotency_key_for_create=data.get("idempotencyKeyForCreate"),
        )


@final
@dataclass(frozen=True)
class TaskRecord(Decode):
    counter: int
    id: str

    @classmethod
    def decode(
        cls, data: dict[str, Any], encoder: IEncoder[str, str] | None = None
    ) -> Self:
        assert encoder is None
        return cls(counter=data["counter"], id=data["id"])


@final
@dataclass(frozen=True)
class Invoke:
    promise: DurablePromiseRecord


@final
@dataclass(frozen=True)
class Resume:
    root_promise: DurablePromiseRecord
    leaf_promise: DurablePromiseRecord
