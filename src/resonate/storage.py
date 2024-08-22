from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Literal, Union

import httpx
from typing_extensions import TypeAlias

Headers: TypeAlias = Union[dict[str, str], None]
Tags: TypeAlias = Union[dict[str, str], None]
IdempotencyKey: TypeAlias = Union[str, None]
Data: TypeAlias = Union[str, None]
State: TypeAlias = Literal[
    "PENDING", "RESOLVED", "REJECTED", "REJECTED_CANCELED", "REJECTED_TIMEDOUT"
]


@dataclass(frozen=True)
class _Param:
    headers: Headers
    data: Data


@dataclass(frozen=True)
class _Value:
    headers: Headers
    data: Data


@dataclass(frozen=True)
class _DurablePromiseRecord:
    state: State
    promise_id: str
    timeout: int
    param: _Param
    value: _Value
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


class IPromiseStore(ABC):
    @abstractmethod
    def create(  # noqa: PLR0913
        self,
        *,
        promise_id: str,
        ikey: IdempotencyKey,
        strict: bool,
        headers: Headers,
        data: Data,
        timeout: int,
        tags: Tags,
    ) -> _DurablePromiseRecord: ...

    @abstractmethod
    def cancel(
        self,
        *,
        promise_id: str,
        ikey: IdempotencyKey,
        strict: bool,
        headers: Headers,
        data: Data,
    ) -> _DurablePromiseRecord: ...

    @abstractmethod
    def resolve(
        self,
        *,
        promise_id: str,
        ikey: IdempotencyKey,
        strict: bool,
        headers: Headers,
        data: Data,
    ) -> _DurablePromiseRecord: ...

    @abstractmethod
    def reject(
        self,
        *,
        promise_id: str,
        ikey: IdempotencyKey,
        strict: bool,
        headers: Headers,
        data: Data,
    ) -> _DurablePromiseRecord: ...

    @abstractmethod
    def get(self, *, promise_id: str) -> _DurablePromiseRecord: ...

    @abstractmethod
    def search(
        self, *, promise_id: str, state: State, tags: Tags, limit: int | None = None
    ) -> list[_DurablePromiseRecord]: ...


class RemotePromiseStore(IPromiseStore):
    def __init__(self, url: str) -> None:
        self.client = httpx.Client(base_url=url)

    def _decode_response(self, data: dict[str, Any]) -> _DurablePromiseRecord:
        return _DurablePromiseRecord(
            state=data["state"],
            promise_id=data["id"],
            timeout=data["timeout"],
            param=data["param"],
            value=data["value"],
            created_on=data["createdOn"],
            completed_on=data.get("completedOn"),
            idempotency_key_for_complete=data.get("idempotencyKeyForComplete"),
            idempotency_key_for_create=data.get("idempotencyKeyForCreate"),
            tags=None,
        )

    def _initialize_headers(
        self, *, strict: bool, ikey: IdempotencyKey
    ) -> dict[str, str]:
        request_headers: dict[str, str] = {"Strict": str(strict)}
        if ikey is not None:
            request_headers["Idempotency-Key"] = ikey

        return request_headers

    def create(  # noqa: PLR0913
        self,
        *,
        promise_id: str,
        ikey: IdempotencyKey,
        strict: bool,
        headers: Headers,
        data: Data,
        timeout: int,
        tags: Tags,
    ) -> _DurablePromiseRecord:
        request_headers = self._initialize_headers(strict=strict, ikey=ikey)

        response = self.client.post(
            url="/promises",
            headers=request_headers,
            json={
                "id": promise_id,
                "param": {
                    "headers": headers,
                    "data": data,
                },
                "timeout": timeout,
                "tags": tags,
            },
        )
        response.raise_for_status()

        return self._decode_response(response.json())

    def cancel(
        self,
        *,
        promise_id: str,
        ikey: IdempotencyKey,
        strict: bool,
        headers: Headers,
        data: Data,
    ) -> _DurablePromiseRecord:
        request_headers = self._initialize_headers(strict=strict, ikey=ikey)
        response = self.client.patch(
            url=f"/promises/{promise_id}",
            headers=request_headers,
            json={
                "state": "REJECTED_CANCELED",
                "value": {"headers": headers, "data": data},
            },
        )
        response.raise_for_status()
        return self._decode_response(response.json())

    def resolve(
        self,
        *,
        promise_id: str,
        ikey: IdempotencyKey,
        strict: bool,
        headers: Headers,
        data: Data,
    ) -> _DurablePromiseRecord:
        request_headers = self._initialize_headers(strict=strict, ikey=ikey)

        response = self.client.patch(
            f"/promises/{promise_id}",
            headers=request_headers,
            json={"state": "RESOLVED", "value": {"headers": headers, "data": data}},
        )
        response.raise_for_status()
        return self._decode_response(response.json())

    def reject(
        self,
        *,
        promise_id: str,
        ikey: IdempotencyKey,
        strict: bool,
        headers: Headers,
        data: Data,
    ) -> _DurablePromiseRecord:
        request_headers = self._initialize_headers(strict=strict, ikey=ikey)

        response = self.client.patch(
            f"/promises/{promise_id}",
            headers=request_headers,
            json={"state": "REJECTED", "value": {"headers": headers, "data": data}},
        )
        response.raise_for_status()
        return self._decode_response(response.json())

    def get(self, *, promise_id: str) -> _DurablePromiseRecord:
        raise NotImplementedError

    def search(
        self, *, promise_id: str, state: State, tags: Tags, limit: int | None = None
    ) -> list[_DurablePromiseRecord]:
        raise NotImplementedError
