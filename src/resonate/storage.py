from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Callable, Literal

import requests

from resonate.errors import ResonateError
from resonate.record import DurablePromiseRecord, Param, Value
from resonate.time import now

if TYPE_CHECKING:
    from resonate.typing import Data, Headers, IdempotencyKey, State, Tags


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
    ) -> DurablePromiseRecord: ...

    @abstractmethod
    def cancel(
        self,
        *,
        promise_id: str,
        ikey: IdempotencyKey,
        strict: bool,
        headers: Headers,
        data: Data,
    ) -> DurablePromiseRecord: ...

    @abstractmethod
    def resolve(
        self,
        *,
        promise_id: str,
        ikey: IdempotencyKey,
        strict: bool,
        headers: Headers,
        data: Data,
    ) -> DurablePromiseRecord: ...

    @abstractmethod
    def reject(
        self,
        *,
        promise_id: str,
        ikey: IdempotencyKey,
        strict: bool,
        headers: Headers,
        data: Data,
    ) -> DurablePromiseRecord: ...

    @abstractmethod
    def get(self, *, promise_id: str) -> DurablePromiseRecord: ...

    @abstractmethod
    def search(
        self, *, promise_id: str, state: State, tags: Tags, limit: int | None = None
    ) -> list[DurablePromiseRecord]: ...


def _timeout(promise_record: DurablePromiseRecord) -> DurablePromiseRecord:
    new_status: State = "REJECTED_TIMEDOUT"
    if promise_record.is_pending() and now() >= promise_record.timeout:
        if promise_record.tags is not None:
            resonate_timeout: str | None = promise_record.tags.get("resonate:timeout")
            if resonate_timeout is not None and resonate_timeout == "true":
                new_status = "RESOLVED"
        return DurablePromiseRecord(
            new_status,
            promise_id=promise_record.promise_id,
            timeout=promise_record.timeout,
            param=promise_record.param,
            value=Value(headers=None, data=None),
            created_on=promise_record.created_on,
            completed_on=promise_record.timeout,
            idempotency_key_for_create=promise_record.idempotency_key_for_create,
            idempotency_key_for_complete=None,
            tags=promise_record.tags,
        )
    return promise_record


class IStorage(ABC):
    @abstractmethod
    def rmw(
        self,
        promise_id: str,
        fn: Callable[[DurablePromiseRecord | None], DurablePromiseRecord],
    ) -> DurablePromiseRecord: ...


class MemoryStorage(IStorage):
    def __init__(self) -> None:
        self._items: dict[str, DurablePromiseRecord] = {}

    def rmw(
        self,
        promise_id: str,
        fn: Callable[[DurablePromiseRecord | None], DurablePromiseRecord],
    ) -> DurablePromiseRecord:
        promise_record = self._items.get(promise_id)
        if promise_record is not None:
            promise_record = _timeout(promise_record)
        item = fn(promise_record)
        self._items[promise_id] = item
        return item


class LocalPromiseStore(IPromiseStore):
    def __init__(self, storage: IStorage) -> None:
        self._storage = storage

    def create(  # noqa: PLR0913
        self,
        *,
        promise_id: str,
        ikey: str | None,
        strict: bool,
        headers: dict[str, str] | None,
        data: str | None,
        timeout: int,
        tags: dict[str, str] | None,
    ) -> DurablePromiseRecord:
        def _create(
            promise_record: DurablePromiseRecord | None,
        ) -> DurablePromiseRecord:
            if promise_record is None:
                return DurablePromiseRecord(
                    state="PENDING",
                    promise_id=promise_id,
                    timeout=timeout,
                    param=Param(headers=headers, data=data),
                    value=Value(headers=None, data=None),
                    created_on=now(),
                    completed_on=None,
                    idempotency_key_for_complete=None,
                    idempotency_key_for_create=ikey,
                    tags=tags,
                )
            if strict and not promise_record.is_pending():
                msg = "Forbidden request: Durable promise previously created"
                raise ResonateError(
                    msg,
                    "STORE_FORBIDDEN",
                )
            if (
                promise_record.idempotency_key_for_create is None
                or ikey != promise_record.idempotency_key_for_create
            ):
                msg = "Forbidden request: Missing idempotency key for create"
                raise ResonateError(msg, "STORE_FORBIDDEN")
            return promise_record

        return self._storage.rmw(promise_id=promise_id, fn=_create)

    def reject(
        self,
        *,
        promise_id: str,
        ikey: str | None,
        strict: bool,
        headers: dict[str, str] | None,
        data: str | None,
    ) -> DurablePromiseRecord:
        def _reject(
            promise_record: DurablePromiseRecord | None,
        ) -> DurablePromiseRecord:
            if promise_record is None:
                msg = "Not found"
                raise ResonateError(msg, "STORE_NOT_FOUND")
            if promise_record.is_pending():
                return DurablePromiseRecord(
                    state="REJECTED",
                    promise_id=promise_record.promise_id,
                    timeout=promise_record.timeout,
                    param=promise_record.param,
                    value=Value(headers=headers, data=data),
                    created_on=promise_record.created_on,
                    completed_on=now(),
                    idempotency_key_for_create=promise_record.idempotency_key_for_create,
                    idempotency_key_for_complete=ikey,
                    tags=promise_record.tags,
                )
            if strict and not promise_record.is_rejected():
                msg = "Forbidden request"
                raise ResonateError(msg, "STORE_FORBIDDEN")
            if not promise_record.is_timeout() and (
                promise_record.idempotency_key_for_complete is None
                or ikey != promise_record.idempotency_key_for_complete
            ):
                msg = "Forbidden request"
                raise ResonateError(msg, "STORE_FORBIDDEN")
            return promise_record

        return self._storage.rmw(promise_id=promise_id, fn=_reject)

    def cancel(
        self,
        *,
        promise_id: str,
        ikey: str | None,
        strict: bool,
        headers: dict[str, str] | None,
        data: str | None,
    ) -> DurablePromiseRecord:
        def _cancel(
            promise_record: DurablePromiseRecord | None,
        ) -> DurablePromiseRecord:
            if promise_record is None:
                msg = "Not Found"
                raise ResonateError(msg, "STORE_NOT_FOUND")
            if promise_record.is_pending():
                return DurablePromiseRecord(
                    state="REJECTED_CANCELED",
                    promise_id=promise_record.promise_id,
                    timeout=promise_record.timeout,
                    param=promise_record.param,
                    value=Value(headers=headers, data=data),
                    created_on=promise_record.created_on,
                    completed_on=now(),
                    idempotency_key_for_create=promise_record.idempotency_key_for_create,
                    idempotency_key_for_complete=ikey,
                    tags=promise_record.tags,
                )
            if strict and not promise_record.is_canceled():
                msg = "Forbidden request"
                raise ResonateError(msg, "STORE_FORBIDDEN")
            if not promise_record.is_timeout() and (
                promise_record.idempotency_key_for_complete is None
                or ikey != promise_record.idempotency_key_for_complete
            ):
                msg = "Forbidden request"
                raise ResonateError(msg, "STORE_FORBIDDEN")
            return promise_record

        return self._storage.rmw(promise_id=promise_id, fn=_cancel)

    def resolve(
        self,
        *,
        promise_id: str,
        ikey: str | None,
        strict: bool,
        headers: dict[str, str] | None,
        data: str | None,
    ) -> DurablePromiseRecord:
        def _resolve(
            promise_record: DurablePromiseRecord | None,
        ) -> DurablePromiseRecord:
            if promise_record is None:
                msg = "Not found"
                raise ResonateError(msg, "STORE_NOT_FOUND")
            if promise_record.is_pending():
                return DurablePromiseRecord(
                    state="RESOLVED",
                    promise_id=promise_record.promise_id,
                    timeout=promise_record.timeout,
                    param=promise_record.param,
                    value=Value(headers=headers, data=data),
                    created_on=promise_record.created_on,
                    completed_on=now(),
                    idempotency_key_for_create=promise_record.idempotency_key_for_create,
                    idempotency_key_for_complete=ikey,
                    tags=promise_record.tags,
                )
            if strict and not promise_record.is_resolved():
                msg = "Forbidden request"
                raise ResonateError(msg, "STORE_FORBIDDEN")
            if not promise_record.is_timeout() and (
                promise_record.idempotency_key_for_complete is None
                or ikey != promise_record.idempotency_key_for_complete
            ):
                msg = "Forbidden request"
                raise ResonateError(msg, "STORE_FORBIDDEN")
            return promise_record

        return self._storage.rmw(promise_id=promise_id, fn=_resolve)

    def get(self, *, promise_id: str) -> DurablePromiseRecord:
        raise NotImplementedError

    def search(
        self,
        *,
        promise_id: str,
        state: Literal[
            "PENDING", "RESOLVED", "REJECTED", "REJECTED_CANCELED", "REJECTED_TIMEDOUT"
        ],
        tags: dict[str, str] | None,
        limit: int | None = None,
    ) -> list[DurablePromiseRecord]:
        raise NotImplementedError


class RemotePromiseStore(IPromiseStore):
    def __init__(self, url: str, request_timeout: int = 10) -> None:
        self.url = url
        self._request_timeout = request_timeout

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
        ikey: str | None,
        strict: bool,
        headers: dict[str, str] | None,
        data: str | None,
        timeout: int,
        tags: dict[str, str] | None,
    ) -> DurablePromiseRecord:
        request_headers = self._initialize_headers(strict=strict, ikey=ikey)

        response = requests.post(
            url=f"{self.url}/promises",
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
            timeout=self._request_timeout,
        )

        response.raise_for_status()

        return DurablePromiseRecord.from_json(response.json())

    def cancel(
        self,
        *,
        promise_id: str,
        ikey: IdempotencyKey,
        strict: bool,
        headers: Headers,
        data: Data,
    ) -> DurablePromiseRecord:
        request_headers = self._initialize_headers(strict=strict, ikey=ikey)
        response = requests.patch(
            url=f"{self.url}/promises/{promise_id}",
            headers=request_headers,
            json={
                "state": "REJECTED_CANCELED",
                "value": {"headers": headers, "data": data},
            },
            timeout=self._request_timeout,
        )
        response.raise_for_status()
        return DurablePromiseRecord.from_json(response.json())

    def resolve(
        self,
        *,
        promise_id: str,
        ikey: IdempotencyKey,
        strict: bool,
        headers: Headers,
        data: Data,
    ) -> DurablePromiseRecord:
        request_headers = self._initialize_headers(strict=strict, ikey=ikey)

        response = requests.patch(
            f"{self.url}/promises/{promise_id}",
            headers=request_headers,
            json={"state": "RESOLVED", "value": {"headers": headers, "data": data}},
            timeout=self._request_timeout,
        )
        response.raise_for_status()
        return DurablePromiseRecord.from_json(response.json())

    def reject(
        self,
        *,
        promise_id: str,
        ikey: IdempotencyKey,
        strict: bool,
        headers: Headers,
        data: Data,
    ) -> DurablePromiseRecord:
        request_headers = self._initialize_headers(strict=strict, ikey=ikey)

        response = requests.patch(
            f"{self.url}/promises/{promise_id}",
            headers=request_headers,
            json={"state": "REJECTED", "value": {"headers": headers, "data": data}},
            timeout=self._request_timeout,
        )
        response.raise_for_status()
        return DurablePromiseRecord.from_json(response.json())

    def get(self, *, promise_id: str) -> DurablePromiseRecord:
        raise NotImplementedError

    def search(
        self, *, promise_id: str, state: State, tags: Tags, limit: int | None = None
    ) -> list[DurablePromiseRecord]:
        raise NotImplementedError
