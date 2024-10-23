from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Callable, Literal, final

import requests

from resonate.encoders import Base64Encoder, IEncoder
from resonate.errors import ResonateError
from resonate.logging import logger
from resonate.record import (
    CallbackRecord,
    DurablePromiseRecord,
    Invoke,
    Param,
    Resume,
    Value,
)
from resonate.time import now

if TYPE_CHECKING:
    from resonate.typing import Data, Headers, IdempotencyKey, PollMessage, State, Tags


class ITaskStore(ABC):
    @abstractmethod
    def claim_task(
        self, *, task_id: str, counter: int, pid: str, ttl: int
    ) -> PollMessage: ...

    @abstractmethod
    def complete_task(self, *, task_id: str, counter: int) -> None: ...

    @abstractmethod
    def heartbeat_tasks(self, *, pid: str) -> int: ...


class ICallbackStore(ABC):
    @abstractmethod
    def create_callback(
        self, *, promise_id: str, root_promise_id: str, timeout: int, recv: str
    ) -> tuple[DurablePromiseRecord, CallbackRecord | None]: ...


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
    def rmw_durable_promise(
        self,
        promise_id: str,
        fn: Callable[[DurablePromiseRecord | None], DurablePromiseRecord],
    ) -> DurablePromiseRecord: ...


class MemoryStorage(IStorage):
    def __init__(self) -> None:
        self._durable_promises: dict[str, DurablePromiseRecord] = {}

    def rmw_durable_promise(
        self,
        promise_id: str,
        fn: Callable[[DurablePromiseRecord | None], DurablePromiseRecord],
    ) -> DurablePromiseRecord:
        promise_record = self._durable_promises.get(promise_id)
        if promise_record is not None:
            promise_record = _timeout(promise_record)
        item = fn(promise_record)
        self._durable_promises[promise_id] = item
        return item


@final
class LocalStore(IPromiseStore):
    def __init__(self, storage: IStorage | None = None) -> None:
        self._storage = storage or MemoryStorage()

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

        return self._storage.rmw_durable_promise(promise_id=promise_id, fn=_create)

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

        return self._storage.rmw_durable_promise(promise_id=promise_id, fn=_reject)

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

        return self._storage.rmw_durable_promise(promise_id=promise_id, fn=_cancel)

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

        return self._storage.rmw_durable_promise(promise_id=promise_id, fn=_resolve)

    def get(self, *, promise_id: str) -> DurablePromiseRecord:
        def _get(
            promise_record: DurablePromiseRecord | None,
        ) -> DurablePromiseRecord:
            if promise_record is None:
                raise ResonateError(msg="Not Found", code="STORE_NOT_FOUND")

            return promise_record

        return self._storage.rmw_durable_promise(promise_id=promise_id, fn=_get)

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


@final
class RemoteServer(IPromiseStore, ICallbackStore, ITaskStore):
    def __init__(
        self,
        url: str,
        request_timeout: int = 10,
        encoder: IEncoder[str, str] | None = None,
    ) -> None:
        self.url = url
        self._request_timeout = request_timeout
        self._encoder = encoder or Base64Encoder()

    def _initialize_headers(
        self, *, strict: bool, ikey: IdempotencyKey
    ) -> dict[str, str]:
        request_headers: dict[str, str] = {"Strict": str(strict)}
        if ikey is not None:
            request_headers["Idempotency-Key"] = ikey

        return request_headers

    def claim_task(
        self, *, task_id: str, counter: int, pid: str, ttl: int
    ) -> PollMessage:
        response = requests.post(
            url=f"{self.url}/tasks/claim",
            headers={},
            json={
                "id": task_id,
                "counter": counter,
                "processId": pid,
                "ttl": ttl,
            },
            timeout=self._request_timeout,
        )
        _ensure_success(response)

        data = response.json()
        kind: Literal["resume", "invoke"] = data["type"]
        assert kind in ("resume", "invoke")

        promises: dict[str, Any] = data["promises"]
        if promises.get("leaf") is None:
            return Invoke.decode(data=promises["root"]["data"], encoder=self._encoder)

        return Resume.decode(data=promises, encoder=self._encoder)

    def complete_task(self, *, task_id: str, counter: int) -> None:
        response = requests.post(
            url=f"{self.url}/tasks/complete",
            headers={},
            json={
                "id": task_id,
                "counter": counter,
            },
            timeout=self._request_timeout,
        )
        _ensure_success(response)

    def heartbeat_tasks(self, *, pid: str) -> int:
        response = requests.post(
            url=f"{self.url}/tasks/heartbeat",
            headers={},
            json={"processId": pid},
            timeout=self._request_timeout,
        )
        _ensure_success(response)
        return response.json()["tasksAffected"]

    def create_callback(
        self, *, promise_id: str, root_promise_id: str, timeout: int, recv: str
    ) -> tuple[DurablePromiseRecord, CallbackRecord | None]:
        response = requests.post(
            url=f"{self.url}/callbacks",
            json={
                "promiseId": promise_id,
                "rootPromiseId": root_promise_id,
                "timeout": timeout,
                "recv": recv,
            },
            timeout=self._request_timeout,
        )

        _ensure_success(response)
        data = response.json()

        callback_data = data["callback"]
        durable_promise = DurablePromiseRecord.decode(
            data["promise"], encoder=self._encoder
        )
        if callback_data is None:
            return durable_promise, None
        return durable_promise, CallbackRecord.decode(
            callback_data, encoder=self._encoder
        )

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
                    "data": self._encode_data(data),
                },
                "timeout": timeout,
                "tags": tags,
            },
            timeout=self._request_timeout,
        )

        _ensure_success(response)

        return DurablePromiseRecord.decode(data=response.json(), encoder=self._encoder)

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
                "value": {"headers": headers, "data": self._encode_data(data)},
            },
            timeout=self._request_timeout,
        )
        _ensure_success(response)
        return DurablePromiseRecord.decode(data=response.json(), encoder=self._encoder)

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
            json={
                "state": "RESOLVED",
                "value": {"headers": headers, "data": self._encode_data(data)},
            },
            timeout=self._request_timeout,
        )
        _ensure_success(response)
        return DurablePromiseRecord.decode(data=response.json(), encoder=self._encoder)

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
            json={
                "state": "REJECTED",
                "value": {"headers": headers, "data": self._encode_data(data)},
            },
            timeout=self._request_timeout,
        )
        _ensure_success(response)
        return DurablePromiseRecord.decode(data=response.json(), encoder=self._encoder)

    def get(self, *, promise_id: str) -> DurablePromiseRecord:
        response = requests.get(
            f"{self.url}/promises/{promise_id}", timeout=self._request_timeout
        )
        _ensure_success(response)
        return DurablePromiseRecord.decode(data=response.json(), encoder=self._encoder)

    def search(
        self, *, promise_id: str, state: State, tags: Tags, limit: int | None = None
    ) -> list[DurablePromiseRecord]:
        raise NotImplementedError

    def _encode_data(self, data: str | None) -> str | None:
        if data is None:
            return None
        return _encode(data, self._encoder)


def _ensure_success(resp: requests.Response) -> None:
    logger.debug(resp.json())
    resp.raise_for_status()


def _encode(value: str, encoder: IEncoder[str, str]) -> str:
    try:
        return encoder.encode(value)
    except Exception as e:
        msg = "Encoder error"
        raise ResonateError(msg, "STORE_ENCODER", e) from e
