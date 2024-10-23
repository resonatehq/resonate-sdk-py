from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal, final

import requests

from resonate.encoders import Base64Encoder, IEncoder
from resonate.errors import ResonateError
from resonate.record import (
    CallbackRecord,
    DurablePromiseRecord,
    Invoke,
    Resume,
)
from resonate.storage.traits import ICallbackStore, IPromiseStore, ITaskStore

if TYPE_CHECKING:
    from resonate.typing import Data, Headers, IdempotencyKey, PollMessage, State, Tags


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
    resp.raise_for_status()


def _encode(value: str, encoder: IEncoder[str, str]) -> str:
    try:
        return encoder.encode(value)
    except Exception as e:
        msg = "Encoder error"
        raise ResonateError(msg, "STORE_ENCODER", e) from e
