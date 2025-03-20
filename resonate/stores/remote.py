from __future__ import annotations

import os
import time
from typing import TYPE_CHECKING, Any

from requests import PreparedRequest, Request, Response, Session

from resonate.encoders.base64 import Base64Encoder
from resonate.encoders.chain import ChainEncoder
from resonate.encoders.json import JsonEncoder
from resonate.errors import ResonateStoreError
from resonate.models.callback import Callback
from resonate.models.durable_promise import DurablePromise
from resonate.models.task import Task

if TYPE_CHECKING:
    from resonate.models.encoder import Encoder


class RemoteStore:
    def __init__(
        self,
        url: str | None = None,
        encoder: Encoder[Any, str | None] | None = None,
    ) -> None:
        self.url = url or os.getenv("RESONATE_STORE_URL", "http://localhost:8001")
        self._encoder = encoder or ChainEncoder(
            JsonEncoder(),
            Base64Encoder(),
        )

    @property
    def promises(self) -> RemotePromiseStore:
        return RemotePromiseStore(self, self._encoder)

    @property
    def tasks(self) -> RemoteTaskStore:
        return RemoteTaskStore(self)


class RemotePromiseStore:
    def __init__(self, store: RemoteStore, encoder: Encoder[Any, str | None]) -> None:
        self._store = store
        self._encoder = encoder

    def _headers(self, *, strict: bool, ikey: str | None) -> dict[str, str]:
        headers: dict[str, str] = {"strict": str(strict)}
        if ikey is not None:
            headers["idempotency-Key"] = ikey
        return headers

    def create(
        self,
        *,
        id: str,
        timeout: int,
        ikey: str | None = None,
        strict: bool = False,
        headers: dict[str, str] | None = None,
        data: Any = None,
        tags: dict[str, str] | None = None,
    ) -> DurablePromise:
        req = Request(
            method="post",
            url=f"{self._store.url}/promises",
            headers=self._headers(strict=strict, ikey=ikey),
            json={
                "id": id,
                "param": {
                    "headers": headers or {},
                    "data": self._encoder.encode(data),
                },
                "timeout": timeout,
                "tags": tags or {},
            },
        )

        res = _call(req.prepare()).json()

        return DurablePromise.from_dict(
            self._store,
            res,
            self._encoder.decode(res["param"].get("data")),
            self._encoder.decode(res["value"].get("data")),
        )

    def create_with_task(
        self,
        *,
        id: str,
        timeout: int,
        pid: str,
        ttl: int,
        ikey: str | None = None,
        strict: bool = False,
        headers: dict[str, str] | None = None,
        data: Any = None,
        tags: dict[str, str] | None = None,
    ) -> tuple[DurablePromise, Task | None]:
        req = Request(
            method="post",
            url=f"{self._store.url}/promises/task",
            headers=self._headers(strict=strict, ikey=ikey),
            json={
                "promise": {
                    "id": id,
                    "param": {
                        "headers": headers or {},
                        "data": self._encoder.encode(data),
                    },
                    "timeout": timeout,
                    "tags": tags or {},
                },
                "task": {
                    "processId": pid,
                    "ttl": ttl,
                },
            },
        )
        res = _call(req.prepare()).json()

        return DurablePromise.from_dict(
            self._store,
            res["promise"],
            self._encoder.decode(res["promise"]["param"].get("data")),
            self._encoder.decode(res["promise"]["value"].get("data")),
        ), Task.from_dict(self._store, res["task"]) if "task" in res else None

    def resolve(
        self,
        *,
        id: str,
        ikey: str | None = None,
        strict: bool = False,
        headers: dict[str, str] | None = None,
        data: Any = None,
    ) -> DurablePromise:
        req = Request(
            method="patch",
            url=f"{self._store.url}/promises/{id}",
            headers=self._headers(strict=strict, ikey=ikey),
            json={
                "state": "RESOLVED",
                "value": {
                    "headers": headers or {},
                    "data": self._encoder.encode(data),
                },
            },
        )

        res = _call(req.prepare()).json()

        return DurablePromise.from_dict(
            self._store,
            res,
            self._encoder.decode(res["param"].get("data")),
            self._encoder.decode(res["value"].get("data")),
        )

    def reject(
        self,
        *,
        id: str,
        ikey: str | None = None,
        strict: bool = False,
        headers: dict[str, str] | None = None,
        data: Any = None,
    ) -> DurablePromise:
        req = Request(
            method="patch",
            url=f"{self._store.url}/promises/{id}",
            headers=self._headers(strict=strict, ikey=ikey),
            json={
                "state": "REJECTED",
                "value": {
                    "headers": headers or {},
                    "data": self._encoder.encode(data),
                },
            },
        )

        res = _call(req.prepare()).json()

        return DurablePromise.from_dict(
            self._store,
            res,
            param=self._encoder.decode(res["param"].get("data")),
            value=self._encoder.decode(res["value"].get("data")),
        )

    def cancel(
        self,
        *,
        id: str,
        ikey: str | None = None,
        strict: bool = False,
        headers: dict[str, str] | None = None,
        data: Any = None,
    ) -> DurablePromise:
        req = Request(
            method="patch",
            url=f"{self._store.url}/promises/{id}",
            headers=self._headers(strict=strict, ikey=ikey),
            json={
                "state": "REJECTED_CANCELED",
                "value": {
                    "headers": headers or {},
                    "data": self._encoder.encode(data),
                },
            },
        )

        res = _call(req.prepare()).json()

        return DurablePromise.from_dict(
            self._store,
            res,
            self._encoder.decode(res["param"].get("data")),
            self._encoder.decode(res["value"].get("data")),
        )

    def callback(
        self,
        *,
        id: str,
        promise_id: str,
        root_promise_id: str,
        timeout: int,
        recv: str,
    ) -> tuple[DurablePromise, Callback | None]:
        req = Request(
            method="post",
            url=f"{self._store.url}/callbacks",
            json={
                "id": id,
                "promiseId": promise_id,
                "rootPromiseId": root_promise_id,
                "timeout": timeout,
                "recv": recv,
            },
        )
        res = _call(req.prepare()).json()

        return DurablePromise.from_dict(
            self._store,
            res["promise"],
            self._encoder.decode(res["promise"]["param"].get("data")),
            self._encoder.decode(res["promise"]["value"].get("data")),
        ), Callback.from_dict(res["callback"]) if "callback" in res else None


class RemoteTaskStore:
    def __init__(self, store: RemoteStore) -> None:
        self._store = store

    def claim(
        self,
        *,
        id: str,
        counter: int,
        pid: str,
        ttl: int,
    ) -> tuple[DurablePromise, DurablePromise | None]:
        req = Request(
            method="post",
            url=f"{self._store.url}/tasks/claim",
            json={
                "id": id,
                "counter": counter,
                "processId": pid,
                "ttl": ttl,
            },
        )

        return _call(req.prepare()).json()

    def complete(self, *, id: str, counter: int) -> bool:
        req = Request(
            method="post",
            url=f"{self._store.url}/tasks/complete",
            json={
                "id": id,
                "counter": counter,
            },
        )
        _call(req.prepare())
        return True

    def heartbeat(
        self,
        *,
        pid: str,
    ) -> int:
        req = Request(
            method="post",
            url=f"{self._store.url}/tasks/heartbeat",
            json={
                "processId": pid,
            },
        )

        res = _call(req.prepare()).json()

        return res["tasksAffected"]


class _status_codes:  # noqa: N801
    BAD_REQUEST = 400
    UNAUTHORIZED = 401
    FORBIDDEN = 403
    NOT_FOUND = 404
    CONFLICT = 409


def _call(req: PreparedRequest) -> Response:
    with Session() as s:
        while True:
            res = s.send(req, timeout=10)
            if res.ok:
                return res
            if res.status_code == _status_codes.BAD_REQUEST:
                msg = "Invalid Request"
                raise ResonateStoreError(msg, "STORE_PAYLOAD")
            if res.status_code == _status_codes.UNAUTHORIZED:
                msg = "Unauthorized request"
                raise ResonateStoreError(msg, "STORE_UNAUTHORIZED")
            if res.status_code == _status_codes.FORBIDDEN:
                msg = "Forbidden request"
                raise ResonateStoreError(msg, "STORE_FORBIDDEN")
            if res.status_code == _status_codes.NOT_FOUND:
                msg = "Not found"
                raise ResonateStoreError(msg, "STORE_NOT_FOUND")
            if res.status_code == _status_codes.CONFLICT:
                msg = "Already exists"
                raise ResonateStoreError(msg, "STORE_ALREADY_EXISTS")
        time.sleep(1)
    raise NotImplementedError
