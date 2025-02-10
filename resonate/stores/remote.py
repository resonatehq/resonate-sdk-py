from __future__ import annotations

import os
import time

from requests import PreparedRequest, Request, Response, Session

from resonate.encoders.base64 import Base64Encoder
from resonate.errors import ResonateError
from resonate.models.durable_promise import DurablePromise
from resonate.models.encoder import Encoder
from resonate.models.message import InvokeMesg, Mesg, ResumeMesg
from resonate.models.task import Task


class RemoteStore:
    def __init__(self, url: str | None = None, encoder: Encoder[str | None, str | None] | None = None) -> None:
        self.url = url or os.getenv("RESONATE_STORE_URL", "http://localhost:8001")
        self.encoder = encoder or Base64Encoder()

    @property
    def promises(self) -> RemotePromiseStore:
        return RemotePromiseStore(self)

    @property
    def tasks(self) -> RemoteTaskStore:
        return RemoteTaskStore(self)

class RemotePromiseStore:
    def __init__(self, store: RemoteStore) -> None:
        self._store = store

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
        data: str | None = None,
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
                    "data": self._store.encoder.encode(data),
                },
                "timeout": timeout,
                "tags": tags or {},
            },
        )

        res = _call(req.prepare()).json()
        # res.raise_for_status()

        return DurablePromise.from_dict(self._store, res)

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
        data: str | None = None,
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
                        "data": self._store.encoder.encode(data),
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
        # res.raise_for_status()

        return DurablePromise.from_dict(self._store, res["promise"]), Task.from_dict(self._store, res["task"]) if "task" in res else None

    def resolve(
        self,
        *,
        id: str,
        ikey: str | None = None,
        strict: bool = False,
        headers: dict[str, str] | None = None,
        data: str | None = None,
    ) -> DurablePromise:
        req = Request(
            method="patch",
            url=f"{self._store.url}/promises/{id}",
            headers=self._headers(strict=strict, ikey=ikey),
            json={
                "state": "RESOLVED",
                "value": {
                    "headers": headers or {},
                    "data": self._store.encoder.encode(data),
                },
            },
        )

        res = _call(req.prepare()).json()
        # res.raise_for_status()

        return DurablePromise.from_dict(self, res)

    def reject(
        self,
        *,
        id: str,
        ikey: str | None = None,
        strict: bool = False,
        headers: dict[str, str] | None = None,
        data: str | None = None,
    ) -> DurablePromise:
        req = Request(
            method="patch",
            url=f"{self._store.url}/promises/{id}",
            headers=self._headers(strict=strict, ikey=ikey),
            json={
                "state": "REJECTED",
                "value": {
                    "headers": headers or {},
                    "data": self._store.encoder.encode(data),
                },
            },
        )

        res = _call(req.prepare()).json()
        # res.raise_for_status()

        return DurablePromise.from_dict(self._store, res)

    def cancel(
        self,
        *,
        id: str,
        ikey: str | None = None,
        strict: bool = False,
        headers: dict[str, str] | None = None,
        data: str | None = None,
    ) -> DurablePromise:
        req = Request(
            method="patch",
            url=f"{self._store.url}/promises/{id}",
            headers=self._headers(strict=strict, ikey=ikey),
            json={
                "state": "REJECTED_CANCELED",
                "value": {
                    "headers": headers or {},
                    "data": self._store.encoder.encode(data),
                },
            },
        )

        res = _call(req.prepare()).json()
        # res.raise_for_status()

        return DurablePromise.from_dict(self._store, res)


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
    ) -> InvokeMesg | ResumeMesg:
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

        res = _call(req.prepare()).json()
        msg = Mesg.from_dict(self._store, res)

        # TODO: handle this better
        assert isinstance(msg, (InvokeMesg, ResumeMesg))

        return msg

    def complete(self, *, id: str, counter: int) -> None:
        req = Request(
            method="post",
            url=f"{self._store.url}/tasks/complete",
            json={
                "id": id,
                "counter": counter,
            },
        )
        _call(req.prepare())
        # resp.raise_for_status()

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
                raise ResonateError(msg, "STORE_PAYLOAD")
            if res.status_code == _status_codes.UNAUTHORIZED:
                msg = "Unauthorized request"
                raise ResonateError(msg, "STORE_UNAUTHORIZED")
            if res.status_code == _status_codes.FORBIDDEN:
                msg = "Forbidden request"
                raise ResonateError(msg, "STORE_FORBIDDEN")
            if res.status_code == _status_codes.NOT_FOUND:
                msg = "Not found"
                raise ResonateError(msg, "STORE_NOT_FOUND")
            if res.status_code == _status_codes.CONFLICT:
                msg = "Already exists"
                raise ResonateError(msg, "STORE_ALREADY_EXISTS")
            # logger.warning(
            #     "Unexpected code response from remote store: %s", res.status_code
            # )
        time.sleep(1)
    raise NotImplementedError
