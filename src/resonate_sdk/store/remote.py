from __future__ import annotations

import time
from typing import TYPE_CHECKING

from requests import PreparedRequest, Request, Response, Session

from resonate_sdk import default, utils
from resonate_sdk.encoder import Base64Encoder
from resonate_sdk.errors import ResonateError
from resonate_sdk.logging import logger
from resonate_sdk.store.models import (
    DurablePromiseRecord,
    InvokeMesg,
    ResumeMesg,
    TaskRecord,
)
from resonate_sdk.store.traits import IPromiseStore, IStore, ITaskStore

if TYPE_CHECKING:
    from resonate_sdk.encoder import IEncoder


class RemotePromiseStore(IPromiseStore):
    def __init__(self, url: str, encoder: IEncoder[str, str]) -> None:
        self._url = url
        self._call = _call
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
        ikey: str | None,
        strict: bool,
        headers: dict[str, str] | None,
        data: str | None,
        timeout: int,
        tags: dict[str, str] | None,
    ) -> DurablePromiseRecord:
        req_headers = self._headers(strict=strict, ikey=ikey)
        req = Request(
            method="post",
            url=f"{self._url}/promises",
            headers=req_headers,
            json={
                "id": id,
                "param": {
                    "headers": headers or {},
                    "data": self._encoder.encode(data) if data else None,
                },
                "timeout": timeout,
                "tags": tags or {},
            },
        )
        resp = self._call(req.prepare())
        resp.raise_for_status()

        decoded = utils.decode(resp.json(), self._encoder)
        assert isinstance(decoded, DurablePromiseRecord)
        return decoded

    def create_with_task(
        self,
        *,
        id: str,
        ikey: str | None,
        strict: bool,
        headers: dict[str, str] | None,
        data: str | None,
        timeout: int,
        tags: dict[str, str] | None,
        pid: str,
        ttl: int,
    ) -> tuple[DurablePromiseRecord, TaskRecord | None]:
        req_headers = self._headers(strict=strict, ikey=ikey)
        req = Request(
            method="post",
            url=f"{self._url}/promises/task",
            headers=req_headers,
            json={
                "promise": {
                    "id": id,
                    "param": {
                        "headers": headers or {},
                        "data": self._encoder.encode(data) if data else None,
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
        resp = self._call(req.prepare())
        resp.raise_for_status()

        decoded = utils.decode(resp.json(), self._encoder)
        assert isinstance(decoded, tuple)
        return decoded

    def resolve(
        self,
        *,
        id: str,
        ikey: str | None,
        strict: bool,
        headers: dict[str, str] | None,
        data: str | None,
    ) -> DurablePromiseRecord:
        req_headers = self._headers(strict=strict, ikey=ikey)
        req = Request(
            method="patch",
            url=f"{self._url}/promises/{id}",
            headers=req_headers,
            json={
                "state": "RESOLVED",
                "value": {
                    "headers": headers or {},
                    "data": self._encoder.encode(data) if data else None,
                },
            },
        )
        resp = self._call(req.prepare())
        resp.raise_for_status()
        decoded = utils.decode(resp.json(), self._encoder)
        assert isinstance(decoded, DurablePromiseRecord)
        return decoded

    def reject(
        self,
        *,
        id: str,
        ikey: str | None,
        strict: bool,
        headers: dict[str, str] | None,
        data: str | None,
    ) -> DurablePromiseRecord:
        req_headers = self._headers(strict=strict, ikey=ikey)
        req = Request(
            method="patch",
            url=f"{self._url}/promises/{id}",
            headers=req_headers,
            json={
                "state": "REJECTED",
                "value": {
                    "headers": headers or {},
                    "data": self._encoder.encode(data) if data else None,
                },
            },
        )
        resp = self._call(req.prepare())
        resp.raise_for_status()
        decoded = utils.decode(resp.json(), self._encoder)
        assert isinstance(decoded, DurablePromiseRecord)
        return decoded

    def cancel(
        self,
        *,
        id: str,
        ikey: str | None,
        strict: bool,
        headers: dict[str, str] | None,
        data: str | None,
    ) -> DurablePromiseRecord:
        req_headers = self._headers(strict=strict, ikey=ikey)
        req = Request(
            method="patch",
            url=f"{self._url}/promises/{id}",
            headers=req_headers,
            json={
                "state": "REJECTED_CANCELED",
                "value": {
                    "headers": headers or {},
                    "data": self._encoder.encode(data) if data else None,
                },
            },
        )
        resp = self._call(req.prepare())
        resp.raise_for_status()
        decoded = utils.decode(resp.json(), self._encoder)
        assert isinstance(decoded, DurablePromiseRecord)
        return decoded


class RemoteTaskStore(ITaskStore):
    def __init__(self, url: str, encoder: IEncoder[str, str]) -> None:
        self._url = url
        self._call = _call
        self._encoder = encoder

    def claim(
        self, *, id: str, counter: int, pid: str, ttl: int
    ) -> InvokeMesg | ResumeMesg:
        req = Request(
            method="post",
            url=f"{self._url}/tasks/claim",
            json={
                "id": id,
                "counter": counter,
                "processId": pid,
                "ttl": ttl,
            },
        )
        resp = self._call(req.prepare())
        resp.raise_for_status()
        decoded = utils.decode(resp.json(), self._encoder)
        assert isinstance(decoded, (InvokeMesg, ResumeMesg))
        return decoded

    def heartbeat(self, *, pid: str) -> int:
        req = Request(
            method="post",
            url=f"{self._url}/tasks/heartbeat",
            json={
                "processId": pid,
            },
        )
        resp = self._call(req.prepare())
        resp.raise_for_status()
        decoded = utils.decode(resp.json(), self._encoder)
        assert isinstance(decoded, int)
        return decoded

    def complete(self, *, id: str, counter: int) -> None:
        req = Request(
            method="post",
            url=f"{self._url}/tasks/complete",
            json={
                "id": id,
                "counter": counter,
            },
        )
        resp = self._call(req.prepare())
        resp.raise_for_status()


class RemoteStore(IStore):
    def __init__(
        self, url: str | None = None, encoder: IEncoder[str, str] | None = None
    ) -> None:
        self._url = url or default.url("store")
        self._encoder = encoder or Base64Encoder()

    @property
    def promises(self) -> RemotePromiseStore:
        return RemotePromiseStore(self._url, self._encoder)

    @property
    def tasks(self) -> RemoteTaskStore:
        return RemoteTaskStore(self._url, self._encoder)


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
            logger.warning(
                "Unexpected code response from remote store: %s", res.status_code
            )
        time.sleep(1)
    raise NotImplementedError
