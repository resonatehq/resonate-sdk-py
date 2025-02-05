from __future__ import annotations

import os
import time
from typing import TYPE_CHECKING, Any, Literal

from requests import Request, Response, Session

from resonate_sdk.encoder import Base64Encoder
from resonate_sdk.errors import ResonateError
from resonate_sdk.logging import logger
from resonate_sdk.store.models import (
    DurablePromiseRecord,
    InvokeMesg,
    ResumeMesg,
    Value,
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
        headers: dict[str, str] = {"Strict": str(strict)}
        if ikey is not None:
            headers["Idempotency-Key"] = ikey
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
        resp = self._call(req)
        resp.raise_for_status()

        decoded = _decode(resp.json(), self._encoder)
        assert isinstance(decoded, DurablePromiseRecord)
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
        resp = self._call(req)
        resp.raise_for_status()
        decoded = _decode(resp.json(), self._encoder)
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
        resp = self._call(req)
        resp.raise_for_status()
        decoded = _decode(resp.json(), self._encoder)
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
        resp = self._call(req)
        resp.raise_for_status()
        decoded = _decode(resp.json(), self._encoder)
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
        resp = self._call(req)
        resp.raise_for_status()
        decoded = _decode(resp.json(), self._encoder)
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
        resp = self._call(req)
        resp.raise_for_status()
        decoded = _decode(resp.json(), self._encoder)
        assert isinstance(decoded, int)
        return decoded

    def complete(self, *, id: str, counter: int) -> None:
        req = Request(
            method="post",
            url=f"{self._url}/tasks/heartbeat",
            json={
                "id": id,
                "counter": counter,
            },
        )
        resp = self._call(req)
        resp.raise_for_status()


def _call(req: Request) -> Response:
    prep_req = req.prepare()
    with Session() as s:
        while True:
            res = s.send(prep_req, timeout=5)
            if res.ok:
                return res
            if res.status_code == status_codes.BAD_REQUEST:
                msg = "Invalid Request"
                raise ResonateError(msg, "STORE_PAYLOAD")
            if res.status_code == status_codes.UNAUTHORIZED:
                msg = "Unauthorized request"
                raise ResonateError(msg, "STORE_UNAUTHORIZED")
            if res.status_code == status_codes.FORBIDDEN:
                msg = "Forbidden request"
                raise ResonateError(msg, "STORE_FORBIDDEN")
            if res.status_code == status_codes.NOT_FOUND:
                msg = "Not found"
                raise ResonateError(msg, "STORE_NOT_FOUND")
            if res.status_code == status_codes.CONFLICT:
                msg = "Already exists"
                raise ResonateError(msg, "STORE_ALREADY_EXISTS")
            logger.warning(
                "Unexpected code response from remote store: %s", res.status_code
            )
        time.sleep(1)
    raise NotImplementedError


class RemoteStore(IStore):
    def __init__(
        self, url: str | None = None, encoder: IEncoder[str, str] | None = None
    ) -> None:
        self._url = os.getenv("RESONATE_STORE_URL", url or "http://localhost:8001")
        self._encoder = encoder or Base64Encoder()

    @property
    def promises(self) -> RemotePromiseStore:
        return RemotePromiseStore(self._url, self._encoder)

    @property
    def tasks(self) -> RemoteTaskStore:
        return RemoteTaskStore(self._url, self._encoder)


class status_codes:  # noqa: N801
    BAD_REQUEST = 400
    UNAUTHORIZED = 401
    FORBIDDEN = 403
    NOT_FOUND = 404
    CONFLICT = 409


def _decode_value(
    data: dict[str, Any], key: Literal["param", "value"], encoder: IEncoder[str, str]
) -> Value:
    if data[key]:
        return Value(
            data=encoder.decode(data[key]["data"]),
            headers=data[key]["headers"],
        )
    return Value(data=None, headers={})


def _decode(
    data: dict[str, Any], encoder: IEncoder[str, str]
) -> DurablePromiseRecord | InvokeMesg | ResumeMesg | int:
    msg = "Unkown object %s"
    match data:
        case {
            "id": id,
            "state": state,
            "timeout": timeout,
            "createdOn": created_on,
            **rest,
        }:
            return DurablePromiseRecord(
                id=id,
                state=state,
                param=_decode_value(rest, "param", encoder),
                value=_decode_value(rest, "value", encoder),
                timeout=timeout,
                tags=rest.get("tags"),
                created_on=created_on,
                completed_on=rest.get("completedOn"),
                ikey_for_create=rest.get("idempotencyKeyForCreate"),
                ikey_for_complete=rest.get("idempotencyKeyForComplete"),
            )
        case {"promises": promises}:
            match promises:
                case {"leaf": leaf, "root": root}:
                    root_promise = _decode(root, encoder)
                    assert isinstance(root_promise, DurablePromiseRecord)
                    leaf_promise = _decode(leaf, encoder)
                    assert isinstance(leaf_promise, DurablePromiseRecord)
                    return ResumeMesg(root=root_promise, leaf=leaf_promise)
                case {"root": root}:
                    root_promise = _decode(root, encoder)
                    assert isinstance(root_promise, DurablePromiseRecord)
                    return InvokeMesg(root=root_promise)
                case {"tasksAffected": count}:
                    return count
                case _:
                    raise RuntimeError(msg, data)

        case _:
            raise RuntimeError(msg, data)
