from __future__ import annotations

from typing import TYPE_CHECKING

from resonate_sdk.encoder import Base64Encoder
from resonate_sdk.errors import ResonateError
from resonate_sdk.store.models import (
    DurablePromiseRecord,
    InvokeMesg,
    ResumeMesg,
    TaskRecord,
    Value,
)
from resonate_sdk.store.traits import IPromiseStore, IStore, ITaskStore
from resonate_sdk.time import now

if TYPE_CHECKING:
    from resonate_sdk.encoder import IEncoder
    from resonate_sdk.typing import State


class LocalPromiseStore(IPromiseStore):
    def __init__(
        self,
        promises: dict[str, DurablePromiseRecord],
        tasks: dict[str, TaskRecord],
        encoder: IEncoder[str, str],
    ) -> None:
        self._promises = promises
        self._tasks = tasks
        self._encoder = encoder

    def _timeout(self, promise: DurablePromiseRecord) -> DurablePromiseRecord:
        new_state: State = "REJECTED_TIMEDOUT"
        if promise.state == "PENDING" and now() >= promise.timeout:
            if promise.tags and promise.tags.get("resonate:timeout") == "true":
                new_state = "RESOLVED"
            return DurablePromiseRecord(
                state=new_state,
                id=promise.id,
                timeout=promise.timeout,
                param=promise.param,
                value=Value(headers={}, data=None),
                tags=promise.tags,
                created_on=promise.created_on,
                completed_on=promise.timeout,
                ikey_for_create=promise.ikey_for_create,
                ikey_for_complete=None,
            )
        return promise

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
        def _create(record: DurablePromiseRecord | None) -> DurablePromiseRecord:
            if record is None:
                return DurablePromiseRecord(
                    state="PENDING",
                    id=id,
                    timeout=timeout,
                    param=Value(
                        headers=headers or {},
                        data=self._encoder.encode(data) if data else None,
                    ),
                    value=Value(headers={}, data=None),
                    created_on=now(),
                    completed_on=None,
                    ikey_for_create=ikey,
                    ikey_for_complete=None,
                    tags=tags or {},
                )
            if strict and record.state != "PENDING":
                msg = "Forbidden request: Durable promise previously created"
                raise ResonateError(msg, "STORE_FORBIDDEN")
            if record.ikey_for_create is None or ikey != record.ikey_for_create:
                msg = "Forbidden request: Missing idempotency key for create"
                raise ResonateError(msg, "STORE_FORBIDDEN")
            return record

        new_item = self._timeout(_create(self._promises.get(id)))
        self._promises[id] = new_item
        return new_item

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
        promise_record = self.create(
            id=id,
            ikey=ikey,
            strict=strict,
            headers=headers,
            data=data,
            timeout=timeout,
            tags=tags,
        )
        return promise_record, None

    def reject(
        self,
        *,
        id: str,
        ikey: str | None,
        strict: bool,
        headers: dict[str, str] | None,
        data: str | None,
    ) -> DurablePromiseRecord:
        def _reject(record: DurablePromiseRecord | None) -> DurablePromiseRecord:
            if record is None:
                msg = "Not found"
                raise ResonateError(msg, "STORE_NOT_FOUND")
            if record.state == "PENDING":
                return DurablePromiseRecord(
                    state="REJECTED",
                    id=record.id,
                    timeout=record.timeout,
                    param=record.param,
                    value=Value(
                        headers=headers or {},
                        data=self._encoder.encode(data) if data else None,
                    ),
                    created_on=record.created_on,
                    completed_on=now(),
                    ikey_for_create=record.ikey_for_create,
                    ikey_for_complete=ikey,
                    tags=record.tags,
                )
            if strict and record.state != "REJECTED":
                msg = "Forbidden request"
                raise ResonateError(msg, "STORE_FORBIDDEN")
            if record.state != "REJECTED_TIMEDOUT" and (
                record.ikey_for_complete is None or ikey != record.ikey_for_complete
            ):
                msg = "Forbidden request"
                raise ResonateError(msg, "STORE_FORBIDDEN")

            return record

        new_item = self._timeout(_reject(self._promises.get(id)))
        self._promises[id] = new_item
        return new_item

    def resolve(
        self,
        *,
        id: str,
        ikey: str | None,
        strict: bool,
        headers: dict[str, str] | None,
        data: str | None,
    ) -> DurablePromiseRecord:
        def _resolve(record: DurablePromiseRecord | None) -> DurablePromiseRecord:
            if record is None:
                msg = "Not found"
                raise ResonateError(msg, "STORE_NOT_FOUND")
            if record.state == "PENDING":
                return DurablePromiseRecord(
                    state="RESOLVED",
                    id=record.id,
                    timeout=record.timeout,
                    param=record.param,
                    value=Value(
                        headers=headers or {},
                        data=self._encoder.encode(data) if data else None,
                    ),
                    created_on=record.created_on,
                    completed_on=now(),
                    ikey_for_create=record.ikey_for_create,
                    ikey_for_complete=ikey,
                    tags=record.tags,
                )
            if strict and record.state != "RESOLVED":
                msg = "Forbidden request"
                raise ResonateError(msg, "STORE_FORBIDDEN")
            if record.state != "REJECTED_TIMEDOUT" and (
                record.ikey_for_complete is None or ikey != record.ikey_for_complete
            ):
                msg = "Forbidden request"
                raise ResonateError(msg, "STORE_FORBIDDEN")

            return record

        new_item = self._timeout(_resolve(self._promises.get(id)))
        self._promises[id] = new_item
        return new_item

    def cancel(
        self,
        *,
        id: str,
        ikey: str | None,
        strict: bool,
        headers: dict[str, str] | None,
        data: str | None,
    ) -> DurablePromiseRecord:
        def _cancel(record: DurablePromiseRecord | None) -> DurablePromiseRecord:
            if record is None:
                msg = "Not Found"
                raise ResonateError(msg, "STORE_NOT_FOUND")
            if record.state == "PENDING":
                return DurablePromiseRecord(
                    state="REJECTED_CANCELED",
                    id=record.id,
                    timeout=record.timeout,
                    param=record.param,
                    value=Value(
                        headers=headers or {},
                        data=self._encoder.encode(data) if data else None,
                    ),
                    created_on=record.created_on,
                    completed_on=now(),
                    ikey_for_create=record.ikey_for_create,
                    ikey_for_complete=ikey,
                    tags=record.tags,
                )
            if strict and record.state != "REJECTED_CANCELED":
                msg = "Forbidden request"
                raise ResonateError(msg, "STORE_FORBIDDEN")
            if record.state != "REJECTED_TIMEDOUT" and (
                record.ikey_for_complete is None or ikey != record.ikey_for_complete
            ):
                msg = "Forbidden request"
                raise ResonateError(msg, "STORE_FORBIDDEN")
            return record

        new_item = self._timeout(_cancel(self._promises.get(id)))
        self._promises[id] = new_item
        return new_item


class LocalTaskStore(ITaskStore):
    def __init__(
        self,
        promises: dict[str, DurablePromiseRecord],
        tasks: dict[str, TaskRecord],
        encoder: IEncoder[str, str],
    ) -> None:
        self._promises = promises
        self._tasks = tasks
        self._encoder = encoder

    def claim(
        self, *, id: str, counter: int, pid: str, ttl: int
    ) -> InvokeMesg | ResumeMesg:
        raise NotImplementedError

    def heartbeat(self, *, pid: str) -> int:
        raise NotImplementedError

    def complete(self, *, id: str, counter: int) -> None:
        raise NotImplementedError


class LocalStore(IStore):
    def __init__(self, encoder: IEncoder[str, str] | None = None) -> None:
        self._encoder = encoder or Base64Encoder()
        self._promises: dict[str, DurablePromiseRecord] = {}
        self._tasks: dict[str, TaskRecord] = {}

    @property
    def promises(self) -> IPromiseStore:
        return LocalPromiseStore(self._promises, self._tasks, self._encoder)

    @property
    def tasks(self) -> ITaskStore:
        return LocalTaskStore(self._promises, self._tasks, self._encoder)
