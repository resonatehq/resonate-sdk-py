from __future__ import annotations

from typing import TYPE_CHECKING, Literal

from typing_extensions import Any

from resonate_sdk.store.models import (
    DurablePromiseRecord,
    InvokeMesg,
    ResumeMesg,
    TaskRecord,
    Value,
)

if TYPE_CHECKING:
    from resonate_sdk.encoder import IEncoder


def decode(
    data: dict[str, Any], value_encoder: IEncoder[str, str]
) -> (
    DurablePromiseRecord
    | InvokeMesg
    | ResumeMesg
    | int
    | TaskRecord
    | tuple[DurablePromiseRecord, TaskRecord | None]
):
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
                param=_decode_value(rest, "param", value_encoder),
                value=_decode_value(rest, "value", value_encoder),
                timeout=timeout,
                tags=rest.get("tags"),
                created_on=created_on,
                completed_on=rest.get("completedOn"),
                ikey_for_create=rest.get("idempotencyKeyForCreate"),
                ikey_for_complete=rest.get("idempotencyKeyForComplete"),
            )
        case {"promises": {"leaf": leaf, "root": root}}:
            root_promise = decode(root["data"], value_encoder)
            leaf_promise = decode(leaf["data"], value_encoder)
            assert isinstance(root_promise, DurablePromiseRecord)
            assert isinstance(leaf_promise, DurablePromiseRecord)
            return ResumeMesg(root=root_promise, leaf=leaf_promise)
        case {"promises": {"root": root}}:
            root_promise = decode(root["data"], value_encoder)
            assert isinstance(root_promise, DurablePromiseRecord)
            return InvokeMesg(root=root_promise)
        case {"id": id, "counter": counter}:
            return TaskRecord(id, counter)
        case {"promise": promise, "task": task}:
            task = decode(task, value_encoder)
            promise = decode(promise, value_encoder)
            assert isinstance(task, TaskRecord)
            assert isinstance(promise, DurablePromiseRecord)
            return promise, task
        case {"promise": promise}:
            promise = decode(promise, value_encoder)
            assert isinstance(promise, DurablePromiseRecord)
            return promise, None
        case {"href": _, "task": task}:
            task = decode(task, value_encoder)
            assert isinstance(task, TaskRecord)
            return task
        case {"tasksAffected": n}:
            return n

        case _:
            raise RuntimeError(msg, data)


def _decode_value(
    data: dict[str, Any], key: Literal["param", "value"], encoder: IEncoder[str, str]
) -> Value:
    if data[key]:
        headers = data[key].get("headers")
        _data = data[key].get("data")
        return Value(
            data=encoder.decode(_data) if _data else None,
            headers=headers,
        )
    return Value(data=None, headers={})
