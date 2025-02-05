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


def _decode_value(
    data: dict[str, Any], key: Literal["param", "value"], encoder: IEncoder[str, str]
) -> Value:
    if data[key]:
        return Value(
            data=encoder.decode(data[key]["data"]),
            headers=data[key]["headers"],
        )
    return Value(data=None, headers={})


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
        case {"promises": promises}:
            match promises:
                case {"leaf": leaf, "root": root}:
                    root_promise = decode(root, value_encoder)
                    leaf_promise = decode(leaf, value_encoder)
                    assert isinstance(root_promise, DurablePromiseRecord)
                    assert isinstance(leaf_promise, DurablePromiseRecord)
                    return ResumeMesg(root=root_promise, leaf=leaf_promise)
                case {"root": root}:
                    root_promise = decode(root, value_encoder)
                    assert isinstance(root_promise, DurablePromiseRecord)
                    return InvokeMesg(root=root_promise)
                case {"tasksAffected": count}:
                    return count
                case _:
                    raise RuntimeError(msg, data)
        case {"id": id, "counter": counter}:
            return TaskRecord(id, counter)
        case {"promise": promise, **rest}:
            promise = decode(promise, value_encoder)
            assert isinstance(promise, DurablePromiseRecord)
            if "task" in rest:
                task = decode(rest["task"], value_encoder)
                assert isinstance(task, TaskRecord)
                return promise, task
            return promise, None
        case _:
            raise RuntimeError(msg, data)
