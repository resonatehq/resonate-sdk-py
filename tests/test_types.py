from __future__ import annotations

from typing import get_args

import msgspec
import pytest

from resonate.error import ApplicationError, SerializationError
from resonate.types import (
    Done,
    DurableKind,
    Outcome,
    PromiseCreateReq,
    PromiseRecord,
    PromiseRegisterCallbackData,
    PromiseSettleReq,
    ScheduleRecord,
    Status,
    Suspended,
    TaskData,
    TaskRecord,
    Value,
)


class Point(msgspec.Struct):
    x: int
    y: int


def from_wire_json(raw: str) -> Value:
    """Decode a JSON string then build a Value, mirroring Rust's `from_str::<Value>`."""
    return Value.from_wire(msgspec.json.decode(raw.encode()))


# --- serialization: omit_defaults mirrors serde `skip_serializing_if = is_none` ---


def test_encode_empty_value_is_empty_object() -> None:
    assert msgspec.json.encode(Value()) == b"{}"


def test_encode_headers_only() -> None:
    assert msgspec.json.encode(Value(headers={"a": "b"})) == b'{"headers":{"a":"b"}}'


def test_encode_data_only() -> None:
    assert msgspec.json.encode(Value(data=42)) == b'{"data":42}'


def test_encode_both_fields_in_field_order() -> None:
    encoded = msgspec.json.encode(Value(headers={"a": "b"}, data=42))
    assert encoded == b'{"headers":{"a":"b"},"data":42}'


# --- data_or_null ---


def test_data_or_null_defaults_to_null() -> None:
    assert Value().data is None


def test_data_or_null_returns_data() -> None:
    assert Value(data=42).data == 42


# --- headers_or_empty ---


def test_headers_or_empty_defaults_to_empty() -> None:
    assert Value().headers_or_empty() == {}


def test_headers_or_empty_returns_headers() -> None:
    assert Value(headers={"a": "b"}).headers_or_empty() == {"a": "b"}


# --- from_serializable ---


def test_from_serializable_wraps_data() -> None:
    v = Value.from_serializable(Point(1, 2))
    assert v.headers is None
    assert v.data == {"x": 1, "y": 2}
    assert msgspec.json.encode(v) == b'{"data":{"x":1,"y":2}}'


def test_from_serializable_unserializable_raises() -> None:
    with pytest.raises(SerializationError):
        Value.from_serializable(object())


# --- decode ---


def test_decode_roundtrip() -> None:
    v = Value.from_serializable(Point(1, 2))
    assert v.decode(Point) == Point(1, 2)


def test_decode_null_into_required_type_raises() -> None:
    with pytest.raises(SerializationError):
        Value().decode(int)


# --- from_wire: mirrors the Rust custom `Deserialize` impl ---


def test_from_wire_null_is_empty_value() -> None:
    v = from_wire_json("null")
    assert v.headers is None
    assert v.data is None


def test_from_wire_object_splits_headers_and_data() -> None:
    v = from_wire_json('{"headers":{"a":"b"},"data":[1,2,3]}')
    assert v.headers_or_empty() == {"a": "b"}
    assert v.data == [1, 2, 3]


def test_from_wire_invalid_headers_are_dropped() -> None:
    # headers that are not a `str -> str` map become None (Rust `.ok()`).
    assert from_wire_json('{"headers":[1,2],"data":1}').headers is None
    assert from_wire_json('{"headers":{"a":1}}').headers is None


def test_from_wire_bare_value_is_treated_as_data() -> None:
    assert from_wire_json("42").data == 42
    assert from_wire_json('"hello"').data == "hello"
    assert from_wire_json("[1,2,3]").data == [1, 2, 3]


def test_from_wire_object_without_data_field() -> None:
    v = from_wire_json("{}")
    assert v.headers is None
    assert v.data is None

    v2 = from_wire_json('{"headers":{"a":"b"}}')
    assert v2.headers_or_empty() == {"a": "b"}
    assert v2.data is None


# --- PromiseRecord: camelCase wire format + `#[serde(default)]` parity ---

PROMISE_FULL = (
    b'{"id":"p1","state":"resolved",'
    b'"param":{"data":1},'
    b'"value":{"headers":{"a":"b"},"data":[1,2]},'
    b'"tags":{"k":"v"},'
    b'"timeoutAt":10,"createdAt":5,"settledAt":9}'
)


def test_promise_record_decode_full() -> None:
    r = msgspec.json.decode(PROMISE_FULL, type=PromiseRecord)
    assert r.id == "p1"
    assert r.state == "resolved"
    assert r.param.data == 1
    assert r.value.headers_or_empty() == {"a": "b"}
    assert r.value.data == [1, 2]
    assert r.tags == {"k": "v"}
    assert r.timeout_at == 10
    assert r.created_at == 5
    assert r.settled_at == 9


def test_promise_record_decode_minimal_applies_defaults() -> None:
    # Only the Rust-required fields; the rest come from `#[serde(default)]`.
    r = msgspec.json.decode(
        b'{"id":"p1","state":"pending","timeoutAt":10}', type=PromiseRecord
    )
    assert r.param.data is None
    assert r.param.headers is None
    assert r.value.data is None
    assert r.tags == {}
    assert r.created_at == 0
    assert r.settled_at is None


def test_promise_record_decode_missing_required_field_raises() -> None:
    with pytest.raises(msgspec.ValidationError):
        msgspec.json.decode(b'{"id":"p1","state":"pending"}', type=PromiseRecord)


def test_promise_record_encode_camel_and_field_order() -> None:
    r = PromiseRecord(
        id="p1",
        state="pending",
        param=Value(data=1),
        tags={"k": "v"},
        timeout_at=10,
        created_at=5,
    )
    assert msgspec.json.encode(r) == (
        b'{"id":"p1","state":"pending","param":{"data":1},"value":{},'
        b'"tags":{"k":"v"},"timeoutAt":10,"createdAt":5,"settledAt":null}'
    )


# --- TaskRecord ---


def test_task_record_decode_minimal_applies_defaults() -> None:
    r = msgspec.json.decode(
        b'{"id":"t1","state":"pending","version":1}', type=TaskRecord
    )
    assert r.resumes is None
    assert r.ttl is None
    assert r.pid is None


def test_task_record_resumes_variants() -> None:
    cases: list[tuple[bytes, list[str] | int | bool | None]] = [
        (b'["a","b"]', ["a", "b"]),
        (b"5", 5),
        (b"true", True),
        (b"null", None),
    ]
    for raw, expected in cases:
        r = msgspec.json.decode(
            b'{"id":"t","state":"pending","version":1,"resumes":' + raw + b"}",
            type=TaskRecord,
        )
        assert r.resumes == expected
        # bool is a subclass of int -- guard against int/bool confusion.
        assert type(r.resumes) is type(expected)


def test_task_record_encode() -> None:
    r = TaskRecord(id="t1", state="acquired", version=2, resumes=["a"], ttl=30, pid="x")
    assert (
        msgspec.json.encode(r)
        == b'{"id":"t1","state":"acquired","version":2,"resumes":["a"],"ttl":30,"pid":"x"}'
    )


def test_task_record_encode_defaults_emit_null() -> None:
    r = TaskRecord(id="t1", state="pending", version=1)
    assert (
        msgspec.json.encode(r)
        == b'{"id":"t1","state":"pending","version":1,"resumes":null,"ttl":null,"pid":null}'
    )


# --- ScheduleRecord ---

SCHEDULE_FULL = (
    b'{"id":"s1","cron":"* * * * *","promiseId":"p1","promiseTimeout":100,'
    b'"promiseParam":{"data":1},"promiseTags":{"k":"v"},'
    b'"createdAt":1,"nextRunAt":2,"lastRunAt":3}'
)


def test_schedule_record_decode_full() -> None:
    r = msgspec.json.decode(SCHEDULE_FULL, type=ScheduleRecord)
    assert r.id == "s1"
    assert r.cron == "* * * * *"
    assert r.promise_id == "p1"
    assert r.promise_timeout == 100
    assert r.promise_param.data == 1
    assert r.promise_tags == {"k": "v"}
    assert r.created_at == 1
    assert r.next_run_at == 2
    assert r.last_run_at == 3


def test_schedule_record_decode_minimal_applies_defaults() -> None:
    r = msgspec.json.decode(
        b'{"id":"s1","cron":"c","promiseId":"p1","promiseTimeout":100}',
        type=ScheduleRecord,
    )
    assert r.promise_param.data is None
    assert r.promise_tags == {}
    assert r.created_at == 0
    assert r.next_run_at == 0
    assert r.last_run_at is None


def test_schedule_record_encode() -> None:
    r = ScheduleRecord(
        id="s1",
        cron="c",
        promise_id="p1",
        promise_timeout=100,
        created_at=1,
        next_run_at=2,
    )
    assert msgspec.json.encode(r) == (
        b'{"id":"s1","cron":"c","promiseId":"p1","promiseTimeout":100,'
        b'"promiseParam":{},"promiseTags":{},"createdAt":1,"nextRunAt":2,"lastRunAt":null}'
    )


# --- PromiseCreateReq: camelCase wire format + default_with_id ---


def test_promise_create_req_encode_camel_and_field_order() -> None:
    r = PromiseCreateReq(id="p1", timeout_at=10, param=Value(data=1), tags={"k": "v"})
    assert msgspec.json.encode(r) == (
        b'{"id":"p1","timeoutAt":10,"param":{"data":1},"tags":{"k":"v"}}'
    )


def test_promise_create_req_decode_camel() -> None:
    r = msgspec.json.decode(
        b'{"id":"p1","timeoutAt":10,"param":{"data":1},"tags":{"k":"v"}}',
        type=PromiseCreateReq,
    )
    assert r.id == "p1"
    assert r.timeout_at == 10
    assert r.param.data == 1
    assert r.tags == {"k": "v"}


def test_promise_create_req_default_with_id() -> None:
    r = PromiseCreateReq(id="p1")
    assert r.id == "p1"
    assert r.timeout_at == 0
    assert r.param.headers is None
    assert r.param.data is None
    assert r.tags == {}


# --- PromiseSettleReq ---


def test_promise_settle_req_encode() -> None:
    r = PromiseSettleReq(id="p1", state="resolved", value=Value(data=1))
    assert (
        msgspec.json.encode(r) == b'{"id":"p1","state":"resolved","value":{"data":1}}'
    )


def test_promise_settle_req_decode() -> None:
    r = msgspec.json.decode(
        b'{"id":"p1","state":"rejected_canceled","value":{}}', type=PromiseSettleReq
    )
    assert r.id == "p1"
    assert r.state == "rejected_canceled"
    assert r.value.data is None


# --- PromiseRegisterCallbackData ---


def test_promise_register_callback_data_roundtrip() -> None:
    d = PromiseRegisterCallbackData(awaited="a", awaiter="b")
    encoded = msgspec.json.encode(d)
    assert encoded == b'{"awaited":"a","awaiter":"b"}'
    back = msgspec.json.decode(encoded, type=PromiseRegisterCallbackData)
    assert back.awaited == "a"
    assert back.awaiter == "b"


# --- TaskData: `func` / `args` (no rename) + `args` default null + into_value ---


def test_task_data_decode_minimal_applies_default_args() -> None:
    d = msgspec.json.decode(b'{"func":"f"}', type=TaskData)
    assert d.func == "f"
    assert d.args is None


def test_task_data_decode_full() -> None:
    d = msgspec.json.decode(b'{"func":"f","args":[1,2]}', type=TaskData)
    assert d.func == "f"
    assert d.args == [1, 2]


def test_task_data_encode() -> None:
    d = TaskData(func="f", args={"x": 1})
    assert msgspec.json.encode(d) == b'{"func":"f","args":{"x":1}}'


def test_task_data_encode_default_args_emit_null() -> None:
    d = TaskData(func="f")
    assert msgspec.json.encode(d) == b'{"func":"f","args":null}'


def test_task_data_into_value_wraps_func_and_args() -> None:
    v = TaskData.into_value("f", [1, 2])
    assert v.headers is None
    # Compare structurally: serde_json (BTreeMap) and Python dict differ in key
    # order on the wire, but the decoded content is identical.
    assert v.data == {"func": "f", "args": [1, 2]}


def test_task_data_into_value_unserializable_raises() -> None:
    with pytest.raises(SerializationError):
        TaskData.into_value("f", object())


# --- DurableKind / Status: internal string literals ---


def test_durable_kind_permitted_values() -> None:
    assert get_args(DurableKind.__value__) == ("function", "workflow")


def test_status_permitted_values() -> None:
    assert get_args(Status.__value__) == ("done", "suspended")


# --- Outcome: sum type mirroring `Outcome<T> { Done(Result<T>), Suspended }` ---


def test_outcome_done_holds_ok_result() -> None:
    o: Outcome[int] = Done(42)
    match o:
        case Done(result):
            assert result == 42
        case _:
            pytest.fail("expected Done")


def test_outcome_done_holds_err_result() -> None:
    err = ApplicationError("boom")
    o: Outcome[int] = Done(err)
    match o:
        case Done(result):
            assert result is err
        case _:
            pytest.fail("expected Done")


def test_outcome_suspended_holds_remote_todos() -> None:
    o: Outcome[None] = Suspended(remote_todos=["a", "b"])
    match o:
        case Suspended(remote_todos):
            assert remote_todos == ["a", "b"]
        case _:
            pytest.fail("expected Suspended")
