from __future__ import annotations

import sys

from resonate.storage import RemotePromiseStore


def test_case_0_transition_from_init_to_pending_via_create() -> None:
    store = RemotePromiseStore(url="http://127.0.0.1:8001")
    promise_record = store.create(
        promise_id="id0",
        ikey=None,
        strict=True,
        headers=None,
        data=None,
        timeout=sys.maxsize,
        tags=None,
    )
    assert promise_record.state == "PENDING"
    assert promise_record.promise_id == "id0"
    assert promise_record.idempotency_key_for_create is None
    assert promise_record.idempotency_key_for_complete is None


def test_case_1_transition_from_init_to_pending_via_create() -> None:
    store = RemotePromiseStore(url="http://127.0.0.1:8001")
    promise_record = store.create(
        promise_id="id1",
        ikey=None,
        strict=True,
        headers=None,
        data=None,
        timeout=sys.maxsize,
        tags=None,
    )
    assert promise_record.state == "PENDING"
    assert promise_record.promise_id == "id1"
    assert promise_record.idempotency_key_for_create is None
    assert promise_record.idempotency_key_for_complete is None


def test_case_2_transition_from_init_to_pending_via_create() -> None:
    store = RemotePromiseStore(url="http://127.0.0.1:8001")
    promise_record = store.create(
        promise_id="id2",
        ikey="ikc",
        strict=True,
        headers=None,
        data=None,
        timeout=sys.maxsize,
        tags=None,
    )
    assert promise_record.state == "PENDING"
    assert promise_record.promise_id == "id2"
    assert promise_record.idempotency_key_for_create == "ikc"
    assert promise_record.idempotency_key_for_complete is None
