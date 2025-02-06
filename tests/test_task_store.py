from __future__ import annotations

import os
import sys
from queue import Queue
from uuid import uuid4

import pytest

from resonate_sdk.store.models import TaskRecord
from resonate_sdk.store.remote import RemoteStore


@pytest.mark.skipif("RESONATE_STORE_URL" not in os.environ, reason="")
def test_case_4_transition_from_enqueued_to_claimed_via_claim() -> None:
    Queue[TaskRecord | None]()
    store = RemoteStore()
    pid = uuid4().hex
    promise_record, task_record = store.promises.create_with_task(
        id="id4",
        ikey=None,
        strict=False,
        headers=None,
        data=None,
        timeout=sys.maxsize,
        tags=None,
        pid=pid,
        ttl=sys.maxsize,
    )
    assert promise_record.state == "PENDING"
    assert promise_record.id == "id4"
    assert promise_record.ikey_for_create is None
    assert promise_record.ikey_for_complete is None
    assert task_record
    assert task_record.id == "id4"
