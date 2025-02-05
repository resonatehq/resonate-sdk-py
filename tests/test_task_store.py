from __future__ import annotations

import os
import sys
from functools import cache
from queue import Queue
from typing import TYPE_CHECKING

import pytest

from resonate_sdk.store.local import LocalStore
from resonate_sdk.store.models import TaskRecord
from resonate_sdk.store.remote import RemoteStore
from resonate_sdk.task_sources.poller import Poller

if TYPE_CHECKING:
    from resonate_sdk.store.traits import IStore
    from resonate_sdk.task_sources.traits import ITaskSource


@cache
def _stores_and_task_sources() -> list[tuple[IStore, ITaskSource]]:
    stores: list[tuple[IStore, ITaskSource]] = [(LocalStore(), Poller())]
    if "RESONATE_STORE_URL" in os.environ and os.environ == "RESONATE_POLLER_URL":
        stores.append((RemoteStore(), Poller()))
    return stores


@pytest.mark.parametrize("store_and_task_poller", _stores_and_task_sources())
def test_case_4_transition_from_enqueued_to_claimed_via_claim(
    store_and_task_poller: tuple[IStore, ITaskSource],
) -> None:
    cq = Queue[TaskRecord | None]()
    store, task_poller = store_and_task_poller
    pid = "pid"
    task_poller.start(cq, pid)
    promise_record, task_record = store.promises.create_with_task(
        id="id4",
        ikey=None,
        strict=True,
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
