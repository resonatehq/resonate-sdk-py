from __future__ import annotations

import os
import sys
from queue import Queue
from typing import TYPE_CHECKING
from uuid import uuid4

import pytest

from resonate_sdk import targets
from resonate_sdk.errors import ResonateError
from resonate_sdk.store.models import DurablePromiseRecord, InvokeMesg, TaskRecord
from resonate_sdk.store.remote import RemoteStore
from resonate_sdk.task_sources.poller import Poller

if TYPE_CHECKING:
    from resonate_sdk.store.traits import IStore


def _prevent_orphan_task(
    store: IStore,
    task_record: TaskRecord,
    promise_record: DurablePromiseRecord,
) -> None:
    store.promises.resolve(
        id=promise_record.id, ikey=None, strict=False, headers=None, data=None
    )
    store.tasks.complete(id=task_record.id, counter=task_record.counter)


@pytest.mark.skipif("RESONATE_STORE_URL" not in os.environ, reason="")
def test_base_case() -> None:
    store, task_source = RemoteStore(), Poller()
    pid = uuid4().hex
    promise_record, task_record = store.promises.create_with_task(
        id="idbase",
        ikey=None,
        strict=False,
        headers=None,
        data=None,
        timeout=sys.maxsize,
        tags={"resonate:invoke": task_source.recv(pid)},
        pid=pid,
        ttl=sys.maxsize,
    )
    assert promise_record.state == "PENDING"
    assert promise_record.id == "idbase"
    assert promise_record.ikey_for_create is None
    assert promise_record.ikey_for_complete is None
    assert task_record
    assert task_record.id == "1"
    assert task_record.counter == 1
    store.promises.resolve(
        id=promise_record.id, ikey=None, strict=False, headers=None, data=None
    )
    store.tasks.complete(id=task_record.id, counter=task_record.counter)


@pytest.mark.skipif("RESONATE_STORE_URL" not in os.environ, reason="")
def test_case_5_transition_from_enqueue_to_claimed_via_claim() -> None:
    store, task_source = RemoteStore(), Poller()
    cq = Queue[TaskRecord]()
    pid = uuid4().hex
    promise_record = store.promises.create(
        id="id5",
        ikey=None,
        strict=False,
        headers=None,
        data=None,
        timeout=sys.maxsize,
        tags={"resonate:invoke": targets.poll(task_source.group, pid)},
    )
    task_source.run(cq=cq, pid=pid)
    task_record = cq.get()
    assert task_record.counter == 1
    store.tasks.claim(
        id=task_record.id, counter=task_record.counter, pid=pid, ttl=sys.maxsize
    )
    _prevent_orphan_task(store, task_record, promise_record)


@pytest.mark.skipif("RESONATE_STORE_URL" not in os.environ, reason="")
def test_case_6_transition_from_enqueue_to_enqueue_via_claim() -> None:
    store, task_source = RemoteStore(), Poller()
    cq = Queue[TaskRecord]()
    pid = uuid4().hex
    promise_record = store.promises.create(
        id="id6",
        ikey=None,
        strict=False,
        headers=None,
        data=None,
        timeout=sys.maxsize,
        tags={"resonate:invoke": targets.poll(task_source.group, pid)},
    )
    task_source.run(cq=cq, pid=pid)
    task_record = cq.get()
    assert task_record.counter == 1
    with pytest.raises(ResonateError):
        store.tasks.claim(
            id=task_record.id,
            counter=task_record.counter + 1,
            pid=pid,
            ttl=sys.maxsize,
        )
    store.tasks.claim(
        id=task_record.id, counter=task_record.counter, pid=pid, ttl=sys.maxsize
    )
    _prevent_orphan_task(store, task_record, promise_record)


@pytest.mark.skipif("RESONATE_STORE_URL" not in os.environ, reason="")
def test_case_8_transition_from_enqueue_to_enqueue_via_complete() -> None:
    store, task_source = RemoteStore(), Poller()
    cq = Queue[TaskRecord]()
    pid = uuid4().hex
    promise_record = store.promises.create(
        id="id8",
        ikey=None,
        strict=False,
        headers=None,
        data=None,
        timeout=sys.maxsize,
        tags={"resonate:invoke": targets.poll(task_source.group, pid)},
    )
    task_source.run(cq=cq, pid=pid)
    task_record = cq.get()
    assert task_record.counter == 1
    with pytest.raises(ResonateError):
        store.tasks.complete(
            id=task_record.id,
            counter=task_record.counter,
        )

    store.tasks.claim(
        id=task_record.id, counter=task_record.counter, pid=pid, ttl=sys.maxsize
    )
    _prevent_orphan_task(store, task_record, promise_record)


@pytest.mark.skipif("RESONATE_STORE_URL" not in os.environ, reason="")
def test_case_10_transition_from_enqueue_to_enqueue_via_hearbeat() -> None:
    store, task_source = RemoteStore(), Poller()
    cq = Queue[TaskRecord]()
    pid = uuid4().hex
    promise_record = store.promises.create(
        id="id10",
        ikey=None,
        strict=False,
        headers=None,
        data=None,
        timeout=sys.maxsize,
        tags={"resonate:invoke": targets.poll(task_source.group, pid)},
    )
    affected_tasks = store.tasks.heartbeat(pid=pid)
    assert affected_tasks == 0
    task_source.run(cq=cq, pid=pid)
    task_record = cq.get()
    store.tasks.claim(
        id=task_record.id, counter=task_record.counter, pid=pid, ttl=sys.maxsize
    )
    _prevent_orphan_task(store, task_record, promise_record)


@pytest.mark.skipif("RESONATE_STORE_URL" not in os.environ, reason="")
def test_case_12_transition_from_claimed_to_claimed_via_claim() -> None:
    store, task_source = RemoteStore(), Poller()
    cq = Queue[TaskRecord]()
    pid = uuid4().hex
    promise_record = store.promises.create(
        id="id12",
        ikey=None,
        strict=False,
        headers=None,
        data=None,
        timeout=sys.maxsize,
        tags={"resonate:invoke": targets.poll(task_source.group, pid)},
    )
    task_source.run(cq=cq, pid=pid)
    task_record = cq.get()
    invoke = store.tasks.claim(
        id=task_record.id, counter=task_record.counter, pid=pid, ttl=sys.maxsize
    )
    assert isinstance(invoke, InvokeMesg)
    with pytest.raises(ResonateError):
        store.tasks.claim(
            id=task_record.id, counter=task_record.counter, pid=pid, ttl=sys.maxsize
        )

    _prevent_orphan_task(store, task_record, promise_record)


@pytest.mark.skipif("RESONATE_STORE_URL" not in os.environ, reason="")
def test_case_13_transition_from_claimed_to_init_via_claim() -> None:
    store, task_source = RemoteStore(), Poller()
    cq = Queue[TaskRecord]()
    pid = uuid4().hex
    promise_record = store.promises.create(
        id="id13",
        ikey=None,
        strict=False,
        headers=None,
        data=None,
        timeout=sys.maxsize,
        tags={"resonate:invoke": targets.poll(task_source.group, pid)},
    )
    task_source.run(cq=cq, pid=pid)
    task_record = cq.get()
    invoke = store.tasks.claim(
        id=task_record.id, counter=task_record.counter, pid=pid, ttl=1
    )
    task_source.run(cq=cq, pid=pid)
    task_record = cq.get()
    invoke = store.tasks.claim(
        id=task_record.id, counter=task_record.counter, pid=pid, ttl=sys.maxsize
    )
    assert isinstance(invoke, InvokeMesg)

    _prevent_orphan_task(store, task_record, promise_record)
