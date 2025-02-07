from __future__ import annotations

import os
import sys
import time
from functools import cache
from queue import Queue
from typing import TYPE_CHECKING

import pytest

from resonate_sdk import targets
from resonate_sdk.errors import ResonateError
from resonate_sdk.models.task import Mesg, TaskRecord
from resonate_sdk.stores import RemoteStore
from resonate_sdk.stores._local import LocalStore, Recv
from resonate_sdk.task_sources import Poller

if TYPE_CHECKING:
    from resonate_sdk.stores.traits import IStore

TICK_TIME = 1


class LocalSender:
    def __init__(self, cq: Queue[TaskRecord]) -> None:
        self.cq = cq

    def send(self, recv: Recv, mesg: Mesg) -> None:
        self.cq.put(mesg.task)


@pytest.fixture
def depedencies() -> list[tuple[IStore, Queue[TaskRecord]]]:
    local = LocalStore()
    cq = Queue[TaskRecord]()
    local.add_sender(targets.poll("default", "pid"), LocalSender(cq=cq))
    stores: list[tuple[IStore, Queue[TaskRecord]]] = [(local, cq)]
    if "RESONATE_STORE_URL" in os.environ:
        p = Poller()
        p.run(cq=cq, pid="pid")
        stores.append((RemoteStore(), cq))
    return stores

def test_case_5_transition_from_enqueue_to_claimed_via_claim(
    depedencies: tuple[IStore, Queue[TaskRecord]]
) -> None:
    store, cq = depedencies
    store.promises.create(
        id="task5",
        ikey=None,
        strict=False,
        headers=None,
        data=None,
        timeout=sys.maxsize,
        tags={"resonate:invoke": targets.poll("default", "pid")},
    )
    task_record = cq.get()
    store.tasks.claim(
        id=task_record.id, counter=task_record.counter, pid="task5", ttl=sys.maxsize
    )


@pytest.mark.parametrize(("store", "cq"), _stores())
def test_case_6_transition_from_enqueue_to_enqueue_via_claim(
    store: IStore, cq: Queue[TaskRecord]
) -> None:
    store.promises.create(
        id="task6",
        ikey=None,
        strict=False,
        headers=None,
        data=None,
        timeout=sys.maxsize,
        tags={"resonate:invoke": targets.poll("default", "pid")},
    )
    task_record = cq.get()
    with pytest.raises(ResonateError):
        store.tasks.claim(
            id=task_record.id,
            counter=task_record.counter + 1,
            pid="task6",
            ttl=sys.maxsize,
        )


@pytest.mark.parametrize(("store", "cq"), _stores())
def test_case_8_transition_from_enqueue_to_enqueue_via_complete(
    store: IStore, cq: Queue[TaskRecord]
) -> None:
    store.promises.create(
        id="task8",
        ikey=None,
        strict=False,
        headers=None,
        data=None,
        timeout=sys.maxsize,
        tags={"resonate:invoke": targets.poll("default", "pid")},
    )
    task_record = cq.get()
    assert task_record.counter == 1
    with pytest.raises(ResonateError):
        store.tasks.complete(
            id=task_record.id,
            counter=task_record.counter,
        )


@pytest.mark.parametrize(("store", "cq"), _stores())
def test_case_10_transition_from_enqueue_to_enqueue_via_hearbeat(
    store: IStore, cq: Queue[TaskRecord]
) -> None:
    store.promises.create(
        id="task10",
        ikey=None,
        strict=False,
        headers=None,
        data=None,
        timeout=sys.maxsize,
        tags={"resonate:invoke": targets.poll("default", "pid")},
    )
    cq.get()
    assert store.tasks.heartbeat(pid="task10") == 0


@pytest.mark.parametrize(("store", "cq"), _stores())
def test_case_12_transition_from_claimed_to_claimed_via_claim(store: IStore, cq: Queue[TaskRecord]) -> None:
    store.promises.create(
        id="task12",
        ikey=None,
        strict=False,
        headers=None,
        data=None,
        timeout=sys.maxsize,
        tags={"resonate:invoke": targets.poll("default", "pid")},
    )
    task_record = cq.get()
    store.tasks.claim(
        id=task_record.id, counter=task_record.counter, pid="pid", ttl=sys.maxsize
    )
    with pytest.raises(ResonateError):
        store.tasks.claim(
            id=task_record.id,
            counter=task_record.counter,
            pid="task12",
            ttl=sys.maxsize,
        )


@pytest.mark.skipif("RESONATE_STORE_URL" not in os.environ, reason="")
def test_case_13_transition_from_claimed_to_init_via_claim(store: IStore, cq: Queue[TaskRecord]) -> None:
    raise NotImplementedError()


@pytest.mark.parametrize(("store", "cq"), _stores())
def test_case_14_transition_from_claimed_to_completed_via_complete(store: IStore, cq: Queue[TaskRecord]) -> None:
    store.promises.create(
        id="task14",
        ikey=None,
        strict=False,
        headers=None,
        data=None,
        timeout=sys.maxsize,
        tags={"resonate:invoke": targets.poll("default", "pid")},
    )
    task_record = cq.get()
    store.tasks.claim(
        id=task_record.id, counter=task_record.counter, pid="pid", ttl=sys.maxsize
    )
    store.tasks.complete(id=task_record.id, counter=task_record.counter)


@pytest.mark.skipif("RESONATE_STORE_URL" not in os.environ, reason="")
def test_case_15_transition_from_claimed_to_init_via_complete() -> None:
    store, task_source, cq = RemoteStore(), Poller(group="task15"), Queue[TaskRecord]()
    store.promises.create(
        id="task15",
        ikey=None,
        strict=False,
        headers=None,
        data=None,
        timeout=sys.maxsize,
        tags={"resonate:invoke": targets.poll(task_source.group, "task15")},
    )
    task_source.run(cq=cq, pid="task15")
    task_record = cq.get()
    store.tasks.claim(
        id=task_record.id, counter=task_record.counter, pid="task15", ttl=0
    )
    time.sleep(TICK_TIME)
    with pytest.raises(ResonateError):
        store.tasks.complete(id=task_record.id, counter=task_record.counter)


@pytest.mark.parametrize(("store", "cq"), _stores())
def test_case_16_transition_from_claimed_to_claimed_via_complete(store: IStore, cq: Queue[TaskRecord]) -> None:
    store.promises.create(
        id="task16",
        ikey=None,
        strict=False,
        headers=None,
        data=None,
        timeout=sys.maxsize,
        tags={"resonate:invoke": targets.poll("default", "pid")},
    )
    task_record = cq.get()
    store.tasks.claim(
        id=task_record.id, counter=task_record.counter, pid="pid", ttl=sys.maxsize
    )
    with pytest.raises(ResonateError):
        store.tasks.complete(id=task_record.id, counter=task_record.counter + 1)


@pytest.mark.skipif("RESONATE_STORE_URL" not in os.environ, reason="")
def test_case_17_transition_from_claimed_to_init_via_complete() -> None:
    store, task_source, cq = RemoteStore(), Poller(group="task17"), Queue[TaskRecord]()
    store.promises.create(
        id="task17",
        ikey=None,
        strict=False,
        headers=None,
        data=None,
        timeout=sys.maxsize,
        tags={"resonate:invoke": targets.poll(task_source.group, "task17")},
    )
    task_source.run(cq=cq, pid="task17")
    task_record = cq.get()
    store.tasks.claim(
        id=task_record.id, counter=task_record.counter, pid="task17", ttl=0
    )
    time.sleep(TICK_TIME)
    with pytest.raises(ResonateError):
        store.tasks.complete(id=task_record.id, counter=task_record.counter)


@pytest.mark.parametrize(("store", "cq"), _stores())
def test_case_18_transition_from_claimed_to_claimed_via_heartbeat(store: IStore, cq: Queue[TaskRecord]) -> None:
    store.promises.create(
        id="task18",
        ikey=None,
        strict=False,
        headers=None,
        data=None,
        timeout=sys.maxsize,
        tags={"resonate:invoke": targets.poll("default", "pid")},
    )
    task_record = cq.get()
    store.tasks.claim(
        id=task_record.id, counter=task_record.counter, pid="pid", ttl=sys.maxsize
    )
    assert store.tasks.heartbeat(pid="pid") == 1


@pytest.mark.parametrize(("store", "cq"), _stores())
def test_case_19_transition_from_claimed_to_init_via_heartbeat(store: IStore, cq: Queue[TaskRecord]) -> None:
    store.promises.create(
        id="task19",
        ikey=None,
        strict=False,
        headers=None,
        data=None,
        timeout=sys.maxsize,
        tags={"resonate:invoke": targets.poll("default", "pid")},
    )
    task_record = cq.get()
    store.tasks.claim(
        id=task_record.id, counter=task_record.counter, pid="pid", ttl=0
    )

    assert store.tasks.heartbeat(pid="pid") == 1


@pytest.mark.parametrize(("store", "cq"), _stores())
def test_case_20_transition_from_completed_to_completed_via_claim(store: IStore, cq: Queue[TaskRecord]) -> None:
    store.promises.create(
        id="task20",
        ikey=None,
        strict=False,
        headers=None,
        data=None,
        timeout=sys.maxsize,
        tags={"resonate:invoke": targets.poll("default", "pid")},
    )
    task_record = cq.get()
    store.tasks.claim(
        id=task_record.id, counter=task_record.counter, pid="pid", ttl=sys.maxsize
    )
    store.tasks.complete(id=task_record.id, counter=task_record.counter)
    with pytest.raises(ResonateError):
        store.tasks.claim(
            id=task_record.id, counter=task_record.counter, pid="pid", ttl=0
        )


@pytest.mark.parametrize(("store", "cq"), _stores())
def test_case_21_transition_from_completed_to_completed_via_complete(store: IStore, cq: Queue[TaskRecord]) -> None:
    store.promises.create(
        id="task21",
        ikey=None,
        strict=False,
        headers=None,
        data=None,
        timeout=sys.maxsize,
        tags={"resonate:invoke": targets.poll("default", "pid")},
    )
    task_record = cq.get()
    store.tasks.claim(
        id=task_record.id, counter=task_record.counter, pid="pid", ttl=sys.maxsize
    )
    store.tasks.complete(id=task_record.id, counter=task_record.counter)
    with pytest.raises(ResonateError):
        store.tasks.complete(id=task_record.id, counter=task_record.counter)


@pytest.mark.parametrize(("store", "cq"), _stores())
def test_case_22_transition_from_completed_to_completed_via_heartbeat(store: IStore, cq: Queue[TaskRecord]) -> None:
    store.promises.create(
        id="task22",
        ikey=None,
        strict=False,
        headers=None,
        data=None,
        timeout=sys.maxsize,
        tags={"resonate:invoke": targets.poll("default", "pid")},
    )
    task_record = cq.get()
    store.tasks.claim(
        id=task_record.id, counter=task_record.counter, pid="pid", ttl=sys.maxsize
    )
    store.tasks.complete(id=task_record.id, counter=task_record.counter)
    assert store.tasks.heartbeat(pid="pid") == 0
