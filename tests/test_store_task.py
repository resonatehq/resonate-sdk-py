from __future__ import annotations

import os
import sys
import time
from typing import Any
import uuid
from collections.abc import Generator
from queue import Queue

import pytest

from resonate.errors import ResonateError
from resonate.models.message import InvokeMesg, Mesg
from resonate.models.store import Store
from resonate.models.task import Task
from resonate.stores.local import LocalStore
from resonate.stores.remote import RemoteStore
from resonate.task_sources.poller import Poller

# fake it till you make it

TICK_TIME = 1


class LocalSender:
    def __init__(self, cq: Queue[tuple[str, int]]) -> None:
        self.cq = cq

    def send(self, recv: str, mesg: Mesg) -> None:
        assert mesg["type"] == "invoke"
        self.cq.put((mesg["task"]["id"], mesg["task"]["counter"]))


class TaskTranslator:
    def __init__(self, store: Store, cq: Queue[tuple[str, int]]) -> None:
        self.store = store
        self.cq = cq

    def enqueue(self, mesg: Mesg) -> None:
        assert mesg["type"] == "invoke"
        self.cq.put((mesg["task"]["id"], mesg["task"]["counter"]))


stores: list[Store] = [
    LocalStore(),
]

if "RESONATE_STORE_URL" in os.environ:
    stores.append(RemoteStore(os.environ["RESONATE_STORE_URL"]))


@pytest.fixture(scope="module", params=stores)
def store(request: pytest.FixtureRequest) -> Store:
    return request.param


@pytest.fixture
def task(store: Store) -> Generator[tuple[str, int]]:
    cq = Queue[tuple[str, int]]()
    assert isinstance(store, (LocalStore, RemoteStore))

    id = str(uuid.uuid4())

    match store:
        case LocalStore():
            store.add_sender("default", LocalSender(cq=cq))
            store.promises.create(
                id=id,
                ikey=None,
                strict=False,
                headers=None,
                data=None,
                timeout=sys.maxsize,
                tags={"resonate:invoke": "default"},
            )

            yield cq.get()

            store.promises.resolve(id=id)
            store.rmv_sender("default")
        case RemoteStore():
            poller = Poller(group="default")
            poller.start(cq=TaskTranslator(store, cq), pid=id)

            store.promises.create(
                id=id,
                ikey=None,
                strict=False,
                headers=None,
                data=None,
                timeout=sys.maxsize,
                tags={"resonate:invoke": "default"},
            )

            yield cq.get()
            store.promises.resolve(id=id)

            poller.stop()


def test_case_5_transition_from_enqueue_to_claimed_via_claim(
    store: Store, task: tuple[str, int]
) -> None:
    id, counter = task
    store.tasks.claim(id=id, counter=counter, pid="task5", ttl=sys.maxsize)


# def test_case_6_transition_from_enqueue_to_enqueue_via_claim(store: Store, task: Task) -> None:
#     with pytest.raises(ResonateError):
#         store.tasks.claim(
#             id=task.id,
#             counter=task.counter + 1,
#             pid="task6",
#             ttl=sys.maxsize,
#         )


# def test_case_8_transition_from_enqueue_to_enqueue_via_complete(store: Store, cq: Queue[TaskRecord]) -> None:
#     store.promises.create(
#         id="task8",
#         ikey=None,
#         strict=False,
#         headers=None,
#         data=None,
#         timeout=sys.maxsize,
#         tags={"resonate:invoke": "poll://default/pid"},
#     )
#     task_record = cq.get()
#     assert task_record.counter == 1
#     with pytest.raises(ResonateError):
#         store.tasks.complete(
#             id=task_record.id,
#             counter=task_record.counter,
#         )


# def test_case_10_transition_from_enqueue_to_enqueue_via_hearbeat(store: Store, cq: Queue[TaskRecord]) -> None:
#     store.promises.create(
#         id="task10",
#         ikey=None,
#         strict=False,
#         headers=None,
#         data=None,
#         timeout=sys.maxsize,
#         tags={"resonate:invoke": "poll://default/pid"},
#     )
#     cq.get()
#     assert store.tasks.heartbeat(pid="task10") == 0


# def test_case_12_transition_from_claimed_to_claimed_via_claim(store: Store, cq: Queue[TaskRecord]) -> None:
#     store.promises.create(
#         id="task12",
#         ikey=None,
#         strict=False,
#         headers=None,
#         data=None,
#         timeout=sys.maxsize,
#         tags={"resonate:invoke": "poll://default/pid"},
#     )
#     task_record = cq.get()
#     store.tasks.claim(
#         id=task_record.id, counter=task_record.counter, pid="pid", ttl=sys.maxsize
#     )
#     with pytest.raises(ResonateError):
#         store.tasks.claim(
#             id=task_record.id,
#             counter=task_record.counter,
#             pid="task12",
#             ttl=sys.maxsize,
#         )


# @pytest.mark.skipif("RESONATE_STORE_URL" not in os.environ, reason="")
# def test_case_13_transition_from_claimed_to_init_via_claim(store: Store, cq: Queue[TaskRecord]) -> None:
#     raise NotImplementedError()


# def test_case_14_transition_from_claimed_to_completed_via_complete(store: Store, cq: Queue[TaskRecord]) -> None:
#     store.promises.create(
#         id="task14",
#         ikey=None,
#         strict=False,
#         headers=None,
#         data=None,
#         timeout=sys.maxsize,
#         tags={"resonate:invoke": "poll://default/pid"},
#     )
#     task_record = cq.get()
#     store.tasks.claim(
#         id=task_record.id, counter=task_record.counter, pid="pid", ttl=sys.maxsize
#     )
#     store.tasks.complete(id=task_record.id, counter=task_record.counter)


# @pytest.mark.skipif("RESONATE_STORE_URL" not in os.environ, reason="")
# def test_case_15_transition_from_claimed_to_init_via_complete(store: Store, cq: Queue[TaskRecord]) -> None:
#     store, task_source, cq = RemoteStore(), Poller(group="task15"), Queue[TaskRecord]()
#     store.promises.create(
#         id="task15",
#         ikey=None,
#         strict=False,
#         headers=None,
#         data=None,
#         timeout=sys.maxsize,
#         tags={"resonate:invoke": targets.poll(task_source.group, "task15")},
#     )
#     task_source.run(cq=cq, pid="task15")
#     task_record = cq.get()
#     store.tasks.claim(
#         id=task_record.id, counter=task_record.counter, pid="task15", ttl=0
#     )
#     time.sleep(TICK_TIME)
#     with pytest.raises(ResonateError):
#         store.tasks.complete(id=task_record.id, counter=task_record.counter)


# def test_case_16_transition_from_claimed_to_claimed_via_complete(store: Store, cq: Queue[TaskRecord]) -> None:
#     store.promises.create(
#         id="task16",
#         ikey=None,
#         strict=False,
#         headers=None,
#         data=None,
#         timeout=sys.maxsize,
#         tags={"resonate:invoke": "poll://default/pid"},
#     )
#     task_record = cq.get()
#     store.tasks.claim(
#         id=task_record.id, counter=task_record.counter, pid="pid", ttl=sys.maxsize
#     )
#     with pytest.raises(ResonateError):
#         store.tasks.complete(id=task_record.id, counter=task_record.counter + 1)


# @pytest.mark.skipif("RESONATE_STORE_URL" not in os.environ, reason="")
# def test_case_17_transition_from_claimed_to_init_via_complete(store: Store, cq: Queue[TaskRecord]) -> None:
#     store, task_source, cq = RemoteStore(), Poller(group="task17"), Queue[TaskRecord]()
#     store.promises.create(
#         id="task17",
#         ikey=None,
#         strict=False,
#         headers=None,
#         data=None,
#         timeout=sys.maxsize,
#         tags={"resonate:invoke": targets.poll(task_source.group, "task17")},
#     )
#     task_source.run(cq=cq, pid="task17")
#     task_record = cq.get()
#     store.tasks.claim(
#         id=task_record.id, counter=task_record.counter, pid="task17", ttl=0
#     )
#     time.sleep(TICK_TIME)
#     with pytest.raises(ResonateError):
#         store.tasks.complete(id=task_record.id, counter=task_record.counter)


# def test_case_18_transition_from_claimed_to_claimed_via_heartbeat(store: Store, cq: Queue[TaskRecord]) -> None:
#     store.promises.create(
#         id="task18",
#         ikey=None,
#         strict=False,
#         headers=None,
#         data=None,
#         timeout=sys.maxsize,
#         tags={"resonate:invoke": "poll://default/pid"},
#     )
#     task_record = cq.get()
#     store.tasks.claim(
#         id=task_record.id, counter=task_record.counter, pid="pid", ttl=sys.maxsize
#     )
#     assert store.tasks.heartbeat(pid="pid") == 1


# def test_case_19_transition_from_claimed_to_init_via_heartbeat(store: Store, cq: Queue[TaskRecord]) -> None:
#     store.promises.create(
#         id="task19",
#         ikey=None,
#         strict=False,
#         headers=None,
#         data=None,
#         timeout=sys.maxsize,
#         tags={"resonate:invoke": "poll://default/pid"},
#     )
#     task_record = cq.get()
#     store.tasks.claim(
#         id=task_record.id, counter=task_record.counter, pid="pid", ttl=0
#     )

#     assert store.tasks.heartbeat(pid="pid") == 1


# def test_case_20_transition_from_completed_to_completed_via_claim(store: Store, cq: Queue[TaskRecord]) -> None:
#     store.promises.create(
#         id="task20",
#         ikey=None,
#         strict=False,
#         headers=None,
#         data=None,
#         timeout=sys.maxsize,
#         tags={"resonate:invoke": "poll://default/pid"},
#     )
#     task_record = cq.get()
#     store.tasks.claim(
#         id=task_record.id, counter=task_record.counter, pid="pid", ttl=sys.maxsize
#     )
#     store.tasks.complete(id=task_record.id, counter=task_record.counter)
#     with pytest.raises(ResonateError):
#         store.tasks.claim(
#             id=task_record.id, counter=task_record.counter, pid="pid", ttl=0
#         )


# def test_case_21_transition_from_completed_to_completed_via_complete(store: Store, cq: Queue[TaskRecord]) -> None:
#     store.promises.create(
#         id="task21",
#         ikey=None,
#         strict=False,
#         headers=None,
#         data=None,
#         timeout=sys.maxsize,
#         tags={"resonate:invoke": "poll://default/pid"},
#     )
#     task_record = cq.get()
#     store.tasks.claim(
#         id=task_record.id, counter=task_record.counter, pid="pid", ttl=sys.maxsize
#     )
#     store.tasks.complete(id=task_record.id, counter=task_record.counter)
#     with pytest.raises(ResonateError):
#         store.tasks.complete(id=task_record.id, counter=task_record.counter)


# def test_case_22_transition_from_completed_to_completed_via_heartbeat(store: Store, cq: Queue[TaskRecord]) -> None:
#     store.promises.create(
#         id="task22",
#         ikey=None,
#         strict=False,
#         headers=None,
#         data=None,
#         timeout=sys.maxsize,
#         tags={"resonate:invoke": "poll://default/pid"},
#     )
#     task_record = cq.get()
#     store.tasks.claim(
#         id=task_record.id, counter=task_record.counter, pid="pid", ttl=sys.maxsize
#     )
#     store.tasks.complete(id=task_record.id, counter=task_record.counter)
#     assert store.tasks.heartbeat(pid="pid") == 0
