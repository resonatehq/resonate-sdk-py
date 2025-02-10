from __future__ import annotations

import os
import sys
import time
import uuid
from queue import Queue
from typing import TYPE_CHECKING

import pytest

from resonate.errors import ResonateError
from resonate.stores.local import LocalStore
from resonate.stores.remote import RemoteStore
from resonate.task_sources.poller import Poller

if TYPE_CHECKING:
    from collections.abc import Generator

    from resonate.models.message import Mesg
    from resonate.models.store import Store

# fake it till you make it
# >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

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

# <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

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
            poller = Poller(group=id, timeout=2)
            poller.start(cq=TaskTranslator(store, cq), pid=id)

            store.promises.create(
                id=id,
                ikey=None,
                strict=False,
                headers=None,
                data=None,
                timeout=sys.maxsize,
                tags={"resonate:invoke": f"poll://{id}"},
            )

            yield cq.get()
            poller.stop()
            store.promises.resolve(id=id)


def test_case_5_transition_from_enqueue_to_claimed_via_claim(
    store: Store, task: tuple[str, int]
) -> None:
    id, counter = task
    store.tasks.claim(id=id, counter=counter, pid="task5", ttl=sys.maxsize)

    # # completing to clean up task. Not part of the actual test
    # store.tasks.complete(id=id, counter=counter)  # noqa: ERA001


def test_case_6_transition_from_enqueue_to_enqueue_via_claim(
    store: Store, task: tuple[str, int]
) -> None:
    id, counter = task
    with pytest.raises(ResonateError):
        store.tasks.claim(
            id=id,
            counter=counter + 1,
            pid="task6",
            ttl=sys.maxsize,
        )


def test_case_8_transition_from_enqueue_to_enqueue_via_complete(
    store: Store, task: tuple[str, int]
) -> None:
    id, counter = task
    with pytest.raises(ResonateError):
        store.tasks.complete(
            id=id,
            counter=counter,
        )


def test_case_10_transition_from_enqueue_to_enqueue_via_hearbeat(
    store: Store, task: tuple[str, int]
) -> None:
    id, counter = task
    assert store.tasks.heartbeat(pid="task10") == 0


def test_case_12_transition_from_claimed_to_claimed_via_claim(
    store: Store, task: tuple[str, int]
) -> None:
    id, counter = task
    store.tasks.claim(id=id, counter=counter, pid="task12", ttl=sys.maxsize)
    with pytest.raises(ResonateError):
        store.tasks.claim(
            id=id,
            counter=counter,
            pid="task12",
            ttl=sys.maxsize,
        )

    # # completing to clean up task. Not part of the actual test
    # store.tasks.complete(id=id, counter=counter)  # noqa: ERA001


def test_case_13_transition_from_claimed_to_init_via_claim(
    store: Store, task: tuple[str, int]
) -> None:
    id, counter = task
    store.tasks.claim(id=id, counter=counter, pid="task13", ttl=0)
    with pytest.raises(ResonateError):
        store.tasks.claim(
            id=id,
            counter=counter,
            pid="task12",
            ttl=sys.maxsize,
        )

    # # completing to clean up task. Not part of the actual test
    # store.tasks.complete(id=id, counter=counter)  # noqa: ERA001


def test_case_14_transition_from_claimed_to_completed_via_complete(
    store: Store, task: tuple[str, int]
) -> None:
    id, counter = task
    store.tasks.claim(id=id, counter=counter, pid="task14", ttl=sys.maxsize)
    store.tasks.complete(id=id, counter=counter)


def test_case_15_transition_from_claimed_to_init_via_complete(
    store: Store, task: tuple[str, int]
) -> None:
    if isinstance(store, LocalStore):
        return
    id, counter = task
    store.tasks.claim(id=id, counter=counter, pid="task15", ttl=0)
    time.sleep(TICK_TIME)
    with pytest.raises(ResonateError):
        store.tasks.complete(id=id, counter=counter)


def test_case_16_transition_from_claimed_to_claimed_via_complete(
    store: Store, task: tuple[str, int]
) -> None:
    id, counter = task
    store.tasks.claim(id=id, counter=counter, pid="task16", ttl=sys.maxsize)
    with pytest.raises(ResonateError):
        store.tasks.complete(id=id, counter=counter + 1)

    # # completing to clean up task. Not part of the actual test
    # store.tasks.complete(id=id, counter=counter)  # noqa: ERA001


def test_case_17_transition_from_claimed_to_init_via_complete(
    store: Store, task: tuple[str, int]
) -> None:
    if isinstance(store, LocalStore):
        return
    id, counter = task
    store.tasks.claim(id=id, counter=counter, pid="task17", ttl=0)
    time.sleep(TICK_TIME)
    with pytest.raises(ResonateError):
        store.tasks.complete(id=id, counter=counter)


def test_case_18_transition_from_claimed_to_claimed_via_heartbeat(
    store: Store, task: tuple[str, int]
) -> None:
    id, counter = task
    store.tasks.claim(id=id, counter=counter, pid="task18", ttl=sys.maxsize)
    assert store.tasks.heartbeat(pid="task18") == 1

    # # completing to clean up task. Not part of the actual test
    # store.tasks.complete(id=id, counter=counter)  # noqa: ERA001


def test_case_19_transition_from_claimed_to_init_via_heartbeat(
    store: Store, task: tuple[str, int]
) -> None:
    id, counter = task
    store.tasks.claim(id=id, counter=counter, pid="task19", ttl=0)
    assert store.tasks.heartbeat(pid="task19") == 1


def test_case_20_transition_from_completed_to_completed_via_claim(
    store: Store, task: tuple[str, int]
) -> None:
    id, counter = task
    store.tasks.claim(id=id, counter=counter, pid="task20", ttl=sys.maxsize)
    store.tasks.complete(id=id, counter=counter)
    with pytest.raises(ResonateError):
        store.tasks.claim(id=id, counter=counter, pid="task20", ttl=0)


def test_case_21_transition_from_completed_to_completed_via_complete(
    store: Store, task: tuple[str, int]
) -> None:
    id, counter = task
    store.tasks.claim(id=id, counter=counter, pid="task21", ttl=sys.maxsize)
    store.tasks.complete(id=id, counter=counter)
    with pytest.raises(ResonateError):
        store.tasks.complete(id=id, counter=counter)


def test_case_22_transition_from_completed_to_completed_via_heartbeat(
    store: Store, task: tuple[str, int]
) -> None:
    id, counter = task
    store.tasks.claim(id=id, counter=counter, pid="task22", ttl=sys.maxsize)
    store.tasks.complete(id=id, counter=counter)
    assert store.tasks.heartbeat(pid="task22") == 0
